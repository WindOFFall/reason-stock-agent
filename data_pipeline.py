"""
data_pipeline.py
每天執行，檢查各資料表前一個交易日是否有資料，缺漏則自動補抓。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta
from database import get_db_client
from sqlalchemy import text
import holidays

# 預載未來 2 年的假日
_YEARS = range(datetime.now().year, datetime.now().year + 2)
_TW_HOLIDAYS = holidays.Taiwan(years=_YEARS)
_US_HOLIDAYS = holidays.UnitedStates(years=_YEARS)

# ════════════════════════════════════════════════════════════════
# 工具函式
# ════════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def is_tw_trading_day(d: datetime) -> bool:
    """台股交易日（排除週末與台灣國定假日）"""
    return d.weekday() < 5 and d.date() not in _TW_HOLIDAYS


def is_us_trading_day(d: datetime) -> bool:
    """美股交易日（排除週末與美國國定假日）"""
    return d.weekday() < 5 and d.date() not in _US_HOLIDAYS


def is_any_weekday(d: datetime) -> bool:
    """一般工作日（只排週末，不排假日），適用新聞類"""
    return d.weekday() < 5


def get_days_back(n_calendar_days: int, filter_fn=None) -> list:
    """
    取得往回 n 個日曆天的日期清單。
    filter_fn: 傳入 datetime → bool，None 表示不過濾（每天都納入）
    """
    days = []
    d = datetime.now() - timedelta(days=1)
    end = datetime.now() - timedelta(days=n_calendar_days)
    while d >= end:
        if filter_fn is None or filter_fn(d):
            days.append(d)
        d -= timedelta(days=1)
    return days

# 舊名稱別名，向下相容
get_working_days_back = get_days_back


def check_table_has_data(table: str, date_col: str, date: datetime) -> bool:
    """確認指定資料表在某日是否有資料"""
    date_str = date.strftime("%Y-%m-%d")
    try:
        with get_db_client() as db:
            result = db.engine.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE {date_col}::date = :d"),
                {"d": date_str}
            ).scalar()
            return (result or 0) > 0
    except Exception as e:
        log(f"  ⚠️ 查詢 {table} 失敗：{e}")
        return False


def check_table_has_data_v2(table: str, date_col: str, date: datetime, min_count: int = 1) -> bool:
    """確認資料表在某日的資料筆數是否達到門檻"""
    date_str = date.strftime("%Y-%m-%d")
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE {date_col}::date = :d"),
                    {"d": date_str}
                ).scalar() or 0
                if count > 0 and count < min_count:
                    log(f"  ⚠️ {table} {date_str} 筆數不足：{count} 筆（門檻 {min_count}），視為不完整")
                return count >= min_count
    except Exception as e:
        log(f"  ⚠️ 查詢 {table} 失敗：{e}")
        return False


# ════════════════════════════════════════════════════════════════
# 各資料來源的補抓邏輯
# ════════════════════════════════════════════════════════════════

def ensure_tw_price(target_date: datetime) -> bool:
    """確保台股股價資料存在（門檻 1500 筆）"""
    if check_table_has_data_v2("tw_daily_prices", "date", target_date, min_count=1500):
        log(f"  ✅ 台股股價 {target_date.strftime('%Y-%m-%d')} 已存在")
        return True

    log(f"  🔄 台股股價 {target_date.strftime('%Y-%m-%d')} 缺漏，開始補抓...")
    try:
        from crawlers.price import TWStockCrawler
        crawler = TWStockCrawler()
        crawler.run(mode="backfill", days_back=5)
        ok = check_table_has_data_v2("tw_daily_prices", "date", target_date)
        log(f"  {'✅ 補抓成功' if ok else '⚠️ 補抓後仍無資料（可能為假日）'}")
        return ok
    except Exception as e:
        log(f"  ❌ 台股股價補抓失敗：{e}")
        return False


def ensure_institutional(target_date: datetime) -> bool:
    """確保三大法人資料存在（門檻 800 筆）"""
    if check_table_has_data_v2("tw_institutional_trades", "date", target_date, min_count=800):
        log(f"  ✅ 三大法人 {target_date.strftime('%Y-%m-%d')} 已存在")
        return True

    log(f"  🔄 三大法人 {target_date.strftime('%Y-%m-%d')} 缺漏，開始補抓...")
    try:
        from crawlers.institutional import TWInstitutionalCrawler
        crawler = TWInstitutionalCrawler()
        crawler.run(mode="backfill", days_back=5)
        ok = check_table_has_data_v2("tw_institutional_trades", "date", target_date)
        log(f"  {'✅ 補抓成功' if ok else '⚠️ 補抓後仍無資料（可能為假日）'}")
        return ok
    except Exception as e:
        log(f"  ❌ 三大法人補抓失敗：{e}")
        return False


NEWS_SOURCE_THRESHOLDS = {
    "Anue":        30,
    "GoogleNews":  30,
    "PTT_Stock":   15,
}

def _check_news_by_source(target_date: datetime) -> dict:
    """回傳各來源當天實際筆數 {source: count}"""
    date_str = target_date.strftime("%Y-%m-%d")
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT source, COUNT(*) as cnt
                    FROM market_intelligence
                    WHERE publish_date::date = :d
                    GROUP BY source
                """), {"d": date_str}).fetchall()
        return {r.source: r.cnt for r in rows}
    except Exception as e:
        log(f"  ⚠️ 查詢新聞來源失敗：{e}")
        return {}


def ensure_news(target_date: datetime) -> bool:
    """確保三個新聞來源各自達到門檻（Anue≥20、GoogleNews≥20、PTT≥10）"""
    date_str = target_date.strftime("%Y-%m-%d")
    counts = _check_news_by_source(target_date)

    missing = [
        src for src, threshold in NEWS_SOURCE_THRESHOLDS.items()
        if counts.get(src, 0) < threshold
    ]

    if not missing:
        log(f"  ✅ 新聞資料 {date_str} 完整（"
            + "、".join(f"{s}:{counts.get(s,0)}筆" for s in NEWS_SOURCE_THRESHOLDS) + "）")
        return True

    log(f"  🔄 新聞資料 {date_str} 不足，補抓來源：{missing}")
    log(f"     目前：" + "、".join(f"{s}:{counts.get(s,0)}筆" for s in NEWS_SOURCE_THRESHOLDS))
    try:
        from crawlers.news import AnueCrawler, GoogleNewsCrawler, PTTCrawler
        if "Anue" in missing:
            AnueCrawler().run(mode="backfill", days_back=3)
        if "GoogleNews" in missing:
            GoogleNewsCrawler().run(keyword="台股", mode="backfill", days_back=3)
        if "PTT_Stock" in missing:
            PTTCrawler().run(mode="backfill", days_back=3)

        counts2 = _check_news_by_source(target_date)
        still_missing = [
            src for src, threshold in NEWS_SOURCE_THRESHOLDS.items()
            if counts2.get(src, 0) < threshold
        ]
        if not still_missing:
            log(f"  ✅ 補抓成功（"
                + "、".join(f"{s}:{counts2.get(s,0)}筆" for s in NEWS_SOURCE_THRESHOLDS) + "）")
            return True
        else:
            log(f"  ⚠️ 補抓後仍不足：{still_missing}（"
                + "、".join(f"{s}:{counts2.get(s,0)}筆" for s in NEWS_SOURCE_THRESHOLDS) + "）")
            return False
    except Exception as e:
        log(f"  ❌ 新聞補抓失敗：{e}")
        return False


def ensure_conference_calendar() -> bool:
    """每次執行都抓取最新法說會時程，upsert by (stock_id, conf_date)"""
    log("  🔄 啟動 Selenium 抓取最新法說會時程...")
    try:
        from mops_calendar_crawler import run as run_calendar
        upcoming, all_data = run_calendar()
        log(f"  ✅ 抓取完成，共 {len(all_data)} 筆，即將召開 {len(upcoming)} 場")
        return True
    except Exception as e:
        log(f"  ❌ 法說會時程抓取失敗：{e}")
        return False


def ensure_eps() -> bool:
    """確保季度 EPS 資料存在（門檻：至少 500 支股票有歷史資料）"""
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count = conn.execute(
                    text("SELECT COUNT(DISTINCT stock_id) FROM tw_financial_statements")
                ).scalar() or 0
    except Exception as e:
        log(f"  ⚠️ 查詢 tw_financial_statements 失敗：{e}")
        count = 0

    if count >= 500:
        log(f"  ✅ EPS 資料已有 {count} 支股票")
        return True

    log(f"  🔄 EPS 資料僅 {count} 支，開始補抓...")
    try:
        from crawlers.eps_crawler import run as run_eps
        run_eps()
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count_after = conn.execute(
                    text("SELECT COUNT(DISTINCT stock_id) FROM tw_financial_statements")
                ).scalar() or 0
        ok = count_after >= 500
        log(f"  {'✅ 補抓成功' if ok else '⚠️ 補抓後仍不足'} ({count_after} 支)")
        return ok
    except Exception as e:
        log(f"  ❌ EPS 補抓失敗：{e}")
        return False


def ensure_monthly_revenue() -> bool:
    """確保上個月的月營收資料已補齊（門檻：至少 500 支股票有資料）"""
    now = datetime.now()
    last_month = (now.replace(day=1) - timedelta(days=1))
    y, m = last_month.year, last_month.month

    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count = conn.execute(
                    text("""
                        SELECT COUNT(DISTINCT stock_id) FROM tw_monthly_revenue
                        WHERE revenue_year = :y AND revenue_month = :m
                    """),
                    {"y": y, "m": m}
                ).scalar() or 0
    except Exception as e:
        log(f"  ⚠️ 查詢 tw_monthly_revenue 失敗：{e}")
        count = 0

    if count >= 500:
        log(f"  ✅ 月營收 {y}/{m:02d} 已有 {count} 支股票，資料完整")
        return True

    log(f"  🔄 月營收 {y}/{m:02d} 僅 {count} 支，開始補抓...")
    try:
        from crawlers.monthly_revenue import run as run_monthly_revenue
        run_monthly_revenue()
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count_after = conn.execute(
                    text("""
                        SELECT COUNT(DISTINCT stock_id) FROM tw_monthly_revenue
                        WHERE revenue_year = :y AND revenue_month = :m
                    """),
                    {"y": y, "m": m}
                ).scalar() or 0
        ok = count_after >= 500
        log(f"  {'✅ 補抓成功' if ok else '⚠️ 補抓後仍不足'} ({count_after} 支)")
        return ok
    except Exception as e:
        log(f"  ❌ 月營收補抓失敗：{e}")
        return False


# ════════════════════════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════════════════════════

def run_pipeline(days_back: int = 7) -> dict:
    """
    執行資料完整性檢查並補抓。
    days_back: 往回幾個日曆天（預設 7 天）
    """
    log("=" * 50)
    log(f"📋 資料管線檢查開始（往回 {days_back} 天）")
    log("=" * 50)

    log("\n📅 法說會時程檢查")
    ensure_conference_calendar()

    results = {}

    # 各來源獨立的日期清單
    tw_days   = get_days_back(days_back, is_tw_trading_day)  # 台股：排台灣假日+週末
    news_days = get_days_back(days_back, is_any_weekday)      # 新聞：只排週末

    # 合併所有需要檢查的日期（去重）
    all_dates = sorted(set(d.date() for d in tw_days + news_days), reverse=True)

    for date in all_dates:
        target = datetime.combine(date, datetime.min.time())
        date_str = date.strftime("%Y-%m-%d")
        log(f"\n📅 檢查日期：{date_str}")

        row = {}
        if is_tw_trading_day(target):
            row["tw_price"]      = ensure_tw_price(target)
            row["institutional"] = ensure_institutional(target)
        else:
            log("  ⏭️  台股假日，跳過股價與法人")
            row["tw_price"]      = None
            row["institutional"] = None

        if is_any_weekday(target):
            row["news"] = ensure_news(target)
        else:
            row["news"] = None

        results[date_str] = row

    log("\n" + "=" * 50)
    log("📊 檢查結果摘要")
    log("=" * 50)
    for date_str, status in results.items():
        def fmt(v):
            if v is None: return "⏭️ "
            return "✅" if v else "❌"
        log(f"  {date_str} | 股價:{fmt(status['tw_price'])} 法人:{fmt(status['institutional'])} 新聞:{fmt(status['news'])}")

    return results


def _eps_this_month_complete() -> bool:
    """檢查本月 EPS 資料是否已足夠（>= 500 支）"""
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count = conn.execute(
                    text("SELECT COUNT(DISTINCT stock_id) FROM tw_financial_statements")
                ).scalar() or 0
        return count >= 500
    except Exception:
        return False


def _revenue_last_month_complete() -> bool:
    """檢查上個月月營收是否已足夠（>= 500 支）"""
    now = datetime.now()
    last_month = (now.replace(day=1) - timedelta(days=1))
    y, m = last_month.year, last_month.month
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                count = conn.execute(text("""
                    SELECT COUNT(DISTINCT stock_id) FROM tw_monthly_revenue
                    WHERE revenue_year = :y AND revenue_month = :m
                """), {"y": y, "m": m}).scalar() or 0
        return count >= 500
    except Exception:
        return False


def run_all(days_back: int = 7):
    """
    一鍵執行所有資料管線，依日期自動決定要跑哪些：
    - 每天：股價、法人、新聞、法說會時程
    - 每月 5 號後，EPS 資料不足時繼續補抓
    - 每月 12 號後，月營收資料不足時繼續補抓
    """
    today = datetime.now()
    log("=" * 50)
    log(f"🚀 run_all 啟動（{today.strftime('%Y-%m-%d')}）")
    log("=" * 50)

    # EPS：5 號後，資料不足就跑
    if today.day >= 5:
        if _eps_this_month_complete():
            log("\n📊 EPS 已完整，略過")
        else:
            log("\n📊 [每月 5 號後] EPS 資料不足，開始補抓")
            ensure_eps()
    else:
        log(f"\n📊 EPS 略過（今日 {today.day} 號，5 號後才開始）")

    # 月營收：12 號後，資料不足就跑
    if today.day >= 12:
        if _revenue_last_month_complete():
            log("\n📅 月營收已完整，略過")
        else:
            log("\n📅 [每月 12 號後] 月營收資料不足，開始補抓")
            ensure_monthly_revenue()
    else:
        log(f"\n📅 月營收略過（今日 {today.day} 號，12 號後才開始）")

    # 每天跑每日 pipeline（股價、法人、新聞、法說會）
    log("\n📋 每日資料管線")
    run_pipeline(days_back=days_back)

    # 亞股（韓股 + 日股）每日更新
    log("\n🌏 亞股每日更新")
    try:
        from crawlers.price import AsiaStockCrawler
        AsiaStockCrawler().run(mode="daily")
    except Exception as e:
        log(f"  ❌ 亞股更新失敗：{e}")


if __name__ == "__main__":
    # 預設補查最近 1 個工作日；可改成 3 補連假
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    run_pipeline(days_back=days)
