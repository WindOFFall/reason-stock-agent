"""
╔══════════════════════════════════════════════════════════════╗
║          台股 AI 量化交易系統 - 完整整合版                      ║
║                                                              ║
║  架構：三階段漏斗                                              ║
║  第一階段：海選池  - 從資料庫篩選候選股                          ║
║  第二階段：深度決策 - AI 判斷要不要買                            ║
║  第三階段：持股監控 - 判斷何時該賣                              ║
╚══════════════════════════════════════════════════════════════╝

使用方式（Jupyter）：
    在最下方選擇要執行的模式，然後執行整個 cell

需要安裝的套件：
    pip install requests pandas selenium pdfplumber
    pip install google-generativeai  （如果要用 Gemini）
"""

import requests
import pandas as pd
import pdfplumber
import io
import time
import json
import re
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys

# 引入你專案寫好的資料庫連線工具與 SQLAlchemy
from database import get_db_client
from sqlalchemy import text


# ════════════════════════════════════════════════════════════════
# ⚙️  系統設定（改這裡）
# ════════════════════════════════════════════════════════════════

load_dotenv()
GEMINI_KEY   = os.getenv("GEMINI_API_KEY")  # 從 .env 安全讀取金鑰
USE_LLM      = True                         # 啟用 LLM
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# 篩選條件
MIN_INSTITUTIONAL_BUY = 100_000        # 只排雜訊（100張），靠金額排序+量比過濾做真正篩選
VOL_RATIO_THRESHOLD   = 1.2            # 放寬量比，避免大型股因難以爆量被剔除
DAYS_AHEAD_CALENDAR   = 7              # 法說會日曆看未來幾天

# 持股監控條件
STOP_LOSS_PCT    = 0.93   # 停損：跌 7%
STOP_PROFIT_PCT  = 1.20   # 停利：漲 20%

def load_stock_mapping():
    """動態讀取股票代號與名稱的對應表"""
    mapping_file = "stock_mapping.json"
    if os.path.exists(mapping_file):
        with open(mapping_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        default_map = {
            "2330": "台積電", "2303": "聯電", "3711": "日月光", "2449": "京元電子",
            "2454": "聯發科", "2379": "瑞昱", "3034": "聯詠", "3443": "創意",
            "2317": "鴻海", "2382": "廣達", "3231": "緯創", "6669": "緯穎", "2356": "英業達", "2376": "技嘉",
            "2308": "台達電", "3017": "奇鋐", "3324": "雙鴻", "2421": "建準",
            "2345": "智邦", "3163": "波若威", "4979": "華星光",
            "2344": "華邦電", "2337": "旺宏", "8299": "群聯",
            "2368": "金像電", "3037": "欣興", "2313": "華通",
            "2357": "華碩", "2353": "宏碁", "2395": "研華",
        }
        with open(mapping_file, "w", encoding="utf-8") as f:
            json.dump(default_map, f, ensure_ascii=False, indent=4)
        return default_map

STOCK_NAME_MAP = load_stock_mapping()


# ════════════════════════════════════════════════════════════════
# 🛠️  工具函數
# ════════════════════════════════════════════════════════════════

# 將原本的 sqlite3 取代為你的 Postgres Client
def get_db():
    return get_db_client()


def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)


_LOG_BUFFER: list[str] = []   # 收集全部 log，最後一併推到 TG

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    _LOG_BUFFER.append(msg)     # 不含時間戳，TG 看起來比較乾淨

def send_telegram_message(text: str):
    """發送純文字訊息到 Telegram（保留供內部短訊使用）"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("⚠️ 找不到 Telegram 設定，略過發送推播。")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    for attempt in range(3):
        try:
            response = requests.post(url, json=payload, timeout=20)
            if response.status_code == 200:
                log("  ✅ 成功發送推播到 Telegram！")
                return
            else:
                log(f"  ❌ Telegram 發送失敗（{response.status_code}）：{response.text}")
                return
        except Exception as e:
            log(f"  ⚠️ Telegram 連線失敗（第 {attempt+1} 次）：{e}")
            if attempt < 2:
                time.sleep(3)
    log("  ❌ Telegram 推播失敗，已重試 3 次")


def send_telegram_report(caption: str, document_text: str, filename: str = "report.txt"):
    """以單一訊息傳送完整報告：caption 為摘要，document 為完整內文 .txt 檔"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("⚠️ 找不到 Telegram 設定，略過發送推播。")
        return

    # caption 上限 1024 字，截斷保護
    if len(caption) > 1020:
        caption = caption[:1020] + "…"

    url   = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files = {"document": (filename, document_text.encode("utf-8"), "text/plain")}
    data  = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "caption":    caption,
        "parse_mode": "HTML",
    }
    for attempt in range(3):
        try:
            response = requests.post(url, data=data, files=files, timeout=30)
            if response.status_code == 200:
                log("  ✅ 成功發送報告到 Telegram！")
                return
            else:
                log(f"  ❌ Telegram 發送失敗（{response.status_code}）：{response.text}")
                return
        except Exception as e:
            log(f"  ⚠️ Telegram 連線失敗（第 {attempt+1} 次）：{e}")
            if attempt < 2:
                time.sleep(3)
    log("  ❌ Telegram 推播失敗，已重試 3 次")

# ════════════════════════════════════════════════════════════════
# 📊  技術指標計算
# ════════════════════════════════════════════════════════════════

def calc_indicators(conn, stock_id: str, days: int = 60) -> dict:
    """計算單一股票的技術指標"""
    df = pd.read_sql(text("""
        SELECT date, open, high, low, close, volume
        FROM tw_daily_prices
        WHERE stock_id = :sid
        ORDER BY date DESC
        LIMIT :days
    """), conn.engine, params={"sid": stock_id, "days": days})

    df = df.dropna(subset=["close", "volume"])
    df = df.sort_values("date").reset_index(drop=True)

    if df.empty or len(df) < 20:
        return {}

    df["ma5"]  = df["close"].rolling(5).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()

    # 量能比
    df["avg_vol"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["avg_vol"]

    # RSI
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi"] = 100 - (100 / (1 + gain / loss))

    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) > 1 else latest

    return {
        "stock_id":  stock_id,
        "close":     latest["close"],
        "ma5":       latest["ma5"],
        "ma20":      latest["ma20"],
        "ma60":      latest["ma60"],
        "vol_ratio": latest["vol_ratio"],
        "rsi":       latest["rsi"],
        "ma20_up":   latest["ma20"] > prev["ma20"],   # 月線向上
        "above_ma20": latest["close"] > latest["ma20"],
        "above_ma5":  latest["close"] > latest["ma5"],
        "golden_cross": (                              # 多頭排列
            latest["close"] > latest["ma5"] > latest["ma20"]
        ),
    }


# ════════════════════════════════════════════════════════════════
# 🔍  第一階段：海選池
# ════════════════════════════════════════════════════════════════

def screen_institutional(conn) -> tuple:
    """策略 A：三大法人買超
    A1 外資波段桶：金額 > 1.5億 AND 外資 ADV% ≥ 5%（戰略性進場）
    A2 投信認養桶：投信 ADV% > 10% AND 金額 ≥ 5000萬（縮籌碼式認養）
    """
    log("策略 A：掃描法人買超...")

    # A1 外資門檻（依 MA20 流動性分層）
    LARGE_LIQUIDITY  = 500_000_000   # MA20 日均成交金額 > 5 億 → 大型股
    FOREIGN_AMT_LARGE = 1_000_000_000 # 大型股外資買超金額 > 10 億
    FOREIGN_AMT_MID   = 150_000_000   # 中小型股外資買超金額 > 1.5 億
    FOREIGN_ADV_MIN   = 0.05          # 外資買超佔日均量 ≥ 5%（兩者共用）
    # A2 投信門檻
    TRUST_ADV_MIN    = 0.10           # 投信買超佔日均量 > 10%
    TRUST_AMT_MIN    = 50_000_000     # 投信買超金額 ≥ 5000 萬

    base_sql = """
        WITH latest_price AS (
            SELECT DISTINCT ON (stock_id) stock_id, close AS last_close
            FROM tw_daily_prices
            ORDER BY stock_id, date DESC
        ),
        avg_vol AS (
            SELECT stock_id,
                   AVG(volume)               AS avg_volume,
                   AVG(close * volume)       AS ma20_liquidity
            FROM tw_daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY stock_id
            HAVING COUNT(*) >= 15
        )
        SELECT t.stock_id, t.total, t.foreign_investor, t.investment_trust,
               p.last_close, v.avg_volume, v.ma20_liquidity
        FROM tw_institutional_trades t
        JOIN latest_price p ON p.stock_id = t.stock_id
        JOIN avg_vol      v ON v.stock_id = t.stock_id
        WHERE t.date = (SELECT MAX(date) FROM tw_institutional_trades)
          AND v.avg_volume > 0
    """

    try:
        with conn.engine.connect() as db_conn:
            # A1：外資波段桶（依 MA20 流動性分層門檻 + ADV% ≥ 5%）
            rows_a1 = db_conn.execute(text(base_sql + """
                AND t.foreign_investor > 0
                AND t.foreign_investor::float / v.avg_volume >= :adv_min
                AND (
                    (v.ma20_liquidity >= :large_liq AND t.foreign_investor * p.last_close >= :amt_large)
                    OR
                    (v.ma20_liquidity <  :large_liq AND t.foreign_investor * p.last_close >= :amt_mid)
                )
                ORDER BY t.foreign_investor * p.last_close DESC
                LIMIT 8
            """), {
                "adv_min":   FOREIGN_ADV_MIN,
                "large_liq": LARGE_LIQUIDITY,
                "amt_large": FOREIGN_AMT_LARGE,
                "amt_mid":   FOREIGN_AMT_MID,
            }).fetchall()

            # A2：投信認養桶（ADV% > 10% AND 金額 ≥ 5000萬）
            rows_a2 = db_conn.execute(text(base_sql + """
                AND t.investment_trust > 0
                AND t.investment_trust::float / v.avg_volume > :adv_min
                AND t.investment_trust * p.last_close >= :amt_min
                ORDER BY t.investment_trust::float / v.avg_volume DESC
                LIMIT 8
            """), {"adv_min": TRUST_ADV_MIN, "amt_min": TRUST_AMT_MIN}).fetchall()

    except Exception as e:
        log(f"  ❌ 法人資料查詢失敗：{e}")
        return [], []

    # 觀察 log
    log(f"  [A1 外資波段] 大型股>10億/中小型>1.5億 + ADV≥{FOREIGN_ADV_MIN*100:.0f}%（MA20流動性分層），共 {len(rows_a1)} 支：")
    for idx, r in enumerate(rows_a1):
        name    = STOCK_NAME_MAP.get(r[0], r[0])
        adv     = r[2] / r[5] * 100 if r[5] else 0
        liq_tag = "大型" if (r[6] or 0) >= LARGE_LIQUIDITY else "中小"
        log(f"    {idx+1}. {r[0]} {name}[{liq_tag}]：外資買超 {r[2]:,} 股，金額 {r[2]*r[4]/1_000_000:.0f}百萬元，ADV {adv:.1f}%")

    log(f"  [A2 投信認養] ADV>{TRUST_ADV_MIN*100:.0f}% + 金額≥{TRUST_AMT_MIN/1_000_000:.0f}百萬，共 {len(rows_a2)} 支：")
    for idx, r in enumerate(rows_a2):
        name = STOCK_NAME_MAP.get(r[0], r[0])
        adv  = r[3] / r[5] * 100 if r[5] else 0
        log(f"    {idx+1}. {r[0]} {name}：投信買超 {r[3]:,} 股，ADV {adv:.1f}%，金額 {r[3]*r[4]/1_000_000:.0f}百萬元")

    def build_candidates(rows, label, inst_col_idx):
        """inst_col_idx: 2=foreign_investor, 3=investment_trust"""
        result = []
        for stock_id, _, foreign, trust, last_close, avg_volume, ma20_liq in rows:
            inst_vol = foreign if inst_col_idx == 2 else trust
            ind      = calc_indicators(conn, stock_id) or {}
            name     = STOCK_NAME_MAP.get(stock_id, stock_id)
            adv_pct  = inst_vol / avg_volume * 100 if avg_volume else 0
            amt_m    = inst_vol * last_close / 1_000_000
            detail_extra = f"ADV {adv_pct:.1f}%，金額 {amt_m:.0f}百萬元"

            if not ind:
                result.append({
                    "stock_id": stock_id, "name": name, "source": f"法人買超({label})",
                    "score": 1, "detail": f"{detail_extra}，無技術指標",
                    "indicators": {},
                })
                log(f"  ⚠️ [{label}] {stock_id} {name}：{detail_extra}，無技術指標（score=1）")
                continue

            above_ma20 = ind.get("above_ma20", False)
            vol_ratio  = ind.get("vol_ratio", 0)
            vol_ok     = vol_ratio > VOL_RATIO_THRESHOLD

            if above_ma20 and vol_ok:
                score, reason, emoji = 3, "站上月線＋量比達標", "✅"
            elif above_ma20:
                score, reason, emoji = 2, f"站上月線，量比偏低（{vol_ratio:.1f}x）", "🟡"
            elif vol_ok:
                score, reason, emoji = 2, "跌破月線但法人積極承接", "🟡"
            else:
                score, reason, emoji = 1, f"跌破月線，量比偏低（{vol_ratio:.1f}x）", "⚪"

            size_tag = "大型" if (ma20_liq or 0) >= LARGE_LIQUIDITY else "中小"
            result.append({
                "stock_id": stock_id, "name": name, "source": f"法人買超({label})",
                "score": score, "detail": f"{detail_extra}，{reason}",
                "indicators": ind,
                "size_tag": size_tag,
                "ma20_liquidity": ma20_liq,
            })
            log(f"  {emoji} [{label}] {stock_id} {name}：{detail_extra}，量比 {vol_ratio:.1f}x，{reason}")
        return result

    # A3：外資賣超觀察（純觀察，不進入候選池）
    try:
        with conn.engine.connect() as db_conn:
            rows_a3 = db_conn.execute(text("""
                WITH latest_price AS (
                    SELECT DISTINCT ON (stock_id) stock_id, close AS last_close
                    FROM tw_daily_prices ORDER BY stock_id, date DESC
                )
                SELECT t.stock_id, t.foreign_investor, p.last_close
                FROM tw_institutional_trades t
                JOIN latest_price p ON p.stock_id = t.stock_id
                WHERE t.date = (SELECT MAX(date) FROM tw_institutional_trades)
                  AND t.foreign_investor < 0
                ORDER BY t.foreign_investor * p.last_close ASC
                LIMIT 8
            """)).fetchall()

        log(f"  [A3 外資賣超] 賣超金額前 8（僅觀察，不進入分析）：")
        for idx, r in enumerate(rows_a3):
            name   = STOCK_NAME_MAP.get(r[0], r[0])
            amt_m  = abs(r[1]) * r[2] / 1_000_000
            log(f"    {idx+1}. {r[0]} {name}：外資賣超 {abs(r[1]):,} 股，金額 {amt_m:.0f}百萬元")
    except Exception as e:
        log(f"  ⚠️ A3 外資賣超查詢失敗：{e}")

    list_a1 = build_candidates(rows_a1, "A1外資", inst_col_idx=2)
    list_a2 = build_candidates(rows_a2, "A2投信", inst_col_idx=3)
    return list_a1, list_a2


def screen_news_hot(conn) -> list:
    """策略 C：新聞 / PTT 熱門股（全市場動態股票名稱）"""
    log("策略 C：掃描輿情熱門...")

    try:
        with conn.engine.connect() as db_conn:
            # 從 DB 取全市場最新的 stock_id / stock_name 對照
            name_rows = db_conn.execute(text("""
                SELECT DISTINCT ON (stock_id) stock_id, stock_name
                FROM tw_daily_prices
                WHERE stock_name IS NOT NULL
                ORDER BY stock_id, date DESC
            """)).fetchall()
            # 今日新聞標題
            news_rows = db_conn.execute(text("""
                SELECT title, source
                FROM market_intelligence
                WHERE publish_date >= CURRENT_DATE - INTERVAL '1 day'
            """)).fetchall()
    except Exception as e:
        log(f"  ❌ 資料查詢失敗：{e}")
        return []

    # 合併 DB 名稱與靜態名稱表（靜態優先覆蓋 DB）
    full_name_map = {r[0]: r[1] for r in name_rows if r[1]}
    full_name_map.update(STOCK_NAME_MAP)
    log(f"  股票名稱對照表：{len(full_name_map)} 支（DB {len(name_rows)} + 靜態 {len(STOCK_NAME_MAP)}）")

    # 統計每支股票被提及次數
    mention_count = {}
    for title, _source in news_rows:
        for stock_id, name in full_name_map.items():
            if name in title:
                if stock_id not in mention_count:
                    mention_count[stock_id] = {"count": 0, "titles": [], "name": name}
                mention_count[stock_id]["count"] += 1
                mention_count[stock_id]["titles"].append(title[:50])

    candidates = []
    for stock_id, info in mention_count.items():
        cnt = info["count"]
        if cnt >= 3:
            name = info.get("name") or STOCK_NAME_MAP.get(stock_id, stock_id)
            # 按提及次數給分：10次以上=3分，5次以上=2分，其餘=1分
            if cnt >= 10:
                score = 3
            elif cnt >= 5:
                score = 2
            else:
                score = 1
            candidates.append({
                "stock_id": stock_id,
                "name":     name,
                "source":   "輿情熱門",
                "score":    score,
                "detail":   f"新聞提及 {cnt} 次：{info['titles'][0]}",
                "indicators": calc_indicators(conn, stock_id),
            })
            log(f"  ✅ {stock_id} {name}：提及 {cnt} 次（score={score}）")

    return candidates


def get_market_environment(conn) -> dict:
    """分析大盤環境：多頭/盤整/空頭，作為全域風險過濾器"""
    try:
        with conn.engine.connect() as db_conn:
            rows = db_conn.execute(text("""
                SELECT date, close, volume
                FROM us_daily_prices
                WHERE ticker = '^TWII'
                ORDER BY date DESC
                LIMIT 60
            """)).fetchall()
    except Exception as e:
        log(f"  ⚠️ 大盤資料讀取失敗：{e}")
        return {"trend": "未知", "score": 0, "summary": "無法取得大盤資料"}

    if len(rows) < 20:
        return {"trend": "資料不足", "score": 0, "summary": "大盤資料不足 20 天"}

    df = pd.DataFrame(rows, columns=["date", "close", "volume"])
    df = df.sort_values("date").reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    close   = float(df["close"].iloc[-1])
    ma20    = float(df["close"].rolling(20).mean().iloc[-1])
    ma60    = float(df["close"].rolling(60).mean().iloc[-1]) if len(df) >= 60 else None
    ma20_5d_ago = float(df["close"].rolling(20).mean().iloc[-5]) if len(df) >= 25 else ma20

    above_ma20   = close > ma20
    ma20_rising  = ma20 > ma20_5d_ago
    above_ma60   = (close > ma60) if ma60 else None

    # 近 5 天漲跌幅
    pct_5d = (close - float(df["close"].iloc[-5])) / float(df["close"].iloc[-5]) * 100 if len(df) >= 5 else 0

    # 判斷趨勢
    if above_ma20 and ma20_rising and (above_ma60 is None or above_ma60):
        trend = "多頭"
        score = 1      # 加分
    elif not above_ma20 and not ma20_rising:
        trend = "空頭"
        score = -1     # 扣分
    else:
        trend = "盤整"
        score = 0

    ma60_str = f"MA60={ma60:.0f}" if ma60 else "MA60資料不足"
    summary  = (
        f"加權指數 {close:.0f}（MA20={ma20:.0f} {'↑' if ma20_rising else '↓'}，{ma60_str}），"
        f"近5日 {'↑' if pct_5d >= 0 else '↓'}{abs(pct_5d):.1f}%，大盤趨勢：{trend}"
    )

    log(f"  大盤環境：{summary}")
    return {"trend": trend, "score": score, "summary": summary,
            "close": close, "ma20": ma20, "above_ma20": above_ma20}


def screen_macro_events(conn) -> tuple[list, str]:
    """策略 D：宏觀事件掃描（Gemini Google Search Grounding）
    回傳 (candidates, macro_summary)
    macro_summary 會注入到第二階段每支股票的 prompt 中
    """
    log("策略 D：掃描宏觀事件（Google Search Grounding）...")

    if not USE_LLM or not GEMINI_KEY:
        log("  ⚠️ LLM 未啟用，略過宏觀掃描")
        return [], ""

    prompt = f"""你是台股分析師。今天是 {datetime.now().strftime('%Y-%m-%d')}。

請根據下方提供的今日新聞標題，識別重大國際財經與地緣政治事件，並分析對台股的影響。

請依以下 JSON 格式回答，不要有其他文字：
{{
  "events": [
    {{
      "title": "事件標題（一句話）",
      "impact": "對台股的影響方向（利多/利空/中性）",
      "sectors": ["受影響產業1", "受影響產業2"],
      "stock_ids": ["台股代號1", "台股代號2"]
    }}
  ],
  "summary": "今日宏觀環境一句話摘要（給個股分析用的背景）"
}}

注意：
- stock_ids 只填台股四到五碼代號，如 2330、2317
- 若不確定具體股票代號，stock_ids 留空陣列
- 最多列出 3 個事件，每個事件最多 5 支股票"""

    try:
        # 從 DB 抓今日新聞標題作為宏觀分析素材
        with conn.engine.connect() as db_conn:
            news_rows = db_conn.execute(text("""
                SELECT title FROM market_intelligence
                WHERE publish_date >= CURRENT_DATE - INTERVAL '1 days'
                ORDER BY publish_date DESC
                LIMIT 30
            """)).fetchall()
        news_titles = "\n".join(f"- {r[0]}" for r in news_rows)

        if not news_titles:
            log("  ⚠️ 無今日新聞資料，略過宏觀掃描")
            return [], ""

        prompt += f"\n\n今日新聞標題（請從中識別宏觀事件）：\n{news_titles}"
        raw = call_gemini(prompt).strip()

        # 解析 JSON
        import json, re
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not json_match:
            log(f"  ⚠️ 宏觀掃描回應格式錯誤，略過")
            return [], ""

        data         = json.loads(json_match.group())
        macro_summary = data.get("summary", "")
        events        = data.get("events", [])

        log(f"  宏觀摘要：{macro_summary}")

        candidates = []
        seen_ids   = set()
        for event in events:
            log(f"  📰 {event.get('title','')}（{event.get('impact','')}）→ {event.get('sectors',[])}")
            for stock_id in event.get("stock_ids", []):
                if stock_id in seen_ids:
                    continue
                seen_ids.add(stock_id)
                name = STOCK_NAME_MAP.get(stock_id, stock_id)
                ind  = calc_indicators(conn, stock_id) or {}
                candidates.append({
                    "stock_id":   stock_id,
                    "name":       name,
                    "source":     "宏觀事件",
                    "score":      2,
                    "detail":     f"宏觀事件：{event.get('title','')}（{event.get('impact','')}）",
                    "indicators": ind,
                })
                log(f"    ✅ {stock_id} {name}")

        return candidates, macro_summary

    except Exception as e:
        log(f"  ❌ 宏觀掃描失敗：{e}")
        return [], ""


def screen_event_calendar(conn, upcoming_list: list) -> list:
    """策略 B：未來 7 天有法說會的公司"""
    log("策略 B：掃描即將開法說會的公司...")

    cutoff  = datetime.now() + timedelta(days=DAYS_AHEAD_CALENDAR)
    today   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    candidates = []
    for item in upcoming_list:
        if today <= item["date"] <= cutoff:
            ind = calc_indicators(conn, item["stock_id"]) or {}
            # 技術面過濾：跌破月線且非大型股，直接跳過，避免塞滿候選池
            if ind and not ind.get("above_ma20") and ind.get("close", 0) <= 100:
                log(f"  ⏭️  {item['stock_id']} {item['name']}：跌破月線，略過")
                continue
            candidates.append({
                "stock_id": item["stock_id"],
                "name":     item["name"],
                "source":   "法說會事件",
                "score":    2,
                "detail":   f"{item['date_str']} {item['time']} 法說會",
                "indicators": ind,
            })
            log(f"  ✅ {item['stock_id']} {item['name']}：{item['date_str']} 法說會")

    return candidates


def merge_candidates(buckets: dict) -> list:
    """配額制合併：每個策略保留固定席次，避免單一策略佔滿候選池

    buckets: {"a1": list, "a2": list, "b": list, "c": list}
    配額:     A1=4, A2=3, B=4, C=3  → 共 14 席（有重疊自動補位）
    """
    QUOTAS   = {"a1": 4, "a2": 3, "b": 5, "c": 4, "d": 3}
    TOTAL    = 20

    # 第一步：把全部候選股合併，累加跨策略分數
    merged = {}
    for key, candidates in buckets.items():
        for c in candidates:
            sid = c["stock_id"]
            if sid not in merged:
                merged[sid] = c.copy()
                merged[sid]["sources"] = [c["source"]]
            else:
                merged[sid]["score"]  += c["score"]
                merged[sid]["sources"].append(c["source"])
                merged[sid]["detail"] += f" | {c['detail']}"

    # 第二步：依配額從各桶挑出代表股（以累積分數排序）
    selected_ids = set()
    result       = []

    for key, quota in QUOTAS.items():
        bucket_stocks = buckets.get(key, [])
        # 用合併後的累積分數排序
        sorted_bucket = sorted(
            [merged[c["stock_id"]] for c in bucket_stocks if c["stock_id"] in merged],
            key=lambda x: x["score"], reverse=True
        )
        count = 0
        for c in sorted_bucket:
            if count >= quota:
                break
            if c["stock_id"] not in selected_ids:
                selected_ids.add(c["stock_id"])
                result.append(c)
                count += 1

    # 第三步：剩餘席次由未入選的候選股（分數最高者）補滿
    overflow = sorted(
        [c for sid, c in merged.items() if sid not in selected_ids],
        key=lambda x: x["score"], reverse=True
    )
    result.extend(overflow[:TOTAL - len(result)])

    # 最終依分數排序
    result.sort(key=lambda x: x["score"], reverse=True)

    log(f"\n  海選結果：{len(result)} 支候選股（A1={sum(1 for c in result if 'A1' in ''.join(c.get('sources',[])))}"
        f" A2={sum(1 for c in result if 'A2' in ''.join(c.get('sources',[])))}"
        f" B={sum(1 for c in result if '法說會' in ''.join(c.get('sources',[])))}"
        f" C={sum(1 for c in result if '輿情' in ''.join(c.get('sources',[])))}"
        f" 跨策略={sum(1 for c in result if len(c.get('sources',[])) > 1)}）")
    return result


# ════════════════════════════════════════════════════════════════
# 📄  外部資料抓取（法說會、分點）
# ════════════════════════════════════════════════════════════════

def fetch_broker_summary(stock_id: str) -> str:
    """抓取嗨投資的券商分點買賣超前五名"""
    try:
        url = f'https://histock.tw/stock/branch.aspx?no={stock_id}'
        res = requests.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=10
        )
        # 使用 io.StringIO 避免 pandas 警告
        tables = pd.read_html(io.StringIO(res.text))
        
        results = []
        for t in tables:
            col_str = "".join(str(c) for c in t.columns)
            if '券商' in col_str or '買' in col_str or '賣' in col_str:
                t = t.fillna("") # 把 NaN 換成空字串讓排版乾淨
                results.append(t.head(5).to_string(index=False))
                
        if results:
            # 通常前兩個表格就是「買超前15名」與「賣超前15名」，我們各取前 5 轉成文字
            return "\n\n".join(results[:2])
        return "無分點資料"
    except Exception as e:
        return f"分點資料抓取失敗：{e}"

def fetch_conference_pdf(stock_id: str) -> str:
    """用 Selenium 從 MOPS 抓取法說會 PDF 並轉成文字"""
    driver = get_chrome_driver()
    pdf_text = ""

    try:
        driver.get("https://mops.twse.com.tw/mops/#/web/t100sb07_1")
        time.sleep(5)

        inputs  = driver.find_elements(By.TAG_NAME, "input")
        buttons = driver.find_elements(By.TAG_NAME, "button")

        # 填入股票代號
        target = inputs[2]
        driver.execute_script("arguments[0].value = arguments[1]", target, stock_id)
        driver.execute_script("arguments[0].dispatchEvent(new Event('input'))", target)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", target)
        time.sleep(2)

        # 點查詢
        driver.execute_script("arguments[0].click()", buttons[3])
        time.sleep(4)

        # 點彈出結果
        popup = driver.find_element(By.XPATH, "//button[contains(text(),'彈出結果')]")
        driver.execute_script("arguments[0].click()", popup)
        time.sleep(3)
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(3)

        # 找英文版 PDF（優先）
        pdf_url = None
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href") or ""
            if href.endswith("E001.pdf"):
                pdf_url = href
                break
        if not pdf_url:
            for a in driver.find_elements(By.TAG_NAME, "a"):
                href = a.get_attribute("href") or ""
                if ".pdf" in href.lower():
                    pdf_url = href
                    break

        if pdf_url:
            resp = requests.get(pdf_url,
                                headers={"User-Agent": "Mozilla/5.0"},
                                timeout=15)
            import logging
            logging.getLogger("pdfminer").setLevel(logging.ERROR)
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        pdf_text += t + "\n"

    except Exception as e:
        log(f"  ⚠️ {stock_id} 法說會抓取失敗：{e}")
    finally:
        driver.quit()

    return pdf_text


def extract_key_signals(text: str) -> dict:
    """從法說會文字抽取關鍵訊號（不需要 LLM）"""
    lines = text.split("\n")

    demand_kw  = ["demand", "revenue", "growth", "guidance", "outlook",
                  "AI", "data center", "next quarter", "展望", "成長"]
    risk_kw    = ["risk", "uncertainty", "headwind", "China", "export",
                  "decline", "下修", "風險", "庫存"]
    guidance_kw = ["guidance", "revenue to be", "expects", "forecast",
                   "展望", "預期", "目標"]

    def find_hits(keywords, max_hits=5):
        hits, seen = [], set()
        for line in lines:
            for kw in keywords:
                if kw.lower() in line.lower():
                    s = line.strip()[:150]
                    if s not in seen and len(s) > 15:
                        seen.add(s)
                        hits.append(s)
                    break
            if len(hits) >= max_hits:
                break
        return hits

    # 判斷整體情緒
    pos_words = ["growth", "record", "strong", "increase", "exceed", "成長", "創高"]
    neg_words = ["decline", "weak", "cautious", "cut", "下修", "庫存", "保守"]
    tl = text.lower()
    pos = sum(1 for w in pos_words if w in tl)
    neg = sum(1 for w in neg_words if w in tl)
    sentiment = "正面" if pos > neg else "負面" if neg > pos else "中性"

    return {
        "sentiment":      sentiment,
        "demand_signals": find_hits(demand_kw),
        "risk_signals":   find_hits(risk_kw),
        "guidance":       find_hits(guidance_kw),
    }


# ════════════════════════════════════════════════════════════════
# 🤖  LLM 整合（Gemini）
# ════════════════════════════════════════════════════════════════

def call_gemini(prompt: str) -> str:
    """呼叫 Gemini API，503/429 自動重試（指數退避）"""
    if not USE_LLM or not GEMINI_KEY:
        return "（LLM 未啟用）"

    from google import genai
    client = genai.Client(api_key=GEMINI_KEY)

    wait_times = [10, 30, 60]   # 三次重試：10秒、30秒、60秒
    for attempt, wait in enumerate(wait_times, 1):
        try:
            response = client.models.generate_content(
                model="gemini-3.1-flash-lite-preview",
                contents=prompt
            )
            time.sleep(5)   # 限制在 15 RPM 以內（每分鐘最多 12 次）
            return response.text
        except Exception as e:
            err = str(e)
            if "429" in err and "per_minute" in err.lower():
                log(f"  ⏳ 超過 RPM 限制（第 {attempt} 次），等待 60 秒...")
                time.sleep(60)
            elif "503" in err or "429" in err:
                log(f"  ⏳ Gemini 暫時不可用（第 {attempt} 次），{wait} 秒後重試...")
                time.sleep(wait)
            else:
                log(f"  ❌ LLM 呼叫發生錯誤：{e}")
                return f"LLM 呼叫失敗：{e}"

    log("  ❌ Gemini 重試 3 次仍失敗，略過此次分析")
    return "LLM 呼叫失敗：服務暫時不可用"


def get_company_profile(stock_id: str, name: str) -> str:
    """用訓練資料取得公司背景簡介"""
    if not USE_LLM or not GEMINI_KEY:
        return ""

    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_KEY)
        resp = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=(
                f"請用1-2句繁體中文描述台股上市公司「{name}」（股票代號 {stock_id}）"
                f"的主要業務與產品。若不確定，直接寫「業務資料不明」。"
            )
        )
        return resp.text.strip()
    except Exception:
        return ""


def llm_buy_decision(candidate: dict, conference_signals: dict,
                     recent_news: list, broker_info: str,
                     macro_summary: str = "",
                     company_profile: str = "") -> dict:
    """AI 買進決策"""
    ind = candidate.get("indicators", {})

    tech_summary = (
        f"股價 {ind.get('close', 'N/A')}，"
        f"{'站上' if ind.get('above_ma20') else '跌破'}月線，"
        f"量比 {ind.get('vol_ratio', 0):.1f}x，"
        f"RSI {ind.get('rsi', 0):.1f}"
    )

    conf_summary = "\n".join(conference_signals.get("guidance", [])[:3]) or "無法說會資料"
    news_summary = "\n".join([n[:80] for n in recent_news[:5]]) or "無近期新聞"

    # 針對法說會前夕的動態提示
    event_hint = ""
    if "法說會事件" in candidate.get("sources", []) and conf_summary == "無法說會資料":
        event_hint = "\n【特別提示】該公司即將舉辦法說會，目前尚無最新簡報。這是一筆「法說會押寶行情」交易，請把評估重心放在「技術面強弱」與「新聞題材」，若技術面轉強（如站上月線）即可考慮買進。"

    market_env_str = candidate.get("_market_env", {}).get("summary", "")
    macro_combined = "\n".join(filter(None, [market_env_str, macro_summary]))
    macro_section  = f"\n【今日宏觀背景】\n{macro_combined}" if macro_combined else ""

    profile_section = f"\n【公司簡介】\n{company_profile}" if company_profile else ""

    prompt = f"""
你是台股短線交易員，根據以下資料對這支股票做出明確的進場決策。

股票：{candidate['name']}（{candidate['stock_id']}）
篩選來源：{', '.join(candidate.get('sources', []))}
{event_hint}{profile_section}{macro_section}

【技術面】
{tech_summary}

【主力分點進出（前五大買賣超）】
{broker_info}

【法說會展望】
{conf_summary}

【近期新聞】
{news_summary}

────────────────────────────────
【決策規則】

第一步：個股積分
  加分項（每項 +1）：
    T. 股價站上月線（技術面）
    V. 量比 > 1.0x（量能）
    I. 法人買超（來源包含「法人買超」）
    F. 法說會情緒「正面」或有正面新聞題材
    B. 主力分點淨買超為正（籌碼）
  扣分項（每項 -1）：
    X. 股價跌破月線
    N. 法說會情緒「負面」或有重大個股利空

  個股建議：總分 ≥ 3 → 買進 ／ 總分 = 2 → 觀望 ／ 總分 ≤ 1 → 不買

第二步：宏觀事件修正（請判斷宏觀嚴重程度）
  【嚴重】主動戰爭波及台灣供應鏈、金融系統性危機、大國直接制裁台灣科技業
    → 強制降一級（買進→觀望，觀望→不買），信心降低
  【中等】區域衝突（中東/俄烏）、外資階段性提款、貿易摩擦
    → 信心降低一級（高→中，中→低），行動不變
  【輕微】一般市場波動、背景地緣風險、短期情緒
    → 不影響行動與信心

注意：
- 買進理由 必須引用上方具體數據（法說會展望內容、主力分點進出、新聞標題），不可只說「技術面良好」
- 最大風險 必須是「此股專屬」的風險（法說會雷點、籌碼異常、財務疑慮），大盤空頭是背景，不可當最大風險

請依以下格式回答（不要有其他內容）：
積分：（列出符合的加分項字母，如 A,C,D）
宏觀等級：嚴重 / 中等 / 輕微
買進理由：（一句話，引用具體數據）
最大風險：（一句話，此股專屬風險）
建議行動：買進 / 觀望 / 不買
信心程度：高 / 中 / 低
"""

    response = call_gemini(prompt)

    # 解析回應
    result = {
        "action":      "",
        "confidence":  "中",
        "reason":      "",
        "risk":        "",
        "score_items": "",
        "macro_level": "",
        "raw":         response,
    }
    for line in response.split("\n"):
        line = line.strip()
        if "積分" in line:
            result["score_items"] = line.split("：")[-1].strip()
        elif "宏觀等級" in line:
            result["macro_level"] = line.split("：")[-1].strip()
        elif "買進理由" in line:
            result["reason"] = line.split("：")[-1].strip()
        elif "最大風險" in line:
            result["risk"] = line.split("：")[-1].strip()
        elif "建議行動" in line:
            val = line.split("：")[-1].strip()
            if val in ("買進", "觀望", "不買"):
                result["action"] = val
        elif "信心程度" in line:
            val = line.split("：")[-1].strip()
            if val in ("高", "中", "低"):
                result["confidence"] = val

    # 若解析不到行動，記 log 並預設觀望
    if not result["action"]:
        log(f"  ⚠️ 解析失敗，原始回應：{response[:200]}")
        result["action"] = "觀望"

    return result


def llm_sell_decision(stock_id: str, stock_name: str,
                      recent_news: list) -> str:
    """AI 賣出監控"""
    news_text = "\n".join([n[:80] for n in recent_news[:10]]) or "無近期新聞"

    prompt = f"""
你正在監控我的持股【{stock_name}（{stock_id}）】。

請閱讀今日新聞，若出現「砍單、延遲、下修展望、競爭加劇、重大虧損」
等嚴重負面訊號，請回覆【🚨 強烈賣出建議：原因】。
若無，請回覆【✅ 繼續持有】。

今日新聞：
{news_text}
"""
    return call_gemini(prompt)


# ════════════════════════════════════════════════════════════════
# 📅  法說會日曆（MOPS）
# ════════════════════════════════════════════════════════════════

def get_conference_calendar() -> list:
    """從 MOPS 抓取全市場法說會日曆"""
    log("抓取法說會日曆...")

    now      = datetime.now()
    roc_year = str(now.year - 1911)
    all_results = []

    def query_market(market_name):
        driver = get_chrome_driver()
        results = []
        try:
            driver.get("https://mops.twse.com.tw/mops/#/web/t100sb02_1")
            time.sleep(5)

            selects    = driver.find_elements(By.TAG_NAME, "select")
            buttons    = driver.find_elements(By.TAG_NAME, "button")
            year_input = driver.find_element(
                By.CSS_SELECTOR, "input[placeholder*='民國年']"
            )

            Select(selects[0]).select_by_visible_text(market_name)
            time.sleep(1)

            year_input.click()
            year_input.send_keys(Keys.CONTROL + "a")
            year_input.send_keys(Keys.DELETE)
            year_input.send_keys(roc_year)
            year_input.send_keys(Keys.TAB)
            time.sleep(1)

            Select(selects[1]).select_by_index(0)
            time.sleep(1)

            query_btn = next(b for b in buttons if b.text.strip() == "查詢")
            driver.execute_script("arguments[0].click()", query_btn)
            time.sleep(4)

            popup = driver.find_element(
                By.XPATH, "//button[contains(text(),'彈出結果')]"
            )
            driver.execute_script("arguments[0].click()", popup)
            time.sleep(3)
            driver.switch_to.window(driver.window_handles[-1])
            time.sleep(3)

            tables = driver.find_elements(By.TAG_NAME, "table")
            if tables:
                rows = tables[0].find_elements(By.TAG_NAME, "tr")
                for row in rows[2:]:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 4:
                        continue
                    try:
                        parts     = cols[2].text.strip().split("/")
                        conf_date = datetime(
                            int(parts[0]) + 1911,
                            int(parts[1]),
                            int(parts[2])
                        )
                        links    = row.find_elements(By.TAG_NAME, "a")
                        pdf_urls = [
                            a.get_attribute("href") for a in links
                            if a.get_attribute("href") and
                            ".pdf" in (a.get_attribute("href") or "").lower()
                        ]
                        results.append({
                            "market":   market_name,
                            "stock_id": cols[0].text.strip(),
                            "name":     cols[1].text.strip(),
                            "date":     conf_date,
                            "date_str": conf_date.strftime("%Y/%m/%d"),
                            "time":     cols[3].text.strip(),
                            "pdf_en":   pdf_urls[1] if len(pdf_urls) > 1 else
                                        pdf_urls[0] if pdf_urls else "",
                        })
                    except Exception:
                        continue
        except Exception as e:
            log(f"  ⚠️ {market_name} 日曆抓取失敗：{e}")
        finally:
            driver.quit()
        return results

    for market in ["上市", "上櫃"]:
        r = query_market(market)
        all_results.extend(r)
        log(f"  {market}：{len(r)} 場")
        time.sleep(2)

    all_results.sort(key=lambda x: x["date"])
    return all_results


# ════════════════════════════════════════════════════════════════
# 💼  持股管理
# ════════════════════════════════════════════════════════════════

def init_trade_log(conn):
    """初始化交易紀錄表"""
    conn.execute_raw("""
        CREATE TABLE IF NOT EXISTS trade_log (
            id           SERIAL PRIMARY KEY,
            date         DATE,
            stock_id     VARCHAR(10),
            name         VARCHAR(50),
            action       VARCHAR(10),
            entry_price  NUMERIC,
            exit_price   NUMERIC,
            reason       TEXT,
            sources      TEXT,
            llm_decision TEXT
        )
    """)


def add_to_watchlist(conn, stock_id: str, name: str,
                     entry_price: float, reason: str,
                     sources: list, llm_decision: dict):
    """加入持股監控清單"""
    with conn.engine.begin() as db_conn:
        db_conn.execute(text("""
            INSERT INTO trade_log
            (date, stock_id, name, action, entry_price, reason, sources, llm_decision)
            VALUES (:date, :stock_id, :name, 'BUY', :entry_price, :reason, :sources, :llm_decision)
        """), {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "stock_id": stock_id, "name": name, 
            "entry_price": entry_price, "reason": reason,
            "sources": json.dumps(sources, ensure_ascii=False),
            "llm_decision": json.dumps(llm_decision, ensure_ascii=False)
        })
    log(f"  📝 {stock_id} {name} 加入持股清單，進場價 {entry_price}")


def get_watchlist(conn) -> list:
    """取得持股中的股票"""
    try:
        with conn.engine.connect() as db_conn:
            rows = db_conn.execute(text("""
                SELECT stock_id, name, entry_price, date, reason
                FROM trade_log
                WHERE action = 'BUY'
                  AND stock_id NOT IN (
                      SELECT stock_id FROM trade_log WHERE action = 'SELL'
                  )
            """)).fetchall()
        return [
            {
                "stock_id":    r[0],
                "name":        r[1],
                "entry_price": r[2],
                "buy_date":    r[3],
                "reason":      r[4],
            }
            for r in rows
        ]
    except Exception:
        return []


def record_sell(conn, stock_id: str, exit_price: float, reason: str):
    """記錄賣出"""
    with conn.engine.begin() as db_conn:
        db_conn.execute(text("""
            INSERT INTO trade_log
            (date, stock_id, action, exit_price, reason)
            VALUES (:date, :stock_id, 'SELL', :exit_price, :reason)
        """), {"date": datetime.now().strftime("%Y-%m-%d"), "stock_id": stock_id, 
               "exit_price": exit_price, "reason": reason})
    log(f"  📝 {stock_id} 賣出，出場價 {exit_price}，原因：{reason}")


# ════════════════════════════════════════════════════════════════
# 📡  第三階段：持股監控
# ════════════════════════════════════════════════════════════════

def monitor_holdings(conn):
    """每日監控持股，決定是否賣出"""
    log("\n" + "=" * 60)
    log("📡 第三階段：持股監控")
    log("=" * 60)

    holdings = get_watchlist(conn)
    if not holdings:
        log("  目前無持股")
        return

    alerts = []

    for h in holdings:
        stock_id    = h["stock_id"]
        name        = h["name"]
        entry_price = h["entry_price"]

        log(f"\n  監控 {stock_id} {name}（進場 {entry_price}）...")

        # 取最新收盤價
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT close FROM tw_daily_prices
                WHERE stock_id = :sid
                ORDER BY date DESC LIMIT 1
            """), {"sid": stock_id}).fetchone()

        if not row:
            log(f"  ⚠️ 找不到 {stock_id} 股價資料")
            continue

        current_price = row[0]
        pnl_pct       = current_price / entry_price

        sell_reason = None

        # ── 停損 ──
        if pnl_pct < STOP_LOSS_PCT:
            sell_reason = f"停損：現價 {current_price}，虧損 {(1-pnl_pct)*100:.1f}%"

        # ── 停利 ──
        elif pnl_pct > STOP_PROFIT_PCT:
            sell_reason = f"停利：現價 {current_price}，獲利 {(pnl_pct-1)*100:.1f}%"

        else:
            # ── 技術面破線 ──
            ind = calc_indicators(conn, stock_id)
            if ind and not ind.get("above_ma20"):
                sell_reason = f"跌破月線：現價 {current_price}，MA20 {ind.get('ma20', 0):.1f}"

            # ── 法人由買轉賣 ──
            if not sell_reason:
                with conn.engine.connect() as db_conn:
                    sell_rows = db_conn.execute(text("""
                        SELECT COUNT(*) FROM tw_institutional_trades
                        WHERE stock_id = :sid
                          AND date >= CURRENT_DATE - INTERVAL '3 days'
                          AND total < -1000000
                    """), {"sid": stock_id}).fetchone()
                if sell_rows and sell_rows[0] >= 2:
                    sell_reason = "法人連續賣超 2 天"

            # ── LLM 消息面監控 ──
            if not sell_reason and USE_LLM:
                with conn.engine.connect() as db_conn:
                    news_rows = db_conn.execute(text("""
                        SELECT title FROM market_intelligence
                        WHERE publish_date >= CURRENT_DATE - INTERVAL '1 day'
                    """)).fetchall()
                news_list  = [r[0] for r in news_rows]
                llm_result = llm_sell_decision(stock_id, name, news_list)

                if "強烈賣出" in llm_result:
                    sell_reason = f"AI 消息面警告：{llm_result[:80]}"

        # ── 輸出監控結果 ──
        if sell_reason:
            alerts.append({
                "stock_id": stock_id,
                "name":     name,
                "price":    current_price,
                "reason":   sell_reason,
            })
            log(f"  🚨 {stock_id} {name} 觸發賣出：{sell_reason}")
        else:
            pnl_str = f"+{(pnl_pct-1)*100:.1f}%" if pnl_pct > 1 else f"{(pnl_pct-1)*100:.1f}%"
            log(f"  ✅ {stock_id} {name}：現價 {current_price}（{pnl_str}），繼續持有")

    # 輸出警報摘要
    if alerts:
        log("\n" + "🚨" * 20)
        log("  需要處理的賣出警報：")
        for a in alerts:
            log(f"  {a['stock_id']} {a['name']}：{a['reason']}")
        log("🚨" * 20)

    return alerts


# ════════════════════════════════════════════════════════════════
# 🚀  主流程
# ════════════════════════════════════════════════════════════════

def run_daily_agent():
    """每日完整執行一次"""
    log("\n" + "=" * 60)
    log(f"🤖 台股 AI 交易系統啟動")
    log(f"   {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log("=" * 60)

    conn = get_db()
    init_trade_log(conn)

    # ── 法說會日曆（每日更新）──
    log("\n📅 更新法說會日曆...")
    upcoming = get_conference_calendar()
    log(f"  全年 {len(upcoming)} 場，今日之後 {sum(1 for u in upcoming if u['date'] >= datetime.now())} 場")

    # ────────────────────────────────────────
    # 第一階段：海選池
    # ────────────────────────────────────────
    log("\n" + "=" * 60)
    log("🔍 第一階段：海選池")
    log("=" * 60)

    # 大盤環境（全域風險過濾器）
    log("\n📊 大盤環境分析...")
    market_env = get_market_environment(conn)
    log(f"  趨勢：{market_env['trend']}（大盤加分：{market_env['score']:+d}）")

    list_a1, list_a2 = screen_institutional(conn)
    list_b           = screen_event_calendar(conn, upcoming)
    list_c           = screen_news_hot(conn)
    list_d, macro_summary = screen_macro_events(conn)

    candidates = merge_candidates({"a1": list_a1, "a2": list_a2, "b": list_b, "c": list_c, "d": list_d})

    # 大盤空頭時，所有候選股分數扣 1（降低進場意願）
    if market_env["score"] < 0:
        log(f"  ⚠️ 大盤空頭，所有候選股 score -1")
        for c in candidates:
            c["score"] = max(0, c["score"] - 1)
        candidates.sort(key=lambda x: x["score"], reverse=True)

    log(f"\n  海選結果：{len(candidates)} 支候選股")

    if not candidates:
        log("  今日無候選股，結束")
        conn.close()
        return

    # ────────────────────────────────────────
    # 第二階段：AI 深度決策
    # ────────────────────────────────────────
    log("\n" + "=" * 60)
    log("🔀 海選池 → 第二階段 篩選說明")
    log("=" * 60)

    STAGE2_LIMIT = 12

    # ── 動態配額：依策略 A 今日訊號強度分配第二階段席次 ──────
    strong_a = sum(1 for c in candidates
                   if any("法人買超" in s for s in c.get("sources", [c.get("source", "")]))
                   and c.get("score", 0) >= 3)

    if strong_a >= 4:
        stage2_quota = {"a": 4, "b": 4, "c": 4}
        signal_label = f"強（{strong_a} 支 score≥3）"
    elif strong_a >= 2:
        stage2_quota = {"a": 3, "b": 5, "c": 4}
        signal_label = f"普通（{strong_a} 支 score≥3）"
    else:
        stage2_quota = {"a": 2, "b": 6, "c": 4}
        signal_label = f"弱（{strong_a} 支 score≥3）"

    log(f"\n  法人訊號強度：{signal_label}")
    log(f"  第二階段配額 → 法人:{stage2_quota['a']} 法說會:{stage2_quota['b']} 輿情:{stage2_quota['c']}")

    # 依配額選出進入第二階段的股票
    selected_ids, entering = set(), []
    for _bucket_key, quota, match_fn in [
        ("a", stage2_quota["a"], lambda s: "法人買超" in s),
        ("b", stage2_quota["b"], lambda s: "法說會"   in s),
        ("c", stage2_quota["c"], lambda s: "輿情"     in s),
    ]:
        pool = [c for c in candidates
                if any(match_fn(s) for s in c.get("sources", [c.get("source", "")]))
                and c["stock_id"] not in selected_ids]
        for c in pool[:quota]:
            selected_ids.add(c["stock_id"])
            entering.append(c)

    # 若還有剩餘席次，從未入選的候選股補滿
    remaining = [c for c in candidates if c["stock_id"] not in selected_ids]
    entering.extend(remaining[:STAGE2_LIMIT - len(entering)])
    entering = entering[:STAGE2_LIMIT]

    # ── 補充輪：跳過名單中 score≥2 且 量比≥1.0x 的股票自動補入 ──
    STAGE2_MAX = 20   # 絕對上限，避免 API 費用失控
    bonus_pool = [
        c for c in candidates
        if c["stock_id"] not in {e["stock_id"] for e in entering}
        and c.get("score", 0) >= 2
        and c.get("indicators", {}).get("vol_ratio", 0) >= 1.0
    ]
    bonus_pool.sort(key=lambda x: (x.get("score", 0), x.get("indicators", {}).get("vol_ratio", 0)), reverse=True)
    if bonus_pool:
        log(f"\n  🔁 補充輪：補入 {min(len(bonus_pool), STAGE2_MAX - len(entering))} 支（score≥2 且 量比≥1.0x）")
    entering.extend(bonus_pool[:STAGE2_MAX - len(entering)])

    dropped = [c for c in candidates if c["stock_id"] not in {e["stock_id"] for e in entering}]

    log(f"\n  ✅ 進入第二階段（共 {len(entering)} 支）：")
    for c in entering:
        ind    = c.get("indicators") or {}
        ma_str = "站上月線" if ind.get("above_ma20") else "跌破月線"
        vr_str = f"量比 {ind.get('vol_ratio', 0):.1f}x"
        log(f"    → {c['stock_id']} {c['name']}｜score={c['score']}｜{ma_str}｜{vr_str}"
            f"｜來源：{', '.join(c.get('sources', [c.get('source','')]))}")

    if dropped:
        log(f"\n  ⛔ 本次未分析（超出上限 {STAGE2_LIMIT} 支，共 {len(dropped)} 支被跳過）：")
        for c in dropped:
            ind    = c.get("indicators") or {}
            ma_str = "站上月線" if ind.get("above_ma20") else "跌破月線"
            vr_str = f"量比 {ind.get('vol_ratio', 0):.1f}x"
            log(f"    ✗ {c['stock_id']} {c['name']}｜score={c['score']}｜{ma_str}｜{vr_str}"
                f"｜來源：{', '.join(c.get('sources', [c.get('source','')]))}"
                f"｜跳過原因：排名第 {candidates.index(c)+1}，超出本日分析上限")
    else:
        log(f"\n  （候選股數量未超過上限，全數進入第二階段）")

    log("\n" + "=" * 60)
    log("🤖 第二階段：AI 深度決策")
    log("=" * 60)

    buy_list = []
    candidate_analysis_msg = ""

    for c in entering:  # 最多分析 STAGE2_LIMIT 支
        c["_market_env"] = market_env   # 把大盤環境帶入每支候選股供 LLM 使用
        stock_id = c["stock_id"]
        name     = c["name"]
        log(f"\n  分析 {stock_id} {name}（來源：{', '.join(c.get('sources', []))}）...")

        # 公司簡介
        profile = get_company_profile(stock_id, name)
        c["profile"] = profile
        if profile:
            log(f"  🏢 {profile}")

        # 抓法說會
        pdf_text = fetch_conference_pdf(stock_id)
        if pdf_text:
            conf_signals = extract_key_signals(pdf_text)
            log(f"  法說會情緒：{conf_signals['sentiment']}")
        else:
            conf_signals = {"sentiment": "無資料", "guidance": []}
            log("  法說會：無資料")

        # 抓近期新聞
        with conn.engine.connect() as db_conn:
            news_rows = db_conn.execute(text("""
                SELECT title FROM market_intelligence
                WHERE publish_date >= CURRENT_DATE - INTERVAL '3 days'
            """)).fetchall()
        news_list = [r[0] for r in news_rows if c["name"] in r[0]]

        # 抓主力分點
        log("  抓取主力分點資料...")
        broker_info = fetch_broker_summary(stock_id)

        # LLM 決策（或規則決策）
        if USE_LLM:
            decision = llm_buy_decision(c, conf_signals, news_list, broker_info, macro_summary, c.get("profile", ""))
        else:
            # 無 LLM 時用規則決策
            ind = c.get("indicators", {})
            score = c.get("score", 0)
            if (score >= 3
                    and conf_signals["sentiment"] == "正面"
                    and ind.get("golden_cross")):
                action     = "買進"
                confidence = "高"
            elif score >= 2 and ind.get("above_ma20"):
                action     = "觀望"
                confidence = "中"
            else:
                action     = "不買"
                confidence = "低"

            decision = {
                "action":     action,
                "confidence": confidence,
                "reason":     c.get("detail", ""),
                "risk":       "無 LLM，規則判斷",
            }

        yahoo_url = f"https://tw.stock.yahoo.com/quote/{stock_id}"

        macro_tag = f" / 宏觀：{decision['macro_level']}" if decision.get('macro_level') else ""
        log(f"  積分：{decision.get('score_items','—')}  決策：{decision['action']} / 信心：{decision['confidence']}{macro_tag}")
        log(f"  理由：{decision.get('reason', '')}")
        log(f"  風險：{decision.get('risk', '')}")
        log(f"  走勢：{yahoo_url}")
        
        if not decision.get("reason"):
            log(f"  ⚠️ [除錯] 解析失敗，LLM 原始回應如下：\n{decision.get('raw', '')}")

        c["decision"] = decision

        # 收集每一檔分析的決策推播文字 (把所有決策都推到 TG)
        candidate_analysis_msg += f"🔹 <b>{stock_id} {name}</b> (來源：{', '.join(c.get('sources', []))})\n"
        candidate_analysis_msg += f"💡 決策：<b>{decision['action']}</b> (信心：{decision['confidence']})\n"
        candidate_analysis_msg += f"👉 理由：{decision.get('reason', '').replace('<', '〈').replace('>', '〉')}\n"
        candidate_analysis_msg += f"⚠️ 風險：{decision.get('risk', '').replace('<', '〈').replace('>', '〉')}\n"
        candidate_analysis_msg += f"📈 走勢：<a href='{yahoo_url}'>Yahoo 奇摩股市</a>\n\n"

        if decision["action"] == "買進" and decision["confidence"] in ("高", "中"):
            buy_list.append(c)

    # ────────────────────────────────────────
    # 輸出買進建議
    # ────────────────────────────────────────
    log("\n" + "=" * 60)
    log("  📋 今日買進建議")
    log("=" * 60)

    if not buy_list:
        log("  今日無買進建議")
    else:
        for c in buy_list:
            d = c["decision"]
            log(f"  ✅ {c['stock_id']} {c['name']}")
            log(f"     來源：{', '.join(c.get('sources', []))}")
            log(f"     理由：{d.get('reason', '')}")
            log(f"     風險：{d.get('risk', '')}")
            log(f"     信心：{d.get('confidence', '')}")
            log(f"     走勢：https://tw.stock.yahoo.com/quote/{c['stock_id']}")

    # ────────────────────────────────────────
    # 第三階段：持股監控
    # ────────────────────────────────────────
    alerts = monitor_holdings(conn) or []

    # ────────────────────────────────────────
    # 發送 Telegram 推播
    # ────────────────────────────────────────
    log("\n📲 準備發送 Telegram 推播...")

    # 買進的排最前面，確保理由一定看得到
    analyzed     = sorted(entering, key=lambda x: 0 if x["stock_id"] in {b["stock_id"] for b in buy_list} else 1)
    now_str      = datetime.now().strftime("%Y-%m-%d %H:%M")
    ACTION_EMOJI = {"買進": "🟢", "觀望": "🟡", "不買": "🔴"}

    msg  = f"<b>📈 台股 AI 日報  {now_str}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"

    # 每支分析（全部都顯示，買進的在前）
    for c in analyzed:
        d         = c.get("decision", {})
        ind       = c.get("indicators", {})
        action    = d.get("action", "—")
        conf      = d.get("confidence", "—")
        emoji     = ACTION_EMOJI.get(action, "⚪")
        ma_str    = "▲月線" if ind.get("above_ma20") else "▼月線"
        vol_r     = ind.get("vol_ratio", 0)
        rsi       = ind.get("rsi", 0)
        yahoo_url = f"https://tw.stock.yahoo.com/quote/{c['stock_id']}"
        reason    = d.get("reason", "").replace("<", "〈").replace(">", "〉")
        risk      = d.get("risk",   "").replace("<", "〈").replace(">", "〉")
        src       = "＋".join(c.get("sources", []))

        profile  = c.get("profile", "")
        size_tag = c.get("size_tag", "")
        ma20_liq = c.get("ma20_liquidity") or 0
        liq_str  = f"  日均成交{ma20_liq/1_000_000:.0f}百萬" if ma20_liq else ""
        msg += f"{emoji} <b>{c['stock_id']} {c['name']}</b>  {action}／{conf}\n"
        if profile:
            msg += f"   🏢 {profile}\n"
        if size_tag:
            msg += f"   📊 規模：{size_tag}股{liq_str}\n"
        msg += f"   {src}  |  {ma_str}  量比{vol_r:.1f}x  RSI{rsi:.0f}\n"
        msg += f"   📌 {reason}\n"
        msg += f"   ⚠️ {risk}\n"
        msg += f"   <a href='{yahoo_url}'>走勢圖</a>\n\n"

    msg += "━━━━━━━━━━━━━━━━━━━━━\n"

    # 買進結論
    msg += f"<b>🛒 買進建議（{len(buy_list)} 支）</b>\n"
    if not buy_list:
        msg += "今日無買進建議\n"
    else:
        for c in buy_list:
            msg += f"✅ <b>{c['stock_id']} {c['name']}</b>\n"

    # 賣出警報
    msg += f"\n<b>🚨 賣出警報（{len(alerts)} 支）</b>\n"
    if not alerts:
        msg += "今日無賣出警報\n"
    else:
        for a in alerts:
            msg += f"🔴 <b>{a['stock_id']} {a['name']}</b>  {a['reason'][:50]}\n"

    # 超過 TG 上限時，拆成多則訊息發送（每則 3800 字）
    CHUNK = 3800
    if len(msg) <= CHUNK:
        send_telegram_message(msg)
    else:
        # 第一則固定包含 header + 買進股票的詳情
        parts, cur = [], ""
        for line in msg.split("\n"):
            if len(cur) + len(line) + 1 > CHUNK:
                parts.append(cur)
                cur = ""
            cur += line + "\n"
        if cur:
            parts.append(cur)
        for i, part in enumerate(parts):
            prefix = f"<b>（{i+1}/{len(parts)}）</b>\n" if len(parts) > 1 else ""
            send_telegram_message(prefix + part)

    conn.engine.dispose()
    log("\n✅ 系統執行完成\n")
    return buy_list


def run_monitor_only():
    """只執行持股監控（不找新股票）"""
    conn = get_db()
    init_trade_log(conn)
    monitor_holdings(conn)
    conn.engine.dispose()


def run_screen_only():
    """只執行海選池（不做深度分析）"""
    conn = get_db()
    log("🔍 執行海選池...")

    upcoming = []  # 如果要包含法說會策略，先抓日曆

    list_a1, list_a2 = screen_institutional(conn)
    list_b           = screen_event_calendar(conn, upcoming)
    list_c           = screen_news_hot(conn)
    list_d, _macro_summary = screen_macro_events(conn)

    candidates = merge_candidates({"a1": list_a1, "a2": list_a2, "b": list_b, "c": list_c, "d": list_d})

    log(f"\n{'='*60}")
    log(f"  候選股清單（共 {len(candidates)} 支）")
    log(f"{'='*60}")
    for c in candidates:
        log(f"  {c['stock_id']} {c['name']}  分數：{c['score']}  來源：{', '.join(c.get('sources',[]))}")
        log(f"    {c['detail']}")

    conn.engine.dispose()
    return candidates


# ════════════════════════════════════════════════════════════════
# ▶️  執行入口（改這裡選擇模式）
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    # ── 選擇執行模式（把不需要的行加 # 註解）──

    # 模式 1：完整執行（海選 + 深度分析 + 持股監控）
    run_daily_agent()

    # 模式 2：只執行海選池（最快，看今天有什麼股票值得關注）
    # run_screen_only()

    # 模式 3：只監控持股（已買進的股票要不要賣）
    # run_monitor_only()