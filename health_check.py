"""
系統健康檢查 - 確認每個流程是否正常
執行方式：直接跑此檔案，或貼到 Jupyter 執行
"""

import sys
import os
sys.path.insert(0, 'C:/Users/Qoo/Desktop/my_workspace/reason-stock-agent')

from datetime import datetime
import requests

OK  = "✅"
ERR = "❌"
WRN = "⚠️"

results = []

def check(name, status, detail=""):
    icon = OK if status == "ok" else (WRN if status == "warn" else ERR)
    msg = f"{icon} {name}"
    if detail:
        msg += f"  →  {detail}"
    results.append((status, name, detail))
    print(msg)

def section(title):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")

# ─────────────────────────────────────────────
# 1. 資料庫連線
# ─────────────────────────────────────────────
section("第一階段：資料庫 & 基礎資料")

try:
    from database import get_db_client
    from sqlalchemy import text
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            db_conn.execute(text("SELECT 1"))
    check("資料庫連線", "ok", "PostgreSQL 連線正常")
except Exception as e:
    check("資料庫連線", "error", str(e))

# ─────────────────────────────────────────────
# 2. 大盤資料 (^TWII)
# ─────────────────────────────────────────────
try:
    from database import get_db_client
    from sqlalchemy import text
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT date, close FROM us_daily_prices
                WHERE ticker = '^TWII' AND close IS NOT NULL AND volume > 0
                ORDER BY date DESC LIMIT 1
            """)).fetchone()
    if row:
        days_ago = (datetime.now().date() - row[0]).days if hasattr(row[0], 'date') else 0
        status = "ok" if days_ago <= 5 else "warn"
        check("大盤資料 ^TWII", status, f"最新：{row[0]}  收盤：{row[1]}  ({days_ago}天前)")
    else:
        check("大盤資料 ^TWII", "error", "查無資料")
except Exception as e:
    check("大盤資料 ^TWII", "error", str(e))

# ─────────────────────────────────────────────
# 3. 台股股價
# ─────────────────────────────────────────────
try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT COUNT(DISTINCT stock_id), MAX(date) FROM tw_daily_prices
            """)).fetchone()
    count, max_date = row
    days_ago = (datetime.now().date() - max_date).days if max_date else 999
    status = "ok" if days_ago <= 5 else "warn"
    check("台股股價", status, f"共 {count} 支股票，最新日期：{max_date}（{days_ago}天前）")
except Exception as e:
    check("台股股價", "error", str(e))

# ─────────────────────────────────────────────
# 4. 三大法人
# ─────────────────────────────────────────────
try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT COUNT(DISTINCT stock_id), MAX(date) FROM tw_institutional_trades
            """)).fetchone()
    count, max_date = row
    days_ago = (datetime.now().date() - max_date).days if max_date else 999
    status = "ok" if days_ago <= 5 else "warn"
    check("三大法人籌碼", status, f"共 {count} 支股票，最新日期：{max_date}（{days_ago}天前）")
except Exception as e:
    check("三大法人籌碼", "error", str(e))

# ─────────────────────────────────────────────
# 5. 月營收
# ─────────────────────────────────────────────
try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT COUNT(DISTINCT stock_id), MAX(revenue_year), MAX(revenue_month)
                FROM tw_monthly_revenue
            """)).fetchone()
    count, yr, mo = row
    status = "ok" if count and count > 0 else "error"
    check("月營收", status, f"共 {count} 支股票，最新：{yr}年{mo}月")
except Exception as e:
    check("月營收", "error", str(e))

# ─────────────────────────────────────────────
# 6. EPS
# ─────────────────────────────────────────────
try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT COUNT(DISTINCT stock_id), MAX(date) FROM tw_financial_statements
                WHERE type = 'EPS'
            """)).fetchone()
    count, max_date = row
    status = "ok" if count and count > 0 else "warn"
    check("EPS 資料", status, f"共 {count} 支股票，最新：{max_date}")
except Exception as e:
    check("EPS 資料", "error", str(e))

# ─────────────────────────────────────────────
# 7. 新聞資料庫
# ─────────────────────────────────────────────
section("第二階段：新聞 & 外部資料")

try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            row = db_conn.execute(text("""
                SELECT COUNT(*), MAX(publish_date) FROM market_intelligence
                WHERE publish_date >= CURRENT_DATE - INTERVAL '3 days'
            """)).fetchone()
    count, max_date = row
    status = "ok" if count and count > 10 else ("warn" if count > 0 else "error")
    check("宏觀新聞 (近3天)", status, f"{count} 筆，最新：{max_date}")
except Exception as e:
    check("宏觀新聞", "error", str(e))

# ─────────────────────────────────────────────
# 8. Google News RSS fallback
# ─────────────────────────────────────────────
try:
    import feedparser
    rss_url = "https://news.google.com/rss/search?q=台積電&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    res = requests.get(rss_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
    feed = feedparser.parse(res.content)
    count = len(feed.entries)
    status = "ok" if count > 0 else "warn"
    check("Google News RSS", status, f"抓到 {count} 筆")
except Exception as e:
    check("Google News RSS", "error", str(e))

# ─────────────────────────────────────────────
# 9. 法說會 MOPS (Selenium)
# ─────────────────────────────────────────────
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    import time
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.get("https://mops.twse.com.tw/mops/#/web/t100sb07_1")
    time.sleep(5)
    inputs = driver.find_elements(By.TAG_NAME, "input")
    driver.quit()
    status = "ok" if len(inputs) >= 3 else "warn"
    check("法說會 MOPS (Selenium)", status, f"頁面載入正常，找到 {len(inputs)} 個輸入欄")
except Exception as e:
    check("法說會 MOPS (Selenium)", "error", str(e))

# ─────────────────────────────────────────────
# 10. Gemini API
# ─────────────────────────────────────────────
try:
    from google import genai
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        check("Gemini API Key", "error", "GEMINI_API_KEY 未設定")
    else:
        client = genai.Client(api_key=api_key)
        resp = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents="請回覆數字1",
        )
        check("Gemini API", "ok", f"回應：{resp.text.strip()[:30]}")
except Exception as e:
    check("Gemini API", "error", str(e))

# ─────────────────────────────────────────────
# 11. Telegram Bot
# ─────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        check("Telegram Bot", "warn", "TOKEN 或 CHAT_ID 未設定（可選）")
    else:
        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
        name = res.json().get("result", {}).get("username", "?")
        check("Telegram Bot", "ok", f"Bot 名稱：@{name}")
except Exception as e:
    check("Telegram Bot", "error", str(e))

# ─────────────────────────────────────────────
# 12. 持股監控清單
# ─────────────────────────────────────────────
section("第三階段：持股監控")

try:
    with get_db_client() as conn:
        with conn.engine.connect() as db_conn:
            rows = db_conn.execute(text("""
                SELECT stock_id, name, entry_price, date FROM trade_log
                WHERE action = 'BUY'
                  AND stock_id NOT IN (
                      SELECT stock_id FROM trade_log WHERE action = 'SELL'
                  )
                ORDER BY date DESC
            """)).fetchall()
    if rows:
        check("持股清單", "ok", f"目前持有 {len(rows)} 支：{', '.join([r[0] for r in rows])}")
    else:
        check("持股清單", "warn", "目前無持股（尚未買進）")
except Exception as e:
    check("持股清單", "error", str(e))

# ─────────────────────────────────────────────
# 總結
# ─────────────────────────────────────────────
print(f"\n{'═'*50}")
print("  健康檢查總結")
print(f"{'═'*50}")

ok_count   = sum(1 for r in results if r[0] == "ok")
warn_count = sum(1 for r in results if r[0] == "warn")
err_count  = sum(1 for r in results if r[0] == "error")

print(f"✅ 正常：{ok_count}  ⚠️ 警告：{warn_count}  ❌ 異常：{err_count}")

if err_count > 0:
    print("\n需要修復：")
    for r in results:
        if r[0] == "error":
            print(f"  ❌ {r[1]}：{r[2]}")

if warn_count > 0:
    print("\n注意事項：")
    for r in results:
        if r[0] == "warn":
            print(f"  ⚠️  {r[1]}：{r[2]}")
