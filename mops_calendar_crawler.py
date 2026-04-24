from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime
from sqlalchemy import text
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_client

def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def query_single_market(market_name, roc_year):
    """每次開新的 driver，查完就關掉"""
    driver = get_chrome_driver()
    results = []

    try:
        driver.get("https://mops.twse.com.tw/mops/#/web/t100sb02_1")
        time.sleep(5)

        selects = driver.find_elements(By.TAG_NAME, "select")
        buttons = driver.find_elements(By.TAG_NAME, "button")
        all_inputs = driver.find_elements(By.TAG_NAME, "input")

        # 找年度欄位
        year_input = None
        for inp in all_inputs:
            ph = inp.get_attribute("placeholder") or ""
            if "民國" in ph and inp.is_displayed():
                year_input = inp
                break

        if not year_input:
            print(f"  ❌ {market_name} 找不到年度欄位")
            return []

        # 選市場別
        Select(selects[0]).select_by_visible_text(market_name)
        time.sleep(1)

        # 填年度
        year_input.click()
        time.sleep(0.3)
        year_input.send_keys(Keys.CONTROL + "a")
        year_input.send_keys(Keys.DELETE)
        year_input.send_keys(roc_year)
        year_input.send_keys(Keys.TAB)
        time.sleep(1)

        print(f"  年度：{year_input.get_attribute('value')}")

        # 月份留空
        Select(selects[1]).select_by_index(0)
        time.sleep(1)

        # 點查詢
        query_btn = next((b for b in buttons if b.text.strip() == "查詢"), None)
        driver.execute_script("arguments[0].click()", query_btn)
        time.sleep(4)

        # 點彈出結果
        try:
            popup = driver.find_element(
                By.XPATH, "//button[contains(text(),'彈出結果')]"
            )
            driver.execute_script("arguments[0].click()", popup)
            time.sleep(3)
            driver.switch_to.window(driver.window_handles[-1])
            time.sleep(3)
        except:
            print(f"  ⚠️ {market_name} 無彈出結果")
            return []

        # 解析表格
        tables = driver.find_elements(By.TAG_NAME, "table")
        if not tables:
            print(f"  ⚠️ {market_name} 找不到表格")
            return []

        rows = tables[0].find_elements(By.TAG_NAME, "tr")
        print(f"  找到 {len(rows)} 行")
        # 印出表頭，確認有哪些欄位
        if rows:
            headers = [th.text.strip() for th in rows[0].find_elements(By.TAG_NAME, "th")]
            if not headers:
                headers = [td.text.strip() for td in rows[0].find_elements(By.TAG_NAME, "td")]
            print(f"  表頭欄位：{headers}")

        for row in rows[2:]:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 4:
                continue
            try:
                stock_id   = cols[0].text.strip()
                stock_name = cols[1].text.strip()
                date_str   = cols[2].text.strip()
                time_str   = cols[3].text.strip()

                if not date_str or "/" not in date_str:
                    continue

                parts     = date_str.split("/")
                conf_date = datetime(
                    int(parts[0]) + 1911,
                    int(parts[1]),
                    int(parts[2])
                )

                links    = row.find_elements(By.TAG_NAME, "a")
                pdf_urls = [
                    a.get_attribute("href") for a in links
                    if a.get_attribute("href") and
                    ".pdf" in a.get_attribute("href").lower()
                ]

                results.append({
                    "market":   market_name,
                    "stock_id": stock_id,
                    "name":     stock_name,
                    "date":     conf_date,
                    "date_str": conf_date.strftime("%Y/%m/%d"),
                    "time":     time_str,
                    "pdf_zh":   pdf_urls[0] if len(pdf_urls) > 0 else "",
                    "pdf_en":   pdf_urls[1] if len(pdf_urls) > 1 else "",
                })
            except Exception:
                continue

    finally:
        driver.quit()  # 每次查完就關掉這個 driver

    return results


def tool_get_upcoming_conferences():
    print("🚀 啟動法說會雷達...\n")

    now      = datetime.now()
    roc_year = str(now.year - 1911)
    today    = now.replace(hour=0, minute=0, second=0, microsecond=0)
    all_results = []

    for market in ["上市", "上櫃"]:
        print(f"🔍 查詢【{market}】...")
        results = query_single_market(market, roc_year)
        all_results.extend(results)
        print(f"  ✅ 取得 {len(results)} 筆\n")
        time.sleep(2)

    # 排序
    all_results.sort(key=lambda x: x["date"])
    upcoming = [r for r in all_results if r["date"] >= today]

    # 輸出
    sep = "=" * 70
    print(f"\n{sep}")
    print("  🔮 即將召開的法說會")
    print(sep)
    for r in upcoming:
        print(
            f"  {r['date_str']}  {r['time']:>5}  "
            f"[{r['market']}]  {r['stock_id']:<6}  {r['name']}"
        )
    print(f"{sep}")
    print(f"  即將召開 {len(upcoming)} 場 / 全年 {len(all_results)} 場\n")

    return upcoming, all_results


def save_to_db(records: list):
    """將法說會時程存進 mops_conference_calendar，upsert by (stock_id, conf_date)"""
    if not records:
        return
    with get_db_client() as db:
        with db.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mops_conference_calendar (
                    stock_id    TEXT,
                    name        TEXT,
                    market      TEXT,
                    conf_date   DATE,
                    conf_time   TEXT,
                    pdf_zh      TEXT,
                    pdf_en      TEXT,
                    fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stock_id, conf_date)
                )
            """))
            conn.execute(text("""
                INSERT INTO mops_conference_calendar
                    (stock_id, name, market, conf_date, conf_time, pdf_zh, pdf_en, fetched_at)
                VALUES
                    (:stock_id, :name, :market, :conf_date, :conf_time, :pdf_zh, :pdf_en, CURRENT_TIMESTAMP)
                ON CONFLICT (stock_id, conf_date) DO UPDATE SET
                    name       = EXCLUDED.name,
                    market     = EXCLUDED.market,
                    conf_time  = EXCLUDED.conf_time,
                    pdf_zh     = EXCLUDED.pdf_zh,
                    pdf_en     = EXCLUDED.pdf_en,
                    fetched_at = CURRENT_TIMESTAMP
            """), [
                {
                    "stock_id":  r["stock_id"],
                    "name":      r["name"],
                    "market":    r["market"],
                    "conf_date": r["date"].strftime("%Y-%m-%d"),
                    "conf_time": r["time"],
                    "pdf_zh":    r["pdf_zh"],
                    "pdf_en":    r["pdf_en"],
                }
                for r in records
            ])
    print(f"  💾 已儲存 {len(records)} 筆法說會時程至 DB")


def run():
    upcoming, all_data = tool_get_upcoming_conferences()
    save_to_db(all_data)
    return upcoming, all_data


if __name__ == '__main__':
    upcoming, all_data = run()