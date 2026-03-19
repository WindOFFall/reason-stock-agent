import re
import time
import requests
import os
import io
import pdfplumber
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from sqlalchemy import text

from database import get_db_client # 引入統一的資料庫工具

# ════════════════════════════════════════════════════
# 初始化與環境變數 (強烈建議從 .env 讀取，保護您的金鑰！)
# ════════════════════════════════════════════════════
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")


if not api_key:
    # 如果真的還沒設定好 .env，這裡暫時保留讓您測試用的備案 (請務必換成新金鑰)
    api_key = '請換成您新申請的API_KEY'
    print("⚠️ 警告：目前使用寫死的 API 金鑰，請盡快改用 .env 設定！")

# ✅ 修正 1：使用新版 google-genai 的標準 Client 寫法
client = genai.Client(api_key=api_key)


# ════════════════════════════════════════════════════
# 靜態設定與關鍵字
# ════════════════════════════════════════════════════
STOCK_NAME_MAP = {
    "2330": "台積電", "2317": "鴻海", "2382": "廣達",
    "3711": "日月光", "2454": "聯發科", "6669": "緯穎",
    "2308": "台達電", "3231": "緯創", "2379": "瑞昱",
}

# 反查：公司名稱 → 代號
NAME_TO_ID = {v: k for k, v in STOCK_NAME_MAP.items()}

DEMAND_KEYWORDS = [
    "demand", "capacity", "guidance", "outlook", "revenue",
    "growth", " AI ", "HBM", "data center", "inference",
    "training", "cloud", "next quarter",
    "需求", "展望", "成長", "營收", "預期",
]
RISK_KEYWORDS = [
    "risk", "uncertainty", "headwind", "export", "China",
    "restriction", "competition", "macro", "tariff",
    "風險", "不確定", "下修", "庫存",
]

# ════════════════════════════════════════════════════
# 工具函數（Tools）
# ════════════════════════════════════════════════════

def tool_get_hot_stocks() -> str:
    """
    Tool 1：從資料庫找今日有訊號的股票
    回傳文字給 Agent 判斷
    """
    output = []

    # 三大法人買超 Top 10
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                SELECT stock_id, total
                FROM tw_institutional_trades
                WHERE date = (SELECT MAX(date) FROM tw_institutional_trades)
                  AND total > 3000000
                ORDER BY total DESC
                LIMIT 10
                """)).fetchall()
            if rows:
                output.append("【三大法人今日大買超】")
                for r in rows:
                    name = STOCK_NAME_MAP.get(r[0], r[0])
                    output.append(f"  {r[0]} {name}：買超 {r[1]:,} 股")
    except Exception as e:
        output.append(f"法人資料查詢失敗：{e}")

    # 新聞熱門股票
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                SELECT title, source
                FROM market_intelligence
                WHERE publish_date >= CURRENT_DATE - INTERVAL '1 day'
                ORDER BY created_at DESC
                """)).fetchall()
        
            mentioned = {}
            for title, source in rows:
                for name, sid in NAME_TO_ID.items():
                    if name in title:
                        if sid not in mentioned:
                            mentioned[sid] = []
                        mentioned[sid].append(title[:50])
            
            if mentioned:
                output.append("\n【新聞熱門股票】")
                # 依提及次數由大到小排序，並只取前 5 名
                sorted_mentioned = sorted(mentioned.items(), key=lambda x: len(x[1]), reverse=True)
                for sid, titles in sorted_mentioned[:5]:
                    name = STOCK_NAME_MAP.get(sid, sid)
                    output.append(f"  {sid} {name}：出現 {len(titles)} 則新聞")
                    output.append(f"    最新：{titles[0]}")
    except Exception as e:
        output.append(f"新聞資料查詢失敗：{e}")

    return "\n".join(output) if output else "今日無明顯訊號"


def tool_get_conference(stock_id: str) -> str:
    """
    Tool 2：抓指定股票的法說會並分析
    """
    company_name = STOCK_NAME_MAP.get(stock_id, stock_id)
    
    # ── Selenium 抓 MOPS ──────────────────────────
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    
    pdf_url = None
    try:
        driver.get("https://mops.twse.com.tw/mops/#/web/t100sb07_1")
        time.sleep(5)
        inputs = driver.find_elements(By.TAG_NAME, "input")
        driver.execute_script("arguments[0].value = arguments[1];", inputs[2], stock_id)
        driver.execute_script("arguments[0].dispatchEvent(new Event('input'));", inputs[2])
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", inputs[2])
        time.sleep(2)
        buttons = driver.find_elements(By.TAG_NAME, "button")
        driver.execute_script("arguments[0].click();", buttons[3])
        time.sleep(4)
        popup = driver.find_element(By.XPATH, "//button[contains(text(),'彈出結果')]")
        driver.execute_script("arguments[0].click();", popup)
        time.sleep(3)
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(3)
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href") or ""
            if href.endswith("E001.pdf"):   # 優先英文版
                pdf_url = href
                break
        if not pdf_url:
            for a in driver.find_elements(By.TAG_NAME, "a"):
                href = a.get_attribute("href") or ""
                if ".pdf" in href.lower():
                    pdf_url = href
                    break
    finally:
        driver.quit()

    if not pdf_url:
        return f"{company_name}（{stock_id}）：找不到法說會 PDF"

    # ── 下載並解析 PDF ────────────────────────────
    try:
        resp = requests.get(pdf_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        text = ""
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        return f"{company_name} PDF 解析失敗：{e}"

    if not text:
        return f"{company_name}：PDF 為圖片型，無法直接解析"

    # ── 關鍵字分析 ────────────────────────────────
    # ✅ 修正 2：優化關鍵字萃取邏輯，避免無效運算
    def extract(keywords, max_hits=5):
        lines = text.split("\n")
        hits = []
        seen = set()
        for line in lines:
            snippet = line.strip()
            # 提早過濾掉過短或重複的句子
            if len(snippet) < 15 or snippet in seen:
                continue
            
            for kw in keywords:
                if kw.lower() in snippet.lower():
                    seen.add(snippet)
                    hits.append(snippet[:150])
                    break # 找到關鍵字就換下一行
            
            if len(hits) >= max_hits:
                break
        return hits

    demand = extract(DEMAND_KEYWORDS)
    risks  = extract(RISK_KEYWORDS)

    # 展望段落
    guidance = []
    for line in text.split("\n"):
        snippet = line.strip()
        if len(snippet) > 15 and any(kw in snippet.lower() for kw in ["guidance", "outlook", "expect", "revenue to be", "預期", "展望"]):
            guidance.append(snippet[:150])

    # 整理成文字回傳給 Agent
    result = [f"【{company_name}（{stock_id}）法說會分析】"]
    result.append(f"來源：{pdf_url}")
    result.append("\n需求訊號：")
    result += [f"  • {d}" for d in demand] or ["  （無）"]
    result.append("\n風險訊號：")
    result += [f"  • {r}" for r in risks] or ["  （無）"]
    result.append("\n展望：")
    result += [f"  • {g}" for g in guidance[:5]] or ["  （無）"]

    return "\n".join(result)


def tool_summarize(stock_id: str, hot_signal: str, conference: str) -> str:
    """
    Tool 3：整合訊號 + 法說會，給出最終判斷
    交由 LLM (Gemini) 進行語意分析與綜合判斷
    """
    company_name = STOCK_NAME_MAP.get(stock_id, stock_id)
    
    prompt = f"""
    你是一位專業的台股資深分析師。請根據以下收集到的市場訊號與法說會內容，
    對【{company_name} ({stock_id})】給出你的專業總結與判斷。
    
    [市場訊號]:
    {hot_signal}
    
    [法說會重點]:
    {conference}
    
    請用 50 字以內的精煉文字總結，並在開頭明確給出【✅ 正面】、【❌ 負面】或【🔶 中性】的結論。
    """
    
    try:
        # ✅ 修正 3：使用新版 SDK 呼叫模型的正確語法
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return f"【{company_name}（{stock_id}）AI 綜合判斷】\n{response.text.strip()}"
    except Exception as e:
        return f"【{company_name}（{stock_id}）】AI 判斷失敗：{e}"


# ════════════════════════════════════════════════════
# Agent 主流程
# ════════════════════════════════════════════════════

def run_agent():
    print(f"\n{'='*60}")
    print(f"  🤖 股票 Agent 啟動")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # Step 1：找今日有訊號的股票
    print("\n[Step 1] 掃描今日市場訊號...")
    hot_signal = tool_get_hot_stocks()
    print(hot_signal)

    # Step 2：從訊號裡抽出股票代號
    candidates = []
    for sid, name in STOCK_NAME_MAP.items():
        if sid in hot_signal or name in hot_signal:
            candidates.append(sid)

    if not candidates:
        print("\n今日無明顯候選股，結束")
        return []

    print(f"\n[Step 2] 候選股：{candidates}")

    # Step 3：對每支候選股抓法說會
    print("\n[Step 3] 抓取法說會資料...")
    final_results = []

    for stock_id in candidates[:5]:  # 最多 5 支
        print(f"\n  處理 {stock_id}...")
        
        conference = tool_get_conference(stock_id)
        print(conference[:200] + "...")
        
        verdict = tool_summarize(stock_id, hot_signal, conference)
        print(f"  → {verdict}")
        
        final_results.append({
            "stock_id":   stock_id,
            "verdict":    verdict,
            "conference": conference,
        })
        time.sleep(2)

    # Step 4：輸出最終報告
    print(f"\n{'='*60}")
    print("  📋 今日最終報告")
    print(f"{'='*60}")
    for r in final_results:
        print(f"\n  {r['verdict']}")

    return final_results


# ════════════════════════════════════════════════════
# 執行
# ════════════════════════════════════════════════════
if __name__ == "__main__":
    run_agent()