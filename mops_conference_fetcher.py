"""
MOPS 法說會爬蟲 + Gemini 結構化解析
pip install selenium pdfplumber requests google-generativeai
"""

import re
import io
import time
import json
import requests
import pdfplumber
from dataclasses import dataclass, field, asdict
from typing import Optional

from google import genai
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ────────────────────────────────────────────
# 設定 Gemini API Key
# ────────────────────────────────────────────

import os
from dotenv import load_dotenv
load_dotenv()
# api_key = os.getenv("GEMINI_API_KEY")


GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")   # ← 替換成你的 key
_genai_client = genai.Client(api_key=GEMINI_API_KEY)


# ────────────────────────────────────────────
# 資料結構
# ────────────────────────────────────────────
@dataclass
class ConferenceRecord:
    stock_id: str
    company_name: str = ""

    # 時間資訊
    fiscal_year: Optional[int] = None       # 會計年度 e.g. 2024
    fiscal_quarter: Optional[int] = None    # 季度 1-4
    event_date: Optional[str] = None        # 法說會日期 YYYY-MM-DD
    report_date: Optional[str] = None       # 公告日期 YYYY-MM-DD

    # 檔案資訊
    filename: str = ""
    url: str = ""
    lang: str = "zh"                        # zh / en

    # 結構化財務內容
    revenue: Optional[str] = None           # 營收
    gross_margin: Optional[str] = None      # 毛利率
    operating_income: Optional[str] = None  # 營業利益
    net_income: Optional[str] = None        # 淨利
    guidance: Optional[str] = None          # 展望/指引
    key_messages: list = field(default_factory=list)   # 重點摘要
    qa_highlights: list = field(default_factory=list)  # Q&A 重點

    # 原始備用
    raw_text: str = ""


# ────────────────────────────────────────────
# Selenium 爬蟲：抓 MOPS 法說會附件
# ────────────────────────────────────────────
def get_mops_conference(stock_id: str = "2330") -> list[dict]:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)
    results = []

    try:
        driver.get("https://mops.twse.com.tw/mops/#/web/t100sb07_1")
        time.sleep(5)

        # 填入股票代號
        inputs = driver.find_elements(By.TAG_NAME, "input")
        driver.execute_script("arguments[0].value = arguments[1];", inputs[2], stock_id)
        driver.execute_script("arguments[0].dispatchEvent(new Event('input'));",  inputs[2])
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", inputs[2])
        time.sleep(2)

        # 點查詢
        buttons = driver.find_elements(By.TAG_NAME, "button")
        driver.execute_script("arguments[0].click();", buttons[3])
        time.sleep(4)

        # 點彈出結果
        popup_btn = wait.until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(),'彈出結果')]"))
        )
        driver.execute_script("arguments[0].click();", popup_btn)
        time.sleep(3)

        # 切換到新視窗
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(3)

        # 抓頁面摘要文字（含日期、公司名等）
        body_text = driver.find_element(By.TAG_NAME, "body").text

        # 抓所有 PDF 連結
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href") or ""
            text = a.text.strip()
            if ".pdf" in href.lower():
                # 語言判斷：優先用 URL pattern，fallback 用檔名
                lang = detect_lang(href, text)
                results.append({
                    "stock_id": stock_id,
                    "filename": text,
                    "url":      href,
                    "lang":     lang,
                    "summary":  body_text,
                })

    except Exception as e:
        print(f"❌ Selenium 爬取失敗：{e}")
    finally:
        driver.quit()

    return results


def detect_lang(href: str, filename: str) -> str:
    """根據 URL 或檔名判斷語言，比只看結尾更穩"""
    combined = (href + filename).lower()
    if any(k in combined for k in ["_e", "english", "eng", "e001", "_en"]):
        return "en"
    return "zh"


# ────────────────────────────────────────────
# 時間資訊：從頁面文字 regex 粗取
# ────────────────────────────────────────────
# def extract_date_from_text(text: str) -> dict:
#     result = {}

#     # 民國年 → 西元年（e.g. 113年 → 2024）
#     roc = re.search(r'(\d{2,3})\s*年', text)
#     if roc:
#         y = int(roc.group(1))
#         result["fiscal_year"] = y + 1911 if y < 1911 else y

#     # 季度（中文或英文）
#     q_match = re.search(r'第\s*([一二三四1-4])\s*季|Q\s*([1-4])', text)
#     if q_match:
#         zh_map = {"一": 1, "二": 2, "三": 3, "四": 4}
#         raw_q = q_match.group(1) or q_match.group(2)
#         result["fiscal_quarter"] = zh_map.get(raw_q, int(raw_q))

#     # 日期字串（支援民國 / 西元，/ 或 - 分隔）
#     date_match = re.search(r'(\d{3,4})[/\-](\d{1,2})[/\-](\d{1,2})', text)
#     if date_match:
#         y, m, d = date_match.groups()
#         year = int(y) + 1911 if int(y) < 1911 else int(y)
#         result["event_date"] = f"{year}-{int(m):02d}-{int(d):02d}"

#     return result


def extract_date_from_text(text: str) -> dict:
    result = {}

    # 民國年 → 西元年
    roc = re.search(r'(\d{2,3})\s*年', text)
    if roc:
        y = int(roc.group(1))
        result["fiscal_year"] = y + 1911 if y < 1911 else y

    # 季度（中文或英文）
    q_match = re.search(r'第\s*([一二三四1-4])\s*季|Q\s*([1-4])', text)
    if q_match:
        zh_map = {"一": 1, "二": 2, "三": 3, "四": 4}
        raw_q = q_match.group(1) or q_match.group(2)

        # ✅ 改成 if/else，不讓 Python 提前 evaluate int(raw_q)
        if raw_q in zh_map:
            result["fiscal_quarter"] = zh_map[raw_q]
        elif raw_q.isdigit():
            result["fiscal_quarter"] = int(raw_q)
        # 兩者都不符合就跳過，不 crash

    # 日期字串（支援民國 / 西元）
    date_match = re.search(r'(\d{3,4})[/\-](\d{1,2})[/\-](\d{1,2})', text)
    if date_match:
        y, m, d = date_match.groups()
        year = int(y) + 1911 if int(y) < 1911 else int(y)
        result["event_date"] = f"{year}-{int(m):02d}-{int(d):02d}"

    return result



# ────────────────────────────────────────────
# PDF 下載與純文字轉換
# ────────────────────────────────────────────
def download_and_parse_pdf(pdf_url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(pdf_url, headers=headers, timeout=20)
    resp.raise_for_status()

    text = ""
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


# ────────────────────────────────────────────
# Gemini API：PDF 文字 → 結構化 JSON
# ────────────────────────────────────────────
def extract_structured_content(pdf_text: str, lang: str = "zh") -> dict:
    lang_hint = "英文" if lang == "en" else "繁體中文"

    prompt = f"""
以下是一份法說會 PDF 的文字（語言：{lang_hint}）。
請仔細閱讀後，提取以下欄位，並以合法 JSON 格式回傳，不要有任何額外說明或 markdown 符號。

欄位說明：
- fiscal_year: 會計年度，整數，例如 2024
- fiscal_quarter: 季度，整數 1~4，找不到填 null
- event_date: 法說會日期，格式 "YYYY-MM-DD"，找不到填 null
- revenue: 本期營收（含單位，例如 "NT$625.5B"），找不到填 null
- gross_margin: 毛利率（例如 "53.1%"），找不到填 null
- operating_income: 營業利益（含單位），找不到填 null
- net_income: 淨利（含單位），找不到填 null
- guidance: 下一季或下一年度展望摘要（繁體中文，100字以內），找不到填 null
- key_messages: 本次法說會 3~5 個核心重點，字串陣列
- qa_highlights: Q&A 環節中最重要的 2~3 個問答摘要，字串陣列；沒有 Q&A 則回傳空陣列

--- PDF 內容（前 6000 字）---
{pdf_text[:6000]}
"""

    try:
        response = _genai_client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=prompt,
        )
        raw = response.text.strip()

        # 去除可能的 ```json 包裝
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        return json.loads(raw)

    except json.JSONDecodeError as e:
        print(f"   ⚠️ JSON 解析失敗：{e}\n   原始回應：{response.text[:200]}")
        return {}
    except Exception as e:
        print(f"   ⚠️ Gemini API 呼叫失敗：{e}")
        return {}


# ────────────────────────────────────────────
# 主流程
# ────────────────────────────────────────────
def run(stock_id: str = "2330") -> list[ConferenceRecord]:
    print(f"🔍 抓取 {stock_id} 法說會資料...\n")
    raw_list = get_mops_conference(stock_id)

    if not raw_list:
        print("❌ 找不到法說會資料")
        return []

    print(f"✅ 找到 {len(raw_list)} 個附件\n")
    records = []

    for item in raw_list:
        print(f"📄 {item['filename']} ({'英文' if item['lang'] == 'en' else '中文'})")
        print(f"   URL：{item['url']}")

        rec = ConferenceRecord(
            stock_id=item["stock_id"],
            filename=item["filename"],
            url=item["url"],
            lang=item["lang"],
        )

        # Step 1：從頁面摘要粗取時間
        time_info = extract_date_from_text(item["summary"])
        rec.fiscal_year     = time_info.get("fiscal_year")
        rec.fiscal_quarter  = time_info.get("fiscal_quarter")
        rec.event_date      = time_info.get("event_date")

        # Step 2：下載 PDF
        print("   ⬇️  下載 PDF...")
        try:
            pdf_text = download_and_parse_pdf(item["url"])
            rec.raw_text = pdf_text
            print(f"   📝 取得 {len(pdf_text)} 字")
        except Exception as e:
            print(f"   ❌ PDF 下載失敗：{e}\n")
            records.append(rec)
            continue

        # Step 3：Gemini 結構化解析
        print("   🤖 Gemini 解析中...")
        structured = extract_structured_content(pdf_text, item["lang"])

        if structured:
            # 合併結果（Gemini 解析的時間優先覆蓋 regex 結果）
            for key, val in structured.items():
                if val is not None and hasattr(rec, key):
                    setattr(rec, key, val)
            print("   ✅ 結構化完成")
        else:
            print("   ⚠️ 結構化失敗，保留原始文字")

        records.append(rec)
        print()

    # ── 輸出結果 ──────────────────────────────
    print("=" * 60)
    print("📊 結構化結果總覽")
    print("=" * 60)
    for rec in records:
        print(f"\n【{rec.stock_id}】{rec.filename}")
        print(f"  年度／季度  : {rec.fiscal_year} Q{rec.fiscal_quarter}")
        print(f"  法說會日期  : {rec.event_date}")
        print(f"  營收        : {rec.revenue}")
        print(f"  毛利率      : {rec.gross_margin}")
        print(f"  營業利益    : {rec.operating_income}")
        print(f"  淨利        : {rec.net_income}")
        print(f"  展望        : {rec.guidance}")
        if rec.key_messages:
            print("  核心重點    :")
            for msg in rec.key_messages:
                print(f"    • {msg}")
        if rec.qa_highlights:
            print("  Q&A 重點    :")
            for qa in rec.qa_highlights:
                print(f"    • {qa}")

    # 也可以轉成 list of dict 方便後續存 DB 或 JSON
    return records


def records_to_json(records: list[ConferenceRecord], filepath: str = "conferences.json"):
    """把結果存成 JSON 檔"""
    data = [asdict(r) for r in records]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n💾 已儲存至 {filepath}")


# ────────────────────────────────────────────
# 執行入口
# ────────────────────────────────────────────
if __name__ == "__main__":
    records = run("2337")           # 台積電
    if records:
        records_to_json(records)    # 另存成 JSON（可選）