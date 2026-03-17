import yfinance as yf
import pandas as pd
import time
import random
import requests
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import feedparser
import hashlib
from bs4 import BeautifulSoup

import sys, os
# 將上一層目錄加入系統路徑，解決 ModuleNotFoundError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_client

# ==========================================
# 0. 確保 BaseCrawler 絕對有 run_safe
# ==========================================
class BaseCrawler(ABC):
    def __init__(self, name): 
        self.name = name

    def save_system_log(self, status, message=""):
        """寫入系統日誌"""
        try:
            with get_db_client() as db:
                db.execute_raw("""
                    CREATE TABLE IF NOT EXISTS system_execution_logs (
                        id SERIAL PRIMARY KEY, crawler_name VARCHAR(50), execution_time TIMESTAMP, status VARCHAR(20), message TEXT
                    );
                """)
                msg = str(message).replace("'", "''")
                db.execute_raw(f"INSERT INTO system_execution_logs (crawler_name, execution_time, status, message) VALUES ('{self.name}', NOW(), '{status}', '{msg}');")
                print(f"📝 [{self.name}] 系統日誌: {status}")
        except Exception as e: print(f"❌ Log Error: {e}")

    @abstractmethod
    def run(self, **kwargs): 
        pass
    
    # 🔥 這是你原本缺少的關鍵函式
    def run_safe(self, **kwargs):
        print(f"\n🚀 [{self.name}] 啟動中...")
        try: 
            self.run(**kwargs)
            print(f"✅ [{self.name}] 完成")
        except Exception as e: 
            print(f"❌ Error: {e}")
            self.save_system_log("CRITICAL_ERROR", str(e))

# ==========================================
# 1. 台股爬蟲 (重新繼承 BaseCrawler)
# ==========================================
class TWStockCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("TW_Stock")
        self.column_map = {
            "證券代號": "stock_id", "證券名稱": "stock_name",
            "開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close",
            "成交股數": "volume", "成交金額": "turnover", "成交筆數": "transactions",
            "market": "market", "date": "date"
        }

    def _is_clean_stock(self, code):
        return len(str(code).strip()) <= 5

    def _clean_data(self, df):
        exclude = ['證券代號', '證券名稱', 'market', 'date']
        for col in df.columns:
            if col not in exclude:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False).replace(['--', '---'], '')
                df[col] = pd.to_numeric(df[col], errors='coerce')
        available_cols = [c for c in self.column_map.keys() if c in df.columns]
        df = df[available_cols].copy()
        df.rename(columns=self.column_map, inplace=True)
        return df

    def _init_tables(self, db):
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS tw_daily_prices (
                date DATE, stock_id VARCHAR(10), stock_name VARCHAR(50),
                market VARCHAR(10), open NUMERIC, high NUMERIC, low NUMERIC,
                close NUMERIC, volume BIGINT, turnover BIGINT, transactions INTEGER,
                PRIMARY KEY (date, stock_id)
            );
        """)
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS tw_crawler_logs (
                date DATE PRIMARY KEY, status VARCHAR(20), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _get_twse_daily(self, date_str):
        url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
        try:
            res = requests.get(url, timeout=30)
            if res.status_code != 200: return pd.DataFrame()
            json_data = res.json()
            if json_data.get('stat') != 'OK': return pd.DataFrame()
            raw_data, fields = [], []
            if 'tables' in json_data:
                for table in json_data['tables']:
                    if '每日收盤行情' in table.get('title', ''):
                        raw_data = table.get('data', []); fields = table.get('fields', []); break
            if not raw_data: return pd.DataFrame()
            df = pd.DataFrame(raw_data, columns=fields)
            if '證券代號' in df.columns: df = df[df['證券代號'].apply(self._is_clean_stock)]
            return df
        except: return pd.DataFrame()

    def _get_tpex_daily(self, date_str):
        try:
            roc_year = int(date_str[:4]) - 1911
            roc_date = f"{roc_year}/{date_str[4:6]}/{date_str[6:8]}"
            url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={roc_date}&o=json"
            res = requests.get(url, timeout=30)
            if res.status_code != 200: return pd.DataFrame()
            json_data = res.json()
            raw_data, fields = [], []
            if 'tables' in json_data:
                 for table in json_data['tables']:
                    if '上櫃股票行情' in table.get('title', ''):
                        raw_data = table.get('data', []); fields = table.get('fields', []); break
            elif 'aaData' in json_data: raw_data = json_data.get('aaData', [])
            if not raw_data: return pd.DataFrame()
            if fields: df = pd.DataFrame(raw_data, columns=fields)
            else:
                df = pd.DataFrame(raw_data)
                if df.shape[1] >= 15: df.columns = ["證券代號", "證券名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價", "成交股數", "成交金額", "成交筆數"] + list(df.columns[11:])
            rename_map = {'代號': '證券代號', '名稱': '證券名稱', '收盤': '收盤價', '開盤': '開盤價', '最高': '最高價', '最低': '最低價'}
            df.rename(columns=rename_map, inplace=True)
            if '證券代號' in df.columns: df = df[df['證券代號'].apply(self._is_clean_stock)]
            return df
        except: return pd.DataFrame()

    def run(self, mode="daily", days_back=5):
        if mode == "backfill": days_back = 365
        stop_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        start_date = datetime.now() - timedelta(days=1)
        target_date = start_date
        limit_date = datetime.strptime(stop_date, "%Y-%m-%d")

        print(f"🚀 [台股] 模式: {mode}, 檢查區間: {limit_date.date()} -> {target_date.date()}")

        with get_db_client() as db:
            self._init_tables(db)
            existing_df = db.fetch("tw_crawler_logs", cols=["date"])
            existing_dates = set(existing_df['date'].astype(str).tolist()) if not existing_df.empty else set()

            while target_date >= limit_date:
                date_iso = target_date.strftime("%Y-%m-%d")
                date_compact = target_date.strftime("%Y%m%d")

                if mode == "daily" and date_iso in existing_dates:
                    target_date -= timedelta(days=1); continue
                if mode == "backfill" and date_iso in existing_dates:
                    target_date -= timedelta(days=1); continue
                if target_date.weekday() >= 5: 
                    target_date -= timedelta(days=1); continue

                print(f"🕷️ [台股] 抓取: {date_iso} ...")
                try:
                    df_twse = self._get_twse_daily(date_compact)
                    time.sleep(random.uniform(2, 3))
                    df_tpex = self._get_tpex_daily(date_compact)
                    
                    frames = []
                    if not df_twse.empty: df_twse['market'] = 'TWSE'; frames.append(df_twse)
                    if not df_tpex.empty: df_tpex['market'] = 'TPEx'; frames.append(df_tpex)
                    
                    log_status = 'EMPTY'
                    if frames:
                        final_df = pd.concat(frames, ignore_index=True)
                        final_df['date'] = date_iso
                        if df_twse.empty and not df_tpex.empty: print(f"   ⚠️ 異常：僅有上櫃資料，跳過。")
                        else:
                            final_df = self._clean_data(final_df)
                            final_df.drop_duplicates(subset=['date', 'stock_id'], keep='last', inplace=True)
                            count = db.upsert_from_df(table="tw_daily_prices", df=final_df, on=["date", "stock_id"])
                            print(f"   ✅ 寫入: {count} 筆")
                            log_status = 'DONE'
                    else: print(f"   💤 休市")

                    log_df = pd.DataFrame([{ "date": date_iso, "status": log_status }])
                    db.upsert_from_df("tw_crawler_logs", log_df, on=["date"])
                    existing_dates.add(date_iso)
                    time.sleep(random.uniform(5, 8))
                except Exception as e:
                    print(f"   ❌ 錯誤: {e}"); time.sleep(5)
                
                target_date -= timedelta(days=1)
        self.save_system_log("SUCCESS", f"台股執行完畢 ({mode})")

# ==========================================
# 2. 美股爬蟲 (重新繼承 BaseCrawler)
# ==========================================
class USStockCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("US_Stock")
        self.column_map = {
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume"
        }
        self.target_tickers = [
            "AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "AMD", "INTC", "TSM",
            "^GSPC", "^DJI", "^IXIC", "^SOX", "^TWII" 
        ]

    def _init_tables(self, db):
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS us_daily_prices (
                date DATE, ticker VARCHAR(10),
                open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,
                adj_close NUMERIC, volume BIGINT,
                PRIMARY KEY (date, ticker)
            );
        """)
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS us_update_logs (
                ticker VARCHAR(10) PRIMARY KEY, last_updated_date DATE, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _clean_data(self, df, ticker):
        df = df.reset_index()
        if isinstance(df.columns, pd.MultiIndex): df.columns = [col[0] for col in df.columns]
        available = [c for c in self.column_map.keys() if c in df.columns]
        df = df[available].copy()
        df.rename(columns=self.column_map, inplace=True)
        df['ticker'] = ticker
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df

    def run(self, mode="daily", years_back=1):
        print(f"🚀 [美股] 模式: {mode}...")
        with get_db_client() as db:
            self._init_tables(db)
            log_df = db.fetch("us_update_logs")
            last_update_map = dict(zip(log_df['ticker'], log_df['last_updated_date'])) if not log_df.empty else {}

            for ticker in self.target_tickers:
                if mode == "daily":
                    last_date = last_update_map.get(ticker)
                    if last_date: start_date = last_date + timedelta(days=1)
                    else: start_date = datetime.now().date() - timedelta(days=30)
                else:
                    print(f"   [回補] 強制重抓 {ticker} 過去 {years_back} 年...")
                    start_date = datetime.now().date() - timedelta(days=365*years_back)

                if start_date > datetime.now().date():
                    continue

                try:
                    print(f"   🇺🇸 下載 {ticker}: {start_date} ~ Today")
                    df = yf.download(ticker, start=start_date, progress=False, auto_adjust=False)
                    if df.empty: print("      ⚠️ 無資料"); continue

                    final_df = self._clean_data(df, ticker)
                    final_df.drop_duplicates(subset=['date', 'ticker'], keep='last', inplace=True)
                    count = db.upsert_from_df(table="us_daily_prices", df=final_df, on=["date", "ticker"])
                    print(f"      ✅ 寫入: {count} 筆")

                    latest_date = final_df['date'].max()
                    current_log_date = last_update_map.get(ticker)
                    if current_log_date is None or latest_date > current_log_date:
                        log_data = pd.DataFrame([{ "ticker": ticker, "last_updated_date": latest_date }])
                        db.upsert_from_df("us_update_logs", log_data, on=["ticker"])
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    print(f"   ❌ 錯誤 {ticker}: {e}"); time.sleep(2)
        self.save_system_log("SUCCESS", f"美股執行完畢 ({mode})")

import requests
import feedparser
import hashlib
import time
import random
import pandas as pd
# import yfinance as yf # 這邊新聞爬蟲用不到 yfinance，先註解掉保持乾淨
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from database import get_db_client  # 假設你的 database.py 在同目錄

# ==========================================
# 1. 基礎設定與 BaseCrawler
# ==========================================
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/"
}

class BaseCrawler(ABC):
    def __init__(self, name): self.name = name
    def save_system_log(self, status, message=""):
        try:
            with get_db_client() as db:
                db.execute_raw("""
                    CREATE TABLE IF NOT EXISTS system_execution_logs (
                        id SERIAL PRIMARY KEY, crawler_name VARCHAR(50), execution_time TIMESTAMP, status VARCHAR(20), message TEXT
                    );
                """)
                msg = str(message).replace("'", "''")
                db.execute_raw(f"INSERT INTO system_execution_logs (crawler_name, execution_time, status, message) VALUES ('{self.name}', NOW(), '{status}', '{msg}');")
                print(f"📝 [{self.name}] 系統日誌: {status}")
        except Exception as e: print(f"❌ Log Error: {e}")

    @abstractmethod
    def run(self, **kwargs): pass
    
    def run_safe(self, **kwargs):
        print(f"\n🚀 [{self.name}] 啟動中...")
        try: self.run(**kwargs); print(f"✅ [{self.name}] 完成")
        except Exception as e: 
            print(f"❌ Error: {e}")
            self.save_system_log("CRITICAL_ERROR", str(e))

# ==========================================
# 2. 新聞爬蟲父類別 (含雙模式邏輯 - 已修正回補無限迴圈問題)
# ==========================================
class NewsBase(BaseCrawler):
    def _init_news_table(self, db):
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS market_intelligence (
                id SERIAL PRIMARY KEY, url_hash VARCHAR(64) UNIQUE NOT NULL, publish_date TIMESTAMP,
                title TEXT, url TEXT, source VARCHAR(50), content TEXT, summary TEXT, sentiment_score FLOAT,
                process_status VARCHAR(20) DEFAULT 'PENDING', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db.execute_raw("CREATE INDEX IF NOT EXISTS idx_publish_date ON market_intelligence (publish_date);")

    def _save_to_db(self, news_list, source_name):
        if not news_list: return
        df = pd.DataFrame(news_list)
        df.drop_duplicates(subset=['url_hash'], inplace=True)
        with get_db_client() as db:
            self._init_news_table(db)
            count = db.upsert_from_df("market_intelligence", df, on=["url_hash"])
            if count > 0: print(f"      💾 [{source_name}] 新增: {count} 筆")

    # 🔥 關鍵修正：智慧範圍判斷
    def _get_crawl_range(self, source_name, mode, max_history_days=365):
        """
        計算爬蟲的「開始時間」與「結束時間(底線)」
        回傳: (current_end, limit_dt)
        如果回傳 (None, None) 代表不用跑
        """
        today = datetime.now()
        # 設定絕對底線：不管資料庫多舊，我們只關心過去 365 天 (可調整)
        absolute_floor_date = today - timedelta(days=max_history_days)

        if mode == "daily":
            # Daily 模式：只抓最近 3 天，確保不漏掉週末
            return today, today - timedelta(days=3)
        
        # Backfill 模式：檢查資料庫缺哪一段
        try:
            with get_db_client() as db:
                self._init_news_table(db)
                # 找出目前該來源「最舊」的一筆資料
                df = db.fetch("market_intelligence", cols="MIN(publish_date) as min_date", where={"source": source_name})
                
                if not df.empty and df.iloc[0]['min_date'] is not None:
                    db_min_date = pd.to_datetime(df.iloc[0]['min_date'])
                    
                    # 邏輯判斷：
                    # 如果 DB 裡最舊的資料，已經比「絕對底線」還舊 (例如 DB 有 2024年，底線是 2025年)
                    # 代表資料已經滿出來了，不需要再跑
                    if db_min_date <= absolute_floor_date:
                        print(f"      ✅ [{source_name}] 歷史資料已充足 (最舊: {db_min_date.date()})，無需回補。")
                        return None, None 
                    
                    # 否則，從 DB 最舊的那天開始，往回抓到底線
                    print(f"      🧐 [{source_name} 補漏] 從 {db_min_date.date()} 往回補至 {absolute_floor_date.date()}...")
                    return db_min_date, absolute_floor_date

        except Exception as e:
            print(f"      ⚠️ 無法讀取資料庫日期，預設抓取最近 {max_history_days} 天: {e}")

        # 如果資料庫是空的，就從今天抓到一年前
        return today, absolute_floor_date

# ==========================================
# 3. 各大新聞爬蟲
# ==========================================
class AnueCrawler(NewsBase):
    def __init__(self): super().__init__("Anue_News")
    
    def run(self, days_back=365, mode="daily"):
        # 🔥 修改點：使用新的範圍邏輯，傳入 days_back 作為最大歷史限制
        current_end, limit_dt = self._get_crawl_range("Anue", mode, max_history_days=days_back)
        
        if current_end is None: return # 資料充足，直接結束
        
        print(f"   [鉅亨網] 啟動範圍: {current_end.date()} -> {limit_dt.date()}")
        api_url = "https://api.cnyes.com/media/api/v1/newslist/category/tw_stock"
        
        while current_end > limit_dt:
            current_start = current_end - timedelta(days=30)
            if current_start < limit_dt: current_start = limit_dt
            
            print(f"   📅 區間: {current_start.date()} ~ {current_end.date()}")
            params = {"startAt": int(current_start.timestamp()), "endAt": int(current_end.timestamp()), "limit": 30}
            try:
                page = 1
                while True:
                    params["page"] = page
                    res = requests.get(api_url, headers=COMMON_HEADERS, params=params, timeout=10)
                    items = res.json().get("items", {}).get("data", [])
                    if not items: break
                    news_list = []
                    for item in items:
                        if item.get("isAd"): continue
                        link = f"https://news.cnyes.com/news/id/{item.get('newsId')}"
                        news_list.append({
                            "url_hash": hashlib.sha256(link.encode()).hexdigest(),
                            "publish_date": datetime.fromtimestamp(item.get("publishAt", 0)),
                            "title": item.get("title"), "url": link, "source": "Anue"
                        })
                    self._save_to_db(news_list, "Anue")
                    page += 1
                    if page > 20: break
                    time.sleep(0.5)
            except Exception as e: print(f"   ❌ Anue Error: {e}")
            
            current_end = current_start
            time.sleep(1)

class GoogleNewsCrawler(NewsBase):
    def __init__(self): super().__init__("Google_News")
    
    def run(self, keyword="台股", days_back=365, mode="daily"):
        # 🔥 修改點：Google 也套用同樣的防呆邏輯
        # 注意：DB 裡的 source 是 "GoogleNews"
        current_end, limit_dt = self._get_crawl_range("GoogleNews", mode, max_history_days=days_back)
        
        if current_end is None: return 
        
        print(f"   [Google] 啟動範圍: {current_end.date()} -> {limit_dt.date()} | 關鍵字: {keyword}")
        
        while current_end > limit_dt:
            current_start = current_end - timedelta(days=7) # Google 建議一週一週抓
            if current_start < limit_dt: current_start = limit_dt
            
            q = f"{keyword} after:{current_start.strftime('%Y-%m-%d')} before:{current_end.strftime('%Y-%m-%d')}"
            rss_url = f"https://news.google.com/rss/search?q={q}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
            print(f"   🔍 搜尋: {current_start.date()} ~ {current_end.date()}")
            
            try:
                res = requests.get(rss_url, headers=COMMON_HEADERS, timeout=10)
                feed = feedparser.parse(res.content)
                news_list = []
                for entry in feed.entries:
                    news_list.append({
                        "url_hash": hashlib.sha256(entry.link.encode()).hexdigest(),
                        "publish_date": datetime(*entry.published_parsed[:6]),
                        "title": entry.title, "url": entry.link, "source": "GoogleNews"
                    })
                if news_list: self._save_to_db(news_list, "Google")
                else: print("      (無資料)")
            except Exception as e: print(f"   ❌ Google Error: {e}")
            
            current_end = current_start
            time.sleep(random.uniform(2, 4))

class PTTCrawler(NewsBase):
    def __init__(self): super().__init__("PTT_Stock")
    # PTT 維持原樣，因為它是用翻頁的，較難精準控制日期區間
    # 但我們可以確保 days_back 不會無限大
    def run(self, days_back=1, max_pages=5000, mode="daily"):
        # 如果是 backfill 且 days_back 太大，強制限制在 365 天
        if mode == "backfill" and days_back > 365:
            days_back = 365
            
        print(f"   [PTT] 模式: {mode}, 抓取過去 {days_back} 天")
        url = "https://www.ptt.cc/bbs/Stock/index.html"
        cookies = {"over18": "1"}
        limit_dt = datetime.now() - timedelta(days=days_back)
        pages_crawled = 0
        
        while pages_crawled < max_pages:
            try:
                res = requests.get(url, headers=COMMON_HEADERS, cookies=cookies, timeout=10)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, "html.parser")
                articles = soup.select("div.r-ent")
                news_list = []
                min_date = datetime.now()
                
                for art in articles:
                    title_div = art.select_one("div.title a")
                    if not title_div: continue
                    title = title_div.text.strip()
                    if not any(k in title for k in ["[新聞]", "[標的]", "[情報]", "盤中", "大盤"]): continue
                    
                    date_str = art.select_one("div.date").text.strip()
                    try:
                        m, d = map(int, date_str.split('/'))
                        pub_date = datetime(datetime.now().year, m, d)
                        if datetime.now().month==1 and m==12: pub_date = pub_date.replace(year=datetime.now().year-1)
                        if pub_date > datetime.now() + timedelta(days=1): pub_date = pub_date.replace(year=datetime.now().year-1)
                        min_date = min(min_date, pub_date)
                    except: pub_date = datetime.now()
                    
                    if pub_date >= limit_dt:
                        link = "https://www.ptt.cc" + title_div['href']
                        news_list.append({
                            "url_hash": hashlib.sha256(link.encode()).hexdigest(),
                            "publish_date": pub_date, "title": title, "url": link, "source": "PTT_Stock"
                        })
                
                self._save_to_db(news_list, "PTT")
                
                # 如果這頁最新的文章都比 limit_dt 舊了，就不用再翻下一頁了
                if min_date < limit_dt: 
                    print(f"      ✅ PTT 已抓取至限制日期 {limit_dt.date()}，停止翻頁。")
                    break
                    
                prev = soup.select_one("div.btn-group-paging a:nth-child(2)")
                if prev and "上頁" in prev.text:
                    url = "https://www.ptt.cc" + prev["href"]
                    pages_crawled += 1
                    if pages_crawled % 10 == 0: print(f"      🔄 已翻 {pages_crawled} 頁...")
                    time.sleep(random.uniform(1.5, 3))
                else: break
            except: break