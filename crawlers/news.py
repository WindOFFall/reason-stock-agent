# 檔案: crawlers/news.py
import requests
import feedparser
import hashlib
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from database import get_db_client
from .base import BaseCrawler

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/"
}

# ==========================================
# 新聞爬蟲父類別 (NewsBase)
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

    def _get_crawl_range(self, source_name, mode, max_history_days=365):
        today = datetime.now()
        absolute_floor_date = today - timedelta(days=max_history_days)

        if mode == "daily":
            # 強制只抓最近 3 天
            return today, today - timedelta(days=3)
        
        try:
            with get_db_client() as db:
                self._init_news_table(db)
                sql = f"SELECT MIN(publish_date) as min_date, MAX(publish_date) as max_date FROM market_intelligence WHERE source = '{source_name}'"
                df = pd.read_sql(sql, db.engine)
                
                if not df.empty and df.iloc[0]['min_date'] is not None:
                    db_min_date = pd.to_datetime(df.iloc[0]['min_date'])
                    db_max_date = pd.to_datetime(df.iloc[0]['max_date'])
                    
                    # 檢查近期缺口
                    gap_days = (today.date() - db_max_date.date()).days
                    if gap_days >= 1:
                        print(f"      🚨 [{source_name}] 偵測到近期缺口！上次更新是 {gap_days} 天前 ({db_max_date.date()})。")
                        print(f"      🚀 優先啟動「近期補漏」模式: {today.date()} -> {db_max_date.date()}")
                        return today, db_max_date

                    # 檢查歷史缺口
                    if db_min_date.date() > absolute_floor_date.date():
                        print(f"      🧐 [{source_name}] 近期資料完整，轉為「歷史補漏」模式: {db_min_date.date()} -> {absolute_floor_date.date()}")
                        return db_min_date, absolute_floor_date
                    
                    print(f"      ✅ [{source_name}] 資料完美連續 (涵蓋 {db_max_date.date()} ~ {db_min_date.date()})，無需執行。")
                    return None, None
        except Exception as e:
            print(f"      ⚠️ 資料庫讀取異常 (可能是空表)，執行完整初始化: {e}")

        return today, absolute_floor_date

# ==========================================
# 具體新聞爬蟲實作
# ==========================================
class AnueCrawler(NewsBase):
    def __init__(self): super().__init__("Anue_News")
    
    def run(self, days_back=365, mode="daily"):
        current_end, limit_dt = self._get_crawl_range("Anue", mode, max_history_days=days_back)
        
        if current_end is None: return 
        
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
        current_end, limit_dt = self._get_crawl_range("GoogleNews", mode, max_history_days=days_back)
        
        if current_end is None: return 
        
        print(f"   [Google] 啟動範圍: {current_end.date()} -> {limit_dt.date()} | 關鍵字: {keyword}")
        
        while current_end > limit_dt:
            current_start = current_end - timedelta(days=7)
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
    
    def run(self, days_back=60, max_pages=5000, mode="daily"):
        current_end, limit_dt = self._get_crawl_range("PTT_Stock", mode, max_history_days=days_back)
        
        if current_end is None: return 
        
        print(f"   [PTT] 啟動智慧補漏，目標推至: {limit_dt.date()}")
        
        url = "https://www.ptt.cc/bbs/Stock/index.html"
        cookies = {"over18": "1"}
        
        pages_crawled = 0
        
        while pages_crawled < max_pages:
            try:
                res = requests.get(url, headers=COMMON_HEADERS, cookies=cookies, timeout=10)
                if res.status_code != 200: break
                soup = BeautifulSoup(res.text, "html.parser")
                articles = soup.select("div.r-ent")
                news_list = []
                min_date_in_page = datetime.now()
                
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
                        
                        min_date_in_page = min(min_date_in_page, pub_date)
                    except: pub_date = datetime.now()
                    
                    if pub_date >= limit_dt:
                        link = "https://www.ptt.cc" + title_div['href']
                        news_list.append({
                            "url_hash": hashlib.sha256(link.encode()).hexdigest(),
                            "publish_date": pub_date, "title": title, "url": link, "source": "PTT_Stock"
                        })
                
                self._save_to_db(news_list, "PTT")
                
                if min_date_in_page < limit_dt: 
                    print(f"      ✅ PTT 已抓取至目標日期 {limit_dt.date()}，任務完成，停止翻頁。")
                    break
                    
                prev = soup.select_one("div.btn-group-paging a:nth-child(2)")
                if prev and "上頁" in prev.text:
                    url = "https://www.ptt.cc" + prev["href"]
                    pages_crawled += 1
                    if pages_crawled % 10 == 0: print(f"      🔄 已翻 {pages_crawled} 頁...")
                    time.sleep(random.uniform(1.5, 3))
                else: break
            except Exception as e: 
                print(f"   ❌ PTT Error: {e}")
                break