# 檔案: crawlers/price.py
import yfinance as yf
import pandas as pd
import time
import random
import requests
from datetime import datetime, timedelta
from database import get_db_client
from .base import BaseCrawler  # 繼承剛剛的 base.py

# ==========================================
# 1. 台股爬蟲
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
# 2. 美股爬蟲
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