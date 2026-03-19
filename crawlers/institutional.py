import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from database import get_db_client
from .base import BaseCrawler

class TWInstitutionalCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("TW_Institutional")

    def _init_tables(self, db):
        # 建立法人買賣超資料表 (記錄每日每檔股票的三大法人動向)
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS tw_institutional_trades (
                date DATE,
                stock_id VARCHAR(10),
                foreign_investor BIGINT,   -- 外資買賣超
                investment_trust BIGINT,   -- 投信買賣超
                dealer BIGINT,             -- 自營商買賣超
                total BIGINT,              -- 三大法人合計
                PRIMARY KEY (date, stock_id)
            );
        """)
        # 建立更新紀錄表
        db.execute_raw("""
            CREATE TABLE IF NOT EXISTS tw_institutional_logs (
                date DATE PRIMARY KEY, status VARCHAR(20), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _clean_number(self, series):
        """將含有逗號的字串數字轉為整數"""
        return series.astype(str).str.replace(',', '', regex=False).str.replace('--', '0', regex=False).apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

    def _get_twse_institutional(self, date_str):
        url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.twse.com.tw/"
        }
        try:
            res = requests.get(url, headers=headers, timeout=15)
            data = res.json()
            if data.get('stat') != 'OK' or not data.get('data'): 
                return pd.DataFrame()
            
            df = pd.DataFrame(data['data'], columns=data['fields'])
            
            # 過濾出一般股票 (排除權證等，代號通常小於等於 5 碼)
            df = df[df['證券代號'].astype(str).str.len() <= 5].copy()
            
            # 動態尋找欄位名稱 (證交所偶爾會微調欄位名稱，用關鍵字比對最穩)
            cols = df.columns
            col_foreign = next((c for c in cols if '外資' in c and '買賣超' in c and '自營商' not in c), None)
            if not col_foreign: col_foreign = next((c for c in cols if '外陸資買賣超' in c), None)
            col_trust = next((c for c in cols if '投信買賣超' in c), None)
            col_dealer = next((c for c in cols if '自營商買賣超' in c and '自行' not in c and '避險' not in c), None)
            col_total = next((c for c in cols if '三大法人買賣超' in c), None)

            result_df = pd.DataFrame({
                'stock_id': df['證券代號'],
                'foreign_investor': self._clean_number(df[col_foreign]) if col_foreign else 0,
                'investment_trust': self._clean_number(df[col_trust]) if col_trust else 0,
                'dealer': self._clean_number(df[col_dealer]) if col_dealer else 0,
                'total': self._clean_number(df[col_total]) if col_total else 0
            })
            return result_df
        except Exception as e:
            print(f"      [TWSE 法人錯誤] {e}")
            return pd.DataFrame()

    def run(self, days_back=30, mode="daily"):
        start_date = datetime.now() - timedelta(days=days_back)
        target_date = datetime.now() - timedelta(days=1)
        
        with get_db_client() as db:
            self._init_tables(db)
            existing_df = db.fetch("tw_institutional_logs", cols=["date"])
            existing_dates = set(existing_df['date'].astype(str).tolist()) if not existing_df.empty else set()

            while target_date >= start_date:
                date_iso = target_date.strftime("%Y-%m-%d")
                date_compact = target_date.strftime("%Y%m%d")

                if date_iso in existing_dates or target_date.weekday() >= 5:
                    target_date -= timedelta(days=1)
                    continue

                print(f"   📊 [法人籌碼] 抓取上市資料: {date_iso} ...")
                df = self._get_twse_institutional(date_compact)
                
                if not df.empty:
                    df['date'] = date_iso
                    count = db.upsert_from_df("tw_institutional_trades", df, on=["date", "stock_id"])
                    print(f"      ✅ 寫入: {count} 筆")
                    log_status = 'DONE'
                else:
                    print(f"      💤 無資料 (可能休市或尚未更新)")
                    log_status = 'EMPTY'

                log_df = pd.DataFrame([{ "date": date_iso, "status": log_status }])
                db.upsert_from_df("tw_institutional_logs", log_df, on=["date"])
                existing_dates.add(date_iso)
                
                target_date -= timedelta(days=1)
                time.sleep(random.uniform(3, 5)) # 避免被證交所封 IP
                
        self.save_system_log("SUCCESS", f"三大法人執行完畢 ({mode})")
