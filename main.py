from pgvector.sqlalchemy import Vector

from database import get_db_client
from crawler import TWStockCrawler, USStockCrawler, AnueCrawler,  GoogleNewsCrawler, PTTCrawler

import time
from datetime import datetime, timedelta

# ==========================================
# 🛠️ 歷史回補專用腳本
# ==========================================
def backfill_job():
    print(f"\n🚀 [歷史回補啟動] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   目標：補齊過去 1 年 (365天) 的缺漏資料")
    print("---------------------------------------------------")
    
    # ----------------------------------
    # 1. 股市數據 (Backfill 模式)
    # ----------------------------------
    try:
        # 台股：backfill 模式預設會掃描過去 365 天，找出沒資料的日子補抓
        print("   📈 [1/5] 台股回補 (TWStock)...")
        TWStockCrawler().run_safe(mode="backfill", days_back=365) 
    except Exception as e: print(f"   ❌ 台股錯誤: {e}")

    try:
        # 美股：強制重抓過去 1 年的資料
        print("   🇺🇸 [2/5] 美股回補 (USStock)...")
        USStockCrawler().run_safe(mode="backfill", years_back=1)
    except Exception as e: print(f"   ❌ 美股錯誤: {e}")

    # ----------------------------------
    # 2. 財經新聞 (Backfill 模式)
    # ----------------------------------
    # 新聞部分會啟用「斷點續傳」，從資料庫最舊的日期繼續往回挖
    
    try:
        # 鉅亨網：補 1 年
        print("   📰 [3/5] 鉅亨網回補 (過去 365 天)...")
        AnueCrawler().run_safe(days_back=365, mode="backfill")
    except Exception as e: print(e)

    try:
        # Google News：補 1 年
        print("   🔍 [4/5] Google News 回補 (過去 365 天)...")
        GoogleNewsCrawler().run_safe(keyword="台股", days_back=365, mode="backfill")
    except Exception as e: print(e)

    try:
        # PTT：PTT 比較慢且容易斷，建議先補 60 天就好，穩定後再改 365
        print("   💬 [5/5] PTT Stock 回補 (過去 60 天)...")
        PTTCrawler().run_safe(days_back=60, mode="backfill")
    except Exception as e: print(e)
    
    print(f"\n✅ [歷史回補結束] 所有任務已完成！")

# ==========================================
# 🚀 立即執行
# ==========================================
# 這裡不需要 schedule 和 while True，因為回補只要跑一次就好
if __name__ == "__main__":
    backfill_job()