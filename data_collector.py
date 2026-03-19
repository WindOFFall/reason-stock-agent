from crawlers.price import TWStockCrawler, USStockCrawler
from crawlers.news import AnueCrawler, GoogleNewsCrawler, PTTCrawler
from crawlers.institutional import TWInstitutionalCrawler
from datetime import datetime

class DataCollector:
    """
    負責執行所有爬蟲任務的資料收集模組。
    供主程式 (AI Agent) 呼叫使用。
    """
    def run_backfill(self, days=30):
        print(f"\n🚀 [資料模組啟動] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   目標：補齊過去 {days} 天 的缺漏資料")
        print("---------------------------------------------------")
        
        # ----------------------------------
        # 1. 股市數據 (Backfill 模式)
        # ----------------------------------
        try:
            print("   📈 [1/6] 台股股價回補 (TWStock)...")
            TWStockCrawler().run_safe(mode="backfill", days_back=days) 
        except Exception as e: print(f"   ❌ 台股錯誤: {e}")

        try:
            print("   📊 [2/6] 台股三大法人籌碼回補 (TWInstitutional)...")
            TWInstitutionalCrawler().run_safe(mode="backfill", days_back=days) 
        except Exception as e: print(f"   ❌ 法人資料錯誤: {e}")

        try:
            print("   🇺🇸 [3/6] 美股股價回補 (USStock)...")
            USStockCrawler().run_safe(mode="backfill", days_back=days)
        except Exception as e: print(f"   ❌ 美股錯誤: {e}")

        # ----------------------------------
        # 2. 財經新聞與輿情 (Backfill 模式)
        # ----------------------------------
        try:
            print(f"   📰 [4/6] 鉅亨網新聞回補 (Anue)...")
            AnueCrawler().run_safe(mode="backfill", days_back=days)
        except Exception as e: print(f"   ❌ 鉅亨網錯誤: {e}")

        try:
            print(f"   🔍 [5/6] Google News 回補 (關鍵字: 台股)...")
            GoogleNewsCrawler().run_safe(keyword="台股", mode="backfill", days_back=days)
        except Exception as e: print(f"   ❌ Google News 錯誤: {e}")

        try:
            print(f"   💬 [6/6] PTT Stock 討論區回補...")
            PTTCrawler().run_safe(mode="backfill", days_back=days)
        except Exception as e: print(f"   ❌ PTT 錯誤: {e}")
        
        print(f"\n✅ [資料收集完成] 所有爬蟲任務結束")

if __name__ == "__main__":
    # 允許單獨執行此檔案進行測試
    # 這裡可以自由更改預設值，例如改成 60 就是統一只抓過去 60 天
    DataCollector().run_backfill(days=15)