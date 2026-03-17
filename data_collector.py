from crawlers.price import TWStockCrawler, USStockCrawler
from crawlers.news import AnueCrawler, GoogleNewsCrawler, PTTCrawler
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
            # 台股：backfill 模式預設會掃描過去指定天數，找出沒資料的日子補抓
            print("   📈 [1/5] 台股回補 (TWStock)...")
            TWStockCrawler().run_safe(mode="backfill", days_back=days) 
        except Exception as e: print(f"   ❌ 台股錯誤: {e}")

        try:
            # 美股：強制重抓過去指定天數的資料
            print("   🇺🇸 [2/5] 美股回補 (USStock)...")
            USStockCrawler().run_safe(mode="backfill", days_back=days)
        except Exception as e: print(f"   ❌ 美股錯誤: {e}")

        # ----------------------------------
        # 2. 財經新聞 (Backfill 模式)
        # ----------------------------------
        try:
            # 鉅亨網
            print(f"   📰 [3/5] 鉅亨網回補 (過去 {days} 天)...")
            AnueCrawler().run_safe(days_back=days, mode="backfill")
        except Exception as e: print(f"   ❌ 鉅亨網錯誤: {e}")

        try:
            # Google News
            print(f"   🔍 [4/5] Google News 回補 (過去 {days} 天)...")
            GoogleNewsCrawler().run_safe(keyword="台股", days_back=days, mode="backfill")
        except Exception as e: print(f"   ❌ Google News 錯誤: {e}")

        try:
            # PTT
            print(f"   💬 [5/5] PTT Stock 回補 (過去 {days} 天)...")
            PTTCrawler().run_safe(days_back=days, mode="backfill")
        except Exception as e: print(f"   ❌ PTT 錯誤: {e}")
        
        print(f"\n✅ [資料收集完成] 所有爬蟲任務結束")

if __name__ == "__main__":
    # 允許單獨執行此檔案進行測試
    # 這裡可以自由更改預設值，例如改成 60 就是統一只抓過去 60 天
    DataCollector().run_backfill(days=15)