from data_collector import DataCollector

# ==========================================
# 🤖 Stock AI Agent 主程式
# ==========================================
def main():
    print("==========================================")
    print("🤖 Stock AI Agent 啟動中...")
    print("==========================================")
    
    # 1. 呼叫資料收集模組 (作為副程式執行)
    print("\n[Step 1] 檢查並更新市場數據...")
    collector = DataCollector()
    collector.run_backfill()

    # 2. AI 核心邏輯 (預留位置)
    print("\n[Step 2] 執行 AI 策略分析...")
    print("   (這裡將會是你的 AI Agent 讀取資料並進行決策的地方)")
    
if __name__ == "__main__":
    main()