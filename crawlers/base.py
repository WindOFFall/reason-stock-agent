# 檔案: crawlers/base.py
from abc import ABC, abstractmethod
from database import get_db_client

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
        except Exception as e: 
            print(f"❌ Log Error: {e}")

    @abstractmethod
    def run(self, **kwargs): 
        pass
    
    def run_safe(self, **kwargs):
        print(f"\n🚀 [{self.name}] 啟動中...")
        try: 
            self.run(**kwargs)
            print(f"✅ [{self.name}] 完成")
        except Exception as e: 
            print(f"❌ Error: {e}")
            self.save_system_log("CRITICAL_ERROR", str(e))