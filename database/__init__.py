import os
from dotenv import load_dotenv
# 使用相對引用 (.) 匯入同資料夾下的 postgres_client
from .postgres_client import PostgresEasyClient 

load_dotenv()

# 設定檔
DB_CONFIG = {
    "user": os.getenv("DB_USER", "ai_user"),
    "password": os.getenv("DB_PASSWORD", "ai_password"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5433"),
    "dbname": os.getenv("DB_NAME", "stock_db")
}

def get_db_client():
    return PostgresEasyClient(**DB_CONFIG)