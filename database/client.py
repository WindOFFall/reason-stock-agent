# 檔案: database.py
# 1. 從你剛剛貼的檔案 (postgres_client.py) 匯入這個 Class
from .postgres_client import PostgresEasyClient 

import os
from dotenv import load_dotenv

load_dotenv()

# 設定檔
DB_CONFIG = {
    "user": os.getenv("DB_USER", "ai_user"),
    "password": os.getenv("DB_PASSWORD", "ai_password"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5433"),
    "dbname": os.getenv("DB_NAME", "stock_db")
}

# 2. 這就是那把萬能鑰匙
def get_db_client():
    return PostgresEasyClient(**DB_CONFIG)