import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Union, Optional

class PostgresEasyClient:
    def __init__(self, user, password, host, port, dbname):
        # 使用 127.0.0.1 避免 IPv6 解析問題
        self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(self.db_url)
        
        # 用來暫存表格定義 (Reflection)
        self.metadata = MetaData()
        
        # 自動啟用 pgvector
        with self.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.engine.dispose()

    def execute_raw(self, sql: str):
        """執行原始 SQL (如 CREATE TABLE, DROP TABLE)"""
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    # --- 1. 查詢 ---
    def fetch(self, table: str, cols: Union[List[str], str] = "*", 
              where: Optional[Dict] = None, limit: int = None) -> pd.DataFrame:
        if isinstance(cols, list): cols = ", ".join(cols)
        
        where_clause = ""
        params = {}
        if where:
            conds = [f"{k} = :{k}" for k in where.keys()]
            where_clause = " WHERE " + " AND ".join(conds)
            params = where
        
        limit_clause = f" LIMIT {limit}" if limit else ""
        sql = f"SELECT {cols} FROM {table}{where_clause}{limit_clause}"
        
        return pd.read_sql(text(sql), self.engine, params=params)

    # --- 2. Upsert (通用版，不要在這裡寫死 drop_duplicates) ---
    def upsert_from_df(self, table: str, df: pd.DataFrame, on: List[str]):
        if df.empty: return 0
        
        # 處理 NaN -> None (SQL NULL)
        df_clean = df.replace({np.nan: None})
        records = df_clean.to_dict(orient='records')
        
        # 自動抓取表格定義
        target_table = Table(table, self.metadata, autoload_with=self.engine)
        
        with self.engine.begin() as conn:
            stmt = insert(target_table).values(records)
            
            # 定義更新邏輯：除了 Key 以外的欄位都更新
            update_dict = {c.name: c for c in stmt.excluded if c.name not in on}
            
            if not update_dict:
                do_stmt = stmt.on_conflict_do_nothing(index_elements=on)
            else:
                do_stmt = stmt.on_conflict_do_update(index_elements=on, set_=update_dict)
            
            result = conn.execute(do_stmt)
            return result.rowcount

    # --- 3. 向量搜尋 ---
    def search_vector(self, table: str, vector_col: str, query_vector: List[float], limit: int = 5):
        sql = f"""
            SELECT *, {vector_col} <=> :qv AS distance
            FROM {table}
            ORDER BY distance ASC
            LIMIT {limit}
        """
        return pd.read_sql(text(sql), self.engine, params={"qv": str(query_vector)})