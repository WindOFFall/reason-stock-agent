import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
from urllib.parse import quote_plus
from typing import List, Dict, Union, Optional, Tuple, Any

class PostgresEasyClient:
    def __init__(self, user, password, host, port, dbname):
        # 1. 處理密碼特殊符號 (如 @)
        safe_user = quote_plus(user)
        safe_password = quote_plus(password)
        self.db_url = f"postgresql://{safe_user}:{safe_password}@{host}:{port}/{dbname}"
        
        self.engine = create_engine(self.db_url)
        self.metadata = MetaData()
        self._table_cache = {}

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.engine.dispose()

    def _get_table_object(self, table_name: str):
        """取得 Table 物件，若有快取則使用快取"""
        if table_name in self._table_cache:
            return self._table_cache[table_name]
        
        # 清除舊的 metadata 避免抓不到剛建好的表
        self.metadata.clear()
        table_obj = Table(table_name, self.metadata, autoload_with=self.engine)
        self._table_cache[table_name] = table_obj
        return table_obj

    def execute_raw(self, sql: str):
        """執行原始 SQL"""
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    # ==========================================
    # 查詢功能 (包含 Dict 和 List 雙模式)
    # ==========================================
    # ==========================================
    # 修正後的 fetch：自動加雙引號解決大小寫問題
    # ==========================================
    def fetch(self, table: str, cols: Union[List[str], str] = "*", 
              where: Union[Dict, List[Tuple[str, str, Any]], None] = None, 
              limit: int = None) -> pd.DataFrame:
        
        # 1. 處理欄位 (自動加雙引號)
        # 如果是清單，把每個欄位都包上雙引號: iHmTmp -> "iHmTmp"
        if isinstance(cols, list): 
            cols = ", ".join([f'"{c}"' for c in cols])
        
        # 2. 處理 Where 條件
        params = {}
        where_clause = ""
        
        if where:
            conditions = []
            criteria_list = []

            # 模式 A: 簡單字典
            if isinstance(where, dict):
                criteria_list = [(k, '=', v) for k, v in where.items()]
            
            # 模式 B: 進階清單
            elif isinstance(where, list):
                criteria_list = where
            
            # 組裝 SQL
            for i, (col, op, val) in enumerate(criteria_list):
                clean_op = op.strip().upper()
                param_name = f"p_{i}"
                
                # 處理 IN 的 Tuple 轉換
                if clean_op == 'IN' and isinstance(val, list):
                    val = tuple(val)

                # 🟢 關鍵修正：這裡把 {col} 改成 "{col}" (加上雙引號)
                conditions.append(f'"{col}" {clean_op} :{param_name}')
                
                params[param_name] = val

            where_clause = " WHERE " + " AND ".join(conditions)
        
        limit_clause = f" LIMIT {limit}" if limit else ""
        
        # 這裡的 table 如果你也怕有大小寫問題，也可以改成 f'"{table}"'
        sql = f"SELECT {cols} FROM {table}{where_clause}{limit_clause}"
        
        return pd.read_sql(text(sql), self.engine, params=params)

    # ==========================================
    # Upsert (含自動建表功能)
    # ==========================================
    def upsert_from_df(self, table: str, df: pd.DataFrame, on: List[str]):
        if df.empty: return 0
        
        # --- 步驟 A: 自動檢查並建表 ---
        inspector = inspect(self.engine)
        if not inspector.has_table(table):
            print(f"✨ 表格 '{table}' 不存在，正在自動建立...")
            # 1. 建結構
            df.head(0).to_sql(table, self.engine, if_exists='fail', index=False)
            # 2. 設主鍵 (這是 Upsert 必須的)
            pk_cols = ", ".join(on)
            with self.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk_cols});"))
            print(f"✅ 自動建表完成 (主鍵: {on})")

        # --- 步驟 B: 執行 Upsert ---
        df_clean = df.replace({np.nan: None})
        records = df_clean.to_dict(orient='records')
        
        # 取得 Table 定義
        target_table = self._get_table_object(table)
        
        with self.engine.begin() as conn:
            stmt = insert(target_table).values(records)
            
            # 除了 Key 以外的欄位都更新
            update_dict = {c.name: c for c in stmt.excluded if c.name not in on}
            
            if not update_dict:
                do_stmt = stmt.on_conflict_do_nothing(index_elements=on)
            else:
                do_stmt = stmt.on_conflict_do_update(index_elements=on, set_=update_dict)
            
            result = conn.execute(do_stmt)
            return result.rowcount

    # ==========================================
    # 資料庫結構查詢 (Meta Data)
    # ==========================================
    def get_tables(self, schema: str = "public") -> List[str]:
        """
        取得資料庫中的所有表格名稱
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema=schema)

    def get_columns(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        """
        取得指定表格的所有欄位資訊 (名稱、型別、是否允許 Null 等)
        回傳 DataFrame 方便閱讀
        """
        inspector = inspect(self.engine)
        
        # 檢查表格是否存在
        if not inspector.has_table(table_name, schema=schema):
            print(f"⚠️ 找不到表格 '{table_name}'")
            return pd.DataFrame()

        # 取得欄位資訊
        columns_info = inspector.get_columns(table_name, schema=schema)
        
        # 轉成 DataFrame 讓輸出更漂亮
        df_cols = pd.DataFrame(columns_info)
        
        # SQLAlchemy 取出的 type 是一個物件，我們把它轉成字串方便閱讀
        if not df_cols.empty and 'type' in df_cols.columns:
            df_cols['type'] = df_cols['type'].astype(str)
            
        # 可以只挑選比較重要的資訊回傳，讓畫面更乾淨
        return df_cols[['name', 'type', 'nullable', 'default']]

    # ==========================================
    # 危險操作 (DDL)
    # ==========================================
    def drop_table(self, table_name: str):
        """
        刪除指定的資料表 (Drop Table)
        使用 IF EXISTS 保護，避免表不存在時報錯
        """
        # 加上雙引號避免大小寫敏感問題
        sql = f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'
        
        self.execute_raw(sql)
        
        # 順便把 Python 記憶體裡的快取清掉，避免殘留
        if table_name in self._table_cache:
            del self._table_cache[table_name]
            
        print(f"🗑️ 已成功刪除表格: {table_name}")
