import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
from urllib.parse import quote_plus
from typing import List, Dict, Union, Optional, Tuple, Any

load_dotenv()

DB_CONFIG = {
    "user":     os.getenv("DB_USER",     "ai_user"),
    "password": os.getenv("DB_PASSWORD", "ai_password"),
    "host":     os.getenv("DB_HOST",     "127.0.0.1"),
    "port":     os.getenv("DB_PORT",     "5433"),
    "dbname":   os.getenv("DB_NAME",     "stock_db"),
}


class PostgresEasyClient:
    def __init__(self, user, password, host, port, dbname):
        safe_user     = quote_plus(user)
        safe_password = quote_plus(password)
        self.db_url   = f"postgresql://{safe_user}:{safe_password}@{host}:{port}/{dbname}"
        self.engine   = create_engine(self.db_url)
        self.metadata = MetaData()
        self._table_cache = {}

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.engine.dispose()

    def _get_table_object(self, table_name: str):
        if table_name in self._table_cache:
            return self._table_cache[table_name]
        self.metadata.clear()
        table_obj = Table(table_name, self.metadata, autoload_with=self.engine)
        self._table_cache[table_name] = table_obj
        return table_obj

    def execute_raw(self, sql: str):
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def fetch(self, table: str, cols: Union[List[str], str] = "*",
              where: Union[Dict, List[Tuple[str, str, Any]], None] = None,
              limit: int = None) -> pd.DataFrame:
        if isinstance(cols, list):
            cols = ", ".join([f'"{c}"' for c in cols])

        params = {}
        where_clause = ""
        if where:
            conditions   = []
            criteria_list = [(k, '=', v) for k, v in where.items()] if isinstance(where, dict) else where
            for i, (col, op, val) in enumerate(criteria_list):
                clean_op   = op.strip().upper()
                param_name = f"p_{i}"
                if clean_op == 'IN' and isinstance(val, list):
                    val = tuple(val)
                conditions.append(f'"{col}" {clean_op} :{param_name}')
                params[param_name] = val
            where_clause = " WHERE " + " AND ".join(conditions)

        limit_clause = f" LIMIT {limit}" if limit else ""
        sql = f"SELECT {cols} FROM {table}{where_clause}{limit_clause}"
        return pd.read_sql(text(sql), self.engine, params=params)

    def upsert_from_df(self, table: str, df: pd.DataFrame, on: List[str]):
        if df.empty: return 0

        inspector = inspect(self.engine)
        if not inspector.has_table(table):
            print(f"✨ 表格 '{table}' 不存在，正在自動建立...")
            df.head(0).to_sql(table, self.engine, if_exists='fail', index=False)
            pk_cols = ", ".join(on)
            with self.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk_cols});"))
            print(f"✅ 自動建表完成 (主鍵: {on})")

        df_clean = df.replace({np.nan: None})
        records  = df_clean.to_dict(orient='records')
        target_table = self._get_table_object(table)

        with self.engine.begin() as conn:
            stmt        = insert(target_table).values(records)
            update_dict = {c.name: c for c in stmt.excluded if c.name not in on}
            do_stmt     = stmt.on_conflict_do_nothing(index_elements=on) if not update_dict \
                          else stmt.on_conflict_do_update(index_elements=on, set_=update_dict)
            return conn.execute(do_stmt).rowcount

    def get_tables(self, schema: str = "public") -> List[str]:
        return inspect(self.engine).get_table_names(schema=schema)

    def get_columns(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name, schema=schema):
            print(f"⚠️ 找不到表格 '{table_name}'")
            return pd.DataFrame()
        df_cols = pd.DataFrame(inspector.get_columns(table_name, schema=schema))
        if not df_cols.empty and 'type' in df_cols.columns:
            df_cols['type'] = df_cols['type'].astype(str)
        return df_cols[['name', 'type', 'nullable', 'default']]

    def drop_table(self, table_name: str):
        self.execute_raw(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
        self._table_cache.pop(table_name, None)
        print(f"🗑️ 已成功刪除表格: {table_name}")


def get_db_client() -> PostgresEasyClient:
    return PostgresEasyClient(**DB_CONFIG)
