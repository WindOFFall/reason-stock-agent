"""
季度 EPS 爬蟲（FinMind API）
- dataset: TaiwanStockFinancialStatements，只取 type='EPS'
- 自動偵測缺少的股票，補齊後存進 tw_quarterly_eps
- 每小時最多 500 次，超過自動等待
"""

import sys, os, time, requests, pandas as pd
from collections import deque
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_client

# ── 設定 ──────────────────────────────────────────
START_DATE     = '2022-01-01'
MAX_PER_HOUR   = 500
RETRY_WAIT_SEC = 10
SLEEP_PER_CALL = 1.5
# ──────────────────────────────────────────────────


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class RateLimiter:
    def __init__(self, max_per_hour: int):
        self.max_calls  = max_per_hour
        self.timestamps = deque()

    def wait_if_needed(self):
        now          = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        while self.timestamps and self.timestamps[0] < one_hour_ago:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.max_calls:
            wait_until = self.timestamps[0] + timedelta(hours=1)
            wait_sec   = (wait_until - now).total_seconds() + 1
            log(f"⏳ 已達每小時 {self.max_calls} 次上限，等待 {wait_sec/60:.1f} 分鐘後繼續...")
            time.sleep(max(wait_sec, 0))
        self.timestamps.append(datetime.now())

    @property
    def calls_this_hour(self):
        one_hour_ago = datetime.now() - timedelta(hours=1)
        return sum(1 for t in self.timestamps if t >= one_hour_ago)


def fetch_one(stock_id: str, limiter: RateLimiter, token: str) -> pd.DataFrame:
    limiter.wait_if_needed()
    time.sleep(SLEEP_PER_CALL)

    params = {
        'dataset':    'TaiwanStockFinancialStatements',
        'data_id':    stock_id,
        'start_date': START_DATE,
        'token':      token,
    }

    for attempt in range(3):
        try:
            resp = requests.get(
                'https://api.finmindtrade.com/api/v4/data',
                params=params, timeout=15
            )
            data = resp.json()

            if resp.status_code == 429 or data.get('status') == 429:
                wait = 60 * (attempt + 1)
                log(f"  ⚠️ {stock_id} API 限速，等待 {wait}s...")
                time.sleep(wait)
                continue

            if data.get('status') != 200 or not data.get('data'):
                return pd.DataFrame()

            df = pd.DataFrame(data['data'])
            return df

        except Exception as e:
            log(f"  ❌ {stock_id} 第{attempt+1}次失敗：{e}")
            time.sleep(RETRY_WAIT_SEC)

    return pd.DataFrame()


def get_stock_list(conn) -> list:
    with conn.engine.connect() as db:
        rows = db.execute(text("""
            SELECT DISTINCT stock_id FROM tw_daily_prices
            WHERE stock_id ~ '^[0-9]{4}$'
              AND stock_id NOT LIKE '00%'
            ORDER BY stock_id
        """)).fetchall()
    return [r[0] for r in rows]


def get_already_fetched(conn) -> set:
    try:
        with conn.engine.connect() as db:
            rows = db.execute(text(
                "SELECT DISTINCT stock_id FROM tw_financial_statements"
            )).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def save_to_db(conn, df: pd.DataFrame):
    if df.empty:
        return
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql('tw_financial_statements', conn.engine,
              if_exists='append', index=False, method='multi')
    try:
        with conn.engine.begin() as db:
            db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'pk_financial_statements'
                    ) THEN
                        ALTER TABLE tw_financial_statements
                        ADD CONSTRAINT pk_financial_statements
                        PRIMARY KEY (stock_id, date, type);
                    END IF;
                END $$;
            """))
    except Exception:
        pass


def run():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(env_path)
    token = os.getenv('FINMIND_TOKEN', '')
    if not token:
        log("❌ 找不到 FINMIND_TOKEN")
        return
    log(f"Token 載入：前5碼 {token[:5]}...")

    conn    = get_db_client()
    limiter = RateLimiter(MAX_PER_HOUR)

    all_stocks   = get_stock_list(conn)
    already_done = get_already_fetched(conn)
    need_fetch   = [s for s in all_stocks if s not in already_done]

    log(f"全部股票：{len(all_stocks)} 支")
    log(f"已有資料：{len(already_done)} 支")
    log(f"待補齊：  {len(need_fetch)} 支")

    if not need_fetch:
        log("✅ 所有股票都已有 EPS 資料")
        conn.engine.dispose()
        return

    success, skipped, failed = 0, 0, 0

    for i, sid in enumerate(need_fetch):
        df = fetch_one(sid, limiter, token)

        if df.empty:
            skipped += 1
        else:
            try:
                save_to_db(conn, df)
                success += 1
            except Exception as e:
                log(f"  ❌ {sid} 存 DB 失敗：{e}")
                failed += 1

        if (i + 1) % 50 == 0 or (i + 1) == len(need_fetch):
            log(f"進度 {i+1}/{len(need_fetch)} | "
                f"✅{success} ⬛{skipped} ❌{failed} | "
                f"本小時已用 {limiter.calls_this_hour}/{MAX_PER_HOUR} 次")

    log(f"\n🎉 完成！成功 {success}，無資料 {skipped}，失敗 {failed}")
    conn.engine.dispose()


if __name__ == '__main__':
    run()
