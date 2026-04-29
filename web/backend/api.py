"""
web/backend/main.py
FastAPI 後端 — 提供股票資料 API
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd
from datetime import datetime, timedelta

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

from database import get_db_client
from data_pipeline import check_table_has_data_v2, is_tw_trading_day

app = FastAPI(title="Stock AI Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 工具函式 ──────────────────────────────────────────────────

def last_trading_day() -> datetime:
    for i in range(1, 8):
        d = datetime.now() - timedelta(days=i)
        if is_tw_trading_day(d):
            return d
    return datetime.now() - timedelta(days=1)


# ── API：資料狀態 ─────────────────────────────────────────────

@app.get("/api/status")
def get_status():
    d = last_trading_day()
    price = check_table_has_data_v2("tw_daily_prices",        "date",         d, min_count=1500)
    inst  = check_table_has_data_v2("tw_institutional_trades", "date",        d, min_count=800)
    news  = check_table_has_data_v2("market_intelligence",     "publish_date", d, min_count=5)
    return {
        "date":    d.strftime("%Y-%m-%d"),
        "price":   price,
        "inst":    inst,
        "news":    news,
        "all_ok":  price and inst and news,
    }


# ── API：選股結果 ─────────────────────────────────────────────

@app.get("/api/recommendations")
def get_recommendations(limit: int = 20):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT stock_id, name, action, reason,
                           entry_price, date
                    FROM trade_log
                    WHERE action IN ('BUY', 'WATCH', 'SKIP')
                    ORDER BY date DESC,
                             CASE action WHEN 'BUY' THEN 0 WHEN 'WATCH' THEN 1 ELSE 2 END
                    LIMIT :limit
                """), {"limit": limit}).fetchall()
                data = [dict(r._mapping) for r in rows]
                for d in data:
                    d["date"] = str(d["date"])
                return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：股價走勢 ─────────────────────────────────────────────

@app.get("/api/stocks/{stock_id}/price")
def get_stock_price(stock_id: str, days: int = 60):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT date, open, high, low, close, volume
                    FROM tw_daily_prices
                    WHERE stock_id = :s
                    ORDER BY date DESC
                    LIMIT :days
                """), {"s": stock_id, "days": days}).fetchall()
                data = [dict(r._mapping) for r in rows]
                for d in data:
                    d["date"] = str(d["date"])
                return data[::-1]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：三大法人 ─────────────────────────────────────────────

@app.get("/api/stocks/{stock_id}/institutional")
def get_institutional(stock_id: str, days: int = 30):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT date,
                           foreign_investor AS foreign_net,
                           investment_trust AS invest_net,
                           dealer           AS dealer_net,
                           total
                    FROM tw_institutional_trades
                    WHERE stock_id = :s
                    ORDER BY date DESC
                    LIMIT :days
                """), {"s": stock_id, "days": days}).fetchall()
                data = [dict(r._mapping) for r in rows]
                for d in data:
                    d["date"] = str(d["date"])
                return data[::-1]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：外資買超排行 ─────────────────────────────────────────

@app.get("/api/institutional/top")
def get_institutional_top(limit: int = 20):
    try:
        d = last_trading_day()
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT stock_id,
                           foreign_investor AS foreign_net,
                           investment_trust AS invest_net,
                           dealer           AS dealer_net,
                           total
                    FROM tw_institutional_trades
                    WHERE date::date = :d
                    ORDER BY foreign_investor DESC
                    LIMIT :limit
                """), {"d": d.strftime("%Y-%m-%d"), "limit": limit}).fetchall()
                return [dict(r._mapping) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：法說會行事曆 ─────────────────────────────────────────

@app.get("/api/conferences")
def get_conferences(limit: int = 30):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT stock_id, name, market, conf_date, conf_time
                    FROM mops_conference_calendar
                    WHERE conf_date >= CURRENT_DATE
                    ORDER BY conf_date ASC
                    LIMIT :limit
                """), {"limit": limit}).fetchall()
                data = [dict(r._mapping) for r in rows]
                for d in data:
                    d["conf_date"] = str(d["conf_date"])
                return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：觀察清單 ─────────────────────────────────────────────

@app.get("/api/watchlist")
def get_watchlist():
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT stock_id, name, action, entry_price, reason, date
                    FROM trade_log
                    WHERE exit_price IS NULL
                    ORDER BY date DESC
                """)).fetchall()
                data = [dict(r._mapping) for r in rows]
                for d in data:
                    d["date"] = str(d["date"])
                return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：EPS 趨勢 ─────────────────────────────────────────────

@app.get("/api/stocks/{stock_id}/eps")
def get_eps(stock_id: str):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT date, type, value
                    FROM tw_financial_statements
                    WHERE stock_id = :s AND type = 'EPS'
                    ORDER BY date DESC
                    LIMIT 8
                """), {"s": stock_id}).fetchall()
                data = [dict(r._mapping) for r in rows[::-1]]
                for d in data:
                    d["date"] = str(d["date"])
                return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：全球指數（台 + 美 + 韓 + 日） ───────────────────────

@app.get("/api/market/all-indices")
def get_all_indices(days: int = 60):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                # 美股指數（from us_daily_prices）
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                us_rows = conn.execute(text("""
                    SELECT ticker, date, close FROM us_daily_prices
                    WHERE ticker IN ('^GSPC','^IXIC','^DJI','^SOX')
                      AND close IS NOT NULL
                      AND date >= :cutoff
                    ORDER BY ticker, date
                """), {"cutoff": cutoff}).fetchall()

                # 亞股指數（from asia_daily_prices）
                asia_rows = conn.execute(text("""
                    SELECT ticker, date, close FROM asia_daily_prices
                    WHERE ticker IN ('^KS11','^KQ11','^N225')
                      AND close IS NOT NULL
                      AND date >= :cutoff
                    ORDER BY ticker, date
                """), {"cutoff": cutoff}).fetchall()

        NAME_MAP = {
            "^GSPC": "S&P 500", "^IXIC": "NASDAQ", "^DJI": "道瓊", "^SOX": "費城半導體",
            "^KS11": "KOSPI",   "^KQ11": "KOSDAQ",  "^N225": "日經225",
        }

        from collections import defaultdict
        grouped = defaultdict(list)
        for r in list(us_rows) + list(asia_rows):
            grouped[r.ticker].append({"date": str(r.date), "close": float(r.close)})

        result = {}
        for ticker, arr in grouped.items():
            result[NAME_MAP.get(ticker, ticker)] = arr

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：大盤指數 ─────────────────────────────────────────────

@app.get("/api/market/index")
def get_market_index(days: int = 60):
    try:
        import yfinance as yf
        indices = {
            "台灣加權": "^TWII",
            "上櫃指數": "^TPEX",
        }
        result = {}
        for name, symbol in indices.items():
            try:
                start_dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                df = yf.download(symbol, start=start_dt, interval="1d", progress=False, auto_adjust=True)
                if df.empty:
                    continue
                # 處理多層欄位（新版 yfinance）
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                date_col  = "Date" if "Date" in df.columns else df.columns[0]
                close_col = "Close" if "Close" in df.columns else "Adj Close"
                df[date_col] = df[date_col].astype(str).str[:10]
                result[name] = [
                    {"date": str(row[date_col]), "close": round(float(row[close_col]), 2)}
                    for _, row in df.iterrows()
                    if pd.notna(row[close_col])
                ]
            except Exception:
                continue
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：技術指標（含 MA 時序、RSI 時序）────────────────────────

@app.get("/api/stocks/{stock_id}/indicators")
def get_indicators(stock_id: str, days: int = 120):
    try:
        with get_db_client() as db:
            with db.engine.connect() as conn:
                df = pd.read_sql(text("""
                    SELECT date, close, volume
                    FROM tw_daily_prices
                    WHERE stock_id = :s
                    ORDER BY date DESC
                    LIMIT :days
                """), conn, params={"s": stock_id, "days": days})

        if df.empty:
            return []

        df = df.sort_values("date").reset_index(drop=True)
        df["ma5"]  = df["close"].rolling(5).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()

        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        df["rsi"] = 100 - (100 / (1 + gain / loss))

        df["avg_vol"]  = df["volume"].rolling(20).mean()
        df["vol_ratio"] = (df["volume"] / df["avg_vol"]).round(2)

        df["date"] = df["date"].astype(str)
        df = df.replace([float('inf'), float('-inf')], None)
        df = df.where(pd.notnull(df), None)

        result = df[["date", "close", "ma5", "ma20", "ma60", "rsi", "vol_ratio"]].to_dict(orient="records")
        for row in result:
            for k, v in row.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    row[k] = None
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：亞股概覽（韓股 + 日股） ─────────────────────────────

@app.get("/api/asia/overview")
def get_asia_overview():
    try:
        from collections import defaultdict
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT ticker, date, close
                    FROM asia_daily_prices
                    ORDER BY ticker, date
                """)).fetchall()

        grouped = defaultdict(list)
        for r in rows:
            if r.close is not None:
                grouped[r.ticker].append({"date": str(r.date), "close": float(r.close)})

        KR_NAMES = {
            "^KS11":     "KOSPI",
            "^KQ11":     "KOSDAQ",
            "005930.KS": "三星電子 Samsung",
            "000660.KS": "SK 海力士 SK Hynix",
            "005380.KS": "現代汽車 Hyundai",
            "035720.KS": "Kakao",
            "000270.KS": "起亞汽車 Kia",
        }
        JP_NAMES = {
            "^N225": "Nikkei 225",
            "7203.T": "Toyota", "6758.T": "Sony", "9984.T": "SoftBank",
            "7974.T": "Nintendo", "6861.T": "Keyence",
        }
        ALL_NAMES = {**KR_NAMES, **JP_NAMES}

        korea, japan = [], []
        for ticker, arr in sorted(grouped.items()):
            if len(arr) < 2:
                continue
            latest     = arr[-1]["close"]
            prev       = arr[-2]["close"]
            change     = round(latest - prev, 2)
            change_pct = round((change / prev) * 100, 2)
            item = {
                "ticker":     ticker,
                "name":       ALL_NAMES.get(ticker, ticker),
                "latest":     latest,
                "change":     change,
                "change_pct": change_pct,
                "dates":      [d["date"] for d in arr],
                "closes":     [d["close"] for d in arr],
                "is_index":   ticker.startswith("^"),
            }
            if ticker in KR_NAMES:
                korea.append(item)
            elif ticker in JP_NAMES:
                japan.append(item)

        return {"korea": korea, "japan": japan}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── API：美股概覽（個股 + 指數分開） ──────────────────────────

@app.get("/api/us/overview")
def get_us_overview():
    try:
        from collections import defaultdict
        with get_db_client() as db:
            with db.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT ticker, date, close
                    FROM us_daily_prices
                    ORDER BY ticker, date
                """)).fetchall()

        grouped = defaultdict(list)
        for r in rows:
            if r.close is not None:
                grouped[r.ticker].append({"date": str(r.date), "close": float(r.close)})

        stocks, indices = [], []
        for ticker, arr in sorted(grouped.items()):
            if len(arr) < 2:
                continue
            latest     = arr[-1]["close"]
            prev       = arr[-2]["close"]
            change     = round(latest - prev, 2)
            change_pct = round((change / prev) * 100, 2)
            item = {
                "ticker":     ticker,
                "latest":     latest,
                "change":     change,
                "change_pct": change_pct,
                "dates":      [d["date"] for d in arr],
                "closes":     [d["close"] for d in arr],
            }
            (indices if ticker.startswith("^") else stocks).append(item)

        return {"stocks": stocks, "indices": indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
