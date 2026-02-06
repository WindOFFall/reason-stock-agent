import pandas as pd
import numpy as np

def calculate_technical_indicators(df):
    """
    輸入: 包含 date, stock_id, close, volume 的原始 DataFrame
    輸出: 增加技術指標欄位的 DataFrame
    """
    # 確保資料排序正確 (對 Time Series 很重要)
    df = df.sort_values(['stock_id', 'date'])

    # --- 1. 趨勢指標 (Trend) ---
    # 計算均線
    df['price_ma5'] = df.groupby('stock_id')['close'].transform(lambda x: x.rolling(window=5).mean())
    df['price_ma20'] = df.groupby('stock_id')['close'].transform(lambda x: x.rolling(window=20).mean())
    
    # 計算均線斜率 (True 代表均線向上)
    df['ma20_slope_up'] = df['price_ma20'] > df.groupby('stock_id')['price_ma20'].shift(1)

    # --- 2. 動能指標 (Volume) ---
    # 計算均量
    df['vol_ma5'] = df.groupby('stock_id')['volume'].transform(lambda x: x.rolling(window=5).mean())
    df['vol_ma20'] = df.groupby('stock_id')['volume'].transform(lambda x: x.rolling(window=20).mean()) # Shift 1 避免包含今日
    
    # 量能爆發比 (當日量 / 過去20日均量)
    # 這裡建議 vol_ma20 使用 shift(1) 計算出來的基準，這樣比較才客觀
    df['avg_vol_baseline'] = df.groupby('stock_id')['volume'].transform(lambda x: x.shift(1).rolling(window=20).mean())
    df['vol_ratio'] = df['volume'] / df['avg_vol_baseline']

    # --- 3. 風險濾網 (Risk Filters) ---
    # 漲跌幅 (%)
    df['pct_change'] = df.groupby('stock_id')['close'].transform(lambda x: x.pct_change() * 100)
    
    # 乖離率 (Bias): 股價距離月線多遠
    df['bias'] = (df['close'] - df['price_ma20']) / df['price_ma20']
    
    # 波動率 (5日標準差/股價): 用來找盤整
    df['volatility'] = df.groupby('stock_id')['close'].transform(lambda x: x.rolling(window=5).std()) / df['close']

    return df

# 使用範例:
# processed_df = calculate_technical_indicators(raw_df)