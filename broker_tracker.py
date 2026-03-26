import requests
import pandas as pd

res = requests.get(
    'https://histock.tw/stock/branch.aspx?no=2337',
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
)

print('狀態碼：', res.status_code)
print('頁面長度：', len(res.text))

tables = pd.read_html(res.text, encoding='utf-8')
print(f'找到 {len(tables)} 張表')
for i, t in enumerate(tables):
    print(f'\n表格 {i} 欄位：', list(t.columns)[:6])  # 只印前6欄
    print(t.head(3))