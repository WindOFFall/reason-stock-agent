# Reason Stock Agent 📈

台股 AI 量化交易系統，整合資料爬蟲、LLM 選股分析、Telegram Bot 推播與 Web 儀表板，每日自動執行三階段選股漏斗並將結果推送到手機與網頁。

---

## 系統架構

```
資料爬蟲 (data_pipeline.py)
    │
    ▼
PostgreSQL 資料庫
    │
    ├── AI 選股引擎 (main.py)
    │       │
    │       ├── 第一階段：海選池（法人買超 / 法說會 / 輿情 / 宏觀）
    │       ├── 第二階段：Gemini AI 深度決策
    │       └── 第三階段：持股監控（停損 / 停利）
    │
    ├── Telegram Bot (tg_bot.py)  ← 手機遠端控制
    │
    └── Web 儀表板 (web/)         ← 瀏覽器查看
```

---

## 功能特色

### 資料收集
- **台股**：每日股價、三大法人買賣超、月營收、EPS、法說會行事曆
- **美股**：S&P500、NASDAQ、道瓊、費城半導體及個股（NVDA、TSLA、AAPL 等）
- **亞股**：KOSPI、KOSDAQ、Nikkei225 及韓日個股
- **新聞**：鉅亨網財經新聞、Google News、PTT 輿情

### AI 選股引擎（三階段漏斗）
1. **海選池**：從全市場篩選法人大買、法說會前夕、輿情熱門股票
2. **深度決策**：Gemini LLM 分析技術面 + 法說會簡報 + 個股新聞 + 主力分點，給出買進 / 觀望 / 不買
3. **持股監控**：每日追蹤持股，偵測利空新聞自動發出賣出警報

### Telegram Bot
透過手機直接操控整個系統（詳見下方說明）

### Web 儀表板
瀏覽器查看完整分析結果與市場概況（詳見下方說明）

---

## 環境設定

### 1. 安裝套件

```bash
pip install -r requirements.txt
```

### 2. 設定環境變數

建立 `.env` 檔案：

```env
# 資料庫
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=stock_db

# AI API
GEMINI_API_KEY=your_gemini_key
GROQ_API_KEY=your_groq_key        # 備援 LLM

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 3. 資料庫初始化

```bash
python data_pipeline.py --backfill
```

---

## 使用方式

### 方式一：直接執行

```bash
# 每日完整流程（爬蟲 + 選股分析）
python data_pipeline.py
python main.py

# 只執行選股分析
python main.py

# 啟動 Telegram Bot
python tg_bot.py

# 啟動網站（前後端）
start_web.bat
```

### 方式二：透過 Telegram Bot 遠端控制（推薦）

啟動 Bot 後，在 Telegram 傳送指令：

| 指令 | 說明 |
|------|------|
| `/start` | 顯示所有可用指令 |
| `/status` | 查看今日資料狀態（股價 / 法人 / 新聞是否完整） |
| `/crawl` | 執行今日資料爬蟲（約 5～15 分鐘） |
| `/analys` | 執行 AI 選股分析（約 10～20 分鐘） |
| `/watchlist` | 查看目前持股監控清單 |
| `/report` | 查看最新選股結果摘要 |

> Bot 執行期間會即時回報進度，完成後推送完整分析報告到 Telegram。

---

## Web 儀表板

### 啟動方式

```bash
start_web.bat
```

或分別啟動：

```bash
# 後端 API（port 8000）
cd web/backend && python api.py

# 前端（port 5173）
cd web/frontend && npm run dev
```

### 頁面說明

| 頁面 | 說明 |
|------|------|
| **儀表板** | 資料狀態、大盤指數走勢（台灣 / 費城半導體 / KOSPI / 日經）、外資買超 Top10、近期法說會 |
| **選股結果** | AI 分析的所有股票，含買進（綠）/ 觀望（黃）/ 不買（灰）訊號與理由 |
| **股票查詢** | 個股股價走勢、K 線、技術指標（MA / RSI）、三大法人、EPS |
| **觀察清單** | 目前持股監控中的股票 |
| **法說會行事曆** | 未來法說會排程 |
| **全球市場** | 美股 / 韓股 / 日股個股與指數概況（含 sparkline） |

### 手機連線

若手機與電腦在同一網路（或手機開熱點給電腦），可直接用手機瀏覽器開啟：

```
http://<電腦IP>:5173
```

> 需確認防火牆已允許 port 5173 的連線。

---

## 專案結構

```
reason-stock-agent/
├── main.py              # AI 選股主程式（三階段漏斗）
├── tg_bot.py            # Telegram Bot
├── data_pipeline.py     # 每日資料爬取排程
├── indicators.py        # 技術指標計算
├── health_check.py      # 資料健康檢查
├── start_web.bat        # 一鍵啟動網站
├── crawlers/
│   ├── price.py         # 台股 / 美股 / 亞股價格爬蟲
│   ├── institutional.py # 三大法人
│   ├── news.py          # 新聞爬蟲
│   ├── eps_crawler.py   # EPS 財報
│   └── monthly_revenue.py # 月營收
├── database/
│   └── client.py        # PostgreSQL 連線工具
└── web/
    ├── backend/
    │   └── api.py       # FastAPI 後端（提供所有資料 API）
    └── frontend/        # Vue 3 + Element Plus + ECharts
```

---

## 注意事項

- Gemini API 免費方案有每日請求上限，超過後自動切換至 Groq 備援
- 主力分點資料需要 Chrome Driver，請確認已安裝對應版本
- 法說會 PDF 抓取使用 Selenium，首次執行較慢
