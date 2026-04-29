"""
tg_bot.py
Telegram Bot — 控制爬蟲與選股分析

指令：
  /start      — 顯示說明
  /status     — 查看最近交易日資料完整性
  /crawl      — 補每日資料（股價、法人、新聞、法說會），約 5~10 分鐘
  /crawl_full — 補所有資料（含 EPS、月營收），約 30 分鐘以上
  /analyze    — 執行選股分析（需先確認資料完整）
"""

import sys
import os
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from data_pipeline import (
    run_all, run_pipeline, check_table_has_data_v2, is_tw_trading_day
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

_crawl_running   = False
_analyze_running = False


# ── 工具函式 ──────────────────────────────────────────────────

def _last_trading_day() -> datetime:
    """取得最近一個台股交易日"""
    for i in range(1, 8):
        d = datetime.now() - timedelta(days=i)
        if is_tw_trading_day(d):
            return d
    return datetime.now() - timedelta(days=1)


def _check_data_status() -> dict:
    """檢查最近交易日的各資料表狀態"""
    d = _last_trading_day()
    return {
        "date":  d.strftime("%Y-%m-%d"),
        "price": check_table_has_data_v2("tw_daily_prices",         "date",         d, min_count=1500),
        "inst":  check_table_has_data_v2("tw_institutional_trades",  "date",         d, min_count=800),
        "news":  check_table_has_data_v2("market_intelligence",      "publish_date", d, min_count=5),
    }


def _notify(text: str):
    """用 main.py 現有的發送函式推播訊息"""
    try:
        from main import send_telegram_message
        send_telegram_message(text)
    except Exception as e:
        print(f"[TG 通知失敗] {e}")


# ── 指令處理 ──────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 <b>股票 AI Agent Bot</b>\n\n"
        "/crawl      — 補每日資料（股價、法人、新聞）約 5~10 分鐘\n"
        "/crawl_full — 補所有資料（含 EPS、月營收）約 30 分鐘以上\n"
        "/analyze    — 執行選股分析並推薦股票\n"
        "/status     — 查看最近交易日資料狀態\n"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = _check_data_status()
    icon = lambda ok: "✅" if ok else "❌"
    all_ok = s["price"] and s["inst"] and s["news"]
    msg = (
        f"📊 <b>資料狀態（{s['date']}）</b>\n\n"
        f"{icon(s['price'])} 股價資料\n"
        f"{icon(s['inst'])}  三大法人\n"
        f"{icon(s['news'])}  新聞資料\n\n"
        f"{'✅ 資料完整，可執行 /analyze' if all_ok else '❌ 資料不完整，請先執行 /crawl'}"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_crawl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _crawl_running

    if _crawl_running:
        await update.message.reply_text("⏳ 爬蟲正在執行中，請稍候...")
        return

    s = _check_data_status()
    if s["price"] and s["inst"] and s["news"]:
        await update.message.reply_text(
            f"✅ 今天（{s['date']}）的資料已完成！\n"
            f"可以直接執行 /analyze 進行選股分析。"
        )
        return

    # 顯示哪些缺漏
    icon = lambda ok: "✅" if ok else "❌"
    await update.message.reply_text(
        f"🔄 開始爬蟲，完成後會通知你\n\n"
        f"目前缺漏（{s['date']}）：\n"
        f"{icon(s['price'])} 股價\n"
        f"{icon(s['inst'])}  三大法人\n"
        f"{icon(s['news'])}  新聞"
    )

    def run_crawl():
        global _crawl_running
        _crawl_running = True
        try:
            run_pipeline(days_back=3)
            s2 = _check_data_status()
            all_ok = s2["price"] and s2["inst"] and s2["news"]
            if all_ok:
                _notify("✅ 每日資料補齊！可執行 /analyze 進行選股分析。")
            else:
                icon = lambda ok: "✅" if ok else "❌"
                _notify(
                    f"⚠️ 爬蟲完成，但部分資料仍缺漏：\n"
                    f"{icon(s2['price'])} 股價\n"
                    f"{icon(s2['inst'])}  三大法人\n"
                    f"{icon(s2['news'])}  新聞"
                )
        except Exception as e:
            _notify(f"❌ 爬蟲執行失敗：{e}")
        finally:
            _crawl_running = False

    threading.Thread(target=run_crawl, daemon=True).start()


async def cmd_crawl_full(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _crawl_running

    if _crawl_running:
        await update.message.reply_text("⏳ 爬蟲正在執行中，請稍候...")
        return

    await update.message.reply_text(
        "🔄 開始完整資料補抓（含 EPS、月營收）\n"
        "⏱️ 預計需要 30 分鐘以上，完成後會通知你。"
    )

    def run_full():
        global _crawl_running
        _crawl_running = True
        try:
            run_all(days_back=3)
            s2 = _check_data_status()
            all_ok = s2["price"] and s2["inst"] and s2["news"]
            if all_ok:
                _notify("✅ 完整資料補抓完成！可執行 /analyze 進行選股分析。")
            else:
                icon = lambda ok: "✅" if ok else "❌"
                _notify(
                    f"⚠️ 補抓完成，但每日資料仍缺漏：\n"
                    f"{icon(s2['price'])} 股價\n"
                    f"{icon(s2['inst'])}  三大法人\n"
                    f"{icon(s2['news'])}  新聞"
                )
        except Exception as e:
            _notify(f"❌ 完整補抓失敗：{e}")
        finally:
            _crawl_running = False

    threading.Thread(target=run_full, daemon=True).start()


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _analyze_running

    if _analyze_running:
        await update.message.reply_text("⏳ 選股分析正在執行中，請稍候...")
        return

    if _crawl_running:
        await update.message.reply_text("⏳ 爬蟲還在執行中，請等爬蟲完成後再分析。")
        return

    s = _check_data_status()
    if not (s["price"] and s["inst"] and s["news"]):
        icon = lambda ok: "✅" if ok else "❌"
        await update.message.reply_text(
            f"❌ 資料不完整，請先執行 /crawl\n\n"
            f"缺漏（{s['date']}）：\n"
            f"{icon(s['price'])} 股價\n"
            f"{icon(s['inst'])}  三大法人\n"
            f"{icon(s['news'])}  新聞"
        )
        return

    await update.message.reply_text(
        "🤖 開始執行選股分析...\n"
        "⏱️ 預計需要 10～20 分鐘，完成後會傳送推薦結果。"
    )

    def run_analysis():
        global _analyze_running
        _analyze_running = True
        try:
            import importlib, main
            importlib.reload(main)
            main.run_daily_agent()
        except Exception as e:
            _notify(f"❌ 選股分析失敗：{e}")
        finally:
            _analyze_running = False

    threading.Thread(target=run_analysis, daemon=True).start()


# ── 主程式 ────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        print("❌ 找不到 TELEGRAM_BOT_TOKEN，請確認 .env 設定")
        return

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("crawl",      cmd_crawl))
    app.add_handler(CommandHandler("crawl_full", cmd_crawl_full))
    app.add_handler(CommandHandler("analyze",    cmd_analyze))

    print("🤖 Bot 啟動，等待指令...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
