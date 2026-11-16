"""
Standalone Telegram Bot Companion & Setup Wizard for Polymarket BTC 5m Sniper.

Can be run alongside `run_monitor.py` (even if `run_monitor.py` was started earlier):
    python tg_bot.py --db data/live_btc_5m.db

Note: If `TG_BOT_TOKEN` is already set in `.env`, `run_monitor.py` also runs the Telegram
bot automatically in the background (so you don't even need two windows after a restart).
"""
import argparse
import asyncio
import os
from pathlib import Path
import sys
from typing import Optional

import requests

from src.tg_notifier import TG_API_BASE, TelegramBotService, load_dotenv_file

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _verify_bot_token(token: str) -> Optional[dict]:
    if not token or ":" not in token:
        return None
    try:
        r = requests.get(TG_API_BASE.format(token=token, method="getMe"), timeout=8.0)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("ok") and isinstance(data.get("result"), dict):
            return data["result"]
    except Exception:
        pass
    return None


def _upsert_env_key(env_path: Path, key: str, value: str) -> None:
    """Safely insert or update one key in `.env` while keeping all other lines intact."""
    lines: list[str] = []
    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()

    replaced = False
    new_lines: list[str] = []
    for line in lines:
        if line.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            replaced = True
        else:
            new_lines.append(line)

    if not replaced:
        new_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def ensure_bot_token_interactive(env_path: Path) -> tuple[str, dict]:
    token = (os.getenv("TG_BOT_TOKEN") or "").strip()
    if token:
        info = _verify_bot_token(token)
        if info:
            return token, info
        print("⚠️  目前的 TG_BOT_TOKEN 無法通過 Telegram API 驗證，請重新輸入。")

    print("=" * 72)
    print(" 📱 Polymarket BTC 5m Telegram 遠端監控機器人 — 初次設定精靈")
    print("=" * 72)
    print(" 只要 3 個步驟即可取得機器人 Token（約 30 秒）：")
    print("   1. 在手機/電腦 Telegram 搜尋官方帳號：@BotFather")
    print("   2. 傳送指令 /newbot，依指示隨意取一個名稱與結尾為 bot 的帳號名")
    print("   3. 複製 BotFather 回傳的那串 Token（格式類似 123456789:ABCdef...）")
    print("=" * 72)

    while True:
        entered = input("請貼上你的 TG_BOT_TOKEN（按 Ctrl+C 取消）: ").strip()
        info = _verify_bot_token(entered)
        if info:
            _upsert_env_key(env_path, "TG_BOT_TOKEN", entered)
            os.environ["TG_BOT_TOKEN"] = entered
            print(f"✅ 已驗證成功並寫入 {env_path.name}！")
            return entered, info
        print("❌ Token 格式不正確或 Telegram 伺服器未驗證通過，請確認完整複製再試一次。")


async def run_standalone_bot(db_path: str) -> None:
    env_path = load_dotenv_file()
    _token, bot_info = ensure_bot_token_interactive(env_path)
    bot_user = bot_info.get("username", "")
    is_live = "live" in os.path.basename(db_path).lower()

    tg = TelegramBotService(
        db_path=db_path,
        live_mode=is_live,
        live_exec=None,
        default_bet_usdc=25.0,
        env_path=env_path,
    )

    print("=" * 72)
    print(f" 🤖 Telegram Bot 已啟動：@{bot_user}")
    if bot_user:
        print(f" 👉 手機開啟連結：https://t.me/{bot_user}")
    if tg.chat_id:
        print(f" 🔒 已綁定你的 Chat ID：{tg.chat_id}（可直接傳 /status 或點選按鈕）")
    else:
        print(" 🔔 下一步：請在 Telegram 點開上方機器人連結並按「開始 /start」，即會自動綁定！")
    print(f" 📂 監控資料庫：{db_path}（自動同步盤口狀態與成交通知）")
    print(" 按 Ctrl+C 可安全停止。")
    print("=" * 72)

    if tg.chat_id:
        tg.send_async(
            "<b>[TERMINAL LINKED // 遠端監控已連線]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "▸ 實盤成交通知與遠端指令已就緒。"
        )

    try:
        await tg.poll_loop(poll_commands=True, watch_db_changes=True)
    except KeyboardInterrupt:
        print("\n已安全停止 Telegram 監控機器人。")
    finally:
        tg.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket BTC 5m Telegram 遠端監控機器人")
    parser.add_argument("--db", default="data/live_btc_5m.db", help="要監控的 SQLite 資料庫路徑")
    args = parser.parse_args()
    asyncio.run(run_standalone_bot(args.db))
