"""
Telegram Remote Monitor & Control Bot for Polymarket BTC 5m Sniper.

Zero extra third-party dependencies (uses `requests` + `asyncio.run_in_executor`):
1. Runs seamlessly EITHER embedded inside `run_monitor.py` OR standalone via `python tg_bot.py`.
2. Auto-binds the owner's `chat_id` on first `/start` and persists shared control state
   (`chat_id`, `paused`, `bet_usdc`, `bet_updated_at`) in `data/.tg_control.json`.
3. Provides persistent bottom reply-keyboard buttons for mobile monitoring & control:
   - `/status` (`▸ 即時盤況`)
   - `/report` (`▸ 戰績報表`)
   - `/trades` (`▸ 最近10筆`)
   - `/balance` (`▸ 帳戶餘額`)
   - `/pause` / `/resume` (`■ 暫停狙擊` / `▶ 恢復狙擊`)
   - `/bet <金額>` (遠端調整單筆下注金額)
"""
import asyncio
from datetime import datetime
import html
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import requests


TG_API_BASE = "https://api.telegram.org/bot{token}/{method}"
POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

REPLY_KEYBOARD: Dict[str, Any] = {
    "keyboard": [
        [{"text": "▸ 即時盤況"}, {"text": "▸ 戰績報表"}],
        [{"text": "▸ 最近10筆"}, {"text": "▸ 帳戶餘額"}],
        [{"text": "■ 暫停狙擊"}, {"text": "▶ 恢復狙擊"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
}


def load_dotenv_file(env_path: Optional[Path] = None) -> Path:
    """Load project `.env` if present without overriding explicit process environment variables."""
    resolved = env_path or (Path(__file__).resolve().parent.parent / ".env")
    if resolved.exists():
        load_dotenv(dotenv_path=resolved, override=False)
    return resolved


class TelegramControlState:
    """File-backed shared state between `run_monitor.py` and standalone `tg_bot.py`."""

    def __init__(self, state_path: Optional[Path] = None):
        self.path = state_path or (Path(__file__).resolve().parent.parent / "data" / ".tg_control.json")

    def load(self) -> Dict[str, Any]:
        default = {
            "chat_id": None,
            "subscriber_ids": [],
            "paused": False,
            "bet_usdc": None,
            "bet_updated_at": 0.0,
            "last_notified_entry_id": 0,
            "last_notified_exit_id": 0,
            "embedded_bot_heartbeat_ts": 0.0,
        }
        if not self.path.exists():
            return default
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return default
            raw_subs = raw.get("subscriber_ids")
            subs: List[str] = []
            if isinstance(raw_subs, list):
                for x in raw_subs:
                    sx = str(x).strip()
                    if sx and sx not in subs:
                        subs.append(sx)
            return {
                "chat_id": str(raw["chat_id"]) if raw.get("chat_id") else None,
                "subscriber_ids": subs,
                "paused": bool(raw.get("paused", False)),
                "bet_usdc": float(raw["bet_usdc"]) if raw.get("bet_usdc") is not None else None,
                "bet_updated_at": float(raw.get("bet_updated_at") or 0.0),
                "last_notified_entry_id": int(raw.get("last_notified_entry_id") or 0),
                "last_notified_exit_id": int(raw.get("last_notified_exit_id") or 0),
                "embedded_bot_heartbeat_ts": float(raw.get("embedded_bot_heartbeat_ts") or 0.0),
            }
        except Exception:
            return default

    def update(self, **kwargs: Any) -> Dict[str, Any]:
        state = self.load()
        for k, v in kwargs.items():
            if k in state:
                state[k] = v
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.path)
        except Exception:
            pass
        return state

    def add_subscriber(self, cid: str) -> bool:
        cid = str(cid).strip()
        if not cid:
            return False
        state = self.load()
        if cid == state.get("chat_id"):
            return False
        subs = list(state.get("subscriber_ids") or [])
        if cid in subs:
            return False
        subs.append(cid)
        self.update(subscriber_ids=subs)
        return True

    def remove_subscriber(self, cid: str) -> bool:
        cid = str(cid).strip()
        state = self.load()
        subs = list(state.get("subscriber_ids") or [])
        if cid not in subs:
            return False
        subs = [x for x in subs if x != cid]
        self.update(subscriber_ids=subs)
        return True

    def claim_entry_notification(self, trade_id: int) -> bool:
        tid = int(trade_id or 0)
        if tid <= 0:
            return True
        state = self.load()
        if tid <= int(state.get("last_notified_entry_id") or 0):
            return False
        self.update(last_notified_entry_id=tid)
        return True

    def claim_exit_notification(self, trade_id: int) -> bool:
        tid = int(trade_id or 0)
        if tid <= 0:
            return True
        state = self.load()
        if tid <= int(state.get("last_notified_exit_id") or 0):
            return False
        self.update(last_notified_exit_id=tid)
        return True


def _connect_readonly(db_path: str) -> Optional[sqlite3.Connection]:
    if not db_path or not os.path.exists(db_path):
        return None
    return sqlite3.connect(db_path, timeout=3.0)


def _fmt_window_range(window_ts: int) -> str:
    t0 = datetime.fromtimestamp(window_ts).strftime("%H:%M")
    t1 = datetime.fromtimestamp(window_ts + 300).strftime("%H:%M")
    return f"{t0}~{t1}"


def build_status_message(
    db_path: str,
    live_mode: bool = True,
    bet_usdc: float = 25.0,
    paused: bool = False,
) -> str:
    """Format current market snapshot, oracle health, and open position for `/status`."""
    conn = _connect_readonly(db_path)
    if conn is None:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    try:
        cur = conn.cursor()
        snap = cur.execute(
            """
            SELECT ts, window_ts, elapsed_sec, chainlink_price, chainlink_source,
                   cex_lead_price, oracle_gap_bps, z_score, p_true_up,
                   up_ask, down_ask, cvd_imb, vol_1s
            FROM snapshots
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
        if not snap:
            return "[WAIT] 尚無即時盤口快照資料，請稍候再查。"

        (
            ts,
            window_ts,
            elapsed_sec,
            cl_price,
            cl_src,
            cex_price,
            gap_bps,
            z_score,
            p_true_up,
            up_ask,
            down_ask,
            cvd_imb,
            vol_1s,
        ) = snap

        win_row = cur.execute(
            "SELECT strike_price, strike_source FROM windows WHERE window_ts = ?",
            (window_ts,),
        ).fetchone()
        open_row = cur.execute(
            """
            SELECT id, window_ts, side, shares, entry_price, entry_fee_per_share, entry_elapsed_sec
            FROM paper_trades
            WHERE exit_reason IS NULL
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    age_sec = max(0.0, time.time() - float(ts))
    rem_sec = max(0, 300 - int(round(float(elapsed_sec or 0.0))))
    zone = "SNIPE 狙擊區" if 150.0 <= float(elapsed_sec or 0.0) <= 255.0 else "WAIT 觀察區"
    mode_tag = "LIVE 實盤" if live_mode else "PAPER 模擬"
    status_flag = "PAUSED 暫停中" if paused else "ACTIVE 狙擊待命"

    strike = win_row[0] if win_row else None
    strike_src = win_row[1] if win_row else "?"

    lines: List[str] = [
        "<b>[TERMINAL // BTC-5M 即時盤況]</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"▸ <b>系統</b>：{mode_tag}  |  {status_flag}  |  單注 <b>${bet_usdc:.1f}</b>",
        f"▸ <b>盤口</b>：<code>{_fmt_window_range(int(window_ts))}</code>  |  <b>{zone}</b> (剩 <b>{rem_sec}s</b>)",
    ]

    if age_sec > 12.0:
        lines.append(f"▸ <b>警告</b>：<b>監控程式未更新</b> (最後紀錄於 <b>{int(age_sec)}s 前</b>)")

    if cl_price is not None and strike is not None:
        diff = float(cl_price) - float(strike)
        lines.append(f"▸ <b>基準</b>：Strike <code>${float(strike):,.2f}</code> (<code>{html.escape(str(strike_src))}</code>)")
        lines.append(f"▸ <b>現價</b>：Chainlink <code>${float(cl_price):,.2f}</code> (價差 <b>{diff:+.1f}</b>)")
    elif cl_price is not None:
        lines.append(f"▸ <b>現價</b>：Chainlink <code>${float(cl_price):,.2f}</code> (等待鎖定 Strike)")

    if cl_src != "POLY_RTDS_CHAINLINK":
        lines.append(f"▸ <b>預言機</b>：<code>{html.escape(str(cl_src))}</code> [非RTDS 鎖定保護]")
    else:
        lines.append(f"▸ <b>預言機</b>：<code>{html.escape(str(cl_src))}</code> (延遲 {age_sec:.1f}s)")

    if p_true_up is not None and z_score is not None:
        p_dn = 1.0 - float(p_true_up)
        ua_s = f"${float(up_ask):.2f}" if up_ask is not None else "--"
        da_s = f"${float(down_ask):.2f}" if down_ask is not None else "--"
        gap_s = f"{float(gap_bps):+.1f}bp" if gap_bps is not None else "--"
        cvd_s = f"{float(cvd_imb):+.2f}" if cvd_imb is not None else "--"
        vol_s = f"{float(vol_1s) * 1e4:.2f}e-4" if vol_1s is not None else "--"
        lines.append(
            f"▸ <b>模型</b>：Z <code>{float(z_score):+.2f}</code>  |  UP <b>{float(p_true_up)*100:.1f}%</b> / DN <b>{p_dn*100:.1f}%</b>"
        )
        lines.append(f"▸ <b>報價</b>：Ask UP <code>{ua_s}</code>  /  DN <code>{da_s}</code>")
        lines.append(f"▸ <b>動能</b>：CVD <code>{cvd_s}</code>  |  Vol <code>{vol_s}</code>  |  Gap <code>{gap_s}</code>")

    lines.append("━━━━━━━━━━━━━━━━━━")
    if open_row:
        tid, w_ts, side, shares, epx, efee, eel = open_row
        cost = float(epx) + float(efee)
        lines.append(
            f"▸ <b>持倉中 #{tid}</b>：{side} <code>{float(shares):.2f} sh</code> @ <code>${float(epx):.3f}</code> (含費 ${cost:.4f}, {float(eel):.0f}s進)"
        )
    else:
        lines.append("▸ <b>持倉狀態</b>：FLAT (空手待命)")

    return "\n".join(lines)


def build_report_message(db_path: str) -> str:
    """Format win rate, cumulative PnL, today's PnL, streak, and recent trades for `/report`."""
    conn = _connect_readonly(db_path)
    if conn is None:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    try:
        cur = conn.cursor()
        total_win = cur.execute("SELECT COUNT(*) FROM windows").fetchone()[0]
        settled_win = cur.execute("SELECT COUNT(*) FROM windows WHERE resolved_outcome IS NOT NULL").fetchone()[0]
        trades = cur.execute(
            """
            SELECT id, window_ts, side, entry_ts, entry_price, entry_fee_per_share,
                   exit_reason, net_pnl_usdc, won
            FROM paper_trades
            WHERE exit_reason IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()
        open_cnt = cur.execute("SELECT COUNT(*) FROM paper_trades WHERE exit_reason IS NULL").fetchone()[0]
    finally:
        conn.close()

    is_live = "live" in os.path.basename(db_path).lower()
    unit = "pUSD" if is_live else "USDC"
    mode_title = "實盤交易戰績" if is_live else "模擬盤戰績"

    if not trades:
        return (
            f"<b>[PERFORMANCE // {mode_title}]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>已掃描盤數</b>：{total_win} 盤 (已結算 {settled_win})\n"
            f"▸ <b>已結算注單</b>：尚無（持倉中：{open_cnt} 筆）"
        )

    n = len(trades)
    wins = sum(1 for t in trades if t[8] == 1)
    losses = n - wins
    wr = (wins / n) * 100.0
    total_pnl = sum(float(t[7] or 0.0) for t in trades)

    today = datetime.now().date()
    today_trades = [t for t in trades if datetime.fromtimestamp(float(t[3])).date() == today]
    today_wins = sum(1 for t in today_trades if t[8] == 1)
    today_losses = len(today_trades) - today_wins
    today_pnl = sum(float(t[7] or 0.0) for t in today_trades)

    streak_dir = trades[-1][8]
    streak_cnt = 0
    for t in reversed(trades):
        if t[8] == streak_dir:
            streak_cnt += 1
        else:
            break
    streak_str = f"{streak_cnt} 連勝" if streak_dir == 1 else f"{streak_cnt} 連敗"

    lines = [
        f"<b>[PERFORMANCE // {mode_title}]</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"▸ <b>總戰績</b>：<b>{wins} 勝 {losses} 敗</b> (勝率 <b>{wr:.2f}%</b>)  |  <b>{streak_str}</b>",
        f"▸ <b>累積淨損益</b>：<b>{total_pnl:+.2f} {unit}</b>",
        f"▸ <b>今日戰績</b>：<b>{today_wins}W - {today_losses}L</b> (<b>{today_pnl:+.2f} {unit}</b>)",
        f"▸ <b>已掃描盤數</b>：{total_win} 盤 (持倉中：{open_cnt} 筆)",
        "━━━━━━━━━━━━━━━━━━",
        "<b>[RECENT // 最近 5 筆結算]</b>",
    ]
    for tid, wts, side, ets, epx, efee, reason, pnl, won in trades[-5:]:
        dt_s = datetime.fromtimestamp(float(ets)).strftime("%m-%d %H:%M")
        tag = "WIN " if won == 1 else "LOSS"
        lines.append(
            f"▪ <b>{tag}</b> #{tid} <code>{dt_s}</code> {side:<4} @<code>${float(epx):.3f}</code> ➔ <b>{float(pnl):+.2f} U</b>"
        )

    return "\n".join(lines)


def build_trades_message(db_path: str, limit: int = 10) -> str:
    """Format the latest `limit` trades from `paper_trades` for `/trades`."""
    conn = _connect_readonly(db_path)
    if conn is None:
        return f"[ERROR] 找不到資料庫：<code>{html.escape(db_path)}</code>"

    try:
        rows = conn.execute(
            """
            SELECT id, entry_ts, side, shares, entry_price, exit_reason, net_pnl_usdc, won
            FROM paper_trades
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return "[LEDGER] 目前尚無任何成交紀錄。"

    lines = [
        f"<b>[LEDGER // 最近 {len(rows)} 筆成交明細]</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]
    for tid, ets, side, shares, epx, reason, pnl, won in rows:
        dt_s = datetime.fromtimestamp(float(ets)).strftime("%m-%d %H:%M:%S")
        if reason is None:
            lines.append(
                f"▪ <b>OPEN</b> #{tid} <code>{dt_s}</code> {side} <code>{float(shares):.2f}sh</code> @<code>${float(epx):.3f}</code> | <b>持倉中</b>"
            )
        else:
            tag = "WIN " if won == 1 else "LOSS"
            short_reason = {
                "EARLY_TAKE_PROFIT": "止盈",
                "STOP_LOSS_REVERSAL": "止損",
                "EMERGENCY_STOP_LOSS": "急殺止損",
            }.get(str(reason), str(reason).replace("SETTLED_", "結算"))
            lines.append(
                f"▪ <b>{tag}</b> #{tid} <code>{dt_s}</code> {side:<4} @<code>${float(epx):.3f}</code> ({short_reason}) ➔ <b>{float(pnl or 0.0):+.2f} U</b>"
            )
    return "\n".join(lines)


def _fetch_onchain_pusd_balance(wallet_addr: str) -> Optional[float]:
    if not wallet_addr or not wallet_addr.startswith("0x") or len(wallet_addr) != 42:
        return None
    data = "0x70a08231" + wallet_addr[2:].lower().rjust(64, "0")
    try:
        r = requests.post(
            POLYGON_RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": PUSD_CONTRACT, "data": data}, "latest"],
            },
            timeout=4.0,
        )
        res = r.json().get("result")
        if res and str(res).startswith("0x"):
            return int(res, 16) / 1e6
    except Exception:
        pass
    return None


class TelegramBotService:
    """Async/Executor Telegram notification + interactive remote command service."""

    def __init__(
        self,
        db_path: str,
        live_mode: bool = False,
        live_exec: Optional[Any] = None,
        default_bet_usdc: float = 25.0,
        env_path: Optional[Path] = None,
        control_state: Optional[TelegramControlState] = None,
    ) -> None:
        self.resolved_env = load_dotenv_file(env_path)
        self.db_path = db_path
        self.live_mode = live_mode
        self.live_exec = live_exec
        self.default_bet_usdc = float(default_bet_usdc)
        self.bet_usdc = float(default_bet_usdc)

        if control_state is not None:
            self.control = control_state
        else:
            state_file = Path(os.path.dirname(os.path.abspath(db_path))) / ".tg_control.json"
            self.control = TelegramControlState(state_file)

        self.bot_token = (os.getenv("TG_BOT_TOKEN") or "").strip()
        env_chat = (os.getenv("TG_CHAT_ID") or "").strip()
        ctrl = self.control.load()
        self.chat_id: Optional[str] = env_chat or ctrl.get("chat_id")
        if env_chat and ctrl.get("chat_id") != env_chat:
            self.control.update(chat_id=env_chat)

        self._send_http = requests.Session()
        self._poll_http = requests.Session()
        self._send_lock = threading.Lock()
        self._stop_event = asyncio.Event()
        self._update_offset: int = 0
        self._bot_start_ts: float = time.time()

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token)

    def stop(self) -> None:
        self._stop_event.set()

    def get_control_state(self) -> Dict[str, Any]:
        return self.control.load()

    def clear_stale_bet_override(self) -> None:
        """Reset old `.tg_control.json` bet override when monitor starts with explicit CLI `--bet`."""
        self.control.update(bet_usdc=None, bet_updated_at=time.time())

    def send_message_sync(
        self,
        text: str,
        chat_id: Optional[str] = None,
        with_keyboard: bool = True,
    ) -> bool:
        target = chat_id or self.chat_id
        if not self.bot_token or not target:
            return False

        payload: Dict[str, Any] = {
            "chat_id": target,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if with_keyboard:
            payload["reply_markup"] = REPLY_KEYBOARD

        url = TG_API_BASE.format(token=self.bot_token, method="sendMessage")
        with self._send_lock:
            try:
                r = self._send_http.post(url, json=payload, timeout=6.0)
                if r.status_code == 200:
                    return True
                # Fallback to plain text if HTML entity parsing failed
                payload.pop("parse_mode", None)
                r2 = self._send_http.post(url, json=payload, timeout=6.0)
                return r2.status_code == 200
            except Exception:
                return False

    def get_recipient_ids(self) -> List[str]:
        ctrl = self.control.load()
        ids: List[str] = []
        primary = self.chat_id or ctrl.get("chat_id")
        if primary:
            ids.append(str(primary))
        for sub in ctrl.get("subscriber_ids") or []:
            s = str(sub).strip()
            if s and s not in ids:
                ids.append(s)
        return ids

    def broadcast_message_sync(self, text: str, with_keyboard: bool = True) -> bool:
        targets = self.get_recipient_ids()
        if not self.bot_token or not targets:
            return False
        ok_any = False
        for tid in targets:
            if self.send_message_sync(text, chat_id=tid, with_keyboard=with_keyboard):
                ok_any = True
        return ok_any

    def send_async(self, text: str) -> None:
        if not self.enabled or not self.get_recipient_ids():
            return
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, self.broadcast_message_sync, text, True)
        except RuntimeError:
            self.broadcast_message_sync(text, True)

    def notify_entry(
        self,
        trade_id: int,
        side: str,
        window_ts: int,
        elapsed_sec: float,
        shares: float,
        entry_price: float,
        total_cost_per_share: float,
        p_true_at_entry: float,
        z_at_entry: float,
        live_mode: Optional[bool] = None,
        order_id: str = "",
    ) -> None:
        if not self.control.claim_entry_notification(trade_id):
            return
        is_live = self.live_mode if live_mode is None else live_mode
        head = "SNIPE EXECUTED // 實盤進場" if is_live else "PAPER ENTRY // 模擬進場"
        dir_str = "LONG [UP 買漲]" if side == "UP" else "SHORT [DOWN 買跌]"
        spent = shares * total_cost_per_share
        oid_line = f"\n▸ <b>訂單單號</b>：<code>{html.escape(order_id[:18])}</code>" if order_id else ""
        msg = (
            f"<b>[{head} #{trade_id}]</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>狙擊方向</b>：<b>{dir_str}</b>\n"
            f"▸ <b>目標盤口</b>：<code>{_fmt_window_range(window_ts)}</code> (進場秒數 <code>{elapsed_sec:.0f}s</code>)\n"
            f"▸ <b>成交均價</b>：<code>${entry_price:.3f}</code> (含費成本 <code>${total_cost_per_share:.4f}</code>)\n"
            f"▸ <b>部位規模</b>：<code>{shares:.2f} sh</code> (投入 <b>${spent:.2f}</b>)\n"
            f"▸ <b>模型指標</b>：P_true <b>{p_true_at_entry*100:.1f}%</b>  |  Z <code>{z_at_entry:+.2f}</code>"
            f"{oid_line}"
        )
        self.send_async(msg)

    def notify_exit(
        self,
        trade_id: int,
        side: str,
        window_ts: int,
        exit_reason: str,
        net_pnl_usdc: float,
        won: bool,
        exit_payout_per_share: float,
        entry_price: float,
        live_mode: Optional[bool] = None,
    ) -> None:
        if not self.control.claim_exit_notification(trade_id):
            return
        is_live = self.live_mode if live_mode is None else live_mode
        unit = "pUSD" if is_live else "USDC"
        if "TAKE_PROFIT" in exit_reason:
            head = "TARGET HIT // 獲利止盈"
            res_tag = "WIN [提前止盈]"
        elif "STOP_LOSS" in exit_reason:
            head = "STOPPED OUT // 止損平倉"
            res_tag = "LOSS [止損離場]"
        elif won:
            head = "SETTLED WIN // 結算獲勝"
            res_tag = "WIN [官方結算]"
        else:
            head = "SETTLED LOSS // 結算虧損"
            res_tag = "LOSS [官方結算]"

        stats_line = ""
        conn = _connect_readonly(self.db_path)
        if conn is not None:
            try:
                rows = conn.execute(
                    "SELECT net_pnl_usdc, won FROM paper_trades WHERE exit_reason IS NOT NULL ORDER BY id ASC"
                ).fetchall()
                if rows:
                    n = len(rows)
                    w = sum(1 for r in rows if r[1] == 1)
                    tot_pnl = sum(float(r[0] or 0.0) for r in rows)
                    streak_dir = rows[-1][1]
                    scnt = 0
                    for r in reversed(rows):
                        if r[1] == streak_dir:
                            scnt += 1
                        else:
                            break
                    s_txt = f"{scnt} 連勝" if streak_dir == 1 else f"{scnt} 連敗"
                    stats_line = (
                        "\n━━━━━━━━━━━━━━━━━━\n"
                        f"▸ <b>累計戰績</b>：<b>{w}W - {n-w}L</b> (勝率 <b>{w/n*100:.1f}%</b>)\n"
                        f"▸ <b>累積損益</b>：<b>{tot_pnl:+.2f} {unit}</b>  |  <b>{s_txt}</b>"
                    )
            finally:
                conn.close()

        msg = (
            f"<b>[{head} #{trade_id}]  {net_pnl_usdc:+.2f} {unit}</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"▸ <b>結算結果</b>：<b>{res_tag}</b>  |  淨利 <b>{net_pnl_usdc:+.2f} {unit}</b>\n"
            f"▸ <b>交易部位</b>：<b>{side}</b> (盤口 <code>{_fmt_window_range(window_ts)}</code>)\n"
            f"▸ <b>進出場價</b>：<code>${entry_price:.3f}</code> ➔ <code>${exit_payout_per_share:.4f}</code>"
            f"{stats_line}"
        )
        self.send_async(msg)

    def notify_alert(self, title: str, detail: str) -> None:
        msg = f"<b>[ALERT // {html.escape(title)}]</b>\n━━━━━━━━━━━━━━━━━━\n{html.escape(detail)}"
        self.send_async(msg)

    def _get_wallet_balance_message(self) -> str:
        unit = "pUSD" if self.live_mode else "USDC"
        bal: Optional[float] = None
        source = ""

        if self.live_exec is not None:
            try:
                bal = float(self.live_exec.get_pusd_balance())
                source = "CLOB V2 即時餘額"
            except Exception:
                bal = None

        if bal is None:
            funder = (os.getenv("POLY_FUNDER_ADDRESS") or "").strip()
            if funder:
                bal = _fetch_onchain_pusd_balance(funder)
                source = f"Polygon 鏈上錢包 (<code>{funder[:6]}...{funder[-4:]}</code>)"

        ctrl = self.control.load()
        cur_bet = ctrl.get("bet_usdc") or self.bet_usdc
        paused = bool(ctrl.get("paused", False))

        lines = [
            "<b>[VAULT // 帳戶與風控狀態]</b>",
            "━━━━━━━━━━━━━━━━━━",
        ]
        if bal is not None:
            lines.append(f"▸ <b>可用餘額</b>：<b>${bal:,.2f} {unit}</b>")
            lines.append(f"▸ <b>查詢來源</b>：{source}")
        else:
            lines.append("▸ <b>餘額狀態</b>：[WARN] 無法即時讀取鏈上餘額（請確認 <code>.env</code> 內的 <code>POLY_FUNDER_ADDRESS</code>）。")

        if self.live_exec is not None:
            lines.append(
                f"▸ <b>今日累計</b>：<b>{self.live_exec.daily_pnl_usdc:+.2f} pUSD</b> "
                f"(熔斷線 -${self.live_exec.max_daily_loss_usdc:.1f})"
            )
            if self.live_exec.circuit_broken:
                lines.append("▸ <b>風控狀態</b>：[CIRCUIT BREAK] <b>已觸發今日虧損熔斷</b>")

        lines.append(
            f"▸ <b>目前設定</b>：單注 <b>${float(cur_bet):.1f} U</b>  |  {'PAUSED 暫停中' if paused else 'ACTIVE 運行中'}"
        )
        return "\n".join(lines)

    def _fetch_updates_sync(self) -> List[Dict[str, Any]]:
        url = TG_API_BASE.format(token=self.bot_token, method="getUpdates")
        params: Dict[str, Any] = {"timeout": 10, "allowed_updates": ["message"]}
        if self._update_offset > 0:
            params["offset"] = self._update_offset
        try:
            r = self._poll_http.get(url, params=params, timeout=15.0)
            if r.status_code != 200:
                return []
            data = r.json()
            if not data.get("ok"):
                return []
            return data.get("result") or []
        except Exception:
            return []

    def _handle_message_sync(self, msg: Dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        cid = str(chat.get("id") or "")
        chat_title = chat.get("title") or chat.get("first_name") or chat.get("username") or cid
        text = str(msg.get("text") or "").strip()
        msg_date = float(msg.get("date") or 0.0)
        if not cid or not text:
            return

        # Normalize commands (handle `/status@YourBotName`)
        cmd_token = text.split()[0]
        if cmd_token.startswith("/") and "@" in cmd_token:
            cmd_token = cmd_token.split("@", 1)[0]
        norm = cmd_token.lower()

        # Auto-bind owner chat_id on first interaction if unset
        if self.chat_id is None:
            if msg_date < (self._bot_start_ts - 300.0):
                return
            self.chat_id = cid
            self.control.update(chat_id=cid)
            print(f"\n[Telegram] 已自動綁定管理員 Chat ID: {cid}")

        is_owner = (cid == self.chat_id)
        ctrl = self.control.load()

        if not is_owner:
            if norm in ("/unsub", "/stop"):
                self.control.remove_subscriber(cid)
                self.send_message_sync(
                    "<b>[UNSUBSCRIBED // 已取消推播]</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ 您已退出實盤推播名單。若需恢復請輸入 <code>/start</code>。",
                    chat_id=cid,
                )
                return

            if norm in ("/start", "/help", "/sub", "選單", "menu"):
                if msg_date < (self._bot_start_ts - 300.0):
                    return
                added = self.control.add_subscriber(cid)
                if added:
                    print(f"\n[Telegram] 新增推播訂閱者: {chat_title} (Chat ID: {cid})")
                    if self.chat_id:
                        self.send_message_sync(
                            "<b>[SUBSCRIBER JOINED // 新增推播訂閱]</b>\n"
                            "━━━━━━━━━━━━━━━━━━\n"
                            f"▸ <b>對象</b>：{html.escape(str(chat_title))} (<code>{cid}</code>)\n"
                            "▸ <b>權限</b>：唯讀（同步接收開單/結算推播與查看戰績，無權修改設定）\n"
                            f"▸ <b>移除指令</b>：<code>/unsub {cid}</code>",
                            chat_id=self.chat_id,
                        )
                reply = (
                    "<b>[TERMINAL // BTC-5M SNIPER FEED]</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ <b>狀態</b>：<b>已訂閱即時實盤推播（唯讀權限）</b>\n"
                    "▸ 開倉與止盈/結算時將自動同步推播至此對話。\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "▸ <code>/status</code> — 即時 BTC / Chainlink / Z值 / 報價\n"
                    "▸ <code>/report</code> — 累計勝率 / 今日損益 / 連勝紀錄\n"
                    "▸ <code>/trades</code> — 最近 10 筆下單明細\n"
                    "▸ <code>/unsub</code> — 取消接收推播通知\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    f"▸ <b>訂閱 Chat ID</b>：<code>{cid}</code>"
                )
                self.send_message_sync(reply, chat_id=cid)
                return

            if cid not in (ctrl.get("subscriber_ids") or []):
                return

        # Skip stale state-changing commands left in queue before bot started (allow 180s grace)
        is_old_backlog = msg_date < (self._bot_start_ts - 180.0)
        cur_bet = float(ctrl.get("bet_usdc") or self.bet_usdc)
        paused = bool(ctrl.get("paused", False))

        if norm in ("/start", "/help", "選單", "menu"):
            reply = (
                "<b>[TERMINAL // BTC-5M SNIPER COMMAND]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ <code>/status</code> — 即時 BTC / Chainlink / Z值 / 報價\n"
                "▸ <code>/report</code> — 累計勝率 / 今日損益 / 連勝紀錄\n"
                "▸ <code>/trades</code> — 最近 10 筆下單明細\n"
                "▸ <code>/balance</code> — 錢包可用餘額與風控狀態\n"
                "▸ <code>/pause</code> / <code>/resume</code> — 暫停或恢復開新倉\n"
                "▸ <code>/bet 25</code> — 動態調整單注金額 (U)\n"
                "▸ <code>/subs</code> — 管理推播訂閱名單（好友／群組）\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>授權 Chat ID</b>：<code>{cid}</code>"
            )
            self.send_message_sync(reply, chat_id=cid)
            return

        if text in ("▸ 即時盤況", "📊 即時盤況") or norm == "/status":
            self.send_message_sync(
                build_status_message(self.db_path, live_mode=self.live_mode, bet_usdc=cur_bet, paused=paused),
                chat_id=cid,
            )
            return

        if text in ("▸ 戰績報表", "💰 戰績報表") or norm == "/report":
            self.send_message_sync(build_report_message(self.db_path), chat_id=cid)
            return

        if text in ("▸ 最近10筆", "📜 最近10筆") or norm == "/trades":
            self.send_message_sync(build_trades_message(self.db_path, limit=10), chat_id=cid)
            return

        if not is_owner:
            self.send_message_sync(
                "<b>[ACCESS DENIED // 權限不足]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ 此帳號為<b>唯讀推播訂閱者</b>，僅管理員可查看錢包餘額或修改交易設定。",
                chat_id=cid,
            )
            return

        if norm == "/subs":
            subs = ctrl.get("subscriber_ids") or []
            sub_lines = [f"▪ <code>{html.escape(str(x))}</code>" for x in subs] if subs else ["▸ 目前無其他推播訂閱者"]
            msg_out = (
                "<b>[SUBSCRIBERS // 推播接收名單]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>管理員</b>：<code>{self.chat_id}</code>\n"
                f"▸ <b>訂閱人數</b>：{len(subs)} 位\n"
                + "\n".join(sub_lines)
                + "\n━━━━━━━━━━━━━━━━━━\n"
                "▸ 請朋友直接點開此機器人傳送 <code>/start</code>（或把機器人拉進群組傳 <code>/start</code>）即自動加入推播。\n"
                "▸ 移除指令：<code>/unsub ChatID</code>"
            )
            self.send_message_sync(msg_out, chat_id=cid)
            return

        if norm == "/unsub":
            parts = text.split()
            if len(parts) < 2:
                self.send_message_sync(
                    "<b>[COMMAND // UNSUB]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    "▸ 管理員移除訂閱格式：<code>/unsub ChatID</code>（可用 <code>/subs</code> 查詢 ID）",
                    chat_id=cid,
                )
                return
            target_id = parts[1].strip()
            if self.control.remove_subscriber(target_id):
                self.send_message_sync(
                    f"<b>[COMMAND // SUBSCRIBER REMOVED]</b>\n━━━━━━━━━━━━━━━━━━\n▸ 已移除推播對象：<code>{html.escape(target_id)}</code>",
                    chat_id=cid,
                )
            else:
                self.send_message_sync(
                    f"<b>[WARN // NOT FOUND]</b>\n━━━━━━━━━━━━━━━━━━\n▸ 名單中找不到 Chat ID：<code>{html.escape(target_id)}</code>",
                    chat_id=cid,
                )
            return

        if text in ("▸ 帳戶餘額", "👛 錢包餘額") or norm == "/balance":
            self.send_message_sync(self._get_wallet_balance_message(), chat_id=cid)
            return

        if is_old_backlog:
            return

        if text in ("■ 暫停狙擊", "⏸️ 暫停下單") or norm == "/pause":
            self.control.update(paused=True)
            self.send_message_sync(
                "<b>[COMMAND // PAUSED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "▸ <b>狀態</b>：已暫停開新倉（現有持倉仍會自動盯盤止盈/止損）。",
                chat_id=cid,
            )
            return

        if text in ("▶ 恢復狙擊", "▶️ 恢復下單") or norm == "/resume":
            self.control.update(paused=False)
            self.send_message_sync(
                "<b>[COMMAND // RESUMED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>狀態</b>：已恢復自動掃描下單（單注 <b>${cur_bet:.1f} U</b>）。",
                chat_id=cid,
            )
            return

        if norm == "/bet":
            parts = text.split()
            if len(parts) < 2:
                self.send_message_sync(
                    f"<b>[COMMAND // BET SIZE]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    f"▸ 目前單筆下注：<b>${cur_bet:.1f} U</b>\n"
                    f"▸ 調整範例：<code>/bet 25</code> 或 <code>/bet 15</code>",
                    chat_id=cid,
                )
                return
            try:
                new_bet = round(float(parts[1]), 2)
                if not (1.0 <= new_bet <= 1000.0):
                    raise ValueError("out of range")
            except ValueError:
                self.send_message_sync(
                    "<b>[ERROR // INVALID BET]</b>\n━━━━━━━━━━━━━━━━━━\n"
                    "▸ 金額無效，請輸入 <code>1 ~ 1000</code> 之間的數字，例如 <code>/bet 25</code>。",
                    chat_id=cid,
                )
                return

            self.bet_usdc = new_bet
            self.control.update(bet_usdc=new_bet, bet_updated_at=time.time())
            self.send_message_sync(
                "<b>[COMMAND // BET UPDATED]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ 單筆下注金額已更新為 <b>${new_bet:.1f} U</b>（交易程式將於 1 秒內自動套用）。",
                chat_id=cid,
            )

    async def _db_watch_loop(self) -> None:
        """Used only in standalone `tg_bot.py` mode to push live entry/exit alerts by watching SQLite."""
        known_max_id = 0
        open_ids: set[int] = set()
        conn = _connect_readonly(self.db_path)
        if conn is not None:
            try:
                r = conn.execute("SELECT COALESCE(MAX(id), 0) FROM paper_trades").fetchone()
                known_max_id = int(r[0] or 0)
                open_rows = conn.execute("SELECT id FROM paper_trades WHERE exit_reason IS NULL").fetchall()
                open_ids = {int(x[0]) for x in open_rows}
            finally:
                conn.close()

        is_live = "live" in os.path.basename(self.db_path).lower()
        while not self._stop_event.is_set():
            await asyncio.sleep(2.0)
            if not self.get_recipient_ids():
                continue
            ctrl = self.control.load()
            embedded_alive = (time.time() - float(ctrl.get("embedded_bot_heartbeat_ts") or 0.0)) < 25.0
            conn = _connect_readonly(self.db_path)
            if conn is None:
                continue
            try:
                # 1. Detect newly opened or newly opened+settled trades
                new_rows = conn.execute(
                    """
                    SELECT id, window_ts, side, entry_elapsed_sec, shares, entry_price,
                           entry_fee_per_share, p_true_at_entry, z_at_entry,
                           exit_reason, exit_payout_per_share, net_pnl_usdc, won
                    FROM paper_trades
                    WHERE id > ?
                    ORDER BY id ASC
                    """,
                    (known_max_id,),
                ).fetchall()
                for row in new_rows:
                    (
                        tid, wts, side, eel, sh, epx, efee, pt, zv,
                        ex_reason, ex_pay, pnl, won,
                    ) = row
                    known_max_id = max(known_max_id, int(tid))
                    cost = float(epx) + float(efee)
                    if not embedded_alive:
                        self.notify_entry(tid, side, wts, eel, sh, epx, cost, pt, zv, live_mode=is_live)
                    if ex_reason is None:
                        open_ids.add(int(tid))
                    elif not embedded_alive:
                        self.notify_exit(tid, side, wts, ex_reason, float(pnl or 0.0), won == 1, float(ex_pay or 0.0), epx, live_mode=is_live)

                # 2. Detect closed trades among previously open positions
                if open_ids:
                    placeholders = ",".join("?" for _ in open_ids)
                    closed = conn.execute(
                        f"""
                        SELECT id, window_ts, side, entry_price, exit_reason,
                               exit_payout_per_share, net_pnl_usdc, won
                        FROM paper_trades
                        WHERE id IN ({placeholders}) AND exit_reason IS NOT NULL
                        """,
                        tuple(open_ids),
                    ).fetchall()
                    for tid, wts, side, epx, ex_reason, ex_pay, pnl, won in closed:
                        open_ids.discard(int(tid))
                        if not embedded_alive:
                            self.notify_exit(tid, side, wts, ex_reason, float(pnl or 0.0), won == 1, float(ex_pay or 0.0), epx, live_mode=is_live)
            except Exception:
                pass
            finally:
                conn.close()

    async def poll_loop(self, poll_commands: bool = True, watch_db_changes: bool = False) -> None:
        """Main background loop: polls Telegram long-poll updates and optionally watches SQLite."""
        if not self.enabled:
            return

        loop = asyncio.get_running_loop()
        watch_task: Optional[asyncio.Task] = None
        if watch_db_changes:
            watch_task = asyncio.create_task(self._db_watch_loop())

        try:
            while not self._stop_event.is_set():
                if not watch_db_changes:
                    self.control.update(embedded_bot_heartbeat_ts=time.time())
                else:
                    embedded_alive = (
                        time.time() - float(self.control.load().get("embedded_bot_heartbeat_ts") or 0.0)
                    ) < 25.0
                    if embedded_alive:
                        await asyncio.sleep(2.0)
                        continue
                if poll_commands:
                    updates = await loop.run_in_executor(None, self._fetch_updates_sync)
                    for upd in updates:
                        uid = int(upd.get("update_id") or 0)
                        if uid >= self._update_offset:
                            self._update_offset = uid + 1
                        msg = upd.get("message")
                        if isinstance(msg, dict):
                            await loop.run_in_executor(None, self._handle_message_sync, msg)
                else:
                    await asyncio.sleep(2.0)
        finally:
            if watch_task is not None:
                watch_task.cancel()
