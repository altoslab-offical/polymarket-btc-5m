"""
隨時查詢模擬盤（Paper Trading）戰績與已錄製盤口統計報表。
支援在 `run_monitor.py` 執行期間同步開第二個視窗讀取（SQLite WAL 模式不互鎖）。

Usage:
    python report.py
"""
import argparse
from datetime import datetime
import os
import sqlite3
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def show_report(db_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"找不到資料庫檔案: {db_path} (請先執行 `python run_monitor.py`)")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    total_windows = cur.execute("SELECT COUNT(*) FROM windows").fetchone()[0]
    settled_windows = cur.execute("SELECT COUNT(*) FROM windows WHERE resolved_outcome IS NOT NULL").fetchone()[0]
    total_snaps = cur.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    trades = cur.execute(
        """
        SELECT window_ts, side, entry_ts, shares, entry_price, entry_fee_per_share,
               p_true_at_entry, z_at_entry, exit_reason, exit_payout_per_share, net_pnl_usdc, won
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
        ORDER BY id ASC
        """
    ).fetchall()

    open_trades = cur.execute(
        "SELECT window_ts, side, shares, entry_price FROM paper_trades WHERE exit_reason IS NULL"
    ).fetchall()

    is_live = "live" in os.path.basename(db_path).lower()
    mode_label = "實盤（Live Trading）" if is_live else "模擬盤（Paper Trading）"
    trade_label = "實盤交易" if is_live else "模擬交易"

    print("=" * 88)
    print(f" Polymarket BTC 5m {mode_label}即時戰績報表")
    print("=" * 88)
    print(f" 已紀錄 5m 盤數 : {total_windows} 盤 (已結算: {settled_windows} 盤 | 總秒級快照: {total_snaps:,} 筆)")

    if not trades:
        print(f" 已結算{trade_label} : 0 筆 (策略嚴格等待高把握度尾盤訊號，請持續掛機觀察)")
        if open_trades:
            print(f" 持倉中{trade_label} : {len(open_trades)} 筆 -> {open_trades[0]}")
        print("=" * 88)
        conn.close()
        return

    n = len(trades)
    wins = sum(1 for t in trades if t[11] == 1)
    total_pnl = sum(float(t[10] or 0.0) for t in trades)
    avg_entry = sum(float(t[4]) + float(t[5]) for t in trades) / n
    avg_ptrue = sum(float(t[6]) for t in trades) / n * 100.0

    print(f" 已結算{trade_label} : {n} 筆 (勝: {wins} | 敗: {n - wins})")
    print(f" {mode_label[:2]}實際勝率   : {(wins / n) * 100:.2f}% (模型平均預估勝率: {avg_ptrue:.1f}%)")
    print(f" 平均買入總成本 : ${avg_entry:.4f} / 股 (含動態 Taker 手續費)")
    print(f" 累積淨損益 PnL : {total_pnl:+.2f} {'pUSD' if is_live else 'USDC'}")
    print("-" * 88)
    print(f" 最近 15 筆{trade_label}明細：")
    print(f" {'時間':<16} | {'方向':<4} | {'成本(含費)':<10} | {'模型勝率':<8} | {'出場原因':<20} | {'淨損益':<10}")
    print("-" * 88)

    for row in trades[-15:]:
        w_ts, side, entry_ts, shares, price, fee, p_true, z_val, reason, payout, pnl, won = row
        dt_str = datetime.fromtimestamp(entry_ts).strftime("%m-%d %H:%M:%S")
        cost = float(price) + float(fee)
        print(
            f" {dt_str:<16} | {side:<4} | ${cost:<9.4f} | {float(p_true)*100:6.1f}%  | "
            f"{str(reason):<20} | {float(pnl):+8.2f} U"
        )

    print("=" * 88)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="查看 Polymarket BTC 5m 模擬盤戰績")
    parser.add_argument("--db", default="data/polymarket_btc_5m.db", help="SQLite 資料庫路徑")
    args = parser.parse_args()
    show_report(args.db)
