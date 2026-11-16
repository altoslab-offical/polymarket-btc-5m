"""
SQLite data recorder for Polymarket 5m windows, Chainlink Oracle + CEX snapshots, and paper trades.
"""
import os
import sqlite3
from typing import Optional


class MarketDataStorage:
    def __init__(self, db_path: str = "data/polymarket_btc_5m.db"):
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS windows (
                window_ts INTEGER PRIMARY KEY,
                slug TEXT NOT NULL,
                up_token_id TEXT,
                down_token_id TEXT,
                strike_price REAL,
                strike_source TEXT,
                close_price REAL,
                resolved_outcome TEXT
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                window_ts INTEGER NOT NULL,
                elapsed_sec REAL NOT NULL,
                chainlink_price REAL,
                chainlink_twap_60s REAL,
                chainlink_source TEXT,
                cex_lead_price REAL,
                oracle_gap_bps REAL,
                binance_price REAL,
                coinbase_price REAL,
                vol_1s REAL,
                cvd_imb REAL,
                obi REAL,
                z_score REAL,
                p_true_up REAL,
                up_bid REAL,
                up_ask REAL,
                down_bid REAL,
                down_ask REAL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_window ON snapshots(window_ts, elapsed_sec);

            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_ts INTEGER NOT NULL,
                side TEXT NOT NULL,
                entry_ts REAL NOT NULL,
                entry_elapsed_sec REAL NOT NULL,
                shares REAL NOT NULL,
                entry_price REAL NOT NULL,
                entry_fee_per_share REAL NOT NULL,
                p_true_at_entry REAL NOT NULL,
                z_at_entry REAL NOT NULL,
                exit_ts REAL,
                exit_reason TEXT,
                exit_payout_per_share REAL,
                net_pnl_usdc REAL,
                won INTEGER
            );
            """
        )
        self.conn.commit()

    def upsert_window(
        self,
        window_ts: int,
        slug: str,
        up_token_id: Optional[str] = None,
        down_token_id: Optional[str] = None,
        strike_price: Optional[float] = None,
        strike_source: Optional[str] = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO windows (window_ts, slug, up_token_id, down_token_id, strike_price, strike_source)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_ts) DO UPDATE SET
                up_token_id = COALESCE(excluded.up_token_id, windows.up_token_id),
                down_token_id = COALESCE(excluded.down_token_id, windows.down_token_id),
                strike_price = CASE
                    WHEN excluded.strike_source IN ('GAMMA_OFFICIAL', 'BOOK_CALIBRATED') AND excluded.strike_price IS NOT NULL
                    THEN excluded.strike_price
                    WHEN windows.strike_source IN ('UNSET', 'WAITING_GAMMA_OR_NEXT_WINDOW') AND excluded.strike_price IS NOT NULL
                    THEN excluded.strike_price
                    ELSE COALESCE(windows.strike_price, excluded.strike_price)
                END,
                strike_source = CASE
                    WHEN excluded.strike_source IN ('GAMMA_OFFICIAL', 'BOOK_CALIBRATED') AND excluded.strike_price IS NOT NULL
                    THEN excluded.strike_source
                    WHEN windows.strike_source IN ('UNSET', 'WAITING_GAMMA_OR_NEXT_WINDOW') AND excluded.strike_price IS NOT NULL
                    THEN excluded.strike_source
                    ELSE COALESCE(windows.strike_source, excluded.strike_source)
                END
            """,
            (window_ts, slug, up_token_id, down_token_id, strike_price, strike_source),
        )
        self.conn.commit()

    def finalize_window(self, window_ts: int, close_price: float, resolved_outcome: str) -> None:
        self.conn.execute(
            """
            UPDATE windows
            SET close_price = ?, resolved_outcome = ?
            WHERE window_ts = ?
            """,
            (close_price, resolved_outcome, window_ts),
        )
        self.conn.commit()

    def record_snapshot(
        self,
        ts: float,
        window_ts: int,
        elapsed_sec: float,
        chainlink_price: Optional[float],
        chainlink_twap_60s: Optional[float],
        chainlink_source: Optional[str],
        cex_lead_price: Optional[float],
        oracle_gap_bps: Optional[float],
        binance_price: Optional[float],
        coinbase_price: Optional[float],
        vol_1s: float,
        cvd_imb: float,
        obi: float,
        z_score: Optional[float],
        p_true_up: Optional[float],
        up_bid: Optional[float],
        up_ask: Optional[float],
        down_bid: Optional[float],
        down_ask: Optional[float],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO snapshots (
                ts, window_ts, elapsed_sec,
                chainlink_price, chainlink_twap_60s, chainlink_source, cex_lead_price, oracle_gap_bps,
                binance_price, coinbase_price, vol_1s, cvd_imb, obi, z_score, p_true_up,
                up_bid, up_ask, down_bid, down_ask
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                window_ts,
                elapsed_sec,
                chainlink_price,
                chainlink_twap_60s,
                chainlink_source,
                cex_lead_price,
                oracle_gap_bps,
                binance_price,
                coinbase_price,
                vol_1s,
                cvd_imb,
                obi,
                z_score,
                p_true_up,
                up_bid,
                up_ask,
                down_bid,
                down_ask,
            ),
        )
        self.conn.commit()

    def insert_paper_trade(
        self,
        window_ts: int,
        side: str,
        entry_ts: float,
        entry_elapsed_sec: float,
        shares: float,
        entry_price: float,
        entry_fee_per_share: float,
        p_true_at_entry: float,
        z_at_entry: float,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO paper_trades (
                window_ts, side, entry_ts, entry_elapsed_sec, shares,
                entry_price, entry_fee_per_share, p_true_at_entry, z_at_entry
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                window_ts,
                side,
                entry_ts,
                entry_elapsed_sec,
                shares,
                entry_price,
                entry_fee_per_share,
                p_true_at_entry,
                z_at_entry,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def close_paper_trade(
        self,
        trade_id: int,
        exit_ts: float,
        exit_reason: str,
        exit_payout_per_share: float,
        net_pnl_usdc: float,
        won: bool,
    ) -> None:
        self.conn.execute(
            """
            UPDATE paper_trades
            SET exit_ts = ?, exit_reason = ?, exit_payout_per_share = ?, net_pnl_usdc = ?, won = ?
            WHERE id = ?
            """,
            (exit_ts, exit_reason, exit_payout_per_share, net_pnl_usdc, 1 if won else 0, trade_id),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
