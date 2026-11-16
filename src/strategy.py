"""
Late-Window High-Probability Sniper Strategy Engine.

Core Rules:
1. Wait out the noisy first 3 minutes (elapsed < 180s) where p ~ 0.50 and taker fees peak at 3.5%.
2. Between 180s and 255s elapsed:
   - Require spatial safety cushion: |raw_z_score| >= config.min_z_score
   - Require CEX orderflow alignment: directional_orderflow >= config.min_orderflow_score
   - Require model fat-tail probability >= config.min_true_prob
   - Require CLOB ask within [min_entry_price, max_entry_price] (avoid $0.90+ bad skew)
   - Require Net EV after Polymarket dynamic taker fee >= config.min_net_edge
3. Intraday protection ("逃生門"):
   - If BTC violently reverses and directional Z cushion drops below stop_loss_z_score, sell immediately
     on the CLOB bid rather than riding to $0.00.
   - Optional early take-profit if CLOB bid reaches $0.96+ to eliminate final-second Oracle Pin Risk.
"""
from dataclasses import dataclass
from typing import List, Optional

from .config import DEFAULT_CONFIG, StrategyConfig
from .fees import (
    EVAnalysis,
    analyze_trade_ev,
    compute_position_shares,
    effective_exit_proceeds,
)
from .pricing_model import FairValueEstimate


@dataclass
class OrderBookTop:
    up_bid: Optional[float] = None
    up_ask: Optional[float] = None
    down_bid: Optional[float] = None
    down_ask: Optional[float] = None


@dataclass
class Position:
    window_ts: int
    side: str                   # "UP" or "DOWN"
    entry_ts: float
    entry_elapsed_sec: float
    shares: float
    entry_price: float
    fee_per_share: float
    total_cost_per_share: float
    p_true_at_entry: float
    z_at_entry: float
    db_trade_id: Optional[int] = None


@dataclass
class ClosedTrade:
    window_ts: int
    side: str
    shares: float
    entry_price: float
    total_cost_per_share: float
    exit_payout_per_share: float
    exit_reason: str
    net_pnl_usdc: float
    won: bool
    p_true_at_entry: float
    z_at_entry: float


class HighProbSniperStrategy:
    def __init__(self, config: StrategyConfig = DEFAULT_CONFIG):
        self.cfg = config
        self.bankroll: float = config.initial_bankroll_usdc
        self.peak_bankroll: float = config.initial_bankroll_usdc
        self.max_drawdown_pct: float = 0.0
        self.active_position: Optional[Position] = None
        self.traded_windows: set[int] = set()
        self.closed_trades: List[ClosedTrade] = []

    def evaluate_tick(
        self,
        ts: float,
        window_ts: int,
        elapsed_sec: float,
        fv: FairValueEstimate,
        book: OrderBookTop,
    ) -> Optional[str]:
        """
        Process a real-time or backtest tick. Returns a human-readable action string if an entry/exit occurs.
        """
        # 1. Manage existing open position first (Stop-Loss / Early Take-Profit)
        if self.active_position and self.active_position.window_ts == window_ts:
            return self._manage_open_position(ts, elapsed_sec, fv, book)

        # 2. Only 1 sniper shot allowed per 5-minute window
        if window_ts in self.traded_windows:
            return None

        # 3. Enforce strict late-window timing filter
        if elapsed_sec < self.cfg.entry_start_elapsed_sec or elapsed_sec > self.cfg.entry_end_elapsed_sec:
            return None

        # 4. Determine candidate side from Chainlink spatial Z-score cushion
        if fv.raw_z_score >= self.cfg.min_z_score:
            candidate_side = "UP"
            dir_z = fv.raw_z_score
            dir_flow = fv.orderflow_score
            dir_cvd = fv.cvd_imbalance
            dir_gap_bps = fv.oracle_gap_bps
            p_true = fv.p_true_up
            bid_price = book.up_bid
            ask_price = book.up_ask
        elif fv.raw_z_score <= -self.cfg.min_z_score:
            candidate_side = "DOWN"
            dir_z = -fv.raw_z_score
            dir_flow = -fv.orderflow_score
            dir_cvd = -fv.cvd_imbalance
            dir_gap_bps = -fv.oracle_gap_bps
            p_true = fv.p_true_down
            bid_price = book.down_bid
            ask_price = book.down_ask
        else:
            return None

        # 5. Chainlink Oracle-Gap & CEX Orderflow Confirmation:
        # Reject if CEX has already dumped against Chainlink's lead or if un-spoofable market CVD opposes trade
        if dir_gap_bps < -self.cfg.max_adverse_oracle_gap_bps:
            return None
        if dir_flow < self.cfg.min_orderflow_score or dir_cvd < self.cfg.min_directional_cvd:
            return None

        # 6. Minimum Model Win Probability, Valid Price Range, Spread Liquidity & Strike-Mirage Guard
        if p_true < self.cfg.min_true_prob or ask_price is None:
            return None
        if ask_price < self.cfg.min_entry_price or ask_price > self.cfg.max_entry_price:
            return None
        if bid_price is not None and (ask_price < bid_price or (ask_price - bid_price) > self.cfg.max_entry_spread):
            return None
        if (p_true - ask_price) > self.cfg.max_model_market_divergence:
            return None

        # 7. Net Expected Value after Polymarket Dynamic Taker Fee
        ev: EVAnalysis = analyze_trade_ev(
            p_true=p_true,
            entry_price=ask_price,
            is_maker=False,
            fee_rate=self.cfg.crypto_taker_fee_rate,
        )
        if ev.net_ev_per_share < self.cfg.min_net_edge:
            return None

        # 8. Position Sizing (Fixed USDC ticket if configured, otherwise Fractional Kelly)
        if self.cfg.fixed_bet_usdc > 0.0:
            outlay = min(self.cfg.fixed_bet_usdc, self.bankroll)
            shares = round(outlay / ev.total_cost_per_share, 2) if outlay >= 1.0 else 0.0
        else:
            shares = compute_position_shares(
                bankroll_usdc=self.bankroll,
                ev=ev,
                kelly_fraction=self.cfg.kelly_fraction,
                max_risk_pct=self.cfg.max_risk_per_window_pct,
            )
        if shares <= 0:
            return None

        self.active_position = Position(
            window_ts=window_ts,
            side=candidate_side,
            entry_ts=ts,
            entry_elapsed_sec=elapsed_sec,
            shares=shares,
            entry_price=ask_price,
            fee_per_share=ev.fee_per_share,
            total_cost_per_share=ev.total_cost_per_share,
            p_true_at_entry=p_true,
            z_at_entry=dir_z,
        )
        self.traded_windows.add(window_ts)
        return (
            f"[ENTRY {candidate_side}] elapsed={elapsed_sec:.0f}s | ask=${ask_price:.3f} "
            f"(+fee ${ev.fee_per_share:.4f}) | P_true={p_true*100:.1f}% | "
            f"NetEV=+{ev.net_ev_per_share*100:.2f}c | Z={dir_z:.2f} | shares={shares:.1f}"
        )

    def _manage_open_position(
        self,
        ts: float,
        elapsed_sec: float,
        fv: FairValueEstimate,
        book: OrderBookTop,
    ) -> Optional[str]:
        pos = self.active_position
        if pos is None:
            return None

        dir_z = fv.raw_z_score if pos.side == "UP" else -fv.raw_z_score
        current_bid = book.up_bid if pos.side == "UP" else book.down_bid
        current_ask = book.up_ask if pos.side == "UP" else book.down_ask
        p_true_now = fv.p_true_up if pos.side == "UP" else fv.p_true_down

        if current_bid is None or current_bid <= 0.01:
            return None

        net_proceeds = effective_exit_proceeds(
            current_bid, is_maker=False, fee_rate=self.cfg.crypto_taker_fee_rate
        )

        # A. EV-Consistent Smart Take-Profit:
        # Never blindly dump at $0.96 ($0.9573 net) when P_true is 99.4% (which donates 4.3c/sh of EV!).
        # Exit early ONLY when:
        #   (1) Bid reaches >= $0.99 ($0.9893 net -> locks 99c almost fee-free), OR
        #   (2) Bid >= take_profit_bid ($0.95) AND net_proceeds >= current model P_true (momentum stalled,
        #       so selling into the CLOB bid is strictly +EV vs holding to expiry!)
        if net_proceeds > pos.total_cost_per_share:
            if current_bid >= 0.99 or (
                current_bid >= self.cfg.take_profit_bid and net_proceeds >= (p_true_now - 0.002)
            ):
                return self._close_active(ts, net_proceeds, "EARLY_TAKE_PROFIT")

        # B. Anti-Whipsaw Confirmed Structural Stop-Loss:
        # Avoid selling into 1-second flash-wicks with 35c spreads (stale_bid_rescue whipsaw trap).
        # Exit ONLY if Chainlink has genuinely broken across Strike (dir_z < stop_loss_z_score = -0.35),
        # CEX orderflow confirms sustained counter-trend pressure, and CLOB spread is liquid (<= 8c).
        if self.cfg.enable_emergency_exit and elapsed_sec < 292.0:
            dir_flow_now = fv.orderflow_score if pos.side == "UP" else -fv.orderflow_score
            spread_ok = (current_ask is None) or (0.0 <= (current_ask - current_bid) <= 0.08)
            confirmed_breakdown = (
                (dir_z < self.cfg.stop_loss_z_score)
                and (dir_flow_now < -0.25)
                and spread_ok
                and (net_proceeds >= 0.12)
            )
            if confirmed_breakdown:
                return self._close_active(ts, net_proceeds, "EMERGENCY_STOP_LOSS")

        return None

    def settle_window(self, ts: float, window_ts: int, strike_price: float, close_price: float) -> Optional[str]:
        """Settle any remaining position at exact 5m expiration (ST >= S0 => UP wins)."""
        if not self.active_position or self.active_position.window_ts != window_ts:
            return None

        resolved_winner = "UP" if close_price >= strike_price else "DOWN"
        payout = 1.0 if self.active_position.side == resolved_winner else 0.0
        return self._close_active(ts, payout, f"SETTLED_{resolved_winner}")

    def _close_active(self, ts: float, payout_per_share: float, reason: str) -> str:
        pos = self.active_position
        assert pos is not None
        pnl_per_share = payout_per_share - pos.total_cost_per_share
        net_pnl = pos.shares * pnl_per_share
        won = net_pnl > 0

        self.bankroll += net_pnl
        if self.bankroll > self.peak_bankroll:
            self.peak_bankroll = self.bankroll
        dd = (self.peak_bankroll - self.bankroll) / self.peak_bankroll if self.peak_bankroll > 0 else 0.0
        if dd > self.max_drawdown_pct:
            self.max_drawdown_pct = dd

        trade = ClosedTrade(
            window_ts=pos.window_ts,
            side=pos.side,
            shares=pos.shares,
            entry_price=pos.entry_price,
            total_cost_per_share=pos.total_cost_per_share,
            exit_payout_per_share=payout_per_share,
            exit_reason=reason,
            net_pnl_usdc=net_pnl,
            won=won,
            p_true_at_entry=pos.p_true_at_entry,
            z_at_entry=pos.z_at_entry,
        )
        self.closed_trades.append(trade)
        self.active_position = None
        return (
            f"[EXIT {trade.side} | {reason}] payout=${payout_per_share:.3f}/sh | "
            f"PnL={net_pnl:+.2f} USDC | Bankroll=${self.bankroll:.2f}"
        )

    def summary_stats(self) -> dict:
        n = len(self.closed_trades)
        if n == 0:
            return {
                "trades": 0,
                "win_rate_pct": 0.0,
                "net_pnl_usdc": 0.0,
                "final_bankroll": self.bankroll,
                "max_drawdown_pct": 0.0,
                "profit_factor": 0.0,
            }
        wins = sum(1 for t in self.closed_trades if t.won)
        gross_profit = sum(t.net_pnl_usdc for t in self.closed_trades if t.net_pnl_usdc > 0)
        gross_loss = abs(sum(t.net_pnl_usdc for t in self.closed_trades if t.net_pnl_usdc < 0))
        pf = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
        avg_cost = sum(t.total_cost_per_share for t in self.closed_trades) / n
        avg_p_true = sum(t.p_true_at_entry for t in self.closed_trades) / n
        emergency_saves = sum(1 for t in self.closed_trades if t.exit_reason == "EMERGENCY_STOP_LOSS")

        return {
            "trades": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate_pct": (wins / n) * 100.0,
            "avg_cost_per_share": avg_cost,
            "avg_model_p_true_pct": avg_p_true * 100.0,
            "emergency_exits": emergency_saves,
            "net_pnl_usdc": self.bankroll - self.cfg.initial_bankroll_usdc,
            "return_pct": ((self.bankroll / self.cfg.initial_bankroll_usdc) - 1.0) * 100.0,
            "final_bankroll": self.bankroll,
            "max_drawdown_pct": self.max_drawdown_pct * 100.0,
            "profit_factor": pf,
        }
