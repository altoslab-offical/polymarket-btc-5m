"""
Configuration parameters for Polymarket BTC 5m "Up or Down" High-Win-Rate Strategy.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyConfig:
    # --- Polymarket Window & Fee Mechanics ---
    window_seconds: int = 300               # 5-minute candles (288 windows/day)
    crypto_taker_fee_rate: float = 0.07     # Fee = C * 0.07 * p * (1 - p)
    maker_rebate_share: float = 0.20        # 20% of taker fee pool redistributed to makers

    # --- Entry Window (Calibrated on 400+ live Polymarket 5m windows) ---
    # Wait out the noisy first 2.5 minutes (110s-150s had 100% of early-reversal losses in live replay)
    entry_start_elapsed_sec: float = 150.0  # Start scanning at 2m30s elapsed (<=150s left)
    entry_end_elapsed_sec: float = 255.0    # Stop new entries at 4m15s elapsed (45s left)

    # --- High-Probability Spatial Cushion & Pricing Filters ---
    min_z_score: float = 1.30               # |Delta S| / (sigma * sqrt(tau)) >= 1.30
    min_true_prob: float = 0.80             # Model fat-tail win prob must be >= 80%
    min_entry_price: float = 0.68           # Avoid low-conviction chop
    max_entry_price: float = 0.82           # Cap <= $0.82 (~$0.83 w/ fee) to prevent 6.5:1 adverse payoff skew
    max_entry_spread: float = 0.06          # Reject entry if CLOB bid-ask spread > 6c (illiquid/vacuum book)
    min_net_edge: float = 0.035             # P_true - (Ask + TakerFee) >= 3.5%
    max_model_market_divergence: float = 0.15  # Reject entry if P_true - Ask > 15% (uncalibrated strike mirage guard)

    # --- Heavy-Tail / Volatility Calibration ---
    student_t_df: float = 4.5               # Degrees of freedom for BTC 5m fat-tail jumps
    vol_lookback_sec: int = 180             # 3-minute rolling 1s realized volatility window
    min_1s_vol_floor: float = 0.000050      # ~28% annualized floor (prevents $32 dead-market fake Z>=1.45 traps)

    # --- CEX Orderflow & Chainlink Oracle-Gap Confirmation ---
    cvd_window_sec: int = 20                # 20-second Cumulative Volume Delta lookback
    min_orderflow_score: float = 0.0        # Must be >= 0 in trade direction (no counter-dump)
    min_directional_cvd: float = -0.05      # Block entry if real CEX market-order CVD opposes trade (anti-OBI spoof)
    max_adverse_oracle_gap_bps: float = 6.0 # Block entry if CEX leads against Chainlink by > 6 bps (0.06%)

    # --- Intraday Exit & Smart Take-Profit ---
    enable_emergency_exit: bool = True
    stop_loss_z_score: float = -0.35        # Exit ONLY on confirmed structural break below strike (-0.35 sigma)
    take_profit_bid: float = 0.98           # Lock profit at >=$0.98 ($0.979 net, saves ~55c/win vs $0.95 while auto-recycling pUSD)

    # --- Bankroll & Risk Management (Fixed Bet or Fractional Kelly) ---
    initial_bankroll_usdc: float = 1000.0
    fixed_bet_usdc: float = 25.0            # Fixed 25 USDC/pUSD per trade (if <= 0, falls back to Fractional Kelly)
    kelly_fraction: float = 0.20            # 1/5th Kelly for smooth equity curve
    max_risk_per_window_pct: float = 0.05   # Cap outlay at 5% of bankroll per 5m window


DEFAULT_CONFIG = StrategyConfig()
