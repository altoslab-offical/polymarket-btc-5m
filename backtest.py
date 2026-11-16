"""
Backtest & Strategy Verification Suite for Polymarket BTC 5m Up/Down (Chainlink-Settled).

Supports two modes:
1. Replay recorded real Chainlink + Polymarket CLOB data from SQLite:
       python backtest.py --db data/polymarket_btc_5m.db
2. Synthetic CEX Jump-Diffusion + Chainlink Oracle Aggregation Lag Benchmark (--demo):
       python backtest.py --demo --windows 2000

In --demo mode, we explicitly separate:
  - True CEX Spot (`cex_prices`): fast price discovery with fat-tail jumps & orderflow momentum
  - Official Chainlink Oracle (`cl_prices`): multi-node smoothed aggregation with realistic 2-5s
    Oracle Gap lag — **and strictly settle every 5m window on Chainlink (`cl_close >= cl_strike`)!**
"""
import argparse
import math
import random
import sqlite3
from typing import List

from src.config import DEFAULT_CONFIG, StrategyConfig
from src.fees import taker_fee_per_share
from src.pricing_model import RollingMicrostructureTracker, estimate_fair_value, normal_cdf
from src.strategy import HighProbSniperStrategy, OrderBookTop


def replay_sqlite_db(db_path: str, config: StrategyConfig = DEFAULT_CONFIG) -> dict:
    conn = sqlite3.connect(db_path, uri=db_path.startswith("file:"))
    cur = conn.cursor()

    windows = cur.execute(
        """
        SELECT window_ts, strike_price, strike_source, close_price, resolved_outcome
        FROM windows
        WHERE resolved_outcome IS NOT NULL
        ORDER BY window_ts
        """
    ).fetchall()

    strat = HighProbSniperStrategy(config=config)
    tracker = RollingMicrostructureTracker(vol_lookback_sec=config.vol_lookback_sec)

    for w_ts, db_strike, strike_src, cl_close, resolved_outcome in windows:
        rows = cur.execute(
            """
            SELECT ts, elapsed_sec, chainlink_price, cex_lead_price, cvd_imb, obi,
                   up_bid, up_ask, down_bid, down_ask
            FROM snapshots
            WHERE window_ts = ?
            ORDER BY ts ASC
            """,
            (w_ts,),
        ).fetchall()
        if not rows:
            continue

        # Use official Gamma strike if recorded, else exact first tick at t~0s of this window
        first_cl = next((r[2] for r in rows if r[2] is not None and r[1] < 5.0), None)
        eff_strike = db_strike if (strike_src == "GAMMA_OFFICIAL" and db_strike) else (first_cl or db_strike)
        if not eff_strike:
            continue

        for ts, elapsed, cl_spot, cex_lead, cvd_imb, obi, ub, ua, db_p, da in rows:
            if cl_spot is None:
                continue
            if elapsed < 1.5:
                ub, ua, db_p, da = None, None, None, None
            tracker.on_price(ts, cex_lead or cl_spot)
            vol_1s = tracker.get_realized_vol_1s(floor=config.min_1s_vol_floor)
            dyn_gap = tracker.update_oracle_basis(cl_spot, cex_lead)
            rem = max(0.0, 300.0 - elapsed)
            flow = 0.70 * (cvd_imb or 0.0) + 0.30 * (obi or 0.0)
            fv = estimate_fair_value(
                strike_price=eff_strike,
                chainlink_price=cl_spot,
                remaining_sec=rem,
                vol_1s=vol_1s,
                orderflow_score=flow,
                cex_lead_price=cex_lead,
                dynamic_gap_bps=dyn_gap,
                student_t_df=config.student_t_df,
            )
            if fv:
                book = OrderBookTop(up_bid=ub, up_ask=ua, down_bid=db_p, down_ask=da)
                strat.evaluate_tick(ts, w_ts, elapsed, fv, book)

        eff_close = (eff_strike + 1.0) if resolved_outcome == "UP" else (eff_strike - 1.0)
        strat.settle_window(w_ts + 300.0, w_ts, eff_strike, eff_close)

    conn.close()
    return {
        "total_windows": len(windows),
        **strat.summary_stats(),
    }


def run_benchmark_simulation(num_windows: int = 2000, seed: int = 42) -> None:
    """
    Simulate `num_windows` 5-minute BTC candles where:
      - CEX moves first (jump-diffusion + autocorrelated orderflow)
      - Chainlink Oracle aggregates CEX across nodes with realistic multi-second smoothing + tiny basis noise
      - Official Strike (S_0) and Official Settlement (S_T) are strictly Chainlink!
    """
    rng = random.Random(seed)
    cfg = DEFAULT_CONFIG
    our_strat = HighProbSniperStrategy(config=cfg)
    tracker = RollingMicrostructureTracker(vol_lookback_sec=cfg.vol_lookback_sec)

    naive_pnl, naive_wins, naive_trades = 0.0, 0, 0
    cex_only_pnl, cex_only_wins, cex_only_trades = 0.0, 0, 0

    cex_spot = 95000.0
    cl_spot = 95000.0

    for w in range(num_windows):
        window_ts = 1760000000 + w * 300
        cl_strike = cl_spot  # Polymarket locks Chainlink S_0 at exact boundary!
        base_vol = 0.00010 if rng.random() < 0.6 else 0.00019

        cex_prices: List[float] = [cex_spot]
        cl_prices: List[float] = [cl_spot]
        flows: List[float] = [0.0]
        flow = 0.0
        tracker.on_price(window_ts, cl_spot)

        for s in range(1, 301):
            flow = 0.88 * flow + rng.gauss(0.0, 0.32)
            flow = max(-1.0, min(1.0, flow))

            drift = 0.000018 * flow
            diff_shock = rng.gauss(0.0, base_vol)
            jump = 0.0
            if rng.random() < 0.0045:
                jump_dir = 1.0 if (rng.random() < 0.5 + 0.15 * flow) else -1.0
                jump = jump_dir * base_vol * rng.uniform(3.5, 7.5)

            cex_spot = cex_spot * math.exp(drift + diff_shock + jump)
            # Chainlink multi-node aggregation tracks CEX with ~3.5s exponential convergence + basis jitter
            cl_spot = cl_spot + 0.28 * (cex_spot - cl_spot) + cl_spot * rng.gauss(0.0, 0.00002)

            cex_prices.append(cex_spot)
            cl_prices.append(cl_spot)
            flows.append(flow)

        # Official settlement is STRICTLY Chainlink S_T >= Chainlink S_0
        cl_close = cl_prices[-1]
        up_won = cl_close >= cl_strike

        est_vol_1s = base_vol
        for s in range(1, 300):
            tracker.on_price(window_ts + s, cl_prices[s])
            # Refresh realized vol every 5s (5x faster simulation with identical accuracy)
            if s % 5 == 0 or s >= 175:
                est_vol_1s = tracker.get_realized_vol_1s(floor=cfg.min_1s_vol_floor)

            if s not in (30, 230) and s < 180 and our_strat.active_position is None:
                continue

            rem = 300.0 - s
            lag_idx = max(0, s - 3)
            mkt_z = math.log(cl_prices[lag_idx] / cl_strike) / (est_vol_1s * math.sqrt(max(1.0, rem)))
            mkt_mid_up = normal_cdf(0.88 * mkt_z) + rng.gauss(0.0, 0.012)
            mkt_mid_up = max(0.02, min(0.98, mkt_mid_up))

            up_bid = round(max(0.01, mkt_mid_up - 0.01), 2)
            up_ask = round(min(0.99, mkt_mid_up + 0.01), 2)
            dn_bid = round(max(0.01, (1.0 - mkt_mid_up) - 0.01), 2)
            dn_ask = round(min(0.99, (1.0 - mkt_mid_up) + 0.01), 2)
            book = OrderBookTop(up_bid=up_bid, up_ask=up_ask, down_bid=dn_bid, down_ask=dn_ask)

            # [A] Naive Early Taker at s == 30
            if s == 30 and abs(cex_prices[s] - cex_prices[0]) > 5.0:
                buy_up = cex_prices[s] > cex_prices[0]
                ask = up_ask if buy_up else dn_ask
                cost = ask + taker_fee_per_share(ask)
                shares = 25.0 / cost
                won = (buy_up and up_won) or ((not buy_up) and (not up_won))
                naive_pnl += shares * ((1.0 if won else 0.0) - cost)
                naive_trades += 1
                if won:
                    naive_wins += 1

            # [B] Blind >91c Chaser using CEX open instead of Chainlink Strike (Oracle Gap victim!)
            if s == 230:
                if 0.91 <= up_ask <= 0.96 and cex_prices[s] > cex_prices[0]:
                    cost = up_ask + taker_fee_per_share(up_ask)
                    shares = 25.0 / cost
                    cex_only_pnl += shares * ((1.0 if up_won else 0.0) - cost)
                    cex_only_trades += 1
                    if up_won:
                        cex_only_wins += 1
                elif 0.91 <= dn_ask <= 0.96 and cex_prices[s] < cex_prices[0]:
                    cost = dn_ask + taker_fee_per_share(dn_ask)
                    shares = 25.0 / cost
                    won_dn = not up_won
                    cex_only_pnl += shares * ((1.0 if won_dn else 0.0) - cost)
                    cex_only_trades += 1
                    if won_dn:
                        cex_only_wins += 1

            # [C] Our Chainlink-Anchored + Oracle-Gap Protected High-Prob Sniper
            if s >= 180 or our_strat.active_position is not None:
                fv = estimate_fair_value(
                    strike_price=cl_strike,
                    chainlink_price=cl_prices[s],
                    remaining_sec=rem,
                    vol_1s=est_vol_1s,
                    orderflow_score=flows[s],
                    cex_lead_price=cex_prices[s],
                    student_t_df=cfg.student_t_df,
                )
                if fv:
                    our_strat.evaluate_tick(window_ts + s, window_ts, float(s), fv, book)

        our_strat.settle_window(window_ts + 300.0, window_ts, cl_strike, cl_close)

    stats = our_strat.summary_stats()
    flat_25_pnl = sum(
        (25.0 / t.total_cost_per_share) * (t.exit_payout_per_share - t.total_cost_per_share)
        for t in our_strat.closed_trades
    )
    print("=" * 92)
    print(
        f" Polymarket BTC 5m Chainlink-Settled Benchmark ({num_windows} Windows = {num_windows/288:.1f} Days | $25/bet)"
    )
    print("=" * 92)
    print(
        f" [A] Naive Early Taker (t=30s, ~50c)      | Trades: {naive_trades:4d} | "
        f"WinRate: {(naive_wins/max(1,naive_trades))*100:5.1f}% | Flat PnL: {naive_pnl:+8.2f} USDC"
    )
    print(
        f" [B] CEX-Only >91c Chaser (No EV/CL Sync) | Trades: {cex_only_trades:4d} | "
        f"WinRate: {(cex_only_wins/max(1,cex_only_trades))*100:5.1f}% | Flat PnL: {cex_only_pnl:+8.2f} USDC"
    )
    print("-" * 92)
    print(
        f" [C] Chainlink High-Prob Sniper (OURS)    | Trades: {stats['trades']:4d} | "
        f"WinRate: {stats['win_rate_pct']:5.1f}% | Flat PnL: {flat_25_pnl:+8.2f} USDC"
    )
    print(
        f"     -> Avg Cost/Share : ${stats['avg_cost_per_share']:.3f}   | "
        f"Avg Model P_true : {stats['avg_model_p_true_pct']:.1f}% | "
        f"Emergency Saves: {stats['emergency_exits']}"
    )
    print(
        f"     -> Profit Factor  : {stats['profit_factor']:.2f}     | "
        f"Max Drawdown     : {stats['max_drawdown_pct']:.2f}% | "
        f"Kelly Bankroll : ${stats['final_bankroll']:,.2f}"
    )
    print("=" * 92)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest Polymarket BTC 5m Chainlink Strategy")
    parser.add_argument("--db", default="data/polymarket_btc_5m.db", help="Recorded SQLite DB path")
    parser.add_argument("--demo", action="store_true", help="Run Chainlink-settled benchmark")
    parser.add_argument("--windows", type=int, default=2000, help="Number of 5m windows for --demo")
    args = parser.parse_args()

    if args.demo:
        run_benchmark_simulation(num_windows=args.windows)
    else:
        res = replay_sqlite_db(args.db)
        print("SQLite Replay Results:", res)
