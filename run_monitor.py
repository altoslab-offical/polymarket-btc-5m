"""
Live Polymarket BTC 5m Data Recorder + Chainlink Fair-Value Monitor + Shadow Paper Trader.

Usage:
    python run_monitor.py                  # Default: $1,000 bankroll, $25 USDC fixed bet per signal
"""
import argparse
import asyncio
from dataclasses import replace
import shutil
import sys
import time
from typing import Optional
import unicodedata

from src.chainlink_oracle import has_websockets_installed
from src.config import DEFAULT_CONFIG
from src.fees import effective_exit_proceeds, taker_fee_per_share
from src.feeds import LiveMarketHub, fetch_5m_market_metadata
from src.live_executor import PolymarketLiveExecutor
from src.pricing_model import RollingMicrostructureTracker, estimate_fair_value
from src.storage import MarketDataStorage
from src.strategy import HighProbSniperStrategy
from src.tg_notifier import TelegramBotService

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _fit_console_line(text: str) -> str:
    """Pad/clip single-line `\r` status output so it NEVER wraps and scrolls in narrow PowerShell windows!"""
    max_cols = max(60, shutil.get_terminal_size((120, 24)).columns - 2)
    out = []
    cols = 0
    for ch in text:
        w = 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        if cols + w > max_cols:
            break
        out.append(ch)
        cols += w
    if cols < max_cols:
        out.append(" " * min(4, max_cols - cols))
    return "".join(out)


async def resolve_official_polymarket_outcome(
    hub: LiveMarketHub,
    storage: MarketDataStorage,
    strategy: HighProbSniperStrategy,
    prev_window_ts: int,
    prev_strike: Optional[float],
    cl_close: Optional[float],
    last_up_bid: Optional[float],
    last_down_bid: Optional[float],
    active_db_trade_id: Optional[int],
    live_exec: Optional[PolymarketLiveExecutor] = None,
    tg: Optional[TelegramBotService] = None,
) -> None:
    """
    Wait ~6-15s in background after 5m close to verify Polymarket Gamma API's OFFICIAL settled
    outcome (`outcomePrices`), avoiding false `UP` ties when an on-chain push feed didn't tick!
    """
    loop = asyncio.get_running_loop()
    official_winner: Optional[str] = None

    for delay in (8.0, 15.0, 22.0):
        await asyncio.sleep(delay)
        meta = await loop.run_in_executor(None, fetch_5m_market_metadata, prev_window_ts, hub._http)
        if meta and meta.get("resolved_winner"):
            official_winner = meta["resolved_winner"]
            if meta.get("official_strike"):
                prev_strike = meta["official_strike"]
            break

    # Determine final winner priority:
    # 1. Polymarket Gamma official settled outcome (100% authoritative Chainlink Data Streams result)
    # 2. Final CLOB orderbook consensus comparing BOTH sides (whichever ended higher >= $0.94)
    # 3. Live Chainlink comparison ONLY if price actually moved (cl_close != prev_strike)
    winner_src = "GAMMA_OFFICIAL"
    if official_winner is None:
        u_bid = last_up_bid or 0.0
        d_bid = last_down_bid or 0.0
        if max(u_bid, d_bid) >= 0.94 and abs(u_bid - d_bid) >= 0.20:
            official_winner = "UP" if u_bid > d_bid else "DOWN"
            winner_src = "CLOB_CONSENSUS"
        elif cl_close is not None and prev_strike is not None and abs(cl_close - prev_strike) > 0.01:
            official_winner = "UP" if cl_close >= prev_strike else "DOWN"
            winner_src = hub.chainlink.state.source
        else:
            print(f"\n>>> [5m 結算略過 {prev_window_ts}] 無法確認官方結算方向 (預言機未跳動且 Gamma 尚未公佈)")
            return

    now_utc = hub.utc_now()
    eff_strike = prev_strike or cl_close or 0.0
    # Construct synthetic settlement price consistent with official_winner for strategy accounting
    if official_winner == "UP":
        eff_close = cl_close if (cl_close and cl_close >= eff_strike) else (eff_strike + 1.0)
    else:
        eff_close = cl_close if (cl_close and cl_close < eff_strike) else (eff_strike - 1.0)

    storage.finalize_window(prev_window_ts, cl_close or eff_close, official_winner)
    settle_msg = strategy.settle_window(now_utc, prev_window_ts, eff_strike, eff_close)

    s0_str = f"{prev_strike:,.2f}" if prev_strike else "N/A"
    st_str = f"{cl_close:,.2f}" if cl_close else "N/A"
    print(
        f"\n>>> [5m 官方結算 {prev_window_ts}] S0={s0_str} -> ST={st_str} "
        f"=> 結果: {official_winner} (裁判來源: {winner_src})"
    )
    if settle_msg:
        print(f">>> {settle_msg}")
        if active_db_trade_id is not None and strategy.closed_trades:
            last = strategy.closed_trades[-1]
            if live_exec is not None:
                live_exec.record_realized_pnl(last.net_pnl_usdc)
            storage.close_paper_trade(
                active_db_trade_id,
                now_utc,
                last.exit_reason,
                last.exit_payout_per_share,
                last.net_pnl_usdc,
                last.won,
            )
            if tg is not None and tg.enabled:
                tg.notify_exit(
                    trade_id=active_db_trade_id,
                    side=last.side,
                    window_ts=last.window_ts,
                    exit_reason=last.exit_reason,
                    net_pnl_usdc=last.net_pnl_usdc,
                    won=last.won,
                    exit_payout_per_share=last.exit_payout_per_share,
                    entry_price=last.entry_price,
                    live_mode=(live_exec is not None),
                )
                if live_exec is not None and live_exec.circuit_broken:
                    tg.notify_alert(
                        "實盤風控熔斷觸發",
                        f"今日實盤累計虧損已達上限 (-${live_exec.max_daily_loss_usdc:.1f} pUSD)，已自動停止開新倉！",
                    )
        stats = strategy.summary_stats()
        tag = "累積實盤戰績" if live_exec is not None else "累積模擬戰績"
        unit = "pUSD" if live_exec is not None else "USDC"
        print(
            f">>> [{tag}] 總單數={stats['trades']} | "
            f"勝率={stats['win_rate_pct']:.1f}% | "
            f"總損益={stats['net_pnl_usdc']:+.2f} {unit} | "
            f"帳戶餘額=${stats['final_bankroll']:,.2f}"
        )
        if live_exec is not None and strategy.closed_trades and strategy.closed_trades[-1].won:
            print(">>> 💡 [實盤提示] 到期獲勝注單請記得定期至 Polymarket 網頁點擊 Claim 領回 pUSD 餘額。")


async def _bg_prefetch_cond_balance(live_exec: PolymarketLiveExecutor, token_id: str) -> None:
    """Wait 5s after entry for Polygon settlement, then pre-cache exact conditional token balance for 0ms exits."""
    await asyncio.sleep(5.0)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, live_exec.sync_conditional_balance, token_id)


async def _bg_warmup_window(
    live_exec: PolymarketLiveExecutor,
    up_token: Optional[str],
    down_token: Optional[str],
) -> None:
    """Pre-warm CLOB V2 market metadata & HTTP/2 socket in background so sniper orders fire in ~150ms instead of ~4.1s."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, live_exec.warmup_tokens, up_token, down_token)


async def monitor_loop(
    db_path: str,
    bankroll: float,
    bet_usdc: float,
    live_mode: bool = False,
    max_daily_loss: float = 25.0,
) -> None:
    live_exec: Optional[PolymarketLiveExecutor] = None
    if live_mode:
        live_exec = PolymarketLiveExecutor(max_daily_loss_usdc=max_daily_loss)
        live_bal = live_exec.get_pusd_balance()
        if live_bal < max(1.0, bet_usdc):
            raise RuntimeError(
                f"實盤 pUSD 餘額不足 (${live_bal:,.2f} < 下注金額 ${bet_usdc:,.2f})！請先用 python check_account.py 確認餘額。"
            )
        bankroll = live_bal

    cfg = replace(
        DEFAULT_CONFIG,
        initial_bankroll_usdc=bankroll,
        fixed_bet_usdc=bet_usdc,
    )
    tracker = RollingMicrostructureTracker(
        vol_lookback_sec=cfg.vol_lookback_sec,
        cvd_window_sec=cfg.cvd_window_sec,
    )
    hub = LiveMarketHub(tracker=tracker, target_usdc=bet_usdc if bet_usdc > 0 else 25.0)
    storage = MarketDataStorage(db_path=db_path)
    strategy = HighProbSniperStrategy(config=cfg)
    tg = TelegramBotService(
        db_path=db_path,
        live_mode=live_mode,
        live_exec=live_exec,
        default_bet_usdc=bet_usdc,
    )
    last_applied_bet_ts = time.time()
    if tg.enabled:
        tg.clear_stale_bet_override()

    ws_ok = has_websockets_installed()
    sizing_desc = f"Fixed ${bet_usdc:.1f} USDC / trade" if bet_usdc > 0 else "1/5 Fractional Kelly"
    mode_tag = "🔴 LIVE 實盤自動交易 (pUSD)" if live_mode else "🟢 PAPER 模擬盤與錄製"

    print("=" * 98)
    print(f" Polymarket BTC 5m [Chainlink-Oracle High-Prob Sniper] v4.2 | {mode_tag}")
    print(f" 資料庫路徑 : {db_path}")
    print(f" 交易本金   : ${bankroll:,.2f} {'pUSD' if live_mode else 'USDC'} | 下注模式: {sizing_desc}")
    if live_mode:
        print(f" 實盤風控鎖 : 單日最大虧損熔斷 -${max_daily_loss:.1f} pUSD | 全程 FAK 限價不追高 (上限 ${cfg.max_entry_price:.2f})")
    print(
        f" 連線模式   : {'WebSocket 即時串流 + 伺服器 UTC 自動校時 + VWAP 深度過濾' if ws_ok else '純 HTTPS REST 輪詢模式 (建議執行 pip install websockets)'}"
    )
    print(" 預言機來源 : Chainlink BTC/USD (RTDS / Arbitrum L2 + Polygon 8節點輪詢 + Gamma 官方結算核對)")
    if tg.enabled:
        tg_bind_msg = f"已綁定 Chat ID ({tg.chat_id})" if tg.chat_id else "請在 Telegram 對機器人傳送 /start 完成自動綁定"
        print(f" 📱 Telegram 監控 : 已啟用 | {tg_bind_msg}")
    else:
        print(" 📱 Telegram 監控 : 未設定 TG_BOT_TOKEN (可隨時執行 python tg_bot.py 設定)")
    print("=" * 98)

    feed_task = asyncio.create_task(hub.start())
    tg_task: Optional[asyncio.Task] = None
    if tg.enabled:
        tg_task = asyncio.create_task(tg.poll_loop(poll_commands=True, watch_db_changes=False))
        if tg.chat_id:
            tg.send_async(
                f"<b>[SYSTEM ONLINE // {'LIVE 實盤狙擊' if live_mode else 'PAPER 模擬監控'}]</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"▸ <b>單筆規模</b>：<b>${bet_usdc:.1f} U</b>\n"
                f"▸ <b>單日熔斷</b>：<b>-${max_daily_loss:.1f} U</b>"
            )
    prev_window_ts: Optional[int] = None
    prev_strike: Optional[float] = None
    last_up_bid: Optional[float] = None
    last_down_bid: Optional[float] = None
    active_db_trade_id: Optional[int] = None
    last_live_attempt_ts: float = 0.0
    last_warmup_ts: float = 0.0
    last_ka_dispatch_ts: float = 0.0
    bg_tasks: set[asyncio.Task] = set()

    try:
        while True:
            now = hub.utc_now()
            st = hub.state
            cl = hub.chainlink.state
            window_ts = (int(now) // 300) * 300
            elapsed = now - window_ts
            remaining = max(0.0, 300.0 - elapsed)

            ctrl = tg.get_control_state()
            is_tg_paused = bool(ctrl.get("paused", False))
            ctrl_bet = ctrl.get("bet_usdc")
            ctrl_bet_ts = float(ctrl.get("bet_updated_at") or 0.0)
            if (
                ctrl_bet is not None
                and float(ctrl_bet) > 0.0
                and ctrl_bet_ts > last_applied_bet_ts
                and abs(float(ctrl_bet) - cfg.fixed_bet_usdc) > 1e-6
            ):
                last_applied_bet_ts = ctrl_bet_ts
                new_bet = float(ctrl_bet)
                cfg = replace(cfg, fixed_bet_usdc=new_bet)
                strategy.cfg = cfg
                hub.target_usdc = new_bet
                tg.bet_usdc = new_bet
                print(f"\n📱 [Telegram 遠端調整] 單筆下注金額已更新為 ${new_bet:.1f} U")

            # Trigger non-blocking official Gamma + Chainlink settlement verification on rollover
            if prev_window_ts is not None and window_ts != prev_window_ts:
                t = asyncio.create_task(
                    resolve_official_polymarket_outcome(
                        hub=hub,
                        storage=storage,
                        strategy=strategy,
                        prev_window_ts=prev_window_ts,
                        prev_strike=prev_strike,
                        cl_close=cl.settlement_reference_price,
                        last_up_bid=last_up_bid,
                        last_down_bid=last_down_bid,
                        active_db_trade_id=active_db_trade_id,
                        live_exec=live_exec,
                        tg=tg,
                    )
                )
                bg_tasks.add(t)
                t.add_done_callback(bg_tasks.discard)
                active_db_trade_id = None
                prev_strike = None
                last_up_bid, last_down_bid = None, None

            prev_window_ts = window_ts
            # Synchronously align 5m window state BEFORE reading strike_price (prevents stale strike leak!)
            hub.sync_window_boundary(now)
            if st.strike_price is not None:
                prev_strike = st.strike_price
            if st.book.up_bid is not None:
                last_up_bid = st.book.up_bid
            elif st.book.up_ask is not None and st.book.up_ask <= 0.10:
                last_up_bid = 0.0
            if st.book.down_bid is not None:
                last_down_bid = st.book.down_bid
            elif st.book.down_ask is not None and st.book.down_ask <= 0.10:
                last_down_bid = 0.0

            cl_spot = cl.settlement_reference_price
            cex_lead = st.cex_lead_price
            vol_1s = tracker.get_realized_vol_1s(floor=cfg.min_1s_vol_floor)
            calibrated_strike = st.calibrate_strike_from_opening_book(cl_spot, vol_1s, elapsed)
            if calibrated_strike is not None:
                prev_strike = calibrated_strike
                print(
                    f"\n🎯 [開盤盤口自動校正 Strike] {window_ts} 基準價已校準為 ${calibrated_strike:,.2f} (BOOK_CALIBRATED)"
                )
            cvd_imb, obi, flow_score = tracker.get_orderflow_metrics(now)
            dyn_gap_bps = tracker.update_oracle_basis(cl_spot, cex_lead)

            storage.upsert_window(
                window_ts=window_ts,
                slug=f"btc-updown-5m-{window_ts}",
                up_token_id=st.up_token,
                down_token_id=st.down_token,
                strike_price=st.strike_price,
                strike_source=st.strike_source,
            )

            # Pre-warm new 5m token metadata in background during WAIT phase & keep HTTP/2 socket hot in SNIPE phase
            if live_exec is not None:
                if (
                    st.up_token
                    and st.down_token
                    and (
                        st.up_token not in live_exec._warmed_tokens
                        or st.down_token not in live_exec._warmed_tokens
                    )
                    and (now - last_warmup_ts) >= 5.0
                ):
                    last_warmup_ts = now
                    wt = asyncio.create_task(_bg_warmup_window(live_exec, st.up_token, st.down_token))
                    bg_tasks.add(wt)
                    wt.add_done_callback(bg_tasks.discard)
                elif 90.0 <= elapsed <= 265.0 and (now - last_ka_dispatch_ts) >= 22.0:
                    last_ka_dispatch_ts = now
                    asyncio.get_running_loop().run_in_executor(None, live_exec.keepalive_if_needed, 20.0)

            fv = None
            if cl_spot is not None and st.strike_price is not None:
                fv = estimate_fair_value(
                    strike_price=st.strike_price,
                    chainlink_price=cl_spot,
                    remaining_sec=remaining,
                    vol_1s=vol_1s,
                    orderflow_score=flow_score,
                    cex_lead_price=cex_lead,
                    dynamic_gap_bps=dyn_gap_bps,
                    student_t_df=cfg.student_t_df,
                    cvd_imbalance=cvd_imb,
                )

            storage.record_snapshot(
                ts=now,
                window_ts=window_ts,
                elapsed_sec=elapsed,
                chainlink_price=cl_spot,
                chainlink_twap_60s=cl.twap_60s,
                chainlink_source=cl.source,
                cex_lead_price=cex_lead,
                oracle_gap_bps=fv.oracle_gap_bps if fv else None,
                binance_price=st.binance_price,
                coinbase_price=st.coinbase_price,
                vol_1s=vol_1s,
                cvd_imb=cvd_imb,
                obi=obi,
                z_score=fv.raw_z_score if fv else None,
                p_true_up=fv.p_true_up if fv else None,
                up_bid=st.book.up_bid,
                up_ask=st.book.up_ask,
                down_bid=st.book.down_bid,
                down_ask=st.book.down_ask,
            )

            if fv is not None:
                prev_pos = strategy.active_position
                action = strategy.evaluate_tick(now, window_ts, elapsed, fv, st.book)
                if action:
                    if (
                        prev_pos is None
                        and strategy.active_position is not None
                        and strategy.active_position.window_ts == window_ts
                        and active_db_trade_id is None
                    ):
                        if is_tg_paused:
                            strategy.active_position = None
                            strategy.traded_windows.discard(window_ts)
                            await asyncio.sleep(1.0)
                            continue

                        p = strategy.active_position
                        live_order_id = ""
                        if live_exec is not None:
                            abort_live_entry = False
                            cl_age = time.time() - cl.local_recv_ts
                            if cl.source != "POLY_RTDS_CHAINLINK" or cl_age > 4.5:
                                abort_live_entry = True
                            elif (now - last_live_attempt_ts) < 5.0:
                                abort_live_entry = True
                            else:
                                last_live_attempt_ts = now
                                target_token = st.up_token if p.side == "UP" else st.down_token
                                if not target_token:
                                    abort_live_entry = True
                                else:
                                    fill = live_exec.execute_entry(
                                        token_id=target_token,
                                        ask_price=p.entry_price,
                                        shares=p.shares,
                                        max_price=cfg.max_entry_price,
                                        p_true=p.p_true_at_entry,
                                        fee_rate=cfg.crypto_taker_fee_rate,
                                    )
                                    if not fill.filled or fill.filled_shares <= 0.0:
                                        print(f"\n⚠️ [LIVE FAK 未成交] {fill.error or '流動性不足'}，已自動取消不追價。")
                                        abort_live_entry = True
                                    else:
                                        live_order_id = fill.order_id
                                        p.shares = fill.filled_shares
                                        if fill.avg_price > 0:
                                            p.entry_price = fill.avg_price
                                            p.fee_per_share = taker_fee_per_share(
                                                p.entry_price, cfg.crypto_taker_fee_rate
                                            )
                                            p.total_cost_per_share = p.entry_price + p.fee_per_share
                                        pf_task = asyncio.create_task(
                                            _bg_prefetch_cond_balance(live_exec, target_token)
                                        )
                                        bg_tasks.add(pf_task)
                                        pf_task.add_done_callback(bg_tasks.discard)
                                        print(
                                            f"\n🔴 [LIVE 實盤成交] order={fill.order_id[:12]} | "
                                            f"shares={p.shares:.2f} @ ${p.entry_price:.3f} (含費成本 ${p.total_cost_per_share:.4f})"
                                        )
                            if abort_live_entry:
                                strategy.active_position = None
                                strategy.traded_windows.discard(window_ts)
                                await asyncio.sleep(1.0)
                                continue

                        print(f"\n*** {action} ***")
                        active_db_trade_id = storage.insert_paper_trade(
                            window_ts=p.window_ts,
                            side=p.side,
                            entry_ts=p.entry_ts,
                            entry_elapsed_sec=p.entry_elapsed_sec,
                            shares=p.shares,
                            entry_price=p.entry_price,
                            entry_fee_per_share=p.fee_per_share,
                            p_true_at_entry=p.p_true_at_entry,
                            z_at_entry=p.z_at_entry,
                        )
                        if tg.enabled:
                            tg.notify_entry(
                                trade_id=active_db_trade_id,
                                side=p.side,
                                window_ts=p.window_ts,
                                elapsed_sec=p.entry_elapsed_sec,
                                shares=p.shares,
                                entry_price=p.entry_price,
                                total_cost_per_share=p.total_cost_per_share,
                                p_true_at_entry=p.p_true_at_entry,
                                z_at_entry=p.z_at_entry,
                                live_mode=live_mode,
                                order_id=live_order_id,
                            )
                    elif prev_pos is not None and strategy.active_position is None and active_db_trade_id is not None and strategy.closed_trades:
                        last = strategy.closed_trades[-1]
                        if live_exec is not None:
                            target_token = st.up_token if last.side == "UP" else st.down_token
                            bid_px = st.book.up_bid if last.side == "UP" else st.book.down_bid
                            slip = 0.05 if "STOP_LOSS" in last.exit_reason else 0.03
                            exit_fill = (
                                live_exec.execute_exit(
                                    token_id=target_token,
                                    bid_price=bid_px,
                                    shares=last.shares,
                                    slippage_cents=slip,
                                )
                                if (target_token and bid_px)
                                else None
                            )
                            if exit_fill is None or not exit_fill.filled or exit_fill.filled_shares <= 0.0:
                                err_msg = exit_fill.error if exit_fill else "無可用買盤"
                                print(f"\n⚠️ [LIVE 平倉未成交] {err_msg}，保留真實持倉繼續盯盤！")
                                strategy.closed_trades.pop()
                                strategy.bankroll -= last.net_pnl_usdc
                                strategy.active_position = prev_pos
                                await asyncio.sleep(1.0)
                                continue
                            if exit_fill.filled_shares < prev_pos.shares * 0.90:
                                print(
                                    f"\n🔴 [LIVE 部分平倉] filled={exit_fill.filled_shares:.2f}/{prev_pos.shares:.2f} @ ${exit_fill.avg_price:.3f}，剩餘部位繼續盯盤！"
                                )
                                strategy.closed_trades.pop()
                                strategy.bankroll -= last.net_pnl_usdc
                                prev_pos.shares = round(prev_pos.shares - exit_fill.filled_shares, 2)
                                strategy.active_position = prev_pos
                                await asyncio.sleep(1.0)
                                continue
                            if exit_fill.avg_price > 0:
                                real_payout = effective_exit_proceeds(
                                    exit_fill.avg_price,
                                    is_maker=False,
                                    fee_rate=cfg.crypto_taker_fee_rate,
                                )
                                old_pnl = last.net_pnl_usdc
                                last.exit_payout_per_share = real_payout
                                last.net_pnl_usdc = last.shares * (real_payout - last.total_cost_per_share)
                                last.won = last.net_pnl_usdc > 0
                                strategy.bankroll += (last.net_pnl_usdc - old_pnl)
                            print(
                                f"\n🔴 [LIVE 實盤平倉] {last.exit_reason} | filled={exit_fill.filled_shares:.2f} "
                                f"@ ${exit_fill.avg_price:.3f} | 實得淨損益={last.net_pnl_usdc:+.2f} pUSD"
                            )
                            live_exec.record_realized_pnl(last.net_pnl_usdc)
                            if live_exec.circuit_broken:
                                print(f"\n🛑 [風控熔斷] 今日實盤累計虧損已達上限 (-${max_daily_loss:.1f})，已自動停止新開倉！")

                        print(f"\n*** {action} ***")
                        storage.close_paper_trade(
                            active_db_trade_id,
                            now,
                            last.exit_reason,
                            last.exit_payout_per_share,
                            last.net_pnl_usdc,
                            last.won,
                        )
                        if tg.enabled:
                            tg.notify_exit(
                                trade_id=active_db_trade_id,
                                side=last.side,
                                window_ts=last.window_ts,
                                exit_reason=last.exit_reason,
                                net_pnl_usdc=last.net_pnl_usdc,
                                won=last.won,
                                exit_payout_per_share=last.exit_payout_per_share,
                                entry_price=last.entry_price,
                                live_mode=live_mode,
                            )
                            if live_exec is not None and live_exec.circuit_broken:
                                tg.notify_alert(
                                    "實盤風控熔斷觸發",
                                    f"今日實盤累計虧損已達上限 (-${max_daily_loss:.1f} pUSD)，已自動停止開新倉！",
                                )
                        active_db_trade_id = None

                up_ask_str = f"{st.book.up_ask:.2f}" if st.book.up_ask else " -- "
                dn_ask_str = f"{st.book.down_ask:.2f}" if st.book.down_ask else " -- "
                up_edge = (
                    (fv.p_true_up - st.book.up_ask - taker_fee_per_share(st.book.up_ask)) * 100
                    if (st.book.up_ask and st.book.up_ask >= 0.05)
                    else -99.0
                )
                dn_edge = (
                    (fv.p_true_down - st.book.down_ask - taker_fee_per_share(st.book.down_ask)) * 100
                    if (st.book.down_ask and st.book.down_ask >= 0.05)
                    else -99.0
                )
                best_edge = max(up_edge, dn_edge)
                edge_str = f"{best_edge:+5.1f}%" if best_edge > -90 else "  --  "
                diff = cl_spot - st.strike_price
                zone_tag = (
                    "SNIPE"
                    if (cfg.entry_start_elapsed_sec <= elapsed <= cfg.entry_end_elapsed_sec)
                    else "WAIT "
                )
                line = (
                    f"[{zone_tag} 剩{int(remaining):3d}s] CL={cl_spot:,.2f} "
                    f"(差={diff:+6.1f} | Gap={fv.oracle_gap_bps:+5.1f}bp | Z={fv.raw_z_score:+5.2f}) | "
                    f"P(Up)={fv.p_true_up*100:5.1f}% | Ask={up_ask_str}/{dn_ask_str} | Edge={edge_str}"
                )
                print("\r" + _fit_console_line(line), end="", flush=True)
            else:
                cl_str = f"${cl_spot:,.2f}" if cl_spot else "斷線重連中..."
                cex_str = f"${cex_lead:,.2f}" if cex_lead else "連線中..."
                line = f"[等待整點鎖定 Strike | 剩 {int(remaining):3d}s 開新盤] Chainlink={cl_str} ({cl.source}) | CEX={cex_str}"
                print("\r" + _fit_console_line(line), end="", flush=True)

            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        print("\n已安全停止模擬盤監控。")
    finally:
        hub.stop()
        tg.stop()
        feed_task.cancel()
        if tg_task is not None:
            tg_task.cancel()
        storage.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket BTC 5m Chainlink 模擬盤與實盤自動交易")
    parser.add_argument("--db", default="data/polymarket_btc_5m.db", help="SQLite 資料庫路徑")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="模擬初始總本金 (USDC)")
    parser.add_argument("--bet", type=float, default=25.0, help="每筆固定下注金額 USDC (設 0 則改用 1/5 凱利公式)")
    parser.add_argument("--live", action="store_true", help="啟用真實 pUSD 實盤自動下單 (需先通過 check_account.py)")
    parser.add_argument("--max-daily-loss", type=float, default=50.0, help="實盤單日最大虧損熔斷金額 (pUSD)")
    args = parser.parse_args()
    asyncio.run(monitor_loop(args.db, args.bankroll, args.bet, live_mode=args.live, max_daily_loss=args.max_daily_loss))
