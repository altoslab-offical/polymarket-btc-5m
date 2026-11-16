"""
Unit tests for Chainlink Oracle decoders, Polymarket fee math, Student-t fat-tail pricing,
Oracle-Gap trap filtering, CLOB book parsing, and SQLite replay.
"""
import math
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.chainlink_oracle import (
    ChainlinkOracleFeed,
    decode_aggregator_v3_hex,
    decode_chainlink_report_v3_hex,
    normalize_btc_usd_price,
)
from src.fees import analyze_trade_ev, breakeven_win_prob, taker_fee_per_share
from src.feeds import LiveFeedState, extract_best_bid_ask, extract_executable_bid_ask, parse_json_or_list
from src.pricing_model import (
    RollingMicrostructureTracker,
    estimate_fair_value,
    fat_tail_prob_from_z,
    normal_cdf,
    student_t_cdf,
)
from src.storage import MarketDataStorage
from src.strategy import HighProbSniperStrategy, OrderBookTop


class TestPolymarketChainlinkQuantCore(unittest.TestCase):
    def test_chainlink_price_normalizer_and_abi_decoders(self):
        # 1. Auto-normalizer across float USD, 8-decimal AggregatorV3, and 18-decimal Data Streams
        self.assertAlmostEqual(normalize_btc_usd_price(95123.45), 95123.45, places=2)
        self.assertAlmostEqual(normalize_btc_usd_price(9512345000000), 95123.45, places=2)          # 1e8
        self.assertAlmostEqual(normalize_btc_usd_price(95123450000000000000000), 95123.45, places=2) # 1e18

        # 2. Polygon Chainlink AggregatorV3 `latestRoundData()` ABI hex decoding
        round_id = 18446744073709562301
        answer_8dec = int(96420.75 * 1e8)
        started_at = 1760000100
        updated_at = 1760000102
        hex_payload = (
            "0x"
            + f"{round_id:064x}"
            + f"{answer_8dec:064x}"
            + f"{started_at:064x}"
            + f"{updated_at:064x}"
            + f"{round_id:064x}"
        )
        decoded = decode_aggregator_v3_hex(hex_payload)
        self.assertIsNotNone(decoded)
        assert decoded is not None
        self.assertEqual(decoded[0], round_id)
        self.assertAlmostEqual(decoded[1], 96420.75, places=2)
        self.assertEqual(decoded[2], updated_at)

        # 3. Chainlink Data Streams ReportV3 hex decoding
        # Outer words: [0..2] reportContext (96B), [3] offset to reportBlob = 192 (0xc0), [4..5] dummy
        # At offset 192: length = 288 (0x120), followed by 9 words where word[2]=obs_ts, word[6]=price*1e18
        outer_header = b"\x00" * 96 + (192).to_bytes(32, "big") + b"\x00" * 64
        price_18dec = int(96500.25 * (10**18))
        blob_words = [
            b"\x01" * 32,                        # [0] feedId
            (1760000200).to_bytes(32, "big"),    # [1] validFromTimestamp
            (1760000201).to_bytes(32, "big"),    # [2] observationsTimestamp
            b"\x00" * 32,                        # [3] nativeFee
            b"\x00" * 32,                        # [4] linkFee
            (1760086600).to_bytes(32, "big"),    # [5] expiresAt
            price_18dec.to_bytes(32, "big", signed=True),  # [6] benchmarkPrice (int192)
            price_18dec.to_bytes(32, "big", signed=True),  # [7] bid
            price_18dec.to_bytes(32, "big", signed=True),  # [8] ask
        ]
        full_report_hex = "0x" + (outer_header + (288).to_bytes(32, "big") + b"".join(blob_words)).hex()
        rep_decoded = decode_chainlink_report_v3_hex(full_report_hex)
        self.assertIsNotNone(rep_decoded)
        assert rep_decoded is not None
        self.assertAlmostEqual(rep_decoded[0], 96500.25, places=2)
        self.assertEqual(rep_decoded[1], 1760000201)

        # 4. Rolling Chainlink TWAP tracker & Stale-Feed Circuit Breaker
        cl_feed = ChainlinkOracleFeed()
        cl_feed.on_chainlink_tick(95000.0, "POLY_RTDS_CHAINLINK")
        self.assertEqual(cl_feed.state.settlement_reference_price, 95000.0)
        self.assertAlmostEqual(cl_feed.state.twap_60s or 0.0, 95000.0, places=2)
        # Simulate RPC freeze / silence > 18s -> must return None rather than frozen price!
        cl_feed.state.local_recv_ts -= 25.0
        self.assertIsNone(cl_feed.state.settlement_reference_price)

    def test_taker_fee_formula(self):
        self.assertAlmostEqual(taker_fee_per_share(0.50, 0.07), 0.0175, places=5)
        self.assertAlmostEqual(taker_fee_per_share(0.80, 0.07), 0.0112, places=5)
        self.assertEqual(taker_fee_per_share(0.20, 0.07), taker_fee_per_share(0.80, 0.07))
        self.assertAlmostEqual(breakeven_win_prob(0.75, is_maker=True), 0.75, places=5)

    def test_student_t_cdf_exact_identities(self):
        for x in (-3.0, -1.0, 0.0, 0.5, 2.5):
            expected = 0.5 + math.atan(x) / math.pi
            self.assertAlmostEqual(student_t_cdf(x, df=1.0), expected, places=9)
        self.assertLess(fat_tail_prob_from_z(1.8, df=4.5), normal_cdf(1.8))

    def test_oracle_gap_trap_protection(self):
        """
        Verify that if Chainlink is +1.6 Z above Strike, BUT CEX has flash-dumped -12 bps below
        Chainlink (Oracle Gap trap!), the strategy refuses to enter UP.
        """
        strat = HighProbSniperStrategy()
        book = OrderBookTop(up_bid=0.76, up_ask=0.78, down_bid=0.20, down_ask=0.22)

        # Case 1: CEX flash-dumped to 94,980 while Chainlink is still lagging at 95,120 (-14.7 bps gap!)
        fv_trap = estimate_fair_value(
            strike_price=95000.0,
            chainlink_price=95120.0,
            remaining_sec=80.0,
            vol_1s=0.00009,
            orderflow_score=0.2,
            cex_lead_price=94980.0,
        )
        assert fv_trap is not None
        self.assertLess(fv_trap.oracle_gap_bps, -10.0)
        res_trap = strat.evaluate_tick(1760000220.0, 1760000000, 220.0, fv_trap, book)
        self.assertIsNone(res_trap)  # Blocked by Oracle-Gap filter!

        # Case 2: CEX confirms Chainlink lead (95,130 vs 95,120 => +1.05 bps gap) -> Enters cleanly!
        fv_good = estimate_fair_value(
            strike_price=95000.0,
            chainlink_price=95120.0,
            remaining_sec=80.0,
            vol_1s=0.00009,
            orderflow_score=0.2,
            cex_lead_price=95130.0,
        )
        assert fv_good is not None
        res_good = strat.evaluate_tick(1760000221.0, 1760000000, 221.0, fv_good, book)
        self.assertIsNotNone(res_good)
        self.assertIn("ENTRY UP", res_good or "")

    def test_clob_book_and_sqlite_storage(self):
        raw_book = {
            "bids": [{"price": "0.01", "size": "500"}, {"price": "0.78", "size": "120"}],
            "asks": [{"price": "0.99", "size": "500"}, {"price": "0.80", "size": "95"}],
        }
        self.assertEqual(extract_best_bid_ask(raw_book), (0.78, 0.80))
        self.assertEqual(parse_json_or_list('["Up", "Down"]'), ["Up", "Down"])

        with tempfile.TemporaryDirectory() as tmp:
            db_file = os.path.join(tmp, "test.db")
            store = MarketDataStorage(db_file)
            store.upsert_window(
                1760000100, "btc-updown-5m-1760000100", "tok_up", "tok_dn", 95000.0, "POLY_RTDS_CHAINLINK"
            )
            store.record_snapshot(
                1760000290.0, 1760000100, 190.0,
                95080.0, 95075.0, "POLY_RTDS_CHAINLINK", 95084.0, 0.42,
                95085.0, 95083.0, 0.0001, 0.4, 0.2, 1.65, 0.89, 0.79, 0.81, 0.18, 0.20,
            )
            store.finalize_window(1760000100, 95095.0, "UP")
            store.close()

    def test_utc_clock_sync_vwap_depth_and_ev_take_profit(self):
        # 1. Server UTC clock offset sync, 1s deadband & exact boundary price lookup
        cl = ChainlinkOracleFeed()
        cl.update_clock_offset(server_utc_ts=1760000318.0, local_ts=1760000300.0)
        self.assertAlmostEqual(cl.clock_offset_sec, 18.0, places=2)
        cl_small = ChainlinkOracleFeed()
        cl_small.update_clock_offset(server_utc_ts=1760000302.5, local_ts=1760000300.0)
        self.assertAlmostEqual(cl_small.clock_offset_sec, 2.5, places=2)
        cl_tiny = ChainlinkOracleFeed()
        cl_tiny.update_clock_offset(server_utc_ts=1760000300.4, local_ts=1760000300.0)
        self.assertEqual(cl_tiny.clock_offset_sec, 0.0)
        cl.on_chainlink_tick(71500.0, "POLY_RTDS_CHAINLINK", oracle_ts=1760000300.2)
        cl.on_chainlink_tick(71540.0, "POLY_RTDS_CHAINLINK", oracle_ts=1760000318.0)
        self.assertEqual(cl.get_price_near_utc(1760000300.0, max_diff_sec=5.0), 71500.0)

        # 1b. Opening CLOB midpoint calibrates stale rollover strike (~$65,378.46 -> $65,405.00)
        st_cal = LiveFeedState(current_window_ts=1794689100)
        st_cal.strike_price = 65378.46
        st_cal.strike_source = "POLY_RTDS_CHAINLINK"
        st_cal._initial_cl_strike = 65378.46
        st_cal.book.up_bid = 0.49
        st_cal.book.up_ask = 0.51
        self.assertIsNone(st_cal.calibrate_strike_from_opening_book(65405.0, 0.00004, 6.0))
        self.assertIsNone(st_cal.calibrate_strike_from_opening_book(65405.0, 0.00004, 7.0))
        calibrated = st_cal.calibrate_strike_from_opening_book(65405.0, 0.00004, 8.0)
        self.assertAlmostEqual(calibrated, 65405.0, places=2)
        self.assertAlmostEqual(st_cal.strike_price, 65405.0, places=2)
        self.assertEqual(st_cal.strike_source, "BOOK_CALIBRATED")

        # 2. Dust order rejection & VWAP depth sweep
        dust_book = {
            "bids": [{"price": "0.80", "size": "0.5"}],  # Only $0.40 dust -> must be rejected!
            "asks": [{"price": "0.82", "size": "0.5"}],
        }
        self.assertEqual(extract_executable_bid_ask(dust_book, target_usdc=25.0), (None, None))

        deep_book = {
            "bids": [{"price": "0.78", "size": "50"}],
            "asks": [{"price": "0.80", "size": "15"}, {"price": "0.82", "size": "30"}],
        }
        vb, va = extract_executable_bid_ask(deep_book, target_usdc=25.0)
        self.assertEqual(vb, 0.78)
        assert va is not None
        self.assertGreater(va, 0.80)
        self.assertLess(va, 0.82)

        # 3. Basis-neutral CEX lead price (zero jump when Coinbase drops out)
        st = LiveFeedState()
        import time as _time
        now = _time.time()
        st.coinbase_price, st.coinbase_ts = 72000.0, now
        st.binance_price, st.binance_ts = 72090.0, now  # +$90 USDT premium
        st.update_usdt_basis()
        both_lead = st.cex_lead_price
        st.coinbase_ts = now - 30.0  # Simulate Coinbase WS reconnect
        bn_only_lead = st.cex_lead_price
        assert both_lead is not None and bn_only_lead is not None
        self.assertAlmostEqual(both_lead, bn_only_lead, delta=1.0)

        # 4. EV-aware Take Profit holds $0.96 when P_true=99.4%, exits at $0.99
        strat = HighProbSniperStrategy()
        fv_entry = estimate_fair_value(95000.0, 95110.0, 90.0, 0.00009, 0.3, 95115.0)
        assert fv_entry is not None
        # Mirage guard blocks when P_true (~90.8%) - Ask (0.72) > 15%
        mirage_res = strat.evaluate_tick(99.0, 0, 209.0, fv_entry, OrderBookTop(0.70, 0.72, 0.28, 0.30))
        self.assertIsNone(mirage_res)
        # Normal entry succeeds when Ask=0.80 (divergence ~10.8% <= 15%)
        strat.evaluate_tick(100.0, 0, 210.0, fv_entry, OrderBookTop(0.78, 0.80, 0.20, 0.22))
        self.assertIsNotNone(strat.active_position)

        fv_strong = estimate_fair_value(95000.0, 95250.0, 30.0, 0.00009, 0.3, 95255.0)
        assert fv_strong is not None
        # At bid=0.96 ($0.9573 net) while P_true=99.5%, must HOLD for $1.00 rather than donating 4c!
        hold_res = strat.evaluate_tick(160.0, 0, 270.0, fv_strong, OrderBookTop(0.96, 0.97, 0.03, 0.04))
        self.assertIsNone(hold_res)
        # At bid=0.99 ($0.9893 net), locks profit cleanly!
        tp_res = strat.evaluate_tick(165.0, 0, 275.0, fv_strong, OrderBookTop(0.99, 0.995, 0.005, 0.01))
        self.assertIn("EARLY_TAKE_PROFIT", tp_res or "")

        # 5. Live CLOB V2 fill response parser & floor share rounding guard
        from src.live_executor import floor_share_qty, parse_clob_fill_response
        self.assertEqual(floor_share_qty(18.8662), 18.86)
        self.assertEqual(floor_share_qty(18.869999), 18.86)
        self.assertEqual(floor_share_qty(0.29), 0.29)
        self.assertEqual(floor_share_qty(18.87), 18.87)
        fill_ok = parse_clob_fill_response(
            {"orderID": "0xabc123", "makingAmount": "8000000", "takingAmount": "10000000", "status": "matched"},
            requested_price=0.80,
        )
        self.assertTrue(fill_ok.filled)
        self.assertAlmostEqual(fill_ok.filled_shares, 10.0, places=2)
        self.assertAlmostEqual(fill_ok.avg_price, 0.80, places=2)
        fill_err = parse_clob_fill_response({"errorMsg": "not enough balance / allowance"}, requested_price=0.80)
        self.assertFalse(fill_err.filled)

        # 6. Anti-OBI-spoofing CVD guard (blocks DOWN when real market CVD is positive buying)
        strat2 = HighProbSniperStrategy()
        fv_spoof = estimate_fair_value(
            68104.34, 68050.0, 80.0, 0.000050, -0.09, 68048.0, cvd_imbalance=0.26
        )
        assert fv_spoof is not None
        self.assertIsNone(strat2.evaluate_tick(220.0, 100, 220.0, fv_spoof, OrderBookTop(0.18, 0.20, 0.79, 0.80)))

    def test_telegram_bot_formatting_and_control_state(self):
        import time as _time
        from src.tg_notifier import (
            TelegramBotService,
            TelegramControlState,
            build_report_message,
            build_status_message,
            build_trades_message,
        )

        with tempfile.TemporaryDirectory() as tmp:
            db_file = os.path.join(tmp, "live_test.db")
            ctrl_file = os.path.join(tmp, ".tg_control.json")
            from pathlib import Path
            ctrl = TelegramControlState(Path(ctrl_file))

            now = _time.time()
            wts = (int(now) // 300) * 300
            store = MarketDataStorage(db_file)
            store.upsert_window(wts, f"btc-updown-5m-{wts}", "tok_u", "tok_d", 67000.0, "POLY_RTDS_CHAINLINK")
            store.record_snapshot(
                now, wts, 180.0, 67090.0, 67080.0, "POLY_RTDS_CHAINLINK",
                67095.0, 0.75, 67100.0, 67092.0, 0.00008, 0.45, 0.30, 1.75, 0.92,
                0.78, 0.80, 0.19, 0.21,
            )
            tid1 = store.insert_paper_trade(wts - 300, "UP", now - 250.0, 160.0, 31.57, 0.78, 0.012, 0.90, 1.55)
            store.close_paper_trade(tid1, now - 180.0, "EARLY_TAKE_PROFIT", 0.97863, 5.89, True)
            store.insert_paper_trade(wts, "UP", now - 5.0, 175.0, 30.82, 0.80, 0.0112, 0.92, 1.75)
            store.close()

            status_txt = build_status_message(db_file, live_mode=True, bet_usdc=25.0, paused=False)
            self.assertIn("即時盤況", status_txt)
            self.assertIn("67,000.00", status_txt)
            self.assertIn("POLY_RTDS_CHAINLINK", status_txt)
            self.assertIn("持倉中 #2", status_txt)
            self.assertNotIn("\\$", status_txt)

            report_txt = build_report_message(db_file)
            self.assertIn("1 勝 0 敗", report_txt)
            self.assertIn("+5.89 pUSD", report_txt)
            self.assertIn("1 連勝", report_txt)
            self.assertNotIn("\\$", report_txt)

            trades_txt = build_trades_message(db_file, limit=10)
            self.assertIn("#1", trades_txt)
            self.assertIn("#2", trades_txt)
            self.assertIn("止盈", trades_txt)
            self.assertNotIn("\\$", trades_txt)

            # Command auto-bind, authorization, pause/resume & /bet control
            env_file = Path(os.path.join(tmp, ".env"))
            env_file.write_text("TG_BOT_TOKEN=123456:TESTTOKEN\n", encoding="utf-8")
            svc = TelegramBotService(
                db_file, live_mode=True, default_bet_usdc=25.0, env_path=env_file, control_state=ctrl
            )
            svc.bot_token = "123456:TESTTOKEN"
            svc.chat_id = None
            sent_replies = []
            svc.send_message_sync = lambda text, chat_id=None, with_keyboard=True: (  # type: ignore
                sent_replies.append((chat_id, text)) or True
            )

            svc._handle_message_sync({"chat": {"id": 999888}, "text": "/start", "date": now})
            self.assertEqual(svc.chat_id, "999888")
            self.assertEqual(ctrl.load()["chat_id"], "999888")

            # Unauthorized stranger must be ignored
            svc._handle_message_sync({"chat": {"id": 111222}, "text": "/pause", "date": now + 1})
            self.assertFalse(ctrl.load()["paused"])

            # Authorized owner commands work (both new tactical HUD buttons and legacy buttons)
            svc._handle_message_sync({"chat": {"id": 999888}, "text": "▸ 即時盤況", "date": now + 1.5})
            svc._handle_message_sync({"chat": {"id": 999888}, "text": "■ 暫停狙擊", "date": now + 2})
            self.assertTrue(ctrl.load()["paused"])
            svc._handle_message_sync({"chat": {"id": 999888}, "text": "/bet 40", "date": now + 3})
            self.assertEqual(ctrl.load()["bet_usdc"], 40.0)
            svc._handle_message_sync({"chat": {"id": 999888}, "text": "▶ 恢復狙擊", "date": now + 4})
            self.assertFalse(ctrl.load()["paused"])

            # Verify notify_entry & notify_exit tactical HUD formatting
            svc.notify_entry(22, "UP", wts, 185.0, 31.17, 0.79, 0.802, 0.934, 1.52, live_mode=True, order_id="0xorder123")
            entry_msg = sent_replies[-1][1]
            self.assertIn("SNIPE EXECUTED // 實盤進場 #22", entry_msg)
            self.assertIn("LONG [UP 買漲]", entry_msg)
            self.assertNotIn("\\$", entry_msg)

            svc.notify_exit(22, "UP", wts, "EARLY_TAKE_PROFIT", 5.16, True, 0.978, 0.79, live_mode=True)
            exit_msg = sent_replies[-1][1]
            self.assertIn("TARGET HIT // 獲利止盈 #22", exit_msg)
            self.assertIn("+5.16 pUSD", exit_msg)
            self.assertNotIn("\\$", exit_msg)

            # Subscriber friend joins with /start -> read-only access & broadcast pushes
            svc._handle_message_sync({"chat": {"id": 777666, "first_name": "Alex"}, "text": "/start", "date": now + 5})
            self.assertIn("777666", ctrl.load()["subscriber_ids"])

            # Subscriber can query /report but CANNOT pause or alter bet size
            before_friend = len(sent_replies)
            svc._handle_message_sync({"chat": {"id": 777666}, "text": "▸ 戰績報表", "date": now + 6})
            self.assertEqual(sent_replies[-1][0], "777666")
            self.assertIn("1 勝 0 敗", sent_replies[-1][1])

            svc._handle_message_sync({"chat": {"id": 777666}, "text": "■ 暫停狙擊", "date": now + 7})
            self.assertFalse(ctrl.load()["paused"])
            self.assertIn("ACCESS DENIED // 權限不足", sent_replies[-1][1])

            svc._handle_message_sync({"chat": {"id": 777666}, "text": "/bet 100", "date": now + 8})
            self.assertEqual(ctrl.load()["bet_usdc"], 40.0)
            self.assertIn("ACCESS DENIED // 權限不足", sent_replies[-1][1])

            # Entry push must broadcast to both owner (999888) and subscribed friend (777666)
            before_push = len(sent_replies)
            svc.notify_entry(23, "DOWN", wts, 190.0, 31.25, 0.80, 0.811, 0.940, -1.65, live_mode=True)
            push_targets = {cid for cid, _ in sent_replies[before_push:]}
            self.assertEqual(push_targets, {"999888", "777666"})

            # Cross-process deduplication: standalone watcher sharing .tg_control.json must not double-push
            svc2 = TelegramBotService(
                db_file, live_mode=True, default_bet_usdc=25.0, env_path=env_file, control_state=ctrl
            )
            svc2.bot_token = "123456:TESTTOKEN"
            svc2.send_message_sync = svc.send_message_sync  # type: ignore
            before_dup = len(sent_replies)
            svc2.notify_entry(23, "DOWN", wts, 190.0, 31.25, 0.80, 0.811, 0.940, -1.65, live_mode=True)
            svc2.notify_exit(22, "UP", wts, "EARLY_TAKE_PROFIT", 5.16, True, 0.978, 0.79, live_mode=True)
            self.assertEqual(len(sent_replies), before_dup)

            # Owner can remove subscriber with /unsub <id>
            svc._handle_message_sync({"chat": {"id": 999888}, "text": "/unsub 777666", "date": now + 9})
            self.assertNotIn("777666", ctrl.load()["subscriber_ids"])

            self.assertGreaterEqual(len(sent_replies), 10)


if __name__ == "__main__":
    unittest.main()
