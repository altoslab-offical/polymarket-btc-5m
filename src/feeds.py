"""
Real-time Asynchronous Market Data Hub:
1. Chainlink BTC/USD Oracle Engine (`ChainlinkOracleFeed`) -> Authoritative source for
   Strike Price (S_0), Live Oracle Reference Price (S_t), and 60s Chainlink TWAP.
2. Binance + Coinbase WebSockets WITH automatic HTTPS REST fallback (`requests.Session`) ->
   Guarantees `cex_lead_price`, `oracle_gap_bps`, and `OBI` work even if `websockets` is missing!
3. Polymarket Gamma API + CLOB REST/WebSocket -> Active `btc-updown-5m-{ts}` tokens, Best Bid/Ask,
   AND official Gamma `outcomePrices` settlement verification!
"""
import asyncio
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import json
import math
from statistics import NormalDist
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .chainlink_oracle import ChainlinkOracleFeed, has_websockets_installed
from .pricing_model import RollingMicrostructureTracker
from .strategy import OrderBookTop

_STD_NORM = NormalDist(0.0, 1.0)


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
POLY_CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker"
BINANCE_REST_BOOK_URL = "https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol=BTCUSDT"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_TICKER_URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"


def parse_json_or_list(val: Any) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def extract_best_bid_ask(book_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Safely compute best bid (highest bid price) and best ask (lowest ask price)
    regardless of how Polymarket CLOB sorts the arrays.
    """
    bids = book_payload.get("bids") or []
    asks = book_payload.get("asks") or []

    valid_bids = [
        float(b["price"])
        for b in bids
        if float(b.get("size", 0)) > 0 and 0.0 < float(b.get("price", 0)) < 1.0
    ]
    valid_asks = [
        float(a["price"])
        for a in asks
        if float(a.get("size", 0)) > 0 and 0.0 < float(a.get("price", 0)) < 1.0
    ]

    best_bid = max(valid_bids) if valid_bids else None
    best_ask = min(valid_asks) if valid_asks else None
    return best_bid, best_ask


def _sweep_vwap(levels: List[Tuple[float, float]], target_shares: float, max_slip_from_top: float = 0.03) -> Optional[float]:
    """
    Compute executable Volume-Weighted Average Price (VWAP) across `levels` [(price, size), ...]
    already sorted best-first, rejecting dust quotes (< 40% of target_shares within 3c of top).
    """
    if not levels:
        return None
    top_p = levels[0][0]
    # Allow settled/near-settled end-of-window quotes (>= 0.98 or <= 0.02) without strict size filter
    if top_p >= 0.98 or top_p <= 0.02:
        return top_p

    filled = 0.0
    cost = 0.0
    for p, sz in levels:
        if abs(p - top_p) > max_slip_from_top:
            break
        take = min(sz, target_shares - filled)
        filled += take
        cost += take * p
        if filled >= target_shares - 1e-6:
            break

    min_req = max(5.0, 0.40 * target_shares)
    if filled < min_req:
        return None
    return round(cost / filled, 4)


def extract_executable_bid_ask(
    book_payload: Dict[str, Any],
    target_usdc: float = 25.0,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Depth-aware VWAP Bid/Ask for a `target_usdc` order (~30-35 shares), preventing paper-trading
    illusions from 0.5-share dust orders sitting at the top of the CLOB!
    """
    if target_usdc <= 0:
        return extract_best_bid_ask(book_payload)

    bids_raw = book_payload.get("bids") or []
    asks_raw = book_payload.get("asks") or []

    bids = sorted(
        [
            (float(b["price"]), float(b["size"]))
            for b in bids_raw
            if float(b.get("size", 0)) > 0 and 0.0 < float(b.get("price", 0)) < 1.0
        ],
        key=lambda x: x[0],
        reverse=True,
    )
    asks = sorted(
        [
            (float(a["price"]), float(a["size"]))
            for a in asks_raw
            if float(a.get("size", 0)) > 0 and 0.0 < float(a.get("price", 0)) < 1.0
        ],
        key=lambda x: x[0],
    )

    vwap_ask = None
    if asks:
        est_shares = target_usdc / max(0.20, asks[0][0])
        vwap_ask = _sweep_vwap(asks, target_shares=est_shares)

    vwap_bid = None
    if bids:
        est_shares = target_usdc / max(0.20, min(0.85, bids[0][0]))
        vwap_bid = _sweep_vwap(bids, target_shares=est_shares)

    if vwap_bid is not None and vwap_ask is not None and vwap_bid >= vwap_ask:
        return None, None
    return vwap_bid, vwap_ask


def fetch_5m_market_metadata(
    window_ts: int,
    session: Optional[requests.Session] = None,
    timeout: float = 4.0,
) -> Optional[Dict[str, Any]]:
    """
    Query Polymarket Gamma API for `btc-updown-5m-{window_ts}`:
      - Up / Down CLOB token IDs
      - Official `priceToBeat` (when populated)
      - Official settled winner (`resolved_winner` = "UP" or "DOWN" when `outcomePrices` settles)
    """
    slug = f"btc-updown-5m-{window_ts}"
    http = session or requests
    try:
        resp = http.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=timeout)
        if resp.status_code != 200:
            return None
        events = resp.json()
        if not events or not isinstance(events, list):
            return None

        event = events[0]
        markets = event.get("markets") or []
        if not markets:
            return None

        mkt = markets[0]
        outcomes = parse_json_or_list(mkt.get("outcomes"))
        token_ids = parse_json_or_list(mkt.get("clobTokenIds"))
        outcome_prices = parse_json_or_list(mkt.get("outcomePrices"))
        if len(outcomes) < 2 or len(token_ids) < 2:
            return None

        up_token, down_token = None, None
        up_idx, down_idx = 0, 1
        for idx, (label, tid) in enumerate(zip(outcomes, token_ids)):
            norm = str(label).strip().lower()
            if norm in ("up", "yes", "higher"):
                up_token = str(tid)
                up_idx = idx
            elif norm in ("down", "no", "lower"):
                down_token = str(tid)
                down_idx = idx

        if not up_token or not down_token:
            up_token, down_token = str(token_ids[0]), str(token_ids[1])

        meta = event.get("eventMetadata") or mkt.get("eventMetadata") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except json.JSONDecodeError:
                meta = {}
        price_to_beat = (
            meta.get("priceToBeat")
            or meta.get("startPrice")
            or mkt.get("priceToBeat")
            or mkt.get("startPrice")
        )
        official_strike = float(price_to_beat) if price_to_beat is not None else None

        resolved_winner: Optional[str] = None
        if len(outcome_prices) >= 2:
            try:
                p_up_out = float(outcome_prices[up_idx])
                p_dn_out = float(outcome_prices[down_idx])
                if p_up_out >= 0.98 and p_dn_out <= 0.02:
                    resolved_winner = "UP"
                elif p_dn_out >= 0.98 and p_up_out <= 0.02:
                    resolved_winner = "DOWN"
            except (TypeError, ValueError):
                pass

        return {
            "window_ts": window_ts,
            "slug": slug,
            "question": mkt.get("question", slug),
            "up_token": up_token,
            "down_token": down_token,
            "official_strike": official_strike,
            "resolved_winner": resolved_winner,
        }
    except Exception:
        return None


def _parse_book_ts_sec(payload: Dict[str, Any]) -> float:
    raw = payload.get("timestamp")
    if raw is None:
        return 0.0
    try:
        v = float(raw)
        return v / 1000.0 if v > 1e11 else v
    except (TypeError, ValueError):
        return 0.0


def fetch_clob_book_top(
    token_id: str,
    session: Optional[requests.Session] = None,
    timeout: float = 2.5,
    target_usdc: float = 25.0,
) -> Tuple[Optional[float], Optional[float], float]:
    http = session or requests
    try:
        resp = http.get(
            CLOB_BOOK_URL,
            params={"token_id": token_id},
            headers={"Cache-Control": "no-cache"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None, None, 0.0
        data = resp.json()
        bid, ask = extract_executable_bid_ask(data, target_usdc=target_usdc)
        return bid, ask, _parse_book_ts_sec(data)
    except Exception:
        return None, None, 0.0


@dataclass
class LiveFeedState:
    binance_price: Optional[float] = None
    binance_ts: float = 0.0
    coinbase_price: Optional[float] = None
    coinbase_ts: float = 0.0
    _ewma_bn_cb_ratio: float = 1.0
    _ratio_initialized: bool = False

    current_window_ts: int = 0
    slug: str = ""
    up_token: Optional[str] = None
    down_token: Optional[str] = None
    strike_price: Optional[float] = None
    strike_source: str = "UNSET"
    _initial_cl_strike: Optional[float] = None
    _implied_strike_samples: List[float] = field(default_factory=list)

    book: OrderBookTop = None  # type: ignore
    up_book_ts: float = 0.0
    down_book_ts: float = 0.0
    last_ws_book_local_ts: float = 0.0

    def __post_init__(self) -> None:
        if self.book is None:
            self.book = OrderBookTop()

    def calibrate_strike_from_opening_book(
        self,
        cl_spot: Optional[float],
        vol_1s: float,
        elapsed_sec: float,
    ) -> Optional[float]:
        """
        If `POLY_RTDS_CHAINLINK` at t=0s differs from Polymarket's internal `priceToBeat` due to a
        rollover price jump, the opening two-sided CLOB midpoint (`up_mid`) immediately reveals the
        market makers' true Strike. Calibrates `strike_price` when >=3 early samples show a >=$12 gap.
        """
        if (
            self.strike_source == "GAMMA_OFFICIAL"
            or cl_spot is None
            or self.strike_price is None
            or not (1.5 <= elapsed_sec <= 45.0)
            or len(self._implied_strike_samples) >= 8
        ):
            return None
        if self.book.up_bid is None or self.book.up_ask is None:
            return None
        spread = self.book.up_ask - self.book.up_bid
        if spread <= 0.0 or spread > 0.06:
            return None

        up_mid = 0.5 * (self.book.up_bid + self.book.up_ask)
        if not (0.22 <= up_mid <= 0.78):
            return None

        rem_sec = max(60.0, 300.0 - elapsed_sec)
        eff_vol = max(vol_1s, 0.000035)
        sigma_usd = max(cl_spot * eff_vol * math.sqrt(rem_sec), cl_spot * 0.00035)
        z_mkt = _STD_NORM.inv_cdf(up_mid)
        s_implied = cl_spot - z_mkt * sigma_usd
        self._implied_strike_samples.append(s_implied)

        if len(self._implied_strike_samples) >= 3:
            ordered = sorted(self._implied_strike_samples)
            med_implied = ordered[len(ordered) // 2]
            ref_strike = self._initial_cl_strike or self.strike_price
            if abs(med_implied - ref_strike) >= max(12.0, 0.28 * sigma_usd):
                self.strike_price = round(med_implied, 2)
                self.strike_source = "BOOK_CALIBRATED"
                return self.strike_price
        return None

    def update_usdt_basis(self) -> None:
        """Track BTCUSDT / BTC-USD ratio so exchange reconnects never cause a +/-$50 basis jump!"""
        now = time.time()
        if (
            self.coinbase_price
            and self.binance_price
            and self.coinbase_price > 0
            and (now - self.coinbase_ts) < 8.0
            and (now - self.binance_ts) < 8.0
        ):
            r = float(self.binance_price) / float(self.coinbase_price)
            if 0.98 < r < 1.02:
                if not self._ratio_initialized:
                    self._ewma_bn_cb_ratio = r
                    self._ratio_initialized = True
                else:
                    self._ewma_bn_cb_ratio = 0.02 * r + 0.98 * self._ewma_bn_cb_ratio

    @property
    def cex_lead_price(self) -> Optional[float]:
        now = time.time()
        cb_fresh = self.coinbase_price is not None and (now - self.coinbase_ts) < 10.0
        bn_fresh = self.binance_price is not None and (now - self.binance_ts) < 10.0
        bn_usd = (float(self.binance_price) / self._ewma_bn_cb_ratio) if bn_fresh else None

        if cb_fresh and bn_usd is not None:
            return 0.55 * float(self.coinbase_price) + 0.45 * bn_usd
        if cb_fresh:
            return float(self.coinbase_price)
        if bn_usd is not None:
            return bn_usd
        return None


class LiveMarketHub:
    def __init__(self, tracker: RollingMicrostructureTracker, target_usdc: float = 25.0):
        self.tracker = tracker
        self.target_usdc = max(5.0, target_usdc) if target_usdc > 0 else 25.0
        self.chainlink = ChainlinkOracleFeed()
        self.state = LiveFeedState()
        self._stop_event = asyncio.Event()
        self._http = requests.Session()
        self._http.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/json",
            }
        )
        self._prefetched_tokens: Dict[int, Tuple[str, str]] = {}
        self._last_gamma_fetch_ts: float = 0.0
        self._gamma_strike_checks: int = 0
        self._last_clock_sync_ts: float = 0.0

    def utc_now(self) -> float:
        """Authoritative server-aligned UTC timestamp (immune to local Windows clock drift)."""
        return self.chainlink.utc_now()

    def _sync_clock_from_http_date(self, resp: requests.Response) -> None:
        date_hdr = resp.headers.get("Date")
        if not date_hdr:
            return
        try:
            srv_ts = parsedate_to_datetime(date_hdr).timestamp()
            self.chainlink.update_clock_offset(srv_ts)
        except Exception:
            pass

    def sync_window_boundary(self, now_utc: float) -> None:
        """
        Synchronously transition 5m window state at exact server UTC boundary (t=0.0s) so `strike_price`
        never leaks from the previous window or drifts due to local PC clock skew!
        """
        window_ts = (int(now_utc) // 300) * 300
        if window_ts == self.state.current_window_ts:
            # Backfill exact UTC boundary price if Chainlink tick arrived a moment after rollover
            if self.state.strike_price is None:
                near_p = self.chainlink.get_price_near_utc(float(window_ts), max_diff_sec=5.0)
                if near_p is not None:
                    self.state.strike_price = near_p
                    self.state._initial_cl_strike = near_p
                    self.state.strike_source = self.chainlink.state.source
            return

        self.state.current_window_ts = window_ts
        self.state.slug = f"btc-updown-5m-{window_ts}"
        self.state.book = OrderBookTop()
        self.state.up_book_ts = 0.0
        self.state.down_book_ts = 0.0
        self.state.last_ws_book_local_ts = 0.0
        self.state._implied_strike_samples.clear()
        self._gamma_strike_checks = 0

        if window_ts in self._prefetched_tokens:
            self.state.up_token, self.state.down_token = self._prefetched_tokens.pop(window_ts)
        else:
            self.state.up_token = None
            self.state.down_token = None

        near_p = self.chainlink.get_price_near_utc(float(window_ts), max_diff_sec=5.0)
        cl_price = near_p or self.chainlink.state.settlement_reference_price
        if (now_utc - window_ts) < 4.5 and cl_price is not None:
            self.state.strike_price = cl_price
            self.state._initial_cl_strike = cl_price
            self.state.strike_source = self.chainlink.state.source
        else:
            self.state.strike_price = None
            self.state._initial_cl_strike = None
            self.state.strike_source = "WAITING_GAMMA_OR_NEXT_WINDOW"

    async def start(self) -> None:
        tasks = [
            self.chainlink.start(),
            self._cex_http_fallback_loop(),
            self._polymarket_window_and_book_loop(),
        ]
        if has_websockets_installed():
            tasks.extend(
                [
                    self._binance_ws_loop(),
                    self._coinbase_ws_loop(),
                    self._polymarket_clob_ws_loop(),
                ]
            )
        await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        self._stop_event.set()
        self.chainlink.stop()

    def _poll_cex_rest_once(self) -> None:
        """HTTPS REST fallback + HTTP Date server UTC sync."""
        now = time.time()
        need_book = (now - self.state.binance_ts) >= 2.5
        need_clock = (len(self.chainlink._clock_samples) < 3) or ((now - self._last_clock_sync_ts) >= 60.0)
        if need_book or need_clock:
            try:
                r = self._http.get(BINANCE_REST_BOOK_URL, timeout=2.0)
                if r.status_code == 200:
                    self._sync_clock_from_http_date(r)
                    self._last_clock_sync_ts = time.time()
                    if need_book:
                        d = r.json()
                        bp, ap = float(d["bidPrice"]), float(d["askPrice"])
                        bq, aq = float(d["bidQty"]), float(d["askQty"])
                        self.state.binance_price = 0.5 * (bp + ap)
                        self.state.binance_ts = time.time()
                        self.state.update_usdt_basis()
                        self.tracker.on_book_ticker(bq, aq)
            except Exception:
                pass

        if (now - self.state.coinbase_ts) >= 2.5:
            try:
                r = self._http.get(COINBASE_REST_TICKER_URL, timeout=2.0)
                if r.status_code == 200:
                    self._sync_clock_from_http_date(r)
                    d = r.json()
                    if "price" in d:
                        self.state.coinbase_price = float(d["price"])
                        self.state.coinbase_ts = time.time()
                        self.state.update_usdt_basis()
            except Exception:
                pass

        # Feed fast 1s USD-equivalent price series into volatility estimator
        vol_price = self.state.cex_lead_price or self.chainlink.state.settlement_reference_price
        if vol_price is not None:
            self.tracker.on_price(self.utc_now(), vol_price)

    async def _cex_http_fallback_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            await loop.run_in_executor(None, self._poll_cex_rest_once)
            await asyncio.sleep(1.5)

    async def _binance_ws_loop(self) -> None:
        import websockets

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    while not self._stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=8.0)
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})
                        now_local = time.time()
                        ev_ms = data.get("E")
                        if ev_ms is not None and (now_local - self._last_clock_sync_ts) >= 5.0:
                            try:
                                self.chainlink.update_clock_offset(float(ev_ms) / 1000.0, local_ts=now_local)
                                self._last_clock_sync_ts = now_local
                            except (TypeError, ValueError):
                                pass
                        now_utc = self.utc_now()

                        if "aggTrade" in stream:
                            p = float(data["p"])
                            q = float(data["q"])
                            is_buyer_maker = bool(data["m"])
                            self.state.binance_price = p
                            self.state.binance_ts = now_local
                            self.state.update_usdt_basis()
                            self.tracker.on_trade(now_utc, p, q, is_buyer_maker)
                            self.tracker.on_price(now_utc, self.state.cex_lead_price or p)
                        elif "bookTicker" in stream:
                            bid_p, ask_p = float(data["b"]), float(data["a"])
                            bid_q, ask_q = float(data["B"]), float(data["A"])
                            self.state.binance_price = 0.5 * (bid_p + ask_p)
                            self.state.binance_ts = now_local
                            self.state.update_usdt_basis()
                            self.tracker.on_book_ticker(bid_q, ask_q)
            except Exception:
                await asyncio.sleep(2.5)

    async def _coinbase_ws_loop(self) -> None:
        import websockets

        sub_msg = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    COINBASE_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    while not self._stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=8.0)
                        data = json.loads(raw)
                        if data.get("type") == "ticker" and "price" in data:
                            self.state.coinbase_price = float(data["price"])
                            self.state.coinbase_ts = time.time()
                            self.state.update_usdt_basis()
            except Exception:
                await asyncio.sleep(2.5)

    async def _polymarket_window_and_book_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            now_utc = self.utc_now()
            self.sync_window_boundary(now_utc)
            window_ts = self.state.current_window_ts
            elapsed = now_utc - window_ts

            # 1. Fetch Gamma metadata when tokens are missing OR up to twice after open (10s..60s)
            #    to capture `official_strike` (`priceToBeat`) even when tokens were pre-fetched!
            need_tokens = not self.state.up_token
            need_official_strike = (
                self.state.strike_source != "GAMMA_OFFICIAL"
                and 10.0 <= elapsed <= 65.0
                and self._gamma_strike_checks < 2
                and (now_utc - self._last_gamma_fetch_ts) >= 15.0
            )
            if (need_tokens and (now_utc - self._last_gamma_fetch_ts) >= 3.0) or need_official_strike:
                self._last_gamma_fetch_ts = now_utc
                if need_official_strike:
                    self._gamma_strike_checks += 1
                meta = await loop.run_in_executor(None, fetch_5m_market_metadata, window_ts, self._http)
                if meta and self.state.current_window_ts == window_ts:
                    self.state.up_token = meta["up_token"]
                    self.state.down_token = meta["down_token"]
                    if meta.get("official_strike"):
                        self.state.strike_price = meta["official_strike"]
                        self.state.strike_source = "GAMMA_OFFICIAL"
                    elif (
                        self.state.strike_price is None
                        and elapsed < 5.0
                        and self.chainlink.state.settlement_reference_price is not None
                    ):
                        self.state.strike_price = self.chainlink.state.settlement_reference_price
                        self.state.strike_source = self.chainlink.state.source

            # 2. Pre-fetch NEXT 5m window tokens 25s before rollover so t=0.0s book is instant!
            next_w = window_ts + 300
            if elapsed >= 275.0 and next_w not in self._prefetched_tokens and (now_utc - self._last_gamma_fetch_ts) >= 5.0:
                self._last_gamma_fetch_ts = now_utc
                nxt_meta = await loop.run_in_executor(None, fetch_5m_market_metadata, next_w, self._http)
                if nxt_meta and nxt_meta.get("up_token") and nxt_meta.get("down_token"):
                    self._prefetched_tokens[next_w] = (nxt_meta["up_token"], nxt_meta["down_token"])

            # 3. Poll depth-aware executable VWAP CLOB orderbook ONLY when WS is silent (>=2.5s) or book uninitialized
            ws_silent = (time.time() - self.state.last_ws_book_local_ts) >= 2.5
            book_empty = (
                (self.state.book.up_bid is None and self.state.book.up_ask is None)
                or (self.state.book.down_bid is None and self.state.book.down_ask is None)
            )
            if self.state.up_token and self.state.down_token and (ws_silent or book_empty):
                up_t, dn_t = self.state.up_token, self.state.down_token
                up_res, down_res = await asyncio.gather(
                    loop.run_in_executor(None, fetch_clob_book_top, up_t, self._http, 2.5, self.target_usdc),
                    loop.run_in_executor(None, fetch_clob_book_top, dn_t, self._http, 2.5, self.target_usdc),
                )
                if self.state.current_window_ts == window_ts:
                    ub, ua, uts = up_res
                    db, da, dts = down_res
                    if uts <= 0.0 or uts >= self.state.up_book_ts - 0.05:
                        self.state.book.up_bid, self.state.book.up_ask = ub, ua
                        if uts > self.state.up_book_ts:
                            self.state.up_book_ts = uts
                    if dts <= 0.0 or dts >= self.state.down_book_ts - 0.05:
                        self.state.book.down_bid, self.state.book.down_ask = db, da
                        if dts > self.state.down_book_ts:
                            self.state.down_book_ts = dts

            await asyncio.sleep(1.2)

    async def _polymarket_clob_ws_loop(self) -> None:
        import websockets

        subscribed_window = 0
        while not self._stop_event.is_set():
            if not self.state.up_token or not self.state.down_token:
                await asyncio.sleep(0.5)
                continue

            subscribed_window = self.state.current_window_ts
            tokens = [self.state.up_token, self.state.down_token]
            try:
                async with websockets.connect(
                    POLY_CLOB_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
                    last_msg_ts = time.time()
                    while (
                        not self._stop_event.is_set()
                        and self.state.current_window_ts == subscribed_window
                    ):
                        if time.time() - last_msg_ts >= 12.0:
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        last_msg_ts = time.time()
                        payload = json.loads(raw)
                        events = payload if isinstance(payload, list) else [payload]
                        now_u = self.utc_now()
                        for ev in events:
                            if isinstance(ev, dict) and ev.get("event_type") == "book":
                                ev_ts = _parse_book_ts_sec(ev)
                                if ev_ts > 0.0 and (now_u - ev_ts) > 2.5:
                                    continue
                                aid = ev.get("asset_id")
                                bid, ask = extract_executable_bid_ask(ev, target_usdc=self.target_usdc)
                                if aid == self.state.up_token and (ev_ts <= 0.0 or ev_ts >= self.state.up_book_ts - 0.02):
                                    self.state.book.up_bid, self.state.book.up_ask = bid, ask
                                    if ev_ts > self.state.up_book_ts:
                                        self.state.up_book_ts = ev_ts
                                    self.state.last_ws_book_local_ts = time.time()
                                    if bid is not None and bid >= 0.92 and (self.state.book.down_bid or 0.0) >= 0.20:
                                        self.state.book.down_bid = None
                                        self.state.book.down_ask = round(max(0.01, 1.0 - bid), 2)
                                elif aid == self.state.down_token and (ev_ts <= 0.0 or ev_ts >= self.state.down_book_ts - 0.02):
                                    self.state.book.down_bid, self.state.book.down_ask = bid, ask
                                    if ev_ts > self.state.down_book_ts:
                                        self.state.down_book_ts = ev_ts
                                    self.state.last_ws_book_local_ts = time.time()
                                    if bid is not None and bid >= 0.92 and (self.state.book.up_bid or 0.0) >= 0.20:
                                        self.state.book.up_bid = None
                                        self.state.book.up_ask = round(max(0.01, 1.0 - bid), 2)
            except Exception:
                await asyncio.sleep(1.5)
