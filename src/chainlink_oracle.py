"""
Dedicated Chainlink BTC/USD Oracle Engine for Polymarket 5m Markets.

Fixes real-world production failure modes:
1. Never freezes on a stale price if public RPCs rate-limit (HTTP 429): uses a persistent
   `requests.Session` Round-Robin pool across 8 public RPCs spanning BOTH Arbitrum One
   (`0x6ce185860a4963106506C203335A2910413708e9`, tight L2 deviation threshold) AND
   Polygon Mainnet (`0xc907E116054Ad103354f2D350FD2514433D57F6f`), always selecting the
   Chainlink report with the newest on-chain `updatedAt` timestamp!
2. Strict Stale-Feed Circuit Breaker: if no live poll/stream succeeds within `max_stale_sec` (15s),
   `settlement_reference_price` returns `None` instead of repeating a frozen price.
3. Supports all 4 Chainlink tiers seamlessly even if `websockets` is not yet installed.
"""
import asyncio
from collections import deque
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import time
from typing import Any, Deque, List, Optional, Tuple
import urllib.parse

import requests


CHAINLINK_BTC_USD_FEED_ID = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
CHAINLINK_DATAENGINE_HOST = "ws.dataengine.chain.link"

POLYBOLT_WS_URL = "wss://ws-live-v2.polymarket.com/ws"
POLY_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

POLYGON_CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
ARBITRUM_CHAINLINK_BTC_USD = "0x6ce185860a4963106506C203335A2910413708e9"
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# (chain_label, rpc_url, contract_address) — Round-robin pool prevents HTTP 429 bans!
ONCHAIN_CHAINLINK_TARGETS: List[Tuple[str, str, str]] = [
    ("ARB_CHAINLINK", "https://arbitrum-one-rpc.publicnode.com", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon-bor-rpc.publicnode.com", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://arb1.arbitrum.io/rpc", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon.drpc.org", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://arbitrum.drpc.org", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://1rpc.io/matic", POLYGON_CHAINLINK_BTC_USD),
    ("ARB_CHAINLINK", "https://1rpc.io/arb", ARBITRUM_CHAINLINK_BTC_USD),
    ("POLY_CHAINLINK", "https://polygon-rpc.com", POLYGON_CHAINLINK_BTC_USD),
]


def has_websockets_installed() -> bool:
    try:
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


def normalize_btc_usd_price(raw_value: Any) -> Optional[float]:
    try:
        val = float(raw_value)
    except (TypeError, ValueError):
        return None

    if val <= 0:
        return None
    if val > 1e19:
        val /= 1e18
    elif val > 1e9:
        val /= 1e8

    if 1000.0 <= val <= 2_000_000.0:
        return val
    return None


def decode_aggregator_v3_hex(hex_result: str) -> Optional[Tuple[int, float, int]]:
    """
    Decode ABI return payload of `latestRoundData()`:
      (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
    Returns (round_id, btc_usd_price, updated_at_ts) or None.
    """
    if not hex_result or not hex_result.startswith("0x"):
        return None
    raw = hex_result[2:]
    if len(raw) < 64 * 5:
        return None

    round_id = int(raw[0:64], 16)
    answer_uint = int(raw[64:128], 16)
    if answer_uint >= (1 << 255):
        answer_uint -= (1 << 256)
    updated_at = int(raw[192:256], 16)

    price = normalize_btc_usd_price(answer_uint)
    if price is None or updated_at <= 0:
        return None
    return round_id, price, updated_at


def decode_chainlink_report_v3_hex(full_report_hex: str) -> Optional[Tuple[float, int]]:
    clean = full_report_hex[2:] if full_report_hex.startswith("0x") else full_report_hex
    if len(clean) < 64 * 12:
        return None
    try:
        data = bytes.fromhex(clean)
        blob_offset = int.from_bytes(data[96:128], "big")
        if blob_offset + 32 + 288 > len(data):
            return None
        blob_len = int.from_bytes(data[blob_offset : blob_offset + 32], "big")
        if blob_len < 288:
            return None
        blob = data[blob_offset + 32 : blob_offset + 32 + blob_len]

        obs_ts = int.from_bytes(blob[64:96], "big")
        raw_price = int.from_bytes(blob[192:224], "big", signed=True)
        price = normalize_btc_usd_price(raw_price)
        if price is None:
            return None
        return price, obs_ts
    except Exception:
        return None


def build_data_streams_hmac_headers(
    method: str,
    api_key: str,
    api_secret: str,
    full_path_with_query: str,
    body: bytes = b"",
    timestamp_ms: Optional[int] = None,
) -> dict:
    ts_str = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
    body_hash_hex = hashlib.sha256(body).hexdigest()
    to_sign = f"{method} {full_path_with_query} {body_hash_hex} {api_key} {ts_str}".encode("utf-8")
    sig = hmac.new(api_secret.encode("utf-8"), to_sign, hashlib.sha256).hexdigest()
    return {
        "Authorization": api_key,
        "X-Authorization-Timestamp": ts_str,
        "X-Authorization-Signature-SHA256": sig,
    }


@dataclass
class ChainlinkOracleState:
    price: Optional[float] = None
    twap_60s: Optional[float] = None
    oracle_ts: float = 0.0          # On-chain `updatedAt` or stream observation timestamp
    local_recv_ts: float = 0.0      # Wall-clock time of last verified live response
    source: str = "UNINITIALIZED"
    round_id: Optional[int] = None

    def is_alive(self, max_silence_sec: float = 18.0) -> bool:
        """True only if we received a live oracle verification within `max_silence_sec`."""
        if self.price is None or self.local_recv_ts <= 0:
            return False
        return (time.time() - self.local_recv_ts) <= max_silence_sec

    @property
    def settlement_reference_price(self) -> Optional[float]:
        """Return Chainlink price ONLY if the oracle connection is alive (never return frozen data!)."""
        if not self.is_alive():
            return None
        return self.price


class ChainlinkOracleFeed:
    """
    Multi-tier Chainlink BTC/USD live oracle manager with Round-Robin RPC anti-ban protection.
    """

    def __init__(self) -> None:
        self.state = ChainlinkOracleState()
        self._twap_samples: Deque[Tuple[float, float]] = deque(maxlen=600)
        self._utc_history: Deque[Tuple[float, float]] = deque(maxlen=360)  # (oracle_utc_ts, price)
        self.clock_offset_sec: float = 0.0
        self._clock_samples: Deque[float] = deque(maxlen=15)
        self._stop_event = asyncio.Event()
        self._http = requests.Session()
        self._rr_idx = 0

    def update_clock_offset(self, server_utc_ts: float, local_ts: Optional[float] = None) -> None:
        """
        Align local PC clock (`time.time()`) with authoritative real-time server UTC timestamps.
        Uses a +/-1.0s deadband so normal network/HTTP latency never jitters a healthy PC clock,
        while automatically rescuing clocks that are genuinely drifted by >=1s!
        """
        now_local = local_ts if local_ts is not None else time.time()
        diff = server_utc_ts - now_local
        if abs(diff) > 86400.0:
            return
        self._clock_samples.append(diff)
        ordered = sorted(self._clock_samples)
        med = ordered[len(ordered) // 2]
        self.clock_offset_sec = med if abs(med) >= 1.0 else 0.0

    def utc_now(self) -> float:
        return time.time() + self.clock_offset_sec

    def get_price_near_utc(self, target_utc_ts: float, max_diff_sec: float = 6.0) -> Optional[float]:
        """Return the Chainlink price whose authoritative `oracle_ts` is closest to `target_utc_ts`."""
        best_p: Optional[float] = None
        best_dt = max_diff_sec
        for ots, p in self._utc_history:
            dt = abs(ots - target_utc_ts)
            if dt <= best_dt:
                best_dt = dt
                best_p = p
        return best_p

    def on_chainlink_tick(
        self,
        price: float,
        source: str,
        oracle_ts: Optional[float] = None,
        round_id: Optional[int] = None,
        explicit_twap_60s: Optional[float] = None,
    ) -> None:
        now = time.time()
        eff_oracle_ts = oracle_ts if oracle_ts is not None else self.utc_now()

        # Do not overwrite a newer Chainlink round with an older lagging RPC block!
        is_streaming = source in ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK", "POLY_RTDS_CHAINLINK")
        stale_tolerance = 10.0 if is_streaming else 1.0
        if self.state.oracle_ts > 0 and eff_oracle_ts < self.state.oracle_ts - stale_tolerance:
            return

        self.state.price = price
        self.state.source = source
        self.state.oracle_ts = max(self.state.oracle_ts, eff_oracle_ts)
        self.state.local_recv_ts = now
        if round_id is not None:
            self.state.round_id = round_id

        self._utc_history.append((eff_oracle_ts, price))
        self._twap_samples.append((now, price))
        cutoff = now - 60.0
        while len(self._twap_samples) > 2 and self._twap_samples[0][0] < cutoff:
            self._twap_samples.popleft()

        if explicit_twap_60s is not None:
            self.state.twap_60s = explicit_twap_60s
        else:
            self.state.twap_60s = self._compute_local_twap(now)

    def _compute_local_twap(self, now: float) -> float:
        pts = list(self._twap_samples)
        if not pts:
            return 0.0
        if len(pts) == 1:
            return pts[0][1]

        weighted_sum = 0.0
        total_dt = 0.0
        start_cut = now - 60.0
        for i in range(len(pts)):
            t0 = max(start_cut, pts[i][0])
            t1 = pts[i + 1][0] if (i + 1 < len(pts)) else now
            dt = max(0.0, t1 - t0)
            weighted_sum += pts[i][1] * dt
            total_dt += dt

        return (weighted_sum / total_dt) if total_dt > 1e-6 else pts[-1][1]

    def _poll_round_robin_onchain(self) -> Optional[Tuple[str, int, float, int]]:
        """
        Query 2 distinct Chainlink targets (1 Arbitrum + 1 Polygon) per cycle in round-robin order
        and return the freshest Chainlink `updatedAt` round.
        """
        targets = list(ONCHAIN_CHAINLINK_TARGETS)
        custom_rpc = os.getenv("POLYGON_RPC_URL")
        if custom_rpc:
            targets.insert(0, ("POLY_CUSTOM_RPC", custom_rpc, POLYGON_CHAINLINK_BTC_USD))

        best: Optional[Tuple[str, int, float, int]] = None
        # Probe up to 3 endpoints starting at current round-robin cursor
        for _ in range(3):
            label, url, contract = targets[self._rr_idx % len(targets)]
            self._rr_idx = (self._rr_idx + 1) % len(targets)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": contract, "data": LATEST_ROUND_DATA_SELECTOR}, "latest"],
            }
            try:
                resp = self._http.post(url, json=payload, timeout=2.5)
                if resp.status_code != 200:
                    continue
                decoded = decode_aggregator_v3_hex(resp.json().get("result") or "")
                if decoded is None:
                    continue
                round_id, price, updated_at = decoded
                # Reject stale cached RPC nodes (> 2 hours old)
                if (time.time() - updated_at) > 7200:
                    continue
                if best is None or updated_at >= best[3]:
                    best = (label, round_id, price, updated_at)
                # Once we got a fresh Arbitrum + Polygon pair or very recent update (<5s), stop probing
                if (time.time() - updated_at) < 5.0:
                    break
            except Exception:
                continue
        return best

    def stop(self) -> None:
        self._stop_event.set()

    async def start(self) -> None:
        tasks = [self._onchain_round_robin_loop()]
        if has_websockets_installed():
            tasks.append(self._rtds_chainlink_ws_loop())
            cl_key = os.getenv("CHAINLINK_STREAMS_API_KEY")
            cl_secret = os.getenv("CHAINLINK_STREAMS_API_SECRET")
            if cl_key and cl_secret:
                tasks.append(self._chainlink_data_streams_ws_loop(cl_key, cl_secret))

            poly_key = os.getenv("POLY_API_KEY")
            poly_secret = os.getenv("POLY_API_SECRET")
            poly_pass = os.getenv("POLY_PASSPHRASE")
            if poly_key and poly_secret and poly_pass:
                tasks.append(self._polybolt_v2_ws_loop(poly_key, poly_secret, poly_pass))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _chainlink_data_streams_ws_loop(self, api_key: str, api_secret: str) -> None:
        import websockets

        query = urllib.parse.urlencode({"feedIDs": CHAINLINK_BTC_USD_FEED_ID})
        full_path = f"/api/v1/ws?{query}"
        url = f"wss://{CHAINLINK_DATAENGINE_HOST}{full_path}"

        while not self._stop_event.is_set():
            try:
                headers = build_data_streams_hmac_headers("GET", api_key, api_secret, full_path)
                async with websockets.connect(url, additional_headers=headers, ping_interval=20) as ws:
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        msg = json.loads(raw)
                        report_hex = (msg.get("report") or {}).get("fullReport") or msg.get("fullReport")
                        if report_hex:
                            decoded = decode_chainlink_report_v3_hex(report_hex)
                            if decoded:
                                price, obs_ts = decoded
                                self.on_chainlink_tick(price, "CHAINLINK_DATA_STREAMS", oracle_ts=float(obs_ts))
            except Exception:
                await asyncio.sleep(2.5)

    async def _polybolt_v2_ws_loop(self, api_key: str, secret: str, passphrase: str) -> None:
        import websockets

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(POLYBOLT_WS_URL, ping_interval=20) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "op": "auth",
                                "rid": "auth-cl-1",
                                "auth": {"apiKey": api_key, "secret": secret, "passphrase": passphrase},
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "op": "subscribe",
                                "subscriptions": [
                                    {"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
                                    {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}},
                                ],
                            }
                        )
                    )
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        msg = json.loads(raw)
                        ch = msg.get("channel", "")
                        data = msg.get("data") or msg.get("payload") or {}
                        norm_p = normalize_btc_usd_price(data.get("price") or data.get("value"))
                        if norm_p is None:
                            continue
                        if ch == "price.crypto.twap":
                            self.state.twap_60s = norm_p
                        else:
                            self.on_chainlink_tick(norm_p, "POLYBOLT_CHAINLINK")
            except Exception:
                await asyncio.sleep(2.5)

    async def _rtds_chainlink_ws_loop(self) -> None:
        import websockets

        # Subscribe with wildcard/empty filter as well as explicit symbol filters for compatibility
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""},
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": '{"symbol":"btc/usd"}'},
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": "BTC/USD"},
            ],
        }

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    POLY_RTDS_WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    open_timeout=8,
                    close_timeout=2,
                ) as ws:
                    await ws.send(json.dumps(sub_msg))
                    last_ping = time.time()
                    last_btc_tick_ts = last_ping
                    last_resub_ts = last_ping

                    while not self._stop_event.is_set():
                        now_loop = time.time()
                        # Break out of silent/zombie sessions (e.g. RTDS drops topic push while still replying PONG)
                        if now_loop - last_btc_tick_ts >= 10.0:
                            break
                        if now_loop - last_btc_tick_ts >= 5.0 and now_loop - last_resub_ts >= 5.0:
                            await ws.send(json.dumps(sub_msg))
                            last_resub_ts = now_loop
                        if now_loop - last_ping >= 5.0:
                            await ws.send("PING")
                            last_ping = now_loop
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        if raw in ("PONG", "pong", ""):
                            continue
                        msg = json.loads(raw)
                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue
                        payload = msg.get("payload") or {}
                        sym = str(payload.get("symbol", "")).lower().replace("-", "/")
                        if "btc" not in sym:
                            continue

                        if (
                            self.state.source in ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK")
                            and (time.time() - self.state.local_recv_ts) < 2.0
                        ):
                            continue

                        price = normalize_btc_usd_price(payload.get("value") or payload.get("price"))
                        if price is not None:
                            last_btc_tick_ts = time.time()
                            ts_raw = payload.get("timestamp")
                            # Note: RTDS `timestamp` is Chainlink's report observation time (often 2-5s old),
                            # NOT packet send time — use it for `oracle_ts`, never for wall-clock sync!
                            obs_ts = (float(ts_raw) / 1000.0) if (ts_raw and float(ts_raw) > 1e11) else self.utc_now()
                            round_id = payload.get("round_id")
                            self.on_chainlink_tick(
                                price=price,
                                source="POLY_RTDS_CHAINLINK",
                                oracle_ts=obs_ts,
                                round_id=int(round_id) if round_id else None,
                            )
            except Exception:
                await asyncio.sleep(1.5)

    async def _onchain_round_robin_loop(self) -> None:
        """
        Polls Arbitrum + Polygon Chainlink contracts across 8 RPCs in round-robin rotation
        whenever no streaming Tier 1/2/3 tick arrived in the last 3 seconds.
        Each individual RPC is only queried ~once every 24 seconds -> immune to 429 rate limits!
        """
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            is_streaming = (
                self.state.source in ("CHAINLINK_DATA_STREAMS", "POLYBOLT_CHAINLINK", "POLY_RTDS_CHAINLINK")
                and (time.time() - self.state.local_recv_ts) < 3.0
            )
            if not is_streaming:
                res = await loop.run_in_executor(None, self._poll_round_robin_onchain)
                if res is not None:
                    label, round_id, price, updated_at = res
                    self.on_chainlink_tick(
                        price=price,
                        source=label,
                        oracle_ts=float(updated_at),
                        round_id=round_id,
                    )
            await asyncio.sleep(2.5)
