"""
Polymarket CLOB V2 Live Execution Engine (`pUSD` / `py-clob-client-v2`).
Strictly uses FAK (Fill-And-Kill) limit orders with hard price caps and a daily loss circuit breaker.
"""
from dataclasses import dataclass
import math
import os
from pathlib import Path
import time
from typing import Optional

from dotenv import load_dotenv

CLOB_HOST = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137


def floor_share_qty(qty: float) -> float:
    """Round share quantity strictly DOWN to 2 decimals so SELL orders never exceed 4/6-decimal wallet balances."""
    if qty <= 0.0:
        return 0.0
    return math.floor((float(qty) + 1e-9) * 100.0) / 100.0


@dataclass
class LiveFillResult:
    filled: bool
    filled_shares: float
    avg_price: float
    order_id: str = ""
    error: Optional[str] = None


def parse_clob_fill_response(resp: object, requested_price: float) -> LiveFillResult:
    """Parse CLOB V2 post_order / get_order JSON payload into normalized fill shares & price."""
    if not isinstance(resp, dict):
        return LiveFillResult(False, 0.0, requested_price, error=f"Unexpected response: {resp}")

    if resp.get("errorMsg"):
        return LiveFillResult(False, 0.0, requested_price, error=str(resp.get("errorMsg")))

    order_id = str(resp.get("orderID") or resp.get("id") or "")
    # `takingAmount` / `makingAmount` or `size_matched` depending on endpoint
    size_matched = resp.get("size_matched")
    if size_matched is not None:
        try:
            sh = float(size_matched)
            px = float(resp.get("price") or requested_price)
            return LiveFillResult(sh > 0.0, sh, px, order_id=order_id)
        except (TypeError, ValueError):
            pass

    making_raw = resp.get("makingAmount")
    taking_raw = resp.get("takingAmount")
    try:
        making = float(making_raw) if making_raw is not None else 0.0
        taking = float(taking_raw) if taking_raw is not None else 0.0
    except (TypeError, ValueError):
        making, taking = 0.0, 0.0

    # Polymarket V2 amounts may be 6-decimal base units (>= 1000) or human floats
    if taking > 0.0 and making > 0.0:
        if taking > 1000.0 or making > 1000.0:
            taking /= 1e6
            making /= 1e6
        # For BUY: making=pUSD spent, taking=shares received; for SELL: making=shares sold, taking=pUSD
        if requested_price > 0 and abs((making / taking) - requested_price) <= abs((taking / making) - requested_price):
            shares = taking
            avg_px = making / taking
        else:
            shares = making
            avg_px = taking / making
        return LiveFillResult(shares > 0.0, round(shares, 4), round(avg_px, 4), order_id=order_id)

    status = str(resp.get("status") or "").lower()
    if status in ("matched", "filled"):
        return LiveFillResult(True, 0.0, requested_price, order_id=order_id)
    return LiveFillResult(False, 0.0, requested_price, order_id=order_id)


class PolymarketLiveExecutor:
    """Live CLOB V2 execution wrapper with daily loss circuit breaker."""

    def __init__(
        self,
        env_path: Optional[Path] = None,
        max_daily_loss_usdc: float = 25.0,
    ) -> None:
        from py_clob_client_v2 import AssetType, BalanceAllowanceParams, ClobClient

        resolved_env = env_path or (Path(__file__).resolve().parent.parent / ".env")
        if not resolved_env.exists():
            raise FileNotFoundError(f"找不到 .env 設定檔: {resolved_env}")
        load_dotenv(dotenv_path=resolved_env)

        private_key = (os.getenv("POLY_PRIVATE_KEY") or "").strip()
        funder = (os.getenv("POLY_FUNDER_ADDRESS") or "").strip()
        sig_type = int((os.getenv("POLY_SIGNATURE_TYPE") or "2").strip())
        if not private_key:
            raise ValueError(".env 內缺少 POLY_PRIVATE_KEY")
        if not private_key.startswith("0x"):
            private_key = "0x" + private_key

        self.sig_type = sig_type
        self.max_daily_loss_usdc = max(5.0, float(max_daily_loss_usdc))
        self.daily_pnl_usdc: float = 0.0
        self.circuit_broken: bool = False
        self._day_utc: int = int(time.time() // 86400)
        self._tick_size_cache: dict[str, str] = {}
        self._cond_balance_cache: dict[str, float] = {}
        self._warmed_tokens: set[str] = set()
        self._last_keepalive_ts: float = 0.0

        # Extend py-clob-client-v2 HTTP/2 keepalive from default 5s -> 60s to avoid cold TLS handshakes on snipes
        try:
            import httpx
            from py_clob_client_v2.http_helpers import helpers as _clob_helpers

            _clob_helpers._http_client = httpx.Client(
                http2=True,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=60.0),
                timeout=8.0,
            )
        except Exception:
            pass

        self.client = ClobClient(
            host=CLOB_HOST,
            chain_id=POLYGON_CHAIN_ID,
            key=private_key,
            signature_type=sig_type,
            funder=funder or None,
        )
        creds = self.client.create_or_derive_api_key()
        self.client.set_api_creds(creds)

        # Sync collateral balance cache on startup
        self._AssetType = AssetType
        self._BalanceAllowanceParams = BalanceAllowanceParams
        try:
            self.client.update_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=sig_type)
            )
        except Exception:
            pass

    def get_pusd_balance(self) -> float:
        info = self.client.get_balance_allowance(
            params=self._BalanceAllowanceParams(
                asset_type=self._AssetType.COLLATERAL,
                signature_type=self.sig_type,
            )
        )
        return float(info.get("balance", 0)) / 1e6

    def record_realized_pnl(self, pnl_usdc: float) -> None:
        cur_day = int(time.time() // 86400)
        if cur_day != self._day_utc:
            self._day_utc = cur_day
            self.daily_pnl_usdc = 0.0
            self.circuit_broken = False
        self.daily_pnl_usdc += pnl_usdc
        if self.daily_pnl_usdc <= -self.max_daily_loss_usdc:
            self.circuit_broken = True

    def _get_tick_size_cached(self, token_id: str) -> str:
        if token_id not in self._tick_size_cache:
            try:
                self._tick_size_cache[token_id] = self.client.get_tick_size(token_id)
            except Exception:
                self._tick_size_cache[token_id] = "0.01"
        return self._tick_size_cache[token_id]

    def warmup_tokens(self, *token_ids: Optional[str]) -> None:
        """
        Pre-populate py-clob-client-v2 internal caches (tick_size, condition_id, market_info, fee_rate, version)
        during the early WAIT phase so live sniper entries fire in 1 warm HTTP/2 POST (~150ms instead of ~4.1s)!
        """
        from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side

        for tok in token_ids:
            if not tok or tok in self._warmed_tokens:
                continue
            try:
                ts = self._get_tick_size_cached(tok)
                # Dry-run build ONLY (never posted!) populates __token_condition_map, __fee_infos & __cached_version
                self.client.create_market_order(
                    MarketOrderArgs(
                        token_id=tok,
                        amount=5.0,
                        side=Side.BUY,
                        price=0.50,
                        order_type=OrderType.FAK,
                    ),
                    options=PartialCreateOrderOptions(tick_size=ts),
                )
                self._warmed_tokens.add(tok)
                self._last_keepalive_ts = time.time()
            except Exception:
                pass
        if len(self._warmed_tokens) > 40:
            self._warmed_tokens.clear()

    def keepalive_if_needed(self, interval_sec: float = 25.0) -> None:
        """Keep HTTP/2 TLS socket to clob.polymarket.com hot during the late-window sniper zone."""
        now = time.time()
        if (now - self._last_keepalive_ts) < interval_sec:
            return
        self._last_keepalive_ts = now
        try:
            self.client.get_ok()
        except Exception:
            pass

    def sync_conditional_balance(self, token_id: str) -> float:
        """Sync & cache conditional token wallet balance (floored to 2 decimals) so exits fire in 1 RTT."""
        try:
            params = self._BalanceAllowanceParams(
                asset_type=self._AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=self.sig_type,
            )
            self.client.update_balance_allowance(params=params)
            bal_info = self.client.get_balance_allowance(params=params)
            avail = floor_share_qty(float(bal_info.get("balance", 0)) / 1e6)
            if avail > 0.0:
                self._cond_balance_cache[token_id] = avail
            return avail
        except Exception:
            return 0.0

    def _verify_fill_if_needed(
        self,
        res: LiveFillResult,
        fallback_price: float,
        est_shares: float,
    ) -> LiveFillResult:
        """Query get_order if post_order omitted inline fill amounts; never return 0 shares on a matched order."""
        if res.order_id and res.filled_shares <= 0.0:
            for attempt in range(2):
                if attempt == 1:
                    time.sleep(0.15)
                try:
                    detail = self.client.get_order(res.order_id)
                    res2 = parse_clob_fill_response(detail, fallback_price)
                    if res2.filled_shares > 0.0:
                        return res2
                    if not res.filled and not res2.filled:
                        break
                    if res2.filled:
                        res = res2
                except Exception:
                    break
        if res.filled and res.filled_shares <= 0.0 and est_shares > 0.0:
            return LiveFillResult(
                True,
                floor_share_qty(est_shares),
                fallback_price,
                order_id=res.order_id,
            )
        return res

    def execute_entry(
        self,
        token_id: str,
        ask_price: float,
        shares: float,
        max_price: float = 0.84,
        p_true: float = 0.0,
        fee_rate: float = 0.07,
    ) -> LiveFillResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side
        from .fees import taker_fee_per_share

        if self.circuit_broken:
            return LiveFillResult(False, 0.0, ask_price, error="DAILY_LOSS_CIRCUIT_BREAKER_ACTIVE")

        # EV-guarded slippage cushion: allow up to +4c ONLY if NetEV at limit_px remains >= +3.5c (strictly <= max_price)
        limit_px = round(min(max_price, float(ask_price) + 0.01), 2)
        if p_true > 0.0:
            for extra in (0.04, 0.03, 0.02):
                cand = round(min(max_price, float(ask_price) + extra), 2)
                if (p_true - cand - taker_fee_per_share(cand, fee_rate)) >= 0.035:
                    limit_px = cand
                    break
        usdc_amount = round(float(shares) * float(ask_price), 2)
        if usdc_amount < 1.0 or limit_px <= 0.01:
            return LiveFillResult(False, 0.0, limit_px, error="Invalid order size or price")

        tick_size = self._get_tick_size_cached(token_id)

        try:
            # MarketOrderArgs + OrderType.FAK guarantees 2-decimal maker_amount (pUSD) and <=4-decimal taker_amount (shares)
            resp = self.client.create_and_post_market_order(
                order_args=MarketOrderArgs(
                    token_id=token_id,
                    amount=usdc_amount,
                    side=Side.BUY,
                    price=limit_px,
                    order_type=OrderType.FAK,
                ),
                options=PartialCreateOrderOptions(tick_size=tick_size),
                order_type=OrderType.FAK,
            )
            res = parse_clob_fill_response(resp, limit_px)
            res = self._verify_fill_if_needed(res, limit_px, est_shares=usdc_amount / limit_px)
            if res.filled and res.filled_shares > 0.0:
                self._cond_balance_cache.pop(token_id, None)
            return res
        except Exception as exc:
            err_str = str(exc)
            if "not enough balance" in err_str.lower():
                try:
                    self.client.update_balance_allowance(
                        params=self._BalanceAllowanceParams(
                            asset_type=self._AssetType.COLLATERAL,
                            signature_type=self.sig_type,
                        )
                    )
                except Exception:
                    pass
                err_str = f"可用 pUSD 餘額不足（若有到期獲勝注單尚未領取，請至 Polymarket 點擊 Claim 領回 pUSD）| {err_str}"
            return LiveFillResult(False, 0.0, limit_px, error=err_str)

    def execute_exit(
        self,
        token_id: str,
        bid_price: float,
        shares: float,
        slippage_cents: float = 0.03,
    ) -> LiveFillResult:
        from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions, Side

        # Fast-path: use background-prefetched conditional balance (0ms extra latency); sync on demand if missing
        cached_avail = self._cond_balance_cache.get(token_id, 0.0)
        if cached_avail <= 0.0:
            cached_avail = self.sync_conditional_balance(token_id)

        sell_qty = floor_share_qty(shares)
        if cached_avail > 0.0:
            sell_qty = floor_share_qty(min(sell_qty, cached_avail))

        if sell_qty < 0.1:
            return LiveFillResult(False, 0.0, bid_price, error="No conditional balance to sell")

        limit_px = round(max(0.01, float(bid_price) - slippage_cents), 2)
        tick_size = self._get_tick_size_cached(token_id)

        for attempt in range(2):
            try:
                resp = self.client.create_and_post_market_order(
                    order_args=MarketOrderArgs(
                        token_id=token_id,
                        amount=sell_qty,
                        side=Side.SELL,
                        price=limit_px,
                        order_type=OrderType.FAK,
                    ),
                    options=PartialCreateOrderOptions(tick_size=tick_size),
                    order_type=OrderType.FAK,
                )
                res = parse_clob_fill_response(resp, limit_px)
                res = self._verify_fill_if_needed(res, limit_px, est_shares=sell_qty)
                if res.filled and res.filled_shares > 0.0:
                    rem = floor_share_qty(max(0.0, sell_qty - res.filled_shares))
                    if rem >= 0.1:
                        self._cond_balance_cache[token_id] = rem
                    else:
                        self._cond_balance_cache.pop(token_id, None)
                return res
            except Exception as exc:
                err_str = str(exc)
                if attempt == 0 and "balance" in err_str.lower():
                    fresh_avail = self.sync_conditional_balance(token_id)
                    if fresh_avail >= 0.1:
                        sell_qty = floor_share_qty(min(float(shares), fresh_avail))
                        continue
                return LiveFillResult(False, 0.0, limit_px, error=err_str)
        return LiveFillResult(False, 0.0, limit_px, error="Exit retry exhausted")
