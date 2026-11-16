"""
Polymarket Short-Term Crypto CLOB Fee & Position Sizing Math.

Taker Fee Formula (Polymarket Crypto Series):
    Fee = C * feeRate * p * (1 - p)
where:
    C       = number of shares
    feeRate = 0.07 (Crypto baseline parameter)
    p       = share execution price in (0, 1)

Makers (resting limit orders) pay 0 fee and earn a share of the daily 20% maker rebate pool.
"""
from dataclasses import dataclass


def taker_fee_per_share(price: float, fee_rate: float = 0.07) -> float:
    """Return Polymarket taker fee per 1 share at execution price p in (0, 1)."""
    if price <= 0.0 or price >= 1.0:
        return 0.0
    raw_fee = fee_rate * price * (1.0 - price)
    # Polymarket rounds fees to 5 decimal places, minimum threshold 0.00001 USDC
    rounded = round(raw_fee, 5)
    return rounded if rounded >= 0.00001 else 0.0


def effective_entry_cost(price: float, is_maker: bool = False, fee_rate: float = 0.07) -> float:
    """Total USDC outlay per share to acquire 1 share (payout = $1.00 if win, $0.00 if loss)."""
    if is_maker:
        return price
    return price + taker_fee_per_share(price, fee_rate=fee_rate)


def effective_exit_proceeds(bid_price: float, is_maker: bool = False, fee_rate: float = 0.07) -> float:
    """Net USDC received per share when selling 1 existing share early on the CLOB."""
    if bid_price <= 0.0:
        return 0.0
    if is_maker:
        return bid_price
    return max(0.0, bid_price - taker_fee_per_share(bid_price, fee_rate=fee_rate))


def breakeven_win_prob(price: float, is_maker: bool = False, fee_rate: float = 0.07) -> float:
    """Minimum true win probability required to break even when holding to settlement."""
    return effective_entry_cost(price, is_maker=is_maker, fee_rate=fee_rate)


@dataclass(frozen=True)
class EVAnalysis:
    entry_price: float
    fee_per_share: float
    total_cost_per_share: float
    p_true: float
    net_ev_per_share: float
    roi_ev_pct: float
    kelly_full_pct: float


def analyze_trade_ev(
    p_true: float,
    entry_price: float,
    is_maker: bool = False,
    fee_rate: float = 0.07,
) -> EVAnalysis:
    """
    Evaluate Expected Value and Kelly Criterion for buying a binary outcome share.
    Winning share settles at $1.00 USDC; losing share settles at $0.00 USDC.
    """
    fee = 0.0 if is_maker else taker_fee_per_share(entry_price, fee_rate=fee_rate)
    cost = entry_price + fee
    # EV per share = P_true * 1.00 + (1 - P_true) * 0.00 - cost = P_true - cost
    net_ev = p_true - cost
    roi_ev = (net_ev / cost) if cost > 0 else 0.0

    # Exact Kelly fraction of bankroll to risk (outlay / bankroll) on binary payout:
    # Net profit per $1 risked if win: b = (1.0 - cost) / cost
    # Kelly f* = p - (1 - p) / b = (p - cost) / (1.0 - cost)
    if cost >= 1.0 or cost <= 0.0 or net_ev <= 0.0:
        kelly_full = 0.0
    else:
        kelly_full = net_ev / (1.0 - cost)

    return EVAnalysis(
        entry_price=entry_price,
        fee_per_share=fee,
        total_cost_per_share=cost,
        p_true=p_true,
        net_ev_per_share=net_ev,
        roi_ev_pct=roi_ev * 100.0,
        kelly_full_pct=kelly_full,
    )


def compute_position_shares(
    bankroll_usdc: float,
    ev: EVAnalysis,
    kelly_fraction: float = 0.20,
    max_risk_pct: float = 0.05,
) -> float:
    """
    Compute position size (in shares) using Fractional Kelly capped by max_risk_pct.
    """
    if ev.net_ev_per_share <= 0.0 or ev.total_cost_per_share <= 0.0:
        return 0.0
    target_outlay_pct = min(ev.kelly_full_pct * kelly_fraction, max_risk_pct)
    outlay_usdc = bankroll_usdc * max(0.0, target_outlay_pct)
    if outlay_usdc < 1.0:  # Minimum practical Polymarket ticket size
        return 0.0
    return round(outlay_usdc / ev.total_cost_per_share, 2)
