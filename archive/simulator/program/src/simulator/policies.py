from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.simulator.broker import BrokerConfig, build_buy_order, build_sell_order, portfolio_value

HARD_BLOCKERS = {"quality:fetch_failed", "partial-factor-coverage", "extreme-volatility"}


@dataclass(frozen=True)
class PolicySpec:
    portfolio_id: str
    name: str
    target_invested_pct: float
    min_cash_pct: float
    max_positions: int
    max_position_pct: float
    buy_confidence_min: float
    buy_risk_max: float
    allow_top_hold_buy: bool = False
    hold_buy_top_rank: int = 0
    reduce_risk_threshold: float = 75.0


POLICIES = {
    "aggressive": PolicySpec("aggressive", "激進型", 95.0, 5.0, 10, 18.0, 55.0, 80.0, True, 5, 85.0),
    "balanced": PolicySpec("balanced", "穩健型", 75.0, 25.0, 8, 12.0, 65.0, 65.0, False, 0, 75.0),
    "conservative": PolicySpec("conservative", "保守型", 50.0, 50.0, 5, 8.0, 75.0, 45.0, False, 0, 60.0),
}


def make_portfolios(initial_cash: float) -> dict[str, dict[str, Any]]:
    return {
        key: {
            "portfolio_id": spec.portfolio_id,
            "name": spec.name,
            "cash": round(float(initial_cash), 2),
            "settlements": [],
            "positions": {},
            "realized_pnl": 0.0,
        }
        for key, spec in POLICIES.items()
    }


def generate_policy_orders(
    portfolio: dict[str, Any],
    policy: PolicySpec,
    rows: list[dict[str, Any]],
    trade_date: str,
    broker_config: BrokerConfig,
) -> list[dict[str, Any]]:
    row_map = {str(row.get("symbol")): row for row in rows}
    price_map = {str(row.get("symbol")): float(row.get("close") or row.get("close_price") or 0.0) for row in rows}
    value = portfolio_value(portfolio, price_map)
    equity = value["equity"]
    orders: list[dict[str, Any]] = []
    positions = portfolio.get("positions") or {}

    for symbol, position in list(positions.items()):
        row = row_map.get(symbol)
        if not row:
            continue
        quantity = int(position.get("quantity") or 0)
        if quantity <= 0:
            continue
        recommendation = str(row.get("recommendation") or "")
        risk = _num(row.get("risk_score"))
        if recommendation == "賣出":
            order = build_sell_order(policy.portfolio_id, trade_date, row, quantity, "recommendation=賣出，退出持股", "limit")
            if order:
                orders.append(order)
        elif risk >= policy.reduce_risk_threshold:
            reduce_qty = max(1, quantity // 2)
            order = build_sell_order(policy.portfolio_id, trade_date, row, reduce_qty, f"risk_score {risk:.1f} 超過 {policy.reduce_risk_threshold:.1f}，降風險", "stop")
            if order:
                orders.append(order)

    current_symbols = set(positions.keys())
    buyable = [row for row in rows if str(row.get("symbol")) not in current_symbols and _can_buy(row, policy)]
    buyable.sort(key=lambda row: (float(row.get("rank_score") or 0.0), float(row.get("idea_score") or 0.0)), reverse=True)
    slots = max(policy.max_positions - len(current_symbols), 0)
    if slots <= 0 or not buyable:
        return orders

    max_invested = equity * (policy.target_invested_pct / 100.0)
    invested = value["holdings_value"]
    deployable_by_target = max(0.0, max_invested - invested)
    min_cash = equity * (policy.min_cash_pct / 100.0)
    deployable_by_cash = max(0.0, float(portfolio.get("cash") or 0.0) - min_cash)
    deployable = min(deployable_by_target, deployable_by_cash)
    if deployable <= 0:
        return orders

    per_position_cap = equity * (policy.max_position_pct / 100.0)
    per_order_budget = min(per_position_cap, deployable / max(min(slots, len(buyable)), 1))
    for row in buyable[:slots]:
        budget = min(per_order_budget, _row_budget_cap(row, equity, policy))
        order = build_buy_order(policy.portfolio_id, trade_date, row, budget, broker_config, _buy_reason(row, policy))
        if order:
            orders.append(order)
    return orders


def _can_buy(row: dict[str, Any], policy: PolicySpec) -> bool:
    recommendation = str(row.get("recommendation") or "")
    if recommendation == "賣出":
        return False
    confidence = _num(row.get("confidence_score"))
    risk = _num(row.get("risk_score"), 100.0)
    if HARD_BLOCKERS.intersection({str(flag) for flag in (row.get("data_quality_flags") or [])}):
        return False
    buying_tier = str(row.get("buying_tier") or "")
    if buying_tier == "formal_buy":
        return confidence >= policy.buy_confidence_min and risk <= policy.buy_risk_max and not _blocked_by_conservative_rules(row, policy)
    if buying_tier == "risk_adjusted_buy":
        if confidence < policy.buy_confidence_min or risk > policy.buy_risk_max:
            return False
        if policy.portfolio_id == "conservative":
            return risk <= 55.0 and _risk_adjusted_score(row) >= 70.0 and not _blocked_by_conservative_rules(row, policy)
        return policy.portfolio_id in {"balanced", "aggressive"}
    if buying_tier == "tactical_buy":
        return policy.portfolio_id == "aggressive" and confidence >= policy.buy_confidence_min and risk <= policy.buy_risk_max
    if recommendation == "買入":
        return confidence >= policy.buy_confidence_min and risk <= policy.buy_risk_max and not _blocked_by_conservative_rules(row, policy)
    if policy.allow_top_hold_buy and recommendation == "持有":
        rank = int(row.get("rank") or 999)
        return (
            row.get("decision_tier") == "starter_position"
            and rank <= policy.hold_buy_top_rank
            and risk <= policy.buy_risk_max
            and confidence >= policy.buy_confidence_min
        )
    return False


def _row_budget_cap(row: dict[str, Any], equity: float, policy: PolicySpec) -> float:
    if row.get("buying_tier") != "tactical_buy" and row.get("decision_tier") != "starter_position":
        return equity * (policy.max_position_pct / 100.0)
    pct = _num(row.get("starter_position_pct"), 0.25)
    pct = min(max(pct, 0.0), 0.5)
    return equity * (pct / 100.0)


def _blocked_by_conservative_rules(row: dict[str, Any], policy: PolicySpec) -> bool:
    if policy.portfolio_id != "conservative":
        return False
    target = row.get("target_range") or {}
    if not isinstance(target, dict) or target.get("basis") == "insufficient_data":
        return True
    flags = row.get("data_quality_flags") or []
    if flags:
        return True
    volatility = _num(row.get("volatility20"))
    return volatility >= 35.0


def _buy_reason(row: dict[str, Any], policy: PolicySpec) -> str:
    recommendation = row.get("recommendation")
    buying_tier = row.get("buying_tier") or "legacy"
    return f"{policy.name} 依共用 screener：rank {row.get('rank')}、{recommendation}、buying_tier {buying_tier}、confidence {row.get('confidence_score')}、risk {row.get('risk_score')}"


def _risk_adjusted_score(row: dict[str, Any]) -> float:
    metrics = row.get("stock_risk_metrics") or {}
    return _num(row.get("risk_adjusted_score"), _num(metrics.get("risk_adjusted_score"), 0.0))


def _num(value: Any, default: float = 0.0) -> float:
    return float(value) if isinstance(value, (int, float)) else default
