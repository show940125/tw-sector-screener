from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BrokerConfig:
    commission_bps: float = 14.25
    sell_tax_bps: float = 30.0
    min_commission: float = 20.0
    lot_size: int = 1
    price_limit_pct: float = 10.0


def _money(value: float) -> float:
    return round(float(value), 2)


def _shares(value: float, lot_size: int) -> int:
    lot = max(int(lot_size), 1)
    return int(value // lot) * lot


def release_settlements(portfolio: dict[str, Any], trade_date: str) -> None:
    pending = []
    for item in portfolio.get("settlements", []):
        if str(item.get("available_date")) <= trade_date:
            portfolio["cash"] = _money(float(portfolio.get("cash") or 0.0) + float(item.get("amount") or 0.0))
        else:
            pending.append(item)
    portfolio["settlements"] = pending


def portfolio_value(portfolio: dict[str, Any], price_map: dict[str, float]) -> dict[str, float]:
    holdings_value = 0.0
    for symbol, position in (portfolio.get("positions") or {}).items():
        price = float(price_map.get(symbol) or position.get("last_price") or position.get("avg_cost") or 0.0)
        position["last_price"] = price
        holdings_value += float(position.get("quantity") or 0) * price
    unsettled = sum(float(item.get("amount") or 0.0) for item in portfolio.get("settlements", []))
    cash = float(portfolio.get("cash") or 0.0)
    equity = cash + unsettled + holdings_value
    return {
        "cash": _money(cash),
        "unsettled_cash": _money(unsettled),
        "holdings_value": _money(holdings_value),
        "equity": _money(equity),
    }


def build_buy_order(
    portfolio_id: str,
    trade_date: str,
    row: dict[str, Any],
    cash_budget: float,
    config: BrokerConfig,
    reason: str,
    policy_violation: bool = False,
) -> dict[str, Any] | None:
    close = float(row.get("close") or row.get("close_price") or 0.0)
    if close <= 0 or cash_budget <= 0:
        return None
    target = row.get("target_range") or {}
    low = target.get("low") if isinstance(target, dict) else None
    limit_price = float(low) if isinstance(low, (int, float)) and float(low) > 0 else close * 1.01
    limit_price = max(0.01, min(limit_price, close * 1.03))
    per_share_cost = limit_price * (1.0 + (config.commission_bps / 10000.0))
    quantity = _shares(cash_budget / per_share_cost, config.lot_size)
    if quantity <= 0:
        return None
    return {
        "portfolio_id": portfolio_id,
        "date": trade_date,
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "market": row.get("market"),
        "side": "buy",
        "order_type": "limit",
        "quantity": quantity,
        "limit_price": round(limit_price, 2),
        "reason": reason,
        "policy_violation": policy_violation,
        "analysis_ref": _analysis_ref(row),
        "status": "pending",
    }


def build_sell_order(
    portfolio_id: str,
    trade_date: str,
    row: dict[str, Any],
    quantity: int,
    reason: str,
    order_type: str = "limit",
    policy_violation: bool = False,
) -> dict[str, Any] | None:
    close = float(row.get("close") or row.get("close_price") or 0.0)
    if close <= 0 or quantity <= 0:
        return None
    target = row.get("target_range") or {}
    base = target.get("base") if isinstance(target, dict) else None
    atr = float(row.get("atr14") or max(close * 0.03, 1.0))
    if order_type == "stop":
        stop_price = max(0.01, close - (1.5 * atr))
        limit_price = None
    else:
        stop_price = None
        limit_price = float(base) if isinstance(base, (int, float)) and float(base) > 0 else close * 0.99
    return {
        "portfolio_id": portfolio_id,
        "date": trade_date,
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "market": row.get("market"),
        "side": "sell",
        "order_type": order_type,
        "quantity": int(quantity),
        "limit_price": round(limit_price, 2) if limit_price else None,
        "stop_price": round(stop_price, 2) if stop_price else None,
        "reason": reason,
        "policy_violation": policy_violation,
        "analysis_ref": _analysis_ref(row),
        "status": "pending",
    }


def execute_orders(
    portfolio: dict[str, Any],
    orders: list[dict[str, Any]],
    candles: dict[str, dict[str, Any]],
    settlement_dates: dict[str, str],
    config: BrokerConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    final_orders: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for order in orders:
        order = dict(order)
        candle = candles.get(str(order.get("symbol")))
        if not candle:
            order["status"] = "market_data_missing"
            final_orders.append(order)
            continue
        fill_price, status = simulate_fill(order, candle, config)
        if fill_price is None:
            order["status"] = status
            final_orders.append(order)
            continue
        trade = apply_fill(portfolio, order, fill_price, settlement_dates.get(str(order.get("date")), str(order.get("date"))), config)
        order["status"] = trade["status"]
        order["fill_price"] = trade["fill_price"]
        order["filled_quantity"] = trade["quantity"]
        final_orders.append(order)
        if trade["status"] == "filled":
            trades.append(trade)
    return final_orders, trades


def simulate_fill(order: dict[str, Any], candle: dict[str, Any], config: BrokerConfig) -> tuple[float | None, str]:
    open_price = float(candle.get("open") or candle.get("close") or 0.0)
    high = float(candle.get("high") or open_price)
    low = float(candle.get("low") or open_price)
    close = float(candle.get("close") or open_price)
    prev_close = float(order.get("analysis_ref", {}).get("close") or order.get("limit_price") or order.get("stop_price") or close)
    limit_up = prev_close * (1.0 + (config.price_limit_pct / 100.0))
    limit_down = prev_close * (1.0 - (config.price_limit_pct / 100.0))
    locked_up = all(abs(x - limit_up) / max(limit_up, 1.0) < 0.002 for x in [open_price, high, low, close])
    locked_down = all(abs(x - limit_down) / max(limit_down, 1.0) < 0.002 for x in [open_price, high, low, close])

    if order.get("side") == "buy":
        limit_price = float(order.get("limit_price") or 0.0)
        if open_price <= limit_price:
            return open_price, "filled"
        if low <= limit_price <= high:
            return limit_price, "filled"
        return None, "limit_up_blocked" if locked_up else "not_filled"

    if order.get("order_type") == "bracket":
        stop_price = float(order.get("stop_price") or 0.0)
        limit_price = float(order.get("limit_price") or 0.0)
        stop_hit = open_price <= stop_price or low <= stop_price
        target_hit = open_price >= limit_price or high >= limit_price
        if stop_hit and target_hit:
            return min(open_price, stop_price), "filled_conservative_stop_first"
        if stop_hit:
            return (open_price if open_price <= stop_price else stop_price), "filled"
        if target_hit:
            return (open_price if open_price >= limit_price else limit_price), "filled"
        return None, "limit_down_trapped" if locked_down else "not_filled"

    if order.get("order_type") == "stop":
        stop_price = float(order.get("stop_price") or 0.0)
        if open_price <= stop_price:
            return open_price, "filled"
        if low <= stop_price:
            return stop_price, "filled"
        return None, "limit_down_trapped" if locked_down else "not_filled"

    limit_price = float(order.get("limit_price") or 0.0)
    if open_price >= limit_price:
        return open_price, "filled"
    if high >= limit_price:
        return limit_price, "filled"
    return None, "limit_down_trapped" if locked_down else "not_filled"


def apply_fill(
    portfolio: dict[str, Any],
    order: dict[str, Any],
    fill_price: float,
    settlement_date: str,
    config: BrokerConfig,
) -> dict[str, Any]:
    symbol = str(order["symbol"])
    requested_quantity = int(order.get("quantity") or 0)
    quantity = requested_quantity
    gross = fill_price * quantity
    commission = max(gross * (config.commission_bps / 10000.0), config.min_commission)
    tax = 0.0
    status = "filled"
    if order.get("side") == "buy":
        total_cost = gross + commission
        if total_cost > float(portfolio.get("cash") or 0.0):
            affordable = _shares(float(portfolio.get("cash") or 0.0) / (fill_price * (1.0 + (config.commission_bps / 10000.0))), config.lot_size)
            quantity = max(0, affordable)
            gross = fill_price * quantity
            commission = max(gross * (config.commission_bps / 10000.0), config.min_commission) if quantity else 0.0
            total_cost = gross + commission
        if quantity <= 0:
            return {**_trade_base(order, fill_price, 0, 0.0, 0.0, 0.0), "status": "cash_blocked"}
        portfolio["cash"] = _money(float(portfolio.get("cash") or 0.0) - total_cost)
        position = (portfolio.setdefault("positions", {})).setdefault(
            symbol,
            {"symbol": symbol, "name": order.get("name"), "market": order.get("market"), "quantity": 0, "avg_cost": 0.0, "last_price": fill_price},
        )
        old_qty = int(position.get("quantity") or 0)
        old_cost = float(position.get("avg_cost") or 0.0) * old_qty
        position["quantity"] = old_qty + quantity
        position["avg_cost"] = _money((old_cost + gross + commission) / max(position["quantity"], 1))
        position["last_price"] = fill_price
    else:
        position = (portfolio.setdefault("positions", {})).get(symbol)
        if not position or int(position.get("quantity") or 0) <= 0:
            return {**_trade_base(order, fill_price, 0, 0.0, 0.0, 0.0), "status": "no_position"}
        quantity = min(quantity, int(position.get("quantity") or 0))
        gross = fill_price * quantity
        commission = max(gross * (config.commission_bps / 10000.0), config.min_commission)
        tax = gross * (config.sell_tax_bps / 10000.0)
        proceeds = gross - commission - tax
        position["quantity"] = int(position.get("quantity") or 0) - quantity
        position["last_price"] = fill_price
        if position["quantity"] <= 0:
            portfolio["positions"].pop(symbol, None)
        portfolio.setdefault("settlements", []).append({"amount": _money(proceeds), "available_date": settlement_date})
    return {**_trade_base(order, fill_price, quantity, gross, commission, tax), "status": status}


def _trade_base(order: dict[str, Any], fill_price: float, quantity: int, gross: float, commission: float, tax: float) -> dict[str, Any]:
    return {
        "portfolio_id": order.get("portfolio_id"),
        "date": order.get("date"),
        "symbol": order.get("symbol"),
        "name": order.get("name"),
        "side": order.get("side"),
        "quantity": quantity,
        "fill_price": _money(fill_price),
        "gross": _money(gross),
        "commission": _money(commission),
        "tax": _money(tax),
        "reason": order.get("reason"),
        "policy_violation": bool(order.get("policy_violation")),
        "analysis_ref": order.get("analysis_ref") or {},
    }


def _analysis_ref(row: dict[str, Any]) -> dict[str, Any]:
    action = row.get("action_view")
    if isinstance(action, dict):
        action_value = action.get("action") or action.get("research_action")
    else:
        action_value = action
    detail = row.get("recommendation_detail") or {}
    evidence_refs = row.get("evidence_refs") or detail.get("evidence_refs") or []
    return {
        "symbol": row.get("symbol"),
        "as_of": row.get("as_of"),
        "rank": row.get("rank"),
        "idea_score": row.get("idea_score"),
        "recommendation": row.get("recommendation"),
        "confidence_score": row.get("confidence_score"),
        "risk_score": row.get("risk_score"),
        "action_view": action_value,
        "evidence_refs": evidence_refs,
        "close": row.get("close") or row.get("close_price"),
    }
