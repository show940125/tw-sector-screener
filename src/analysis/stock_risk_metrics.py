from __future__ import annotations

import math
from typing import Any


def _empty_contract(status: str = "insufficient_data") -> dict[str, Any]:
    return {
        "status": status,
        "period_count": 0,
        "annualized_return_pct": 0.0,
        "annualized_volatility_pct": 0.0,
        "sharpe_ratio": 0.0,
        "sortino_ratio": 0.0,
        "max_drawdown_pct": 0.0,
        "downside_volatility_pct": 0.0,
        "calmar_ratio": 0.0,
        "win_rate": 0.0,
        "return_to_drawdown": 0.0,
        "risk_adjusted_score": 0.0,
    }


def _prices(series: list[dict[str, Any]]) -> list[float]:
    output: list[float] = []
    for item in series:
        value = item.get("close")
        if isinstance(value, (int, float)) and value > 0:
            output.append(float(value))
    return output


def _returns(values: list[float]) -> list[float]:
    return [(values[idx] / values[idx - 1]) - 1.0 for idx in range(1, len(values)) if values[idx - 1] > 0]


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(max(sum((value - mean) ** 2 for value in values) / (len(values) - 1), 0.0))


def _max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0 if peak > 0 else 0.0
        worst = min(worst, drawdown)
    return worst


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def calculate_stock_risk_metrics(price_series: list[dict[str, Any]], risk_free_rate: float = 0.0) -> dict[str, Any]:
    values = _prices(price_series)
    if len(values) < 2:
        return _empty_contract()
    returns = _returns(values)
    if not returns:
        return _empty_contract()

    period_count = len(returns)
    total_return = (values[-1] / values[0]) - 1.0 if values[0] > 0 else 0.0
    annualized_return = ((1.0 + total_return) ** (252.0 / period_count) - 1.0) if total_return > -1.0 else -1.0
    volatility = _std(returns) * math.sqrt(252.0)
    daily_rf = risk_free_rate / 252.0
    excess = [value - daily_rf for value in returns]
    excess_mean = sum(excess) / len(excess)
    sharpe = (excess_mean / _std(excess) * math.sqrt(252.0)) if _std(excess) > 0 else 0.0
    downside = [min(0.0, value - daily_rf) for value in returns]
    downside_std = _std([value for value in downside if value < 0.0])
    downside_vol = downside_std * math.sqrt(252.0)
    if downside_std > 0:
        sortino = excess_mean / downside_std * math.sqrt(252.0)
    elif annualized_return > 0:
        sortino = 99.0
    else:
        sortino = 0.0
    max_dd = _max_drawdown(values)
    calmar = annualized_return / abs(max_dd) if abs(max_dd) > 1e-12 else (99.0 if annualized_return > 0 else 0.0)
    win_rate = len([value for value in returns if value > 0]) / len(returns)
    return_to_drawdown = total_return / abs(max_dd) if abs(max_dd) > 1e-12 else (99.0 if total_return > 0 else 0.0)

    sharpe_component = _clamp((sharpe + 1.0) / 3.0 * 100.0, 0.0, 100.0)
    sortino_component = _clamp((sortino + 1.0) / 4.0 * 100.0, 0.0, 100.0)
    drawdown_component = _clamp(100.0 + (max_dd * 200.0), 0.0, 100.0)
    volatility_component = _clamp(100.0 - (volatility * 150.0), 0.0, 100.0)
    risk_adjusted_score = (
        sharpe_component * 0.35
        + sortino_component * 0.25
        + drawdown_component * 0.25
        + volatility_component * 0.15
    )

    return {
        "status": "ok",
        "period_count": period_count,
        "annualized_return_pct": round(annualized_return * 100.0, 4),
        "annualized_volatility_pct": round(volatility * 100.0, 4),
        "sharpe_ratio": round(sharpe, 4),
        "sortino_ratio": round(sortino, 4),
        "max_drawdown_pct": round(max_dd * 100.0, 4),
        "downside_volatility_pct": round(downside_vol * 100.0, 4),
        "calmar_ratio": round(calmar, 4),
        "win_rate": round(win_rate, 4),
        "return_to_drawdown": round(return_to_drawdown, 4),
        "risk_adjusted_score": round(risk_adjusted_score, 2),
    }
