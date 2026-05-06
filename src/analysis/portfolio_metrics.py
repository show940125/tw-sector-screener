from __future__ import annotations

import math
from typing import Any


def _empty_contract(status: str = "insufficient_data") -> dict[str, Any]:
    return {
        "status": status,
        "period_count": 0,
        "var95_pct": 0.0,
        "cvar95_pct": 0.0,
        "ulcer_index": 0.0,
        "omega_ratio": 0.0,
        "tail_ratio": 0.0,
        "rolling_sharpe": [],
        "rolling_volatility": [],
        "rolling_drawdown": [],
        "alpha_pct": 0.0,
        "beta": 0.0,
        "information_ratio": 0.0,
        "tracking_error_pct": 0.0,
        "benchmark_status": "not_provided",
    }


def _to_equity_points(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for item in series:
        value = item.get("equity", item.get("close"))
        if not isinstance(value, (int, float)) or value <= 0:
            continue
        points.append({"date": item.get("date"), "equity": float(value)})
    return points


def _returns(values: list[float]) -> list[float]:
    output: list[float] = []
    for idx in range(1, len(values)):
        previous = values[idx - 1]
        current = values[idx]
        if previous > 0:
            output.append((current / previous) - 1.0)
    return output


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * pct
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return ordered[int(pos)]
    return ordered[low] + ((ordered[high] - ordered[low]) * (pos - low))


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(max(variance, 0.0))


def _covariance(left: list[float], right: list[float]) -> float:
    n = min(len(left), len(right))
    if n < 2:
        return 0.0
    left_values = left[:n]
    right_values = right[:n]
    left_mean = sum(left_values) / n
    right_mean = sum(right_values) / n
    return sum((left_values[i] - left_mean) * (right_values[i] - right_mean) for i in range(n)) / (n - 1)


def _rolling_drawdown(values: list[float], dates: list[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    peak = values[0]
    for idx, value in enumerate(values):
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0 if peak else 0.0
        result.append({"date": str(dates[idx]) if dates[idx] is not None else str(idx), "value": round(drawdown * 100.0, 4)})
    return result


def _rolling_stat(returns: list[float], dates: list[Any], window: int, kind: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    if len(returns) < window:
        return result
    for idx in range(window, len(returns) + 1):
        chunk = returns[idx - window : idx]
        std = _std(chunk)
        if kind == "sharpe":
            value = ((sum(chunk) / len(chunk)) / std * math.sqrt(252.0)) if std > 0 else 0.0
        else:
            value = std * math.sqrt(252.0) * 100.0
        date_value = dates[idx] if idx < len(dates) else idx
        result.append({"date": str(date_value), "value": round(value, 4)})
    return result


def calculate_portfolio_diagnostics(
    equity_series: list[dict[str, Any]],
    benchmark_series: list[dict[str, Any]] | None = None,
    risk_free_rate: float = 0.0,
) -> dict[str, Any]:
    points = _to_equity_points(equity_series)
    if len(points) < 2:
        return _empty_contract()

    values = [point["equity"] for point in points]
    dates = [point.get("date") for point in points]
    returns = _returns(values)
    if len(returns) < 1:
        return _empty_contract()

    var95 = _percentile(returns, 0.05)
    cvar_values = [x for x in returns if x <= var95]
    cvar95 = sum(cvar_values) / len(cvar_values) if cvar_values else var95
    drawdowns = [item["value"] for item in _rolling_drawdown(values, dates)]
    ulcer_index = math.sqrt(sum(x**2 for x in drawdowns) / len(drawdowns)) if drawdowns else 0.0
    threshold = risk_free_rate / 252.0
    gains = sum(x - threshold for x in returns if x > threshold)
    losses = -sum(x - threshold for x in returns if x < threshold)
    omega = gains / losses if losses > 0 else 99.0
    p95 = _percentile(returns, 0.95)
    tail_ratio = abs(p95 / var95) if abs(var95) > 1e-12 else 99.0

    benchmark_status = "not_provided"
    alpha = beta = information_ratio = tracking_error = 0.0
    if benchmark_series is not None:
        benchmark_points = _to_equity_points(benchmark_series)
        if len(benchmark_points) != len(points):
            benchmark_status = "length_mismatch"
        else:
            benchmark_returns = _returns([point["equity"] for point in benchmark_points])
            benchmark_std = _std(benchmark_returns)
            if len(benchmark_returns) == len(returns) and benchmark_std > 0:
                benchmark_status = "ok"
                variance = benchmark_std**2
                beta = _covariance(returns, benchmark_returns) / variance if variance > 0 else 0.0
                daily_rf = risk_free_rate / 252.0
                alpha = ((sum(returns) / len(returns)) - daily_rf - beta * ((sum(benchmark_returns) / len(benchmark_returns)) - daily_rf)) * 252.0
                active_returns = [returns[i] - benchmark_returns[i] for i in range(len(returns))]
                tracking_error = _std(active_returns) * math.sqrt(252.0)
                information_ratio = ((sum(active_returns) / len(active_returns)) * 252.0 / tracking_error) if tracking_error > 0 else 0.0
            else:
                benchmark_status = "insufficient_benchmark_data"

    window = min(20, max(2, len(returns)))
    diagnostics = {
        "status": "ok",
        "period_count": len(returns),
        "var95_pct": round(var95 * 100.0, 4),
        "cvar95_pct": round(cvar95 * 100.0, 4),
        "ulcer_index": round(ulcer_index, 4),
        "omega_ratio": round(omega, 4),
        "tail_ratio": round(tail_ratio, 4),
        "rolling_sharpe": _rolling_stat(returns, dates, window, "sharpe"),
        "rolling_volatility": _rolling_stat(returns, dates, window, "volatility"),
        "rolling_drawdown": _rolling_drawdown(values, dates),
        "alpha_pct": round(alpha * 100.0, 4),
        "beta": round(beta, 4),
        "information_ratio": round(information_ratio, 4),
        "tracking_error_pct": round(tracking_error * 100.0, 4),
        "benchmark_status": benchmark_status,
    }
    return diagnostics
