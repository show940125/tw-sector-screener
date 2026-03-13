from __future__ import annotations

import math
from datetime import date
from typing import Any


def _pct_return(start: float, end: float) -> float:
    if start == 0:
        return 0.0
    return (end / start) - 1.0


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0] if equity_curve else 1.0
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0 if peak else 0.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown * 100.0


def _annualized_volatility(period_returns: list[float], periods_per_year: float) -> float:
    if len(period_returns) < 2:
        return 0.0
    mean = sum(period_returns) / len(period_returns)
    variance = sum((x - mean) ** 2 for x in period_returns) / len(period_returns)
    return math.sqrt(variance) * math.sqrt(periods_per_year) * 100.0


def run_cross_sectional_backtest(
    snapshots: list[dict[str, Any]],
    benchmark_series: list[dict[str, Any]],
    top_n: int,
    cost_bps: float = 0.0,
) -> dict[str, Any]:
    if len(snapshots) < 2:
        return {
            "rebalance_count": 0,
            "strategy_total_return_pct": 0.0,
            "benchmark_total_return_pct": 0.0,
            "basket_total_return_pct": 0.0,
            "excess_return_pct": 0.0,
            "cost_adjusted_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "annualized_volatility_pct": 0.0,
            "turnover_pct": 0.0,
            "hit_rate": 0.0,
        }

    benchmark_map = {item["date"]: float(item["close"]) for item in benchmark_series}
    strategy_curve = [1.0]
    benchmark_curve = [1.0]
    basket_curve = [1.0]
    strategy_period_returns: list[float] = []
    hit_count = 0
    total_picks = 0
    turnover_total = 0.0
    previous_holdings: set[str] = set()
    days_deltas: list[int] = []

    for idx in range(len(snapshots) - 1):
        current = snapshots[idx]
        future = snapshots[idx + 1]
        current_rows = sorted(current.get("rows") or [], key=lambda row: float(row.get("score") or 0.0), reverse=True)
        future_map = {row["symbol"]: row for row in future.get("rows") or []}
        picks = current_rows[:top_n]
        holdings = {row["symbol"] for row in picks}
        if idx == 0:
            turnover_total += 1.0 if holdings else 0.0
        else:
            changed = len(holdings.symmetric_difference(previous_holdings))
            base = max(len(holdings | previous_holdings), 1)
            turnover_total += changed / base
        previous_holdings = holdings

        pick_returns: list[float] = []
        basket_returns: list[float] = []
        for row in current_rows:
            future_row = future_map.get(row["symbol"])
            if not future_row:
                continue
            ret = _pct_return(float(row.get("close") or 0.0), float(future_row.get("close") or 0.0))
            basket_returns.append(ret)
        for row in picks:
            future_row = future_map.get(row["symbol"])
            if not future_row:
                continue
            ret = _pct_return(float(row.get("close") or 0.0), float(future_row.get("close") or 0.0))
            pick_returns.append(ret)
            total_picks += 1
            if ret > 0:
                hit_count += 1

        strategy_return = sum(pick_returns) / len(pick_returns) if pick_returns else 0.0
        cost_return = strategy_return - (cost_bps / 10000.0)
        basket_return = sum(basket_returns) / len(basket_returns) if basket_returns else 0.0
        strategy_curve.append(strategy_curve[-1] * (1.0 + cost_return))
        basket_curve.append(basket_curve[-1] * (1.0 + basket_return))
        strategy_period_returns.append(cost_return)

        current_date = current["rebalance_date"]
        next_date = future["rebalance_date"]
        if current_date in benchmark_map and next_date in benchmark_map:
            benchmark_return = _pct_return(benchmark_map[current_date], benchmark_map[next_date])
        else:
            benchmark_return = 0.0
        benchmark_curve.append(benchmark_curve[-1] * (1.0 + benchmark_return))
        days_deltas.append(max((next_date - current_date).days, 1))

    avg_days = (sum(days_deltas) / len(days_deltas)) if days_deltas else 7.0
    periods_per_year = 252.0 / avg_days
    return {
        "rebalance_count": len(snapshots) - 1,
        "strategy_total_return_pct": round((strategy_curve[-1] - 1.0) * 100.0, 2),
        "benchmark_total_return_pct": round((benchmark_curve[-1] - 1.0) * 100.0, 2),
        "basket_total_return_pct": round((basket_curve[-1] - 1.0) * 100.0, 2),
        "excess_return_pct": round((strategy_curve[-1] - benchmark_curve[-1]) * 100.0, 2),
        "cost_adjusted_return_pct": round((strategy_curve[-1] - 1.0) * 100.0, 2),
        "max_drawdown_pct": round(_max_drawdown(strategy_curve), 2),
        "annualized_volatility_pct": round(_annualized_volatility(strategy_period_returns, periods_per_year), 2),
        "turnover_pct": round((turnover_total / max(len(snapshots) - 1, 1)) * 100.0, 2),
        "hit_rate": round(hit_count / max(total_picks, 1), 4),
    }
