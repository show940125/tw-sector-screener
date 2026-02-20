from __future__ import annotations

import math
from typing import Any


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "--", "-", "X", "N/A", "None", "nan"}:
        return None
    if text.startswith("+"):
        text = text[1:]
    try:
        return float(text)
    except ValueError:
        return None


def percentile_rank(current: float | None, values: list[float]) -> float | None:
    if current is None or not values:
        return None
    count = sum(1 for x in values if x <= current)
    return (count / len(values)) * 100.0


def sma(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    chunk = values[-window:]
    return sum(chunk) / window


def rsi_wilder(closes: list[float], window: int = 14) -> float | None:
    if len(closes) < window + 1:
        return None
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(x, 0.0) for x in diffs]
    losses = [max(-x, 0.0) for x in diffs]
    avg_gain = sum(gains[:window]) / window
    avg_loss = sum(losses[:window]) / window
    for i in range(window, len(gains)):
        avg_gain = ((avg_gain * (window - 1)) + gains[i]) / window
        avg_loss = ((avg_loss * (window - 1)) + losses[i]) / window
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr_wilder(candles: list[dict[str, Any]], window: int = 14) -> float | None:
    if len(candles) < window + 1:
        return None
    true_ranges: list[float] = []
    for i in range(1, len(candles)):
        high = float(candles[i]["high"])
        low = float(candles[i]["low"])
        prev_close = float(candles[i - 1]["close"])
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr_value = sum(true_ranges[:window]) / window
    for i in range(window, len(true_ranges)):
        atr_value = ((atr_value * (window - 1)) + true_ranges[i]) / window
    return atr_value


def volatility_annualized(closes: list[float], window: int = 20) -> float | None:
    if len(closes) < window + 1:
        return None
    returns = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev == 0:
            continue
        returns.append((closes[i] - prev) / prev)
    if len(returns) < window:
        return None
    chunk = returns[-window:]
    mean = sum(chunk) / len(chunk)
    variance = sum((x - mean) ** 2 for x in chunk) / len(chunk)
    return math.sqrt(variance) * math.sqrt(252.0) * 100.0


def momentum_return(closes: list[float], lookback: int) -> float | None:
    if len(closes) <= lookback:
        return None
    base = closes[-lookback - 1]
    if base == 0:
        return None
    return (closes[-1] / base - 1.0) * 100.0


def trend_score(close: float, sma20: float | None, sma60: float | None, sma120: float | None, rsi14: float | None) -> float:
    score = 50.0
    if sma20 and sma60 and sma120:
        if close > sma20 > sma60 > sma120:
            score += 30.0
        elif close < sma20 < sma60 < sma120:
            score -= 30.0
        elif close > sma20:
            score += 10.0
        else:
            score -= 10.0
    if rsi14 is not None:
        if 45.0 <= rsi14 <= 65.0:
            score += 10.0
        elif rsi14 > 75.0:
            score -= 8.0
        elif rsi14 < 30.0:
            score += 5.0
    return max(0.0, min(100.0, score))


def position_plan(score: float, close: float, atr14: float | None, volatility20: float | None) -> dict[str, Any]:
    if score >= 80:
        max_position = 12.0
    elif score >= 70:
        max_position = 9.0
    elif score >= 60:
        max_position = 6.0
    else:
        max_position = 3.0

    if volatility20 is None:
        risk_budget = 0.6
    elif volatility20 >= 35:
        risk_budget = 0.4
    elif volatility20 >= 25:
        risk_budget = 0.6
    else:
        risk_budget = 0.8

    result: dict[str, Any] = {
        "max_position_pct": max_position,
        "initial_position_pct": round(max_position * 0.4, 1),
        "add_position_pct_1": round(max_position * 0.3, 1),
        "add_position_pct_2": round(max_position - (round(max_position * 0.4, 1) + round(max_position * 0.3, 1)), 1),
        "risk_budget_pct": risk_budget,
        "share_formula": "可買股數 = (資金 x 單筆風險%) / (進場價 - 停損價)",
    }
    if atr14 is not None and close > 0:
        stop_price = max(0.0, close - (2.0 * atr14))
        result["stop_price"] = round(stop_price, 2)
        result["stop_distance_pct"] = round((2.0 * atr14 / close) * 100.0, 2)
    return result
