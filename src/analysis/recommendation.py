from __future__ import annotations

from typing import Any


HARD_BLOCKERS = {"quality:fetch_failed", "partial-factor-coverage", "extreme-volatility"}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _num(value: Any, default: float = 0.0) -> float:
    return float(value) if isinstance(value, (int, float)) else default


def _target_range(row: dict[str, Any], recommendation: str, no_target_price: bool) -> dict[str, Any]:
    close = row.get("close")
    if no_target_price or not isinstance(close, (int, float)):
        return {"low": None, "base": None, "high": None, "basis": "insufficient_data"}
    close_value = float(close)
    atr = _num(row.get("atr14"), max(close_value * 0.03, 1.0))
    if atr <= 0:
        atr = max(close_value * 0.03, 1.0)
    basis = "mixed" if any(isinstance(row.get(key), (int, float)) for key in ["pe", "pb", "dividend_yield"]) else "technical"
    if recommendation == "買入":
        low, base, high = close_value - 0.5 * atr, close_value + atr, close_value + 2.0 * atr
    elif recommendation == "賣出":
        low, base, high = close_value - 2.0 * atr, close_value - atr, close_value
    else:
        low, base, high = close_value - atr, close_value, close_value + atr
    return {
        "low": round(max(0.0, low), 2),
        "base": round(max(0.0, base), 2),
        "high": round(max(0.0, high), 2),
        "basis": basis,
    }


def build_sector_recommendation(
    row: dict[str, Any],
    recommendation_source: str = "deterministic",
    no_target_price: bool = False,
) -> dict[str, Any]:
    confidence = _num(row.get("confidence_score"), 50.0)
    idea = _num(row.get("idea_score"), 50.0)
    trend = _num(row.get("trend_score"), 50.0)
    momentum = _num(row.get("momentum_score"), 50.0)
    value = _num(row.get("value_score"), 50.0)
    fundamental = _num(row.get("fundamental_score"), 50.0)
    quality = _num(row.get("quality_score"), 50.0)
    benchmark = _num(row.get("benchmark_score"), 50.0)
    risk_control = _num(row.get("risk_control_score"), 50.0)
    volatility = _num(row.get("volatility20"), 0.0)
    flags = list(row.get("data_quality_flags") or [])
    support_count = sum(score >= 60.0 for score in [trend, momentum, value, fundamental, quality, benchmark])
    risk_score = 100.0 - risk_control
    if volatility >= 45.0:
        risk_score += 18.0
    elif volatility >= 35.0:
        risk_score += 10.0
    if flags:
        risk_score += min(20.0, len(flags) * 5.0)
    if row.get("quality_fetch_status") in {"partial", "unavailable", "fetch_failed"}:
        risk_score += 8.0
    risk_score = _clamp(risk_score, 5.0, 95.0)
    blockers = bool(HARD_BLOCKERS.intersection(flags)) or risk_score > 65.0

    trend_break = trend <= 35.0 or momentum <= 35.0 or _num(row.get("rel_to_sector_20d"), 0.0) < -5.0
    if support_count >= 2 and confidence >= 65.0 and idea >= 62.0 and risk_score <= 65.0 and not blockers:
        recommendation = "買入"
    elif trend_break and (confidence < 55.0 or risk_score >= 70.0 or idea <= 42.0):
        recommendation = "賣出"
    else:
        recommendation = "持有"

    if recommendation == "買入":
        action_view = "研究型加碼" if fundamental >= 60.0 or quality >= 60.0 else "交易型加碼"
        max_position = 8.0 if risk_score <= 45.0 else 5.0
        batch_plan = "首筆 40%，相對題材維持強勢後再分兩筆加碼。"
    elif recommendation == "賣出":
        action_view = "賣出/移出觀察"
        max_position = 0.0
        batch_plan = "已有部位先減碼，跌破失效條件時移出觀察。"
    elif risk_score >= 65.0 or trend_break:
        action_view = "降風險"
        max_position = 2.0
        batch_plan = "只留觀察倉，等趨勢或資料修復。"
    else:
        action_view = "續抱觀察"
        max_position = 4.0
        batch_plan = "維持小部位，等升級條件成立再加碼。"

    invalidation = [
        "相對題材 20 日動能轉負且無法修復",
        "confidence 低於 55 或新增重大資料警示",
    ]
    if recommendation == "買入":
        invalidation.append("跌破 SMA20 且 benchmark-relative 同步轉弱")
    elif recommendation == "賣出":
        invalidation.append("重新站回 SMA20/SMA60 且風險分數下降後再評估")
    else:
        invalidation.append("跌破 SMA60 或 quality coverage 惡化時降級")

    upgrade = [
        "相對大盤與相對題材 20 日動能轉正",
        "最新季度品質資料補齊且 confidence 回升",
    ]
    return {
        "symbol": row.get("symbol"),
        "as_of": row.get("as_of"),
        "recommendation": recommendation,
        "recommendation_source": recommendation_source,
        "confidence_score": round(confidence, 2),
        "risk_score": round(risk_score, 2),
        "action_view": action_view,
        "target_range": _target_range(row, recommendation, no_target_price),
        "position_note": {
            "max_position_pct": round(max_position, 1),
            "batch_plan": batch_plan,
            "risk_budget_pct": 0.4 if risk_score >= 65.0 else 0.6 if risk_score >= 45.0 else 0.8,
            "stop_reference": "SMA20 / 相對題材動能 / thesis break",
        },
        "invalidation_conditions": invalidation,
        "upgrade_conditions": upgrade,
        "evidence_refs": [
            "trend_score",
            "momentum_score",
            "value_score",
            "fundamental_score",
            "quality_score",
            "benchmark_score",
            "risk_control_score",
            "data_quality_flags",
        ],
        "review_notes": {
            "bull_case": f"支持理由數 {support_count}，idea score {idea:.1f}。",
            "bear_case": f"主要反方為波動 {volatility:.1f}、risk score {risk_score:.1f} 與資料旗標。",
            "risk_review": "risk gate 通過" if not blockers else "risk gate 觸發，買入建議已受限制。",
            "manager_decision": f"最後研究建議評估為{recommendation}。",
        },
    }
