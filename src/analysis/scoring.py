from __future__ import annotations

from typing import Any


WEIGHTS = {
    "trend_score": 0.28,
    "momentum_score": 0.22,
    "value_score": 0.16,
    "fundamental_score": 0.16,
    "quality_score": 0.10,
    "benchmark_score": 0.05,
    "risk_control_score": 0.03,
}


def _safe_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def score_candidates(
    rows: list[dict[str, Any]],
    weights: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    active_weights = weights or WEIGHTS
    scored: list[dict[str, Any]] = []
    for row in rows:
        weighted_sum = 0.0
        covered_weight = 0.0
        missing_flags: list[str] = []
        factor_breakdown: dict[str, dict[str, float]] = {}

        for key, weight in active_weights.items():
            if key not in row:
                continue
            score = _safe_number(row.get(key))
            if score is None:
                missing_flags.append(f"missing:{key}")
                continue
            weighted_sum += score * weight
            covered_weight += weight
            factor_breakdown[key] = {
                "score": round(score, 2),
                "weight": round(weight, 4),
                "contribution": round(score * weight, 2),
            }

        missing_factor_count = len(missing_flags)
        idea_score = round((weighted_sum / covered_weight), 2) if covered_weight > 0 else 0.0
        factor_coverage_confidence = max(20.0, round((covered_weight * 100.0) - (missing_factor_count * 6.0), 2))
        data_freshness_confidence = 100.0
        data_quality_flags = list(missing_flags)
        quality_status = str(row.get("quality_fetch_status") or "").strip().lower()
        quality_missing_reason = str(row.get("quality_missing_reason") or "").strip().lower()
        if quality_status == "partial":
            data_freshness_confidence -= 12.0
        elif quality_status == "unavailable":
            data_freshness_confidence -= 22.0
        elif quality_status == "fetch_failed":
            data_freshness_confidence -= 35.0
        if quality_missing_reason == "previous_period_unavailable":
            data_freshness_confidence -= 8.0
        if quality_missing_reason == "fetch_failed":
            data_quality_flags.append("quality:fetch_failed")
        if quality_missing_reason == "unavailable":
            data_quality_flags.append("quality:unavailable")
        if covered_weight < 0.75:
            data_quality_flags.append("partial-factor-coverage")
        volatility20 = _safe_number(row.get("volatility20"))
        if volatility20 is not None and volatility20 >= 80:
            data_quality_flags.append("extreme-volatility")
        liquidity20 = _safe_number(row.get("liquidity20"))
        if liquidity20 is not None and liquidity20 <= 0:
            data_quality_flags.append("no-liquidity-signal")
        extra_flags = row.get("data_quality_flags") or []
        if isinstance(extra_flags, list):
            data_quality_flags.extend(str(flag) for flag in extra_flags if str(flag))
        data_quality_flags = list(dict.fromkeys(data_quality_flags))
        data_freshness_confidence = max(25.0, round(data_freshness_confidence, 2))
        confidence_score = round(((factor_coverage_confidence * 0.7) + (data_freshness_confidence * 0.3)), 2)
        rank_score = round(idea_score * (confidence_score / 100.0), 2)
        enriched = {
            **row,
            "idea_score": idea_score,
            "rank_score": rank_score,
            "total_score": rank_score,
            "confidence_score": confidence_score,
            "factor_coverage_confidence": factor_coverage_confidence,
            "data_freshness_confidence": data_freshness_confidence,
            "missing_factor_count": missing_factor_count,
            "data_quality_flags": data_quality_flags,
            "factor_breakdown": factor_breakdown,
        }
        scored.append(enriched)
    return sorted(scored, key=lambda x: (x["rank_score"], x["idea_score"]), reverse=True)
