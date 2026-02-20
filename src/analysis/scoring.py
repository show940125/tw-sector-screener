from __future__ import annotations

from typing import Any


WEIGHTS = {
    "trend_score": 0.35,
    "momentum_score": 0.25,
    "value_score": 0.20,
    "fundamental_score": 0.15,
    "risk_control_score": 0.05,
}


def score_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored: list[dict[str, Any]] = []
    for row in rows:
        total = 0.0
        for key, weight in WEIGHTS.items():
            total += float(row.get(key, 50.0)) * weight
        enriched = {**row, "total_score": round(total, 2)}
        scored.append(enriched)
    return sorted(scored, key=lambda x: x["total_score"], reverse=True)
