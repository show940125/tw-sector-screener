from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def write_json_report(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_candidate_csv(path: Path, picks: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "rank",
        "symbol",
        "name",
        "market",
        "industry",
        "idea_score",
        "rank_score",
        "confidence_score",
        "risk_adjusted_score",
        "buying_tier",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "recommendation",
        "risk_score",
        "research_action_view",
        "action",
        "thesis_summary",
        "catalyst_notes",
        "data_quality_flags",
        "ret_20d",
        "rel_to_taiex_20d",
        "rel_to_sector_20d",
        "rel_to_industry_20d",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for item in picks:
            metrics = item.get("stock_risk_metrics") or {}
            writer.writerow(
                {
                    "rank": item.get("rank"),
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "market": item.get("market"),
                    "industry": item.get("industry"),
                    "idea_score": item.get("idea_score"),
                    "rank_score": item.get("rank_score"),
                    "confidence_score": item.get("confidence_score"),
                    "risk_adjusted_score": item.get("risk_adjusted_score") or metrics.get("risk_adjusted_score"),
                    "buying_tier": item.get("buying_tier"),
                    "sharpe_ratio": metrics.get("sharpe_ratio"),
                    "sortino_ratio": metrics.get("sortino_ratio"),
                    "max_drawdown_pct": metrics.get("max_drawdown_pct"),
                    "annualized_volatility_pct": metrics.get("annualized_volatility_pct"),
                    "recommendation": item.get("recommendation"),
                    "risk_score": item.get("risk_score"),
                    "research_action_view": item.get("research_action_view"),
                    "action": (item.get("action_view") or {}).get("action"),
                    "thesis_summary": item.get("thesis_summary"),
                    "catalyst_notes": " / ".join(item.get("catalyst_notes") or []),
                    "data_quality_flags": " / ".join(item.get("data_quality_flags") or []),
                    "ret_20d": (item.get("trend") or {}).get("ret_20d"),
                    "rel_to_taiex_20d": (item.get("benchmark_view") or {}).get("rel_to_taiex_20d"),
                    "rel_to_sector_20d": (item.get("benchmark_view") or {}).get("rel_to_sector_20d"),
                    "rel_to_industry_20d": (item.get("benchmark_view") or {}).get("rel_to_industry_20d"),
                }
            )
    return path


def write_audit_trail(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_watchlist(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
