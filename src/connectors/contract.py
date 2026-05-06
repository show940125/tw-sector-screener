from __future__ import annotations

from datetime import date
from typing import Any


def build_connector_result(
    *,
    success: bool,
    source: str,
    as_of: date | str,
    data: dict[str, Any] | None = None,
    error: str | None = None,
    freshness: str | None = None,
    license_hint: str = "supplementary",
) -> dict[str, Any]:
    source_name = str(source or "unknown").strip() or "unknown"
    payload = {
        "success": bool(success),
        "data": data or {},
        "error": error,
        "source": source_name,
        "as_of": as_of.isoformat() if isinstance(as_of, date) else str(as_of),
        "freshness": freshness or ("current" if success else "unavailable"),
        "license_hint": license_hint,
        "data_quality_flags": [],
    }
    if not success:
        payload["data_quality_flags"].append(f"connector:{source_name}:failed")
    if license_hint != "tier_a_official":
        payload["data_quality_flags"].append(f"connector:{source_name}:supplementary")
    return payload


def normalize_macro_overlay(result: dict[str, Any]) -> dict[str, Any]:
    source = str(result.get("source") or "unknown")
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    risk_adjustment = data.get("risk_adjustment", 0.0)
    if not isinstance(risk_adjustment, (int, float)):
        risk_adjustment = 0.0
    risk_adjustment = max(-10.0, min(25.0, float(risk_adjustment)))
    evidence_refs = list(data.get("evidence_refs") or [])
    if "macro_regime_overlay" not in evidence_refs:
        evidence_refs.append("macro_regime_overlay")
    if source not in evidence_refs:
        evidence_refs.append(source)
    return {
        "tier": "supplementary",
        "source": source,
        "as_of": result.get("as_of"),
        "freshness": result.get("freshness"),
        "success": bool(result.get("success")),
        "regime": str(data.get("regime") or "neutral"),
        "risk_level": str(data.get("risk_level") or "normal"),
        "risk_adjustment": risk_adjustment,
        "rank_signal": False,
        "evidence_refs": evidence_refs,
        "data_quality_flags": list(result.get("data_quality_flags") or []),
        "license_hint": result.get("license_hint") or "supplementary",
    }


def build_local_macro_overlay(market_overview: dict[str, Any], as_of: date) -> dict[str, Any]:
    ret20 = market_overview.get("ret_20d")
    trend = market_overview.get("trend_score")
    rsi14 = market_overview.get("rsi14")
    risk_adjustment = 0.0
    risk_level = "normal"
    regime = "neutral"
    if isinstance(trend, (int, float)) and trend <= 35:
        risk_adjustment += 10.0
        risk_level = "elevated"
        regime = "risk_off"
    if isinstance(ret20, (int, float)) and ret20 <= -6:
        risk_adjustment += 8.0
        risk_level = "elevated"
        regime = "drawdown"
    if isinstance(rsi14, (int, float)) and rsi14 >= 75:
        risk_adjustment += 5.0
        risk_level = "watch"
        regime = "overheated"
    result = build_connector_result(
        success=True,
        source="macro-regime-local-proxy",
        as_of=as_of,
        data={
            "regime": regime,
            "risk_level": risk_level,
            "risk_adjustment": risk_adjustment,
            "evidence_refs": ["market_overview.trend_score", "market_overview.ret_20d", "market_overview.rsi14"],
        },
        freshness="current" if market_overview else "unavailable",
        license_hint="supplementary",
    )
    return normalize_macro_overlay(result)
