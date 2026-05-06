from __future__ import annotations

from typing import Any


HARD_BLOCKERS = {"quality:fetch_failed", "partial-factor-coverage", "extreme-volatility"}
BUYABLE_TIERS = {"formal_buy", "risk_adjusted_buy", "tactical_buy"}
BUYING_TIER_PRIORITY = {"formal_buy": 3, "risk_adjusted_buy": 2, "tactical_buy": 1, "not_buyable": 0}


def _num(value: Any, default: float = 0.0) -> float:
    return float(value) if isinstance(value, (int, float)) else default


def _flags(row: dict[str, Any]) -> set[str]:
    return {str(flag) for flag in (row.get("data_quality_flags") or []) if str(flag)}


def _buyability_score(row: dict[str, Any]) -> float:
    risk_adjusted = _risk_adjusted_score(row, default=50.0)
    return round(
        (_num(row.get("rank_score")) * 0.35)
        + (_num(row.get("idea_score")) * 0.20)
        + (risk_adjusted * 0.20)
        + ((100.0 - _num(row.get("risk_score"), 100.0)) * 0.15)
        + (_num(row.get("confidence_score")) * 0.10),
        2,
    )


def _risk_adjusted_score(row: dict[str, Any], default: float = 0.0) -> float:
    return _num(row.get("risk_adjusted_score"), _num((row.get("stock_risk_metrics") or {}).get("risk_adjusted_score"), default))


def _hard_blocked(row: dict[str, Any]) -> bool:
    return bool(HARD_BLOCKERS.intersection(_flags(row)))


def _actionability_score(row: dict[str, Any]) -> float:
    return round(
        (_buyability_score(row) * 0.40)
        + (_num(row.get("idea_score")) * 0.25)
        + (_num(row.get("confidence_score")) * 0.20)
        + ((100.0 - _num(row.get("risk_score"), 100.0)) * 0.15),
        2,
    )


def _blocked_by(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    recommendation = str(row.get("recommendation") or "N/A")
    risk = _num(row.get("risk_score"), 100.0)
    confidence = _num(row.get("confidence_score"))
    idea = _num(row.get("idea_score"))
    flags = sorted(HARD_BLOCKERS.intersection(_flags(row)))
    if recommendation == "賣出":
        blockers.append("recommendation=賣出")
    elif recommendation != "買入" and _buying_tier(row) == "not_buyable":
        blockers.append(f"recommendation={recommendation}")
    if risk > 65.0:
        blockers.append(f"risk_score {risk:.1f} > 65")
    if confidence < 65.0:
        blockers.append(f"confidence {confidence:.1f} < 65")
    if idea < 62.0:
        blockers.append(f"idea_score {idea:.1f} < 62")
    blockers.extend(f"hard blocker: {flag}" for flag in flags)
    return blockers


def _decision_tier(row: dict[str, Any]) -> str:
    recommendation = str(row.get("recommendation") or "")
    risk = _num(row.get("risk_score"), 100.0)
    confidence = _num(row.get("confidence_score"))
    idea = _num(row.get("idea_score"))
    hard_blocked = _hard_blocked(row)
    if _is_buyable(row):
        return "buy_now"
    if recommendation == "賣出" or hard_blocked or risk >= 90.0:
        return "avoid"
    if recommendation == "持有" and confidence >= 65.0 and idea >= 62.0 and risk <= 75.0:
        return "near_buy"
    if recommendation == "持有" and confidence >= 65.0 and idea >= 65.0 and risk <= 85.0:
        return "starter_position"
    return "wait_for_trigger"


def _starter_position_pct(row: dict[str, Any], tier: str) -> float:
    if _buying_tier(row) == "tactical_buy":
        risk = _num(row.get("risk_score"), 100.0)
        return 0.5 if risk <= 65.0 else 0.25
    if tier != "starter_position":
        return 0.0
    risk = _num(row.get("risk_score"), 100.0)
    return 0.5 if risk <= 75.0 else 0.25


def _entry_readiness(tier: str) -> str:
    return {
        "buy_now": "ready",
        "near_buy": "near",
        "starter_position": "small-size-only",
        "wait_for_trigger": "waiting",
        "avoid": "blocked",
    }.get(tier, "waiting")


def _trigger_to_upgrade(row: dict[str, Any], tier: str) -> str:
    buying_tier = _buying_tier(row)
    if buying_tier == "risk_adjusted_buy":
        return "若 idea_score 升到 62 以上且 recommendation 升級為買入，可轉 formal buy。"
    if buying_tier == "tactical_buy":
        return "風險降到 65 以下或 RiskAdj 繼續改善後，才提高到正式買進部位。"
    upgrades = row.get("upgrade_conditions") or []
    if tier == "buy_now":
        return "已通過正式買進條件；照 Buying Ranking 風控執行。"
    if tier == "near_buy":
        return "risk_score 降到 65 以下，且 recommendation 升級為買入。"
    if tier == "starter_position":
        return "波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。"
    if upgrades:
        return "；".join(str(item) for item in upgrades)
    if tier == "avoid":
        return "先排除；需移除賣出/硬性阻擋/極端風險後才重新評估。"
    return "等待相對題材動能轉強、資料品質補齊或 risk_score 降到 75 以下。"


def _next_action(row: dict[str, Any], tier: str) -> str:
    symbol = row.get("symbol") or "標的"
    buying_tier = _buying_tier(row)
    if buying_tier == "risk_adjusted_buy":
        return f"{symbol} 屬風險調整買進候選，可用穩健買進邏輯分批，但不把研究建議硬改成買入。"
    if buying_tier == "tactical_buy":
        pct = _starter_position_pct(row, tier)
        return f"{symbol} 屬戰術買進候選，僅適合 {pct:.2f}x 小部位，不用正式買進部位。"
    if tier == "buy_now":
        return f"{symbol} 已在正式買進榜，按 buyability 排序與風控分批。"
    if tier == "near_buy":
        return f"{symbol} 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。"
    if tier == "starter_position":
        pct = _starter_position_pct(row, tier)
        return f"{symbol} 僅適合 {pct:.2f}x 小部位試單，不能當正式買進。"
    if tier == "avoid":
        return f"{symbol} 目前應避開或降風險。"
    return f"{symbol} 先等待觸發條件，不急著進場。"


def _exclusion_reason(row: dict[str, Any]) -> str | None:
    if _buying_tier(row) != "not_buyable":
        return None
    reasons = _blocked_by(row)
    return "；".join(reasons) if reasons else None


def _research_reason(row: dict[str, Any]) -> str:
    return (
        f"research rank {row.get('rank', 'N/A')}，idea {_num(row.get('idea_score')):.1f}，"
        f"confidence {_num(row.get('confidence_score')):.1f}，recommendation {row.get('recommendation') or 'N/A'}。"
    )


def _monitoring_reason(row: dict[str, Any]) -> str:
    recommendation = str(row.get("recommendation") or "N/A")
    if recommendation == "賣出":
        return "已觸發賣出/降風險，保留於追蹤清單以處理既有部位。"
    if recommendation == "持有":
        return "仍屬題材重要標的，但目前未通過買進條件，保留追蹤。"
    reason = _exclusion_reason(row)
    return reason or "研究排名靠前，等待買進條件確認。"


def _with_list_fields(row: dict[str, Any], list_type: str, list_rank: int) -> dict[str, Any]:
    tier = _decision_tier(row)
    buying_tier = _buying_tier(row)
    blocked = _blocked_by(row)
    enriched = dict(row)
    enriched["list_type"] = list_type
    enriched["list_rank"] = list_rank
    enriched["buyability_score"] = _buyability_score(row)
    enriched["risk_adjusted_score"] = _risk_adjusted_score(row)
    enriched["buying_tier"] = buying_tier
    enriched["decision_tier"] = tier
    enriched["actionability_score"] = _actionability_score(row)
    enriched["blocked_by"] = blocked
    enriched["next_action"] = _next_action(row, tier)
    enriched["entry_readiness"] = _entry_readiness(tier)
    enriched["starter_position_pct"] = _starter_position_pct(row, tier)
    enriched["trigger_to_upgrade"] = _trigger_to_upgrade(row, tier)
    if buying_tier == "formal_buy":
        enriched["why_not_buy_now"] = "已通過正式買進條件。"
    elif buying_tier == "risk_adjusted_buy":
        enriched["why_not_buy_now"] = "非 formal buy：研究建議或 idea_score 尚未達正式買入門檻，但風險調整後可買。"
    elif buying_tier == "tactical_buy":
        enriched["why_not_buy_now"] = "非 formal buy：風險略高或題材波動較大，只能小部位。"
    else:
        enriched["why_not_buy_now"] = "；".join(blocked) if blocked else "未通過買進條件。"
    enriched["research_reason"] = _research_reason(row)
    enriched["monitoring_reason"] = _monitoring_reason(row)
    enriched["exclusion_from_buying_reason"] = _exclusion_reason(row)
    return enriched


def _is_buyable(row: dict[str, Any]) -> bool:
    return _buying_tier(row) in BUYABLE_TIERS


def _is_formal_buy(row: dict[str, Any]) -> bool:
    if str(row.get("recommendation") or "") != "買入":
        return False
    if _num(row.get("risk_score"), 100.0) > 65.0:
        return False
    if _num(row.get("confidence_score")) < 65.0:
        return False
    if _hard_blocked(row):
        return False
    return True


def _buying_tier(row: dict[str, Any]) -> str:
    recommendation = str(row.get("recommendation") or "")
    if recommendation == "賣出" or _hard_blocked(row):
        return "not_buyable"
    risk = _num(row.get("risk_score"), 100.0)
    confidence = _num(row.get("confidence_score"))
    idea = _num(row.get("idea_score"))
    risk_adjusted = _risk_adjusted_score(row)
    if _is_formal_buy(row):
        return "formal_buy"
    if (
        recommendation in {"買入", "持有"}
        and risk <= 60.0
        and confidence >= 75.0
        and risk_adjusted >= 70.0
        and idea >= 55.0
    ):
        return "risk_adjusted_buy"
    if (
        recommendation in {"買入", "持有"}
        and idea >= 62.0
        and confidence >= 70.0
        and risk <= 72.0
        and risk_adjusted >= 50.0
    ):
        return "tactical_buy"
    return "not_buyable"


def build_candidate_lists(rows: list[dict[str, Any]], top_n: int) -> dict[str, list[dict[str, Any]]]:
    display_limit = max(int(top_n), 0)
    ranked_base = sorted(
        rows,
        key=lambda row: (_num(row.get("rank_score")), _num(row.get("idea_score"))),
        reverse=True,
    )
    research_base = ranked_base[:display_limit]
    buying_base = sorted(
        [row for row in ranked_base if _is_buyable(row)],
        key=lambda row: (
            BUYING_TIER_PRIORITY.get(_buying_tier(row), 0),
            _buyability_score(row),
            _num(row.get("rank_score")),
            _num(row.get("idea_score")),
        ),
        reverse=True,
    )[:display_limit]
    buying_symbols = {str(row.get("symbol")) for row in buying_base}
    actionable_base = sorted(
        [
            row
            for row in ranked_base
            if str(row.get("symbol")) not in buying_symbols
            and _decision_tier(row) in {"near_buy", "starter_position", "wait_for_trigger"}
            and str(row.get("recommendation") or "") != "賣出"
            and not HARD_BLOCKERS.intersection(_flags(row))
        ],
        key=lambda row: (_actionability_score(row), _buyability_score(row), _num(row.get("idea_score"))),
        reverse=True,
    )[:display_limit]
    watchlist_base = sorted(
        [
            row
            for row in ranked_base
            if str(row.get("symbol")) not in buying_symbols
            and (
                str(row.get("recommendation") or "") in {"持有", "賣出"}
                or _num(row.get("risk_score")) >= 65.0
                or _exclusion_reason(row)
            )
        ],
        key=lambda row: (_num(row.get("rank_score")), _num(row.get("idea_score"))),
        reverse=True,
    )[:display_limit]
    return {
        "buying_ranking": [_with_list_fields(row, "buying_ranking", idx) for idx, row in enumerate(buying_base, start=1)],
        "actionable_queue": [_with_list_fields(row, "actionable_queue", idx) for idx, row in enumerate(actionable_base, start=1)],
        "watchlist_candidates": [_with_list_fields(row, "watchlist", idx) for idx, row in enumerate(watchlist_base, start=1)],
        "research_list": [_with_list_fields(row, "research", idx) for idx, row in enumerate(research_base, start=1)],
        "picks": [_with_list_fields(row, "research", idx) for idx, row in enumerate(research_base, start=1)],
    }
