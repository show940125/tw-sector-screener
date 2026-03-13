from __future__ import annotations

from typing import Any


def build_action_view(
    idea_score: float,
    confidence_score: float,
    close: float,
    atr14: float | None,
    volatility20: float | None,
    rel_to_taiex_20d: float | None,
    rel_to_sector_20d: float | None,
) -> dict[str, Any]:
    if confidence_score < 55 or idea_score < 55:
        action = "Underweight"
    elif confidence_score >= 72 and idea_score >= 72 and (rel_to_sector_20d or 0.0) >= 0:
        action = "Overweight"
    else:
        action = "Neutral"

    atr = atr14 or max(close * 0.03, 1.0)
    entry_low = max(0.0, close - (0.5 * atr))
    entry_high = close + (0.25 * atr)
    why_now: list[str] = []
    why_not: list[str] = []

    if (rel_to_taiex_20d or 0.0) > 0:
        why_now.append("20 日相對大盤仍有超額動能")
    if (rel_to_sector_20d or 0.0) > 0:
        why_now.append("同題材內相對強勢")
    if confidence_score >= 75:
        why_now.append("資料覆蓋度足夠，結論可用度較高")
    if not why_now:
        why_now.append("訊號不差，但優勢沒有強到能重倉")

    if confidence_score < 70:
        why_not.append("資料缺口偏多，判讀需要保守")
    if volatility20 is not None and volatility20 >= 45:
        why_not.append("波動偏高，容易把正確方向洗掉")
    if (rel_to_sector_20d or 0.0) < 0:
        why_not.append("對同題材沒有明顯領先")
    if not why_not:
        why_not.append("主要風險來自題材回檔與事件前後估值收縮")

    add_trigger = "站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。"
    trim_trigger = "跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。"
    if action == "Underweight":
        add_trigger = "先等資料或趨勢修復，不急著撿便宜。"
        trim_trigger = "若已有部位，事件前先把風險降到你睡得著的水位。"

    return {
        "action": action,
        "entry_range": [round(entry_low, 2), round(entry_high, 2)],
        "why_now": why_now,
        "why_not": why_not,
        "add_trigger": add_trigger,
        "trim_trigger": trim_trigger,
        "event_handling": "法說、月營收、AI 伺服器出貨與產業報價公布前，預設降一級槓桿處理。",
    }
