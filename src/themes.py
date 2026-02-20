from __future__ import annotations

from typing import Any


THEME_RULES: dict[str, dict[str, Any]] = {
    "半導體": {
        "industry_keywords": ["半導體"],
        "name_keywords": ["半導體", "晶圓", "矽", "IC"],
        "seed_symbols": ["2330", "2303", "2454", "3711", "3034", "4966", "3443", "6415", "2344", "2408"],
    },
    "記憶體": {
        "industry_keywords": [],
        "name_keywords": ["記憶體", "DRAM", "NAND", "快閃", "NOR", "HBM", "華邦", "旺宏", "威剛", "群聯", "南亞科", "宇瞻", "創見"],
        "seed_symbols": ["2344", "2408", "2337", "3260", "8299", "4967", "3006", "8271", "2451"],
    },
    "AI": {
        "industry_keywords": ["半導體", "電腦及週邊設備", "通信網路", "光電"],
        "name_keywords": ["AI", "伺服器", "雲端", "散熱", "網通", "光通訊", "機器人"],
        "seed_symbols": ["2330", "2317", "2382", "3231", "6669", "3017", "2356", "2376", "2454"],
    },
}


def normalize_theme(theme: str) -> str:
    key = theme.strip()
    if key in {"ai", "AI", "人工智慧"}:
        return "AI"
    if key in {"半導體業"}:
        return "半導體"
    return key


def theme_rule(theme: str) -> dict[str, Any]:
    normalized = normalize_theme(theme)
    return THEME_RULES.get(normalized, {"industry_keywords": [], "name_keywords": [normalized], "seed_symbols": []})
