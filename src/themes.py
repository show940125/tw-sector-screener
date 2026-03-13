from __future__ import annotations

from typing import Any


THEME_LIBRARY: dict[str, dict[str, Any]] = {
    "半導體": {
        "aliases": ["semiconductor"],
        "strict_symbols": [
            "2330",
            "2303",
            "2454",
            "3711",
            "3034",
            "3443",
            "6415",
            "8299",
            "2344",
            "2408",
            "6770",
            "3189",
            "2379",
            "6239",
            "6488",
        ],
        "broad_symbols": ["2449", "6147", "6257", "8150", "2451", "3006", "8271", "2337"],
        "name_keywords": ["半導體", "晶圓", "IC", "矽", "封測"],
        "industry_keywords": ["半導體"],
    },
    "AI": {
        "aliases": ["ai", "人工智慧"],
        "strict_symbols": ["2382", "3231", "6669", "3017", "2356", "2376", "2345", "2454", "2330"],
        "broad_symbols": ["2317", "2357", "2324", "2353", "2409", "3481", "2412", "3045", "4904"],
        "name_keywords": ["AI", "伺服器", "GPU", "ASIC", "散熱", "光通訊", "雲端"],
        "industry_keywords": ["電腦及週邊設備", "半導體", "通信網路", "光電"],
    },
    "AI infra": {
        "aliases": ["ai-infra", "ai infra", "ai infrastructure"],
        "strict_symbols": ["2330", "2454", "2345", "3017", "6669"],
        "broad_symbols": ["3711", "2379"],
        "name_keywords": ["AI", "ASIC", "網通", "交換器", "散熱", "光通訊"],
        "industry_keywords": ["半導體", "通信網路", "電腦及週邊設備"],
    },
    "AI server/ODM": {
        "aliases": ["ai server", "ai odm", "ai-server-odm", "server/odm"],
        "strict_symbols": ["2382", "3231", "6669", "2356", "2376", "2317"],
        "broad_symbols": ["2357", "2324", "2353"],
        "name_keywords": ["伺服器", "雲端", "機櫃", "ODM", "AI"],
        "industry_keywords": ["電腦及週邊設備"],
    },
    "記憶體": {
        "aliases": ["memory"],
        "strict_symbols": ["2344", "2408", "2337", "3260", "8299", "3006", "8271", "2451", "4967"],
        "broad_symbols": [],
        "name_keywords": ["記憶體", "DRAM", "NAND", "HBM", "快閃", "NOR", "SSD"],
        "industry_keywords": ["半導體"],
    },
    "memory": {
        "aliases": ["記憶體"],
        "strict_symbols": ["2344", "2408", "2337", "3260", "8299", "3006", "8271", "2451", "4967"],
        "broad_symbols": [],
        "name_keywords": ["記憶體", "DRAM", "NAND", "HBM", "快閃", "NOR", "SSD"],
        "industry_keywords": ["半導體"],
    },
    "foundry": {
        "aliases": ["晶圓代工"],
        "strict_symbols": ["2330", "2303", "6770", "5347"],
        "broad_symbols": ["3711", "2449"],
        "name_keywords": ["晶圓", "foundry"],
        "industry_keywords": ["半導體"],
    },
    "IC design": {
        "aliases": ["IC設計", "ic design"],
        "strict_symbols": ["2454", "3034", "2379", "3443", "6415", "6526", "4919", "3592"],
        "broad_symbols": ["8299", "2344"],
        "name_keywords": ["IC", "設計", "ASIC"],
        "industry_keywords": ["半導體"],
    },
}


_ALIAS_INDEX: dict[str, str] = {}
for theme_name, payload in THEME_LIBRARY.items():
    _ALIAS_INDEX[theme_name.lower()] = theme_name
    for alias in payload.get("aliases", []):
        _ALIAS_INDEX[str(alias).strip().lower()] = theme_name


CORE_THEME_NAMES = [
    "AI",
    "AI infra",
    "AI server/ODM",
    "半導體",
    "foundry",
    "IC design",
    "memory",
]


def available_themes() -> list[str]:
    return list(THEME_LIBRARY.keys())


def core_themes() -> list[str]:
    return [theme for theme in CORE_THEME_NAMES if theme in THEME_LIBRARY]


def normalize_theme(theme: str) -> str:
    key = theme.strip().lower()
    return _ALIAS_INDEX.get(key, theme.strip())


def theme_rule(theme: str, theme_mode: str = "strict") -> dict[str, Any]:
    normalized = normalize_theme(theme)
    payload = THEME_LIBRARY.get(
        normalized,
        {
            "aliases": [],
            "strict_symbols": [],
            "broad_symbols": [],
            "name_keywords": [normalized],
            "industry_keywords": [],
        },
    )
    mode = "broad" if str(theme_mode).strip().lower() == "broad" else "strict"
    strict_symbols = list(payload.get("strict_symbols") or [])
    broad_symbols = list(payload.get("broad_symbols") or [])
    return {
        "name": normalized,
        "theme_mode": mode,
        "strict_symbols": strict_symbols,
        "broad_symbols": broad_symbols,
        "symbols": strict_symbols + broad_symbols if mode == "broad" else strict_symbols,
        "name_keywords": list(payload.get("name_keywords") or []),
        "industry_keywords": list(payload.get("industry_keywords") or []),
    }
