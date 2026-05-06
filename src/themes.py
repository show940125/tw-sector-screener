from __future__ import annotations

from typing import Any


AI_CORE = ["2382", "3231", "6669", "3017", "2356", "2376", "2345", "2454", "2330"]
AI_BUCKET_MAP = {
    "2382": ["ai_server_odm"],
    "3231": ["ai_server_odm"],
    "6669": ["ai_server_odm"],
    "3017": ["cooling_thermal"],
    "2356": ["ai_server_odm"],
    "2376": ["ai_server_odm"],
    "2345": ["networking_optical"],
    "2454": ["semiconductor", "ic_design"],
    "2330": ["semiconductor", "foundry"],
    "2317": ["ai_server_odm"],
    "2357": ["ai_server_odm"],
    "2324": ["ai_server_odm"],
    "2353": ["ai_server_odm"],
    "2409": ["display_proxy"],
    "3481": ["display_proxy"],
    "2412": ["telecom_proxy"],
    "3045": ["telecom_proxy"],
    "4904": ["telecom_proxy"],
    "2303": ["semiconductor", "foundry"],
    "3711": ["semiconductor", "foundry"],
    "3034": ["semiconductor", "ic_design"],
    "3443": ["semiconductor", "ic_design"],
    "6415": ["semiconductor", "ic_design"],
    "8299": ["semiconductor", "memory_hbm"],
    "2344": ["semiconductor", "memory_hbm"],
    "2408": ["semiconductor", "memory_hbm"],
    "6770": ["semiconductor", "foundry"],
    "3189": ["advanced_packaging_substrate"],
    "2379": ["semiconductor", "ic_design"],
    "6239": ["semiconductor", "testing_equipment"],
    "6488": ["semiconductor", "testing_equipment"],
    "2449": ["semiconductor", "testing_equipment"],
    "6147": ["semiconductor", "materials"],
    "6257": ["semiconductor", "materials"],
    "8150": ["semiconductor", "materials"],
    "2451": ["semiconductor", "memory_hbm"],
    "3006": ["semiconductor", "memory_hbm"],
    "8271": ["semiconductor", "memory_hbm"],
    "2337": ["semiconductor", "memory_hbm"],
    "3260": ["semiconductor", "memory_hbm"],
    "4967": ["semiconductor", "memory_hbm"],
    "5347": ["semiconductor", "foundry"],
    "6526": ["semiconductor", "ic_design"],
    "4919": ["semiconductor", "ic_design"],
    "3592": ["semiconductor", "ic_design"],
    "3037": ["advanced_packaging_substrate", "pcb_ccl"],
    "8046": ["advanced_packaging_substrate", "pcb_ccl"],
    "6274": ["pcb_ccl"],
    "3653": ["cooling_thermal"],
    "3324": ["cooling_thermal"],
    "8996": ["cooling_thermal"],
    "2308": ["power_connector_chassis"],
    "6282": ["power_connector_chassis"],
    "3526": ["power_connector_chassis"],
}
AI_COVERAGE = list(dict.fromkeys([*AI_CORE, *AI_BUCKET_MAP.keys()]))


THEME_LIBRARY: dict[str, dict[str, Any]] = {
    "半導體": {
        "aliases": ["semiconductor"],
        "core_symbols": [
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
        "coverage_symbols": [
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
            "2449",
            "6147",
            "6257",
            "8150",
            "2451",
            "3006",
            "8271",
            "2337",
            "3260",
            "4967",
            "5347",
            "6526",
            "4919",
            "3592",
            "3037",
            "8046",
        ],
        "bucket_map": {symbol: AI_BUCKET_MAP.get(symbol, ["semiconductor"]) for symbol in AI_BUCKET_MAP},
        "broad_symbols": ["2449", "6147", "6257", "8150", "2451", "3006", "8271", "2337"],
        "name_keywords": ["半導體", "晶圓", "IC", "矽", "封測"],
        "industry_keywords": ["半導體"],
    },
    "AI": {
        "aliases": ["ai", "人工智慧"],
        "core_symbols": AI_CORE,
        "strict_symbols": AI_CORE,
        "coverage_symbols": AI_COVERAGE,
        "bucket_map": AI_BUCKET_MAP,
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


def _resolve_universe_mode(theme_mode: str | None = None, universe_mode: str | None = None) -> str:
    if universe_mode:
        value = str(universe_mode).strip().lower()
        if value in {"core", "coverage", "broad"}:
            return value
    legacy = str(theme_mode or "").strip().lower()
    if legacy == "broad":
        return "broad"
    if legacy == "strict":
        return "core"
    return "coverage"


def theme_rule(theme: str, theme_mode: str | None = None, universe_mode: str | None = None) -> dict[str, Any]:
    normalized = normalize_theme(theme)
    payload = THEME_LIBRARY.get(
        normalized,
        {
            "aliases": [],
            "core_symbols": [],
            "strict_symbols": [],
            "coverage_symbols": [],
            "broad_symbols": [],
            "bucket_map": {},
            "name_keywords": [normalized],
            "industry_keywords": [],
        },
    )
    mode = _resolve_universe_mode(theme_mode=theme_mode, universe_mode=universe_mode)
    core_symbols = list(payload.get("core_symbols") or payload.get("strict_symbols") or [])
    strict_symbols = list(payload.get("strict_symbols") or core_symbols)
    coverage_symbols = list(payload.get("coverage_symbols") or core_symbols)
    broad_symbols = list(payload.get("broad_symbols") or [])
    symbols = core_symbols
    if mode == "coverage":
        symbols = coverage_symbols
    elif mode == "broad":
        symbols = list(dict.fromkeys([*coverage_symbols, *broad_symbols]))
    bucket_map = {str(k): list(v) for k, v in (payload.get("bucket_map") or {}).items()}
    return {
        "name": normalized,
        "theme_mode": "broad" if mode == "broad" else "strict",
        "universe_mode": mode,
        "core_symbols": core_symbols,
        "strict_symbols": strict_symbols,
        "coverage_symbols": coverage_symbols,
        "broad_symbols": broad_symbols,
        "symbols": symbols,
        "bucket_map": bucket_map,
        "name_keywords": list(payload.get("name_keywords") or []),
        "industry_keywords": list(payload.get("industry_keywords") or []),
    }
