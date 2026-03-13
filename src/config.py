from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.analysis.scoring import WEIGHTS


DEFAULT_CONFIG: dict[str, Any] = {
    "weights": dict(WEIGHTS),
    "benchmark": {"type": "TAIEX", "symbols": []},
    "portfolio": {
        "overweight_min_idea": 72.0,
        "overweight_min_confidence": 72.0,
        "neutral_min_idea": 55.0,
    },
    "filters": {"min_monthly_revenue": 0.0},
    "theme_overrides": {},
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: str | Path | None) -> dict[str, Any]:
    if not config_path:
        return dict(DEFAULT_CONFIG)
    path = Path(config_path)
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise RuntimeError("YAML config 需要安裝 PyYAML，或改用 JSON config。") from exc
        payload = yaml.safe_load(raw) or {}
    else:
        payload = json.loads(raw or "{}")
    if not isinstance(payload, dict):
        raise RuntimeError("config 檔內容必須是 object。")
    return _deep_merge(DEFAULT_CONFIG, payload)
