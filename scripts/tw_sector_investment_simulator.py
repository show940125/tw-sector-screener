from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.simulator.engine import SimulatorConfig, run_simulation


DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TW Sector Screener 投資模擬器")
    parser.add_argument("--mode", choices=["historical", "daily", "historical-plus-daily"], default="historical-plus-daily")
    parser.add_argument("--themes", default="AI,半導體", help="逗號分隔題材，例如 AI,半導體")
    parser.add_argument("--theme-mode", choices=["strict", "broad"], default=None)
    parser.add_argument("--universe-mode", choices=["core", "coverage", "broad"], default="coverage")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD 或 today；historical 模式未填時用 as-of")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD 或 today；historical 模式未填時用 as-of 或今日")
    parser.add_argument("--as-of", default=None, help="daily 模式執行日 YYYY-MM-DD 或 today；未填時用今日")
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--recommendation-mode", choices=["deterministic", "llm-review", "off"], default="deterministic")
    parser.add_argument("--analysis-cache", choices=["reuse", "refresh"], default="reuse")
    parser.add_argument(
        "--daily-analysis-mode",
        choices=["prior-close", "same-day"],
        default="prior-close",
        help="daily 模式分析日。prior-close 用前一交易日分析執行今日；same-day 用當日盤後資料產生當日報告與委託。",
    )
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--config", default=None, help="simulator JSON config，可覆蓋成本與 lot_size")
    parser.add_argument("--run-id", default=None, help="輸出 run id；未指定時自動產生")
    parser.add_argument("--universe-limit", type=int, default=80)
    parser.add_argument("--lookback", type=int, default=252)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    extra = _load_config(Path(args.config)) if args.config else {}
    as_of = _parse_date(args.as_of) if args.as_of else date.today()
    if args.mode == "daily":
        start = _parse_date(args.start_date) if args.start_date else as_of
        end = as_of
    else:
        start = _parse_date(args.start_date) if args.start_date else as_of
        end = _parse_date(args.end_date) if args.end_date else as_of
    if start > end:
        raise SystemExit("--start-date 不可晚於 --end-date")
    themes = [item.strip() for item in str(args.themes).split(",") if item.strip()]
    if not themes:
        raise SystemExit("--themes 不可為空")
    run_id = args.run_id or _default_run_id(args.mode, themes, start, end)
    config = SimulatorConfig(
        themes=themes,
        theme_mode=args.theme_mode,
        universe_mode=args.universe_mode,
        start_date=start,
        end_date=end,
        initial_cash=float(args.initial_cash),
        top_n=int(args.top_n),
        recommendation_mode=args.recommendation_mode,
        analysis_cache=args.analysis_cache,
        output_root=Path(args.output_root),
        run_id=run_id,
        mode=args.mode,
        universe_limit=int(args.universe_limit),
        lookback=int(args.lookback),
        timeout=float(args.timeout),
        commission_bps=float(extra.get("commission_bps", 14.25)),
        sell_tax_bps=float(extra.get("sell_tax_bps", 30.0)),
        min_commission=float(extra.get("min_commission", 20.0)),
        lot_size=int(extra.get("lot_size", 1)),
        daily_analysis_mode=args.daily_analysis_mode,
    )
    outputs = run_simulation(config)
    for key, value in outputs.items():
        if isinstance(value, list):
            for idx, path in enumerate(value, start=1):
                print(f"[tw-sector-simulator] {key}[{idx}]: {path}")
        else:
            print(f"[tw-sector-simulator] {key}: {value}")
    return 0


def _load_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _parse_date(value: str) -> date:
    if value.strip().lower() == "today":
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _default_run_id(mode: str, themes: list[str], start: date, end: date) -> str:
    theme_tag = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "-", "-".join(themes)).strip("-")
    if mode == "daily":
        return f"daily-{theme_tag}"
    return f"{mode}-{theme_tag}-{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"


if __name__ == "__main__":
    raise SystemExit(main())
