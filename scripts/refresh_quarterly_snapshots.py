from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.providers.tw_market_provider import TwMarketProvider
from src.report.export_structured import write_json_report
from src.themes import core_themes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新核心題材季度快照並輸出覆蓋率摘要")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="分析截止日 (YYYY-MM-DD)")
    parser.add_argument("--theme-mode", choices=["strict", "broad"], default="strict", help="題材池模式")
    parser.add_argument("--themes", default=",".join(core_themes()), help="要刷新的主題，逗號分隔")
    parser.add_argument("--min-monthly-revenue", type=float, default=0.0, help="最低月營收門檻（元）")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 逾時秒數")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="官方輸出根目錄")
    return parser.parse_args()


def _render_markdown(payload: dict[str, object]) -> str:
    summary = payload.get("quality_coverage_summary") or {}
    themes = payload.get("themes") or []
    warnings = payload.get("warnings") or []
    theme_lines = "\n".join(
        f"- `{item['theme']}`：`{item['symbol_count']}` 檔"
        for item in themes
        if isinstance(item, dict)
    ) or "- N/A"
    warning_lines = "\n".join(f"- {item}" for item in warnings) or "- N/A"
    gap_lines = "\n".join(
        f"- rank `{item.get('rank')}` `{item.get('symbol')}`：`{item.get('quality_fetch_status')}` / `{item.get('quality_missing_reason')}`"
        for item in (summary.get("top_candidate_gaps") or [])
    ) or "- N/A"
    return f"""# Quarterly Snapshot Refresh

- as_of：`{payload.get('as_of')}`
- theme mode：`{payload.get('theme_mode')}`
- symbols：`{payload.get('symbol_count')}`

## Theme Universe
{theme_lines}

## Coverage Summary
- current complete：`{summary.get('current_complete_count', 0)}` / `{summary.get('universe_count', 0)}` (`{summary.get('current_complete_pct', 0.0)}%`)
- previous complete：`{summary.get('previous_complete_count', 0)}` / `{summary.get('universe_count', 0)}` (`{summary.get('previous_complete_pct', 0.0)}%`)
- status：ok `{summary.get('ok_count', 0)}` / unavailable `{summary.get('unavailable_count', 0)}` / partial `{summary.get('partial_count', 0)}` / fetch_failed `{summary.get('fetch_failed_count', 0)}`

## Top Candidate Gaps
{gap_lines}

## Warnings
{warning_lines}
"""


def main() -> int:
    args = parse_args()
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    output_root = Path(args.output_root)
    provider = TwMarketProvider(timeout=args.timeout, cache_dir=output_root / "cache" / "market")
    themes = [item.strip() for item in str(args.themes).split(",") if item.strip()]
    payload = provider.refresh_quarterly_snapshots(
        as_of=as_of,
        themes=themes,
        theme_mode=args.theme_mode,
        min_monthly_revenue=args.min_monthly_revenue,
    )
    audit_dir = output_root / "audit" / as_of.strftime("%Y%m%d")
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = write_json_report(audit_dir / f"quarterly-snapshot-refresh-{as_of.strftime('%Y%m%d')}.json", payload)
    md_path = audit_dir / f"quarterly-snapshot-refresh-{as_of.strftime('%Y%m%d')}.md"
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    print(f"[quarterly-refresh] json: {json_path}")
    print(f"[quarterly-refresh] md: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
