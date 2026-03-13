from __future__ import annotations

import argparse
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回補核心題材近 8 季季度品質資料到 SQLite")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="分析截止日 (YYYY-MM-DD)")
    parser.add_argument("--themes", default=",".join(core_themes()), help="要回補的主題，逗號分隔")
    parser.add_argument("--theme-mode", choices=["strict", "broad"], default="strict", help="題材池模式")
    parser.add_argument("--periods", type=int, default=8, help="回補最近幾季")
    parser.add_argument("--only-missing", default="true", choices=["true", "false"], help="是否只回補缺資料的期數")
    parser.add_argument("--limit-symbols", type=int, default=None, help="限制每個主題最多幾檔")
    parser.add_argument("--batch-size", type=int, default=20, help="每批最多處理幾筆 queue")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="官方輸出根目錄")
    parser.add_argument("--force-retry-days", type=int, default=30, help="failed/unavailable 幾天後才強制重試")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 逾時秒數")
    return parser.parse_args(argv)


def _render_markdown(payload: dict[str, object]) -> str:
    summary = payload.get("quality_coverage_summary") or {}
    theme_lines = "\n".join(
        f"- `{item['theme']}`：`{item['symbol_count']}` 檔"
        for item in (payload.get("themes") or [])
        if isinstance(item, dict)
    ) or "- N/A"
    unresolved = "\n".join(f"- `{symbol}`" for symbol in (payload.get("unresolved_symbols") or [])) or "- N/A"
    warnings = "\n".join(f"- {item}" for item in (payload.get("warnings") or [])) or "- N/A"
    return f"""# Quarterly Backfill

- as_of：`{payload.get('as_of')}`
- theme mode：`{payload.get('theme_mode')}`
- target periods：`{len(payload.get('target_periods') or [])}`
- target symbols：`{payload.get('target_symbol_count')}`
- quarterly store：`{payload.get('quarterly_store_path')}`
- backfill run：`{payload.get('backfill_run_id')}`

## Themes
{theme_lines}

## Result Summary
- queued：`{payload.get('queued_count', 0)}`
- completed：`{payload.get('completed_count', 0)}`
- unavailable：`{payload.get('unavailable_count', 0)}`
- failed：`{payload.get('failed_count', 0)}`
- current complete：`{summary.get('current_complete_pct', 0.0)}%`
- previous complete：`{summary.get('previous_complete_pct', 0.0)}%`
- history complete：`{summary.get('history_complete_pct', 0.0)}%`

## Unresolved Symbols
{unresolved}

## Warnings
{warnings}
"""


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    output_root = Path(args.output_root)
    provider = TwMarketProvider(timeout=args.timeout, cache_dir=output_root / "cache" / "market")
    payload = provider.backfill_quarterly_history(
        as_of=as_of,
        themes=[item.strip() for item in str(args.themes).split(",") if item.strip()],
        theme_mode=args.theme_mode,
        periods=args.periods,
        only_missing=str(args.only_missing).lower() == "true",
        limit_symbols=args.limit_symbols,
        batch_size=args.batch_size,
        force_retry_days=args.force_retry_days,
        trigger_type="manual",
    )
    audit_dir = output_root / "audit" / as_of.strftime("%Y%m%d")
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = write_json_report(audit_dir / f"quarterly-backfill-{as_of.strftime('%Y%m%d')}.json", payload)
    md_path = audit_dir / f"quarterly-backfill-{as_of.strftime('%Y%m%d')}.md"
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    print(f"[quarterly-backfill] json: {json_path}")
    print(f"[quarterly-backfill] md: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
