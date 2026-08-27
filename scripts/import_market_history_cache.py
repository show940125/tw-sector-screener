from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.providers.daily_bar_cache_importer import import_market_cache


DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"


def _parse_date(value: str) -> date:
    return date.fromisoformat(str(value).strip())


def _parse_month(value: str) -> date:
    raw = str(value).strip()
    if len(raw) == 7 and raw[4] == "-":
        return date.fromisoformat(f"{raw}-01")
    if len(raw) == 6 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:]), 1)
    raise argparse.ArgumentTypeError(f"月份格式必須為 YYYY-MM 或 YYYYMM：{value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="將已驗證的市場日線 JSON cache 遷移到 daily_bars.sqlite")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--themes", default="AI,半導體", help="逗號分隔題材；只匯入這些題材的 coverage symbols")
    parser.add_argument("--start-month", type=_parse_month, default=date(2024, 1, 1))
    parser.add_argument("--end-month", type=_parse_month, default=None)
    parser.add_argument("--max-trade-date", type=_parse_date, default=None)
    parser.add_argument("--required-lookback", type=int, default=253)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    cache_dir = Path(args.cache_dir) if args.cache_dir else output_root / "cache" / "market"
    database = Path(args.database) if args.database else cache_dir / "daily_bars.sqlite"
    end_month = args.end_month
    if end_month is None:
        anchor = args.max_trade_date or date.today()
        end_month = date(anchor.year, anchor.month, 1)
    themes = [item.strip() for item in str(args.themes).split(",") if item.strip()]
    if not themes:
        raise SystemExit("--themes 不可為空")
    if args.start_month > end_month:
        raise SystemExit("--start-month 不可晚於 --end-month")
    if args.required_lookback <= 0:
        raise SystemExit("--required-lookback 必須為正數")
    summary = import_market_cache(
        cache_dir=cache_dir,
        database_path=database,
        themes=themes,
        start_month=args.start_month,
        end_month=end_month,
        max_trade_date=args.max_trade_date,
        required_lookback=args.required_lookback,
        manifest_path=Path(args.manifest) if args.manifest else None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("status") == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
