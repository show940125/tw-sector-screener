from __future__ import annotations

"""Read-only integrity and coverage verification for canonical market_data.sqlite."""

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.themes import theme_rule


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _read_only_connect(path: Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    uri = f"file:{quote(str(resolved), safe='/:')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _candidate_symbols(theme: str, universe_mode: str) -> list[str]:
    rule = theme_rule(theme, universe_mode=universe_mode)
    values = rule.get("coverage_symbols") or rule.get("symbols") or []
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _market_for_symbol(conn: sqlite3.Connection, symbol: str, as_of: date) -> str:
    rows = conn.execute(
        """
        SELECT market, COUNT(*) AS row_count
        FROM daily_bars
        WHERE symbol = ? AND trade_date <= ? AND data_status = 'verified'
        GROUP BY market
        ORDER BY row_count DESC, market
        """,
        (symbol, as_of.isoformat()),
    ).fetchall()
    if rows:
        return str(rows[0]["market"])
    return "TPEx" if symbol.startswith(("6", "8")) else "TWSE"


def _bar_check(
    conn: sqlite3.Connection,
    *,
    market: str,
    symbol: str,
    as_of: date,
    lookback: int,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS row_count, COUNT(DISTINCT trade_date) AS distinct_dates,
               MIN(trade_date) AS first_trade_date, MAX(trade_date) AS latest_trade_date
        FROM daily_bars
        WHERE market = ? AND symbol = ? AND trade_date <= ? AND data_status = 'verified'
        """,
        (market, symbol, as_of.isoformat()),
    ).fetchone()
    count = int(row["row_count"] or 0)
    distinct_dates = int(row["distinct_dates"] or 0)
    latest = str(row["latest_trade_date"]) if row["latest_trade_date"] else None
    errors: list[str] = []
    if count < lookback:
        errors.append(f"history_short:{count}/{lookback}")
    if distinct_dates != count:
        errors.append("duplicate_trade_dates")
    if as_of.weekday() < 5 and latest != as_of.isoformat():
        errors.append(f"current_day_missing:expected={as_of.isoformat()} actual={latest}")
    return {
        "market": market,
        "symbol": symbol,
        "verified_bar_count": count,
        "distinct_trade_date_count": distinct_dates,
        "first_trade_date": row["first_trade_date"],
        "latest_trade_date": latest,
        "status": "verified" if not errors else "failed",
        "errors": errors,
    }


def verify_database(
    database_path: Path,
    *,
    themes: list[str],
    as_of: date,
    universe_mode: str = "coverage",
    lookback: int = 253,
    benchmark: str = "TAIEX",
) -> dict[str, Any]:
    """Verify the canonical DB without initialising, migrating, or mutating it."""

    required = max(int(lookback), 1)
    payload: dict[str, Any] = {
        "status": "failed",
        "database_path": str(Path(database_path).resolve()),
        "as_of": as_of.isoformat(),
        "expected_trade_date": as_of.isoformat() if as_of.weekday() < 5 else None,
        "universe_mode": universe_mode,
        "lookback": required,
        "themes": themes,
        "benchmark": benchmark,
        "read_only": True,
        "themes_result": {},
        "errors": [],
        "warnings": [],
    }
    try:
        conn = _read_only_connect(database_path)
    except Exception as exc:
        payload["errors"].append(f"open_failed:{exc}")
        return payload

    try:
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        foreign_keys = [dict(row) for row in conn.execute("PRAGMA foreign_key_check").fetchall()]
        payload["sqlite_integrity"] = integrity
        payload["foreign_key_violations"] = foreign_keys
        meta = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key, value FROM schema_meta").fetchall()
        }
        payload["schema_meta"] = meta
        try:
            schema_version = int(meta.get("market_data_schema_version", "0"))
        except ValueError:
            schema_version = 0
        payload["schema_version"] = schema_version
        if schema_version < 2:
            payload["errors"].append(f"schema_version_below_v2:{schema_version}")
        if integrity != "ok":
            payload["errors"].append(f"sqlite_integrity:{integrity}")
        if foreign_keys:
            payload["errors"].append(f"foreign_key_violations:{len(foreign_keys)}")

        for theme in themes:
            candidates = _candidate_symbols(theme, universe_mode)
            rows = [_bar_check(
                conn,
                market=_market_for_symbol(conn, symbol, as_of),
                symbol=symbol,
                as_of=as_of,
                lookback=required,
            ) for symbol in candidates]
            failed = [row for row in rows if row["status"] != "verified"]
            payload["themes_result"][theme] = {
                "coverage_count": len(rows),
                "verified_count": len(rows) - len(failed),
                "failed_count": len(failed),
                "status": "complete" if not failed else "failed",
                "candidates": rows,
            }

        benchmark_row = conn.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(trade_date) AS latest_trade_date
            FROM index_bars
            WHERE index_code = ? AND trade_date <= ? AND data_status = 'verified'
            """,
            (benchmark, as_of.isoformat()),
        ).fetchone()
        benchmark_count = int(benchmark_row["row_count"] or 0)
        benchmark_latest = benchmark_row["latest_trade_date"]
        benchmark_errors: list[str] = []
        if benchmark_count < required:
            benchmark_errors.append(f"history_short:{benchmark_count}/{required}")
        if as_of.weekday() < 5 and benchmark_latest != as_of.isoformat():
            benchmark_errors.append(
                f"current_day_missing:expected={as_of.isoformat()} actual={benchmark_latest}"
            )
        payload["benchmark_result"] = {
            "index_code": benchmark,
            "verified_bar_count": benchmark_count,
            "latest_trade_date": benchmark_latest,
            "status": "verified" if not benchmark_errors else "failed",
            "errors": benchmark_errors,
        }
        if benchmark_errors:
            payload["errors"].extend([f"benchmark:{item}" for item in benchmark_errors])
        for theme, result in payload["themes_result"].items():
            if result["status"] != "complete":
                payload["errors"].append(f"theme:{theme}:coverage_failed")
        payload["status"] = "complete" if not payload["errors"] else "failed"
        return payload
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="只讀驗證 canonical market_data.sqlite")
    parser.add_argument("--database", required=True)
    parser.add_argument("--themes", default="AI,半導體")
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    parser.add_argument("--universe-mode", choices=["coverage", "core", "broad"], default="coverage")
    parser.add_argument("--lookback", type=int, default=253)
    parser.add_argument("--benchmark", default="TAIEX")
    parser.add_argument("--output", default=None, help="optional JSON manifest path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = verify_database(
        Path(args.database),
        themes=[item.strip() for item in str(args.themes).split(",") if item.strip()],
        as_of=_parse_date(args.as_of),
        universe_mode=args.universe_mode,
        lookback=args.lookback,
        benchmark=args.benchmark,
    )
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0 if payload["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
