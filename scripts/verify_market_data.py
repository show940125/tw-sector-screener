from __future__ import annotations

"""Read-only integrity and coverage verification for canonical market_data.sqlite."""

import argparse
import hashlib
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
    sync_state = conn.execute(
        """
        SELECT last_current_day_verified_date
        FROM daily_bar_sync_state
        WHERE market = ? AND symbol = ?
        """,
        (market, symbol),
    ).fetchone()
    current_day_verified = bool(
        sync_state is not None
        and str(sync_state["last_current_day_verified_date"] or "") == as_of.isoformat()
    )
    errors: list[str] = []
    if count < lookback:
        errors.append(f"history_short:{count}/{lookback}")
    if distinct_dates != count:
        errors.append("duplicate_trade_dates")
    if as_of.weekday() < 5 and latest != as_of.isoformat():
        errors.append(f"current_day_missing:expected={as_of.isoformat()} actual={latest}")
    if as_of.weekday() < 5 and not current_day_verified:
        errors.append(f"current_day_unverified:expected={as_of.isoformat()}")
    return {
        "market": market,
        "symbol": symbol,
        "verified_bar_count": count,
        "distinct_trade_date_count": distinct_dates,
        "first_trade_date": row["first_trade_date"],
        "latest_trade_date": latest,
        "current_day_verified": current_day_verified,
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
        "research_dataset_counts": {},
        "pit_query_contract": {
            "available": False,
            "rule": "effective_date <= observation_date and available/published date <= information_cutoff",
        },
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
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        external_payload_missing: list[str] = []
        payload_hash_mismatches: list[str] = []
        if "source_payloads" in existing_tables:
            for row in conn.execute(
                "SELECT payload_id, storage_mode, storage_uri, payload_sha256, raw_payload_json "
                "FROM source_payloads"
            ).fetchall():
                payload_id = str(row["payload_id"])
                storage_mode = str(row["storage_mode"] or "inline")
                if storage_mode == "external":
                    storage_uri = str(row["storage_uri"] or "")
                    if not storage_uri or not Path(storage_uri).exists():
                        external_payload_missing.append(payload_id)
                        continue
                    actual_hash = hashlib.sha256(Path(storage_uri).read_bytes()).hexdigest()
                else:
                    actual_hash = hashlib.sha256(
                        str(row["raw_payload_json"] or "").encode("utf-8")
                    ).hexdigest()
                if actual_hash != str(row["payload_sha256"] or ""):
                    payload_hash_mismatches.append(payload_id)
        source_link_orphans: list[str] = []
        if {"market_data_source_links", "source_payloads"}.issubset(existing_tables):
            source_link_orphans = [
                str(row["record_identity"])
                for row in conn.execute(
                    """
                    SELECT link.record_identity
                    FROM market_data_source_links AS link
                    LEFT JOIN source_payloads AS payload
                      ON payload.payload_id = link.payload_id
                    WHERE payload.payload_id IS NULL
                    """
                ).fetchall()
            ]
        payload["source_payload_integrity"] = {
            "external_payload_missing": external_payload_missing,
            "hash_mismatches": payload_hash_mismatches,
            "source_link_orphans": source_link_orphans,
            "status": (
                "verified"
                if not external_payload_missing
                and not payload_hash_mismatches
                and not source_link_orphans
                else "failed"
            ),
        }
        if external_payload_missing:
            payload["errors"].append(
                "external_payload_missing:" + ",".join(external_payload_missing)
            )
        if payload_hash_mismatches:
            payload["errors"].append(
                "source_payload_hash_mismatch:" + ",".join(payload_hash_mismatches)
            )
        if source_link_orphans:
            payload["errors"].append(
                "source_payload_link_orphan:" + ",".join(source_link_orphans)
            )
        try:
            schema_version = int(meta.get("market_data_schema_version", "0"))
        except ValueError:
            schema_version = 0
        payload["schema_version"] = schema_version
        if schema_version < 4:
            payload["errors"].append(f"schema_version_below_v4:{schema_version}")
        required_research_tables = (
            "financial_fact_observations",
            "market_sessions",
            "security_trading_status",
            "adjustment_factors",
            "adjusted_bars",
            "security_lifecycle",
            "benchmark_membership",
            "daily_market_stats",
            "institutional_flows",
            "margin_short_snapshots",
            "market_events",
            "market_data_source_links",
            "market_data_partition_state",
            "market_data_gap_ledger",
            "market_data_completeness_runs",
        )
        missing_research_tables = [
            table for table in required_research_tables if table not in existing_tables
        ]
        payload["research_dataset_counts"] = {
            table: (
                int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                if table in existing_tables
                else None
            )
            for table in required_research_tables
        }
        payload["research_schema"] = {
            "required_tables": list(required_research_tables),
            "missing_tables": missing_research_tables,
        }
        payload["pit_query_contract"] = {
            "available": not missing_research_tables and schema_version >= 4,
            "rule": "effective_date <= observation_date and available/published date <= information_cutoff",
            "missing_tables": missing_research_tables,
        }
        if missing_research_tables:
            payload["errors"].append(
                "research_schema_missing:" + ",".join(missing_research_tables)
            )
        provenance_contract = {
            "daily_bars": ("effective_date", "available_date", "source_payload_id"),
            "daily_bar_sources": ("effective_date", "available_date"),
            "period_bars": ("available_date", "availability_precision"),
            "index_bars": ("available_date", "source_payload_id", "availability_precision"),
            "security_master_snapshots": (
                "effective_date",
                "available_date",
                "source_payload_id",
                "availability_precision",
            ),
            "universe_membership": (
                "effective_from",
                "available_date",
                "published_at",
                "source_payload_id",
                "availability_precision",
            ),
            "monthly_revenue": ("available_date", "availability_precision", "source_payload_id"),
            "valuation_snapshots": ("available_date", "availability_precision", "source_payload_id"),
            "annual_company_fundamentals": (
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "quarterly_company_fundamentals": (
                "effective_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "financial_fact_observations": (
                "effective_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "corporate_actions": (
                "action_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "market_sessions": (
                "trade_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "security_trading_status": (
                "effective_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "adjustment_factors": (
                "effective_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "adjusted_bars": (
                "trade_date",
                "available_date",
                "derivation_input_sha256",
            ),
            "security_lifecycle": (
                "effective_from",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "benchmark_membership": (
                "effective_from",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "daily_market_stats": (
                "trade_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "institutional_flows": (
                "trade_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "margin_short_snapshots": (
                "trade_date",
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "market_events": (
                "available_date",
                "availability_precision",
                "source_payload_id",
            ),
            "source_payloads": (
                "effective_date",
                "available_date",
                "payload_sha256",
                "availability_precision",
            ),
        }
        missing_provenance_columns: dict[str, list[str]] = {}
        for table, required_columns in provenance_contract.items():
            if table not in existing_tables:
                continue
            columns = {
                str(row[1])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            missing = [column for column in required_columns if column not in columns]
            if missing:
                missing_provenance_columns[table] = missing
        payload["provenance_schema"] = {
            "required_columns": provenance_contract,
            "missing_columns": missing_provenance_columns,
            "status": "verified" if not missing_provenance_columns else "failed",
        }
        if missing_provenance_columns:
            payload["errors"].append(
                "provenance_columns_missing:"
                + json.dumps(missing_provenance_columns, ensure_ascii=False, sort_keys=True)
            )
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
