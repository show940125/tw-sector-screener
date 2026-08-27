from __future__ import annotations

"""Canonical SQLite storage for auditable Taiwan market data.

The existing daily and quarterly stores remain supported as migration sources.
This module owns the unified database envelope and the tables that do not fit
either legacy store.  It deliberately keeps raw payloads and source metadata
separate from analytical tables so a later rebuild can be audited.
"""

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import uuid
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.providers.daily_bar_store import database_integrity as daily_database_integrity
from src.providers.daily_bar_store import init_db as init_daily_db
from src.providers.quarterly_store import init_db as init_quarterly_db


MARKET_DATA_SCHEMA_VERSION = 4
PERIOD_DERIVATION_VERSION = "daily-bars-period-v1"
SOURCE_PAYLOAD_INLINE_LIMIT_BYTES = 10 * 1024 * 1024
VALID_SYNC_STATUSES = frozenset(
    {"migrated", "verified", "partial", "quarantined", "failed", "not_implemented"}
)


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _read_only_connect(db_path: Path) -> sqlite3.Connection:
    """Open the canonical database without triggering WAL/schema writes."""

    path = Path(db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _columns(conn: sqlite3.Connection, table: str, schema: str = "main") -> list[str]:
    return [
        str(row[1])
        for row in conn.execute(f"PRAGMA {schema}.table_info({table})").fetchall()
    ]


def _ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    """Add a nullable/defaulted column without rewriting legacy tables."""

    if column not in _columns(conn, table):
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {definition}')


def _canonicalize_monthly_revenue_periods(conn: sqlite3.Connection) -> int:
    """Normalize legacy ROC revenue keys without creating logical duplicates."""

    rows = conn.execute(
        "SELECT market, symbol, revenue_month FROM monthly_revenue"
    ).fetchall()
    changed = 0
    for row in rows:
        raw_period = str(row["revenue_month"] or "").strip()
        try:
            canonical_period = _normalize_revenue_month(raw_period)
        except ValueError:
            continue
        if canonical_period == raw_period:
            continue
        existing = conn.execute(
            """
            SELECT 1 FROM monthly_revenue
            WHERE market = ? AND symbol = ? AND revenue_month = ?
            """,
            (row["market"], row["symbol"], canonical_period),
        ).fetchone()
        if existing is not None:
            # Prefer the canonical key.  A canonical row may be a newer
            # verified network observation; deleting the legacy alias avoids
            # double-counting the same company/month in completeness gates.
            conn.execute(
                "DELETE FROM monthly_revenue WHERE market = ? AND symbol = ? AND revenue_month = ?",
                (row["market"], row["symbol"], raw_period),
            )
        else:
            conn.execute(
                "UPDATE monthly_revenue SET revenue_month = ? "
                "WHERE market = ? AND symbol = ? AND revenue_month = ?",
                (canonical_period, row["market"], row["symbol"], raw_period),
            )
        changed += 1
    return changed


def _safe_dataset_path(dataset_key: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(dataset_key).strip())
    return value or "unknown"


def _legacy_source_fingerprint(path: Path) -> str:
    """Return a cheap change marker for an immutable legacy SQLite input."""

    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def _backfill_quality_issues_from_legacy(conn: sqlite3.Connection) -> int:
    """Seed the v2 issue ledger from v1 rows without duplicating observations."""

    rows = conn.execute(
        """
        SELECT run_id, dataset_key, market, symbol, trade_date, issue_type,
               detail, fingerprint, created_at
        FROM market_data_sync_issues
        ORDER BY issue_id
        """
    ).fetchall()
    inserted = 0
    for row in rows:
        quality = conn.execute(
            "SELECT issue_id FROM market_data_quality_issues WHERE fingerprint = ?",
            (str(row["fingerprint"]),),
        ).fetchone()
        if quality is None:
            cursor = conn.execute(
                """
                INSERT INTO market_data_quality_issues(
                    dataset_key, market, symbol, effective_date, issue_type, detail,
                    fingerprint, first_seen_at, last_seen_at, occurrence_count,
                    latest_run_id, status
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'open')
                """,
                (
                    str(row["dataset_key"] or "daily_bars"),
                    row["market"],
                    row["symbol"],
                    row["trade_date"],
                    str(row["issue_type"] or "sync_failed"),
                    str(row["detail"] or ""),
                    str(row["fingerprint"]),
                    str(row["created_at"]),
                    str(row["created_at"]),
                    row["run_id"],
                ),
            )
            quality_issue_id = int(cursor.lastrowid)
            inserted += 1
        else:
            quality_issue_id = int(quality["issue_id"])
        occurrence = conn.execute(
            """
            SELECT 1 FROM market_data_quality_issue_occurrences
            WHERE issue_id = ? AND run_id = ?
            LIMIT 1
            """,
            (quality_issue_id, row["run_id"]),
        ).fetchone()
        if occurrence is None:
            conn.execute(
                """
                INSERT INTO market_data_quality_issue_occurrences(
                    issue_id, run_id, observed_at, detail
                ) VALUES(?, ?, ?, ?)
                """,
                (quality_issue_id, row["run_id"], row["created_at"], str(row["detail"] or "")),
            )
    if inserted:
        conn.execute(
            """
            UPDATE market_data_quality_issues
            SET occurrence_count = (
                SELECT COUNT(*)
                FROM market_data_quality_issue_occurrences occurrence
                WHERE occurrence.issue_id = market_data_quality_issues.issue_id
            )
            """
        )
    return inserted


def _meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def init_market_data_db(db_path: Path) -> Path:
    """Create or upgrade the canonical market database in place."""

    db_path = Path(db_path)
    # These two initialisers are intentionally idempotent and retain the
    # public APIs used by the existing quarterly/daily code.
    init_daily_db(db_path)
    init_quarterly_db(db_path)
    with closing(_connect(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS period_bars (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                frequency TEXT NOT NULL,
                period_key TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                trading_day_count INTEGER NOT NULL,
                source_latest_trade_date TEXT NOT NULL,
                derivation_version TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                available_date TEXT,
                data_status TEXT NOT NULL DEFAULT 'derived',
                availability_precision TEXT NOT NULL DEFAULT 'derived_from_daily',
                data_gap_reason TEXT,
                PRIMARY KEY (market, symbol, frequency, period_key)
            );

            CREATE TABLE IF NOT EXISTS index_bars (
                index_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL NOT NULL,
                change_points REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                data_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (index_code, trade_date)
            );

            CREATE TABLE IF NOT EXISTS security_master_snapshots (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                name TEXT,
                industry TEXT,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, effective_date)
            );

            CREATE TABLE IF NOT EXISTS universe_membership (
                theme TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                universe_mode TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                source TEXT NOT NULL,
                source_payload_sha256 TEXT,
                recorded_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                availability_precision TEXT NOT NULL DEFAULT 'unknown',
                data_gap_reason TEXT,
                PRIMARY KEY (theme, symbol, market, universe_mode, effective_from)
            );

            CREATE TABLE IF NOT EXISTS monthly_revenue (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                revenue_month TEXT NOT NULL,
                monthly_revenue REAL,
                revenue_mom REAL,
                revenue_yoy REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, revenue_month)
            );

            CREATE TABLE IF NOT EXISTS valuation_snapshots (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                pe REAL,
                pb REAL,
                dividend_yield REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS annual_company_fundamentals (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                fiscal_year TEXT NOT NULL,
                available_date TEXT NOT NULL,
                published_at TEXT,
                revenue REAL,
                gross_profit REAL,
                net_income REAL,
                equity REAL,
                eps REAL,
                roe REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                raw_payload_json TEXT,
                PRIMARY KEY (market, symbol, fiscal_year, available_date)
            );

            CREATE TABLE IF NOT EXISTS corporate_actions (
                action_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action_date TEXT NOT NULL,
                action_type TEXT NOT NULL,
                ex_date TEXT,
                record_date TEXT,
                payment_date TEXT,
                ratio REAL,
                cash_amount REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                raw_payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS source_payloads (
                payload_id TEXT PRIMARY KEY,
                dataset_key TEXT NOT NULL,
                request_method TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                request_body_sha256 TEXT,
                payload_sha256 TEXT NOT NULL,
                effective_date TEXT,
                available_date TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                cache_file TEXT,
                validation_status TEXT NOT NULL,
                validation_error TEXT,
                raw_payload_json TEXT NOT NULL,
                storage_mode TEXT NOT NULL DEFAULT 'inline',
                storage_uri TEXT,
                byte_size INTEGER,
                content_encoding TEXT NOT NULL DEFAULT 'utf-8'
            );

            CREATE TABLE IF NOT EXISTS market_data_sync_runs (
                run_id TEXT PRIMARY KEY,
                as_of_date TEXT NOT NULL,
                themes_json TEXT NOT NULL,
                database_path TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS market_data_sync_issues (
                issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                dataset_key TEXT NOT NULL,
                market TEXT,
                symbol TEXT,
                trade_date TEXT,
                issue_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES market_data_sync_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS market_data_quarantine (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_key TEXT NOT NULL,
                source_url TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                received_at TEXT NOT NULL,
                reason TEXT NOT NULL,
                raw_payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_data_dataset_catalog (
                dataset_key TEXT PRIMARY KEY,
                frequency TEXT NOT NULL,
                canonical_table TEXT NOT NULL,
                description TEXT NOT NULL,
                source_policy TEXT NOT NULL,
                point_in_time_required INTEGER NOT NULL DEFAULT 1,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_data_source_registry (
                source_endpoint TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                market TEXT,
                base_url TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                active INTEGER NOT NULL DEFAULT 1,
                supports_redirect INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_data_fetch_attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                dataset_key TEXT NOT NULL,
                market TEXT,
                symbol TEXT,
                requested_from TEXT,
                requested_to TEXT,
                request_method TEXT NOT NULL,
                request_url TEXT NOT NULL,
                final_url TEXT,
                redirect_chain_json TEXT,
                http_status INTEGER,
                fallback_level INTEGER NOT NULL DEFAULT 0,
                cache_status TEXT,
                payload_sha256 TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                FOREIGN KEY (run_id) REFERENCES market_data_sync_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS market_data_sync_items (
                sync_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                dataset_key TEXT NOT NULL,
                market TEXT,
                symbol TEXT,
                requested_from TEXT,
                requested_to TEXT,
                expected_trade_date TEXT,
                actual_latest_trade_date TEXT,
                expected_row_count INTEGER,
                actual_row_count INTEGER,
                cache_status TEXT,
                status TEXT NOT NULL,
                error TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                FOREIGN KEY (run_id) REFERENCES market_data_sync_runs(run_id),
                UNIQUE(run_id, dataset_key, market, symbol, requested_from, requested_to)
            );

            CREATE TABLE IF NOT EXISTS market_data_sync_state (
                dataset_key TEXT NOT NULL,
                market TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                partition_key TEXT NOT NULL DEFAULT '',
                first_effective_date TEXT,
                last_effective_date TEXT,
                verified_row_count INTEGER NOT NULL DEFAULT 0,
                last_verified_at TEXT,
                last_status TEXT NOT NULL,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(dataset_key, market, symbol, partition_key)
            );

            CREATE TABLE IF NOT EXISTS market_data_partition_state (
                dataset_key TEXT NOT NULL,
                market TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                partition_key TEXT NOT NULL DEFAULT '',
                requested_from TEXT,
                requested_to TEXT,
                request_method TEXT NOT NULL,
                request_url TEXT NOT NULL,
                request_body_sha256 TEXT,
                payload_sha256 TEXT,
                source_payload_id TEXT,
                first_effective_date TEXT,
                last_effective_date TEXT,
                row_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                last_verified_at TEXT,
                last_run_id TEXT,
                last_completeness_run_id TEXT,
                gap_reason TEXT,
                retry_after TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(dataset_key, market, symbol, partition_key)
            );

            CREATE TABLE IF NOT EXISTS market_data_gap_ledger (
                dataset_key TEXT NOT NULL,
                market TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL DEFAULT '',
                partition_key TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL,
                detail TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                latest_run_id TEXT,
                retry_after TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                PRIMARY KEY(dataset_key, market, symbol, partition_key)
            );

            CREATE TABLE IF NOT EXISTS market_data_completeness_runs (
                completeness_run_id TEXT PRIMARY KEY,
                run_id TEXT,
                dataset_key TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                expected_rows INTEGER,
                actual_rows INTEGER,
                expected_partitions INTEGER,
                actual_partitions INTEGER,
                missing_partitions_json TEXT NOT NULL DEFAULT '[]',
                summary_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS market_data_quality_issues (
                issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_key TEXT NOT NULL,
                market TEXT,
                symbol TEXT,
                effective_date TEXT,
                issue_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                fingerprint TEXT NOT NULL UNIQUE,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                latest_run_id TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                FOREIGN KEY (latest_run_id) REFERENCES market_data_sync_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS market_data_quality_issue_occurrences (
                occurrence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id INTEGER NOT NULL,
                run_id TEXT,
                observed_at TEXT NOT NULL,
                detail TEXT NOT NULL,
                FOREIGN KEY (issue_id) REFERENCES market_data_quality_issues(issue_id),
                FOREIGN KEY (run_id) REFERENCES market_data_sync_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS financial_fact_observations (
                fact_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                fact_code TEXT NOT NULL,
                fiscal_period TEXT NOT NULL,
                value REAL,
                unit TEXT NOT NULL,
                consolidation TEXT NOT NULL,
                dimension_json TEXT NOT NULL DEFAULT '{}',
                effective_date TEXT NOT NULL,
                available_date TEXT NOT NULL,
                published_at TEXT,
                revision_id TEXT NOT NULL,
                revision_sequence INTEGER NOT NULL DEFAULT 1,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                data_gap_reason TEXT,
                raw_payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS market_sessions (
                market TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                is_open INTEGER NOT NULL,
                session_type TEXT NOT NULL DEFAULT 'regular',
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                availability_precision TEXT NOT NULL DEFAULT 'unknown',
                data_gap_reason TEXT,
                PRIMARY KEY (market, trade_date)
            );

            CREATE TABLE IF NOT EXISTS security_trading_status (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                limit_up REAL,
                limit_down REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, effective_date)
            );

            CREATE TABLE IF NOT EXISTS adjustment_factors (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                price_factor REAL NOT NULL,
                cash_dividend REAL NOT NULL DEFAULT 0,
                stock_dividend_ratio REAL,
                split_factor REAL,
                action_hash TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, effective_date)
            );

            CREATE TABLE IF NOT EXISTS adjusted_bars (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                price_mode TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                adjustment_factor REAL NOT NULL,
                derivation_version TEXT NOT NULL,
                source_latest_trade_date TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                available_date TEXT,
                data_status TEXT NOT NULL DEFAULT 'derived',
                derivation_input_sha256 TEXT,
                PRIMARY KEY (market, symbol, trade_date, price_mode)
            );

            CREATE TABLE IF NOT EXISTS security_lifecycle (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                status TEXT NOT NULL,
                name TEXT,
                industry TEXT,
                listing_date TEXT,
                delisting_date TEXT,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, effective_from)
            );

            CREATE TABLE IF NOT EXISTS benchmark_membership (
                benchmark_code TEXT NOT NULL,
                market TEXT,
                symbol TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                weight REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (benchmark_code, symbol, effective_from)
            );

            CREATE TABLE IF NOT EXISTS daily_market_stats (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                trade_value REAL,
                transaction_count INTEGER,
                turnover_rate REAL,
                market_cap REAL,
                shares_outstanding REAL,
                free_float_shares REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS institutional_flows (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                foreign_buy REAL,
                foreign_sell REAL,
                foreign_net REAL,
                trust_buy REAL,
                trust_sell REAL,
                trust_net REAL,
                dealer_buy REAL,
                dealer_sell REAL,
                dealer_net REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS margin_short_snapshots (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                margin_buy REAL,
                margin_sell REAL,
                margin_balance REAL,
                short_buy REAL,
                short_sell REAL,
                short_balance REAL,
                sbl_balance REAL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                published_at TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                PRIMARY KEY (market, symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS market_events (
                event_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                symbol TEXT,
                event_type TEXT NOT NULL,
                effective_date TEXT,
                announced_at TEXT,
                published_at TEXT,
                title TEXT,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_payload_id TEXT,
                source_payload_sha256 TEXT,
                fetched_at TEXT NOT NULL,
                available_date TEXT,
                validation_status TEXT NOT NULL DEFAULT 'verified',
                raw_payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS market_data_source_links (
                dataset_key TEXT NOT NULL,
                record_identity TEXT NOT NULL,
                payload_id TEXT NOT NULL,
                linked_at TEXT NOT NULL,
                PRIMARY KEY (dataset_key, record_identity, payload_id)
            );

            CREATE INDEX IF NOT EXISTS idx_period_bars_symbol_period
            ON period_bars(market, symbol, frequency, period_end);
            CREATE INDEX IF NOT EXISTS idx_index_bars_date
            ON index_bars(index_code, trade_date);
            CREATE INDEX IF NOT EXISTS idx_membership_effective
            ON universe_membership(theme, effective_from, effective_to);
            CREATE INDEX IF NOT EXISTS idx_revenue_symbol_month
            ON monthly_revenue(market, symbol, revenue_month);
            CREATE INDEX IF NOT EXISTS idx_valuation_symbol_date
            ON valuation_snapshots(market, symbol, trade_date);
            CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol_date
            ON corporate_actions(market, symbol, action_date);
            CREATE INDEX IF NOT EXISTS idx_payload_dataset_date
            ON source_payloads(dataset_key, effective_date, fetched_at);
            CREATE INDEX IF NOT EXISTS idx_sync_issue_fingerprint
            ON market_data_sync_issues(fingerprint);
            CREATE INDEX IF NOT EXISTS idx_fetch_attempts_dataset_date
            ON market_data_fetch_attempts(dataset_key, symbol, started_at);
            CREATE INDEX IF NOT EXISTS idx_sync_items_run_status
            ON market_data_sync_items(run_id, status);
            CREATE INDEX IF NOT EXISTS idx_partition_state_dataset_status
            ON market_data_partition_state(dataset_key, status, last_verified_at);
            CREATE INDEX IF NOT EXISTS idx_partition_state_effective_range
            ON market_data_partition_state(dataset_key, first_effective_date, last_effective_date);
            CREATE INDEX IF NOT EXISTS idx_gap_ledger_dataset_status
            ON market_data_gap_ledger(dataset_key, status, last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_completeness_runs_dataset_status
            ON market_data_completeness_runs(dataset_key, status, started_at);
            CREATE INDEX IF NOT EXISTS idx_quality_issue_dataset_status
            ON market_data_quality_issues(dataset_key, status, last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_quality_occurrence_issue_date
            ON market_data_quality_issue_occurrences(issue_id, observed_at);
            CREATE INDEX IF NOT EXISTS idx_financial_fact_lookup
            ON financial_fact_observations(market, symbol, fact_code, effective_date, available_date);
            CREATE INDEX IF NOT EXISTS idx_market_sessions_date
            ON market_sessions(market, trade_date, is_open);
            CREATE INDEX IF NOT EXISTS idx_security_trading_status_date
            ON security_trading_status(market, symbol, effective_date);
            CREATE INDEX IF NOT EXISTS idx_adjustment_factors_symbol_date
            ON adjustment_factors(market, symbol, effective_date);
            CREATE INDEX IF NOT EXISTS idx_adjusted_bars_symbol_date
            ON adjusted_bars(market, symbol, price_mode, trade_date);
            CREATE INDEX IF NOT EXISTS idx_security_lifecycle_symbol_date
            ON security_lifecycle(market, symbol, effective_from, effective_to);
            CREATE INDEX IF NOT EXISTS idx_benchmark_membership_date
            ON benchmark_membership(benchmark_code, effective_from, effective_to);
            CREATE INDEX IF NOT EXISTS idx_daily_market_stats_symbol_date
            ON daily_market_stats(market, symbol, trade_date);
            CREATE INDEX IF NOT EXISTS idx_institutional_flows_symbol_date
            ON institutional_flows(market, symbol, trade_date);
            CREATE INDEX IF NOT EXISTS idx_margin_short_symbol_date
            ON margin_short_snapshots(market, symbol, trade_date);
            CREATE INDEX IF NOT EXISTS idx_market_events_symbol_date
            ON market_events(market, symbol, effective_date, announced_at);
            """
        )
        # These columns are additive migrations for databases created by the
        # v1 implementation.  Keeping the legacy raw_payload_json column
        # non-null lets old readers continue to work while large payloads are
        # stored in the external raw store with a compact descriptor in that
        # column.
        _ensure_column(conn, "monthly_revenue", "available_date", "TEXT")
        _ensure_column(conn, "valuation_snapshots", "available_date", "TEXT")
        # The compatibility quarterly table predates the PIT facts table.  Its
        # fetch/as-of fields are retained, but these nullable columns make the
        # distinction explicit: legacy rows remain non-PIT until a real
        # publication/availability date is supplied by a validated adapter.
        for column, definition in (
            ("effective_date", "TEXT"),
            ("available_date", "TEXT"),
            ("published_at", "TEXT"),
            ("source_payload_id", "TEXT"),
            ("source_payload_sha256", "TEXT"),
            ("revision_id", "TEXT"),
            ("revision_sequence", "INTEGER"),
            ("validation_status", "TEXT NOT NULL DEFAULT 'migrated'"),
            ("availability_precision", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("data_gap_reason", "TEXT"),
        ):
            _ensure_column(conn, "quarterly_company_fundamentals", column, definition)
        for column, definition in (
            ("revision_id", "TEXT"),
            ("revision_sequence", "INTEGER"),
            ("data_gap_reason", "TEXT"),
        ):
            _ensure_column(conn, "annual_company_fundamentals", column, definition)
        for table in (
            "daily_bars",
            "daily_bar_sources",
            "index_bars",
            "security_master_snapshots",
            "universe_membership",
            "monthly_revenue",
            "valuation_snapshots",
            "annual_company_fundamentals",
            "corporate_actions",
        ):
            _ensure_column(conn, table, "source_payload_id", "TEXT")
        for table in (
            "period_bars",
            "daily_bars",
            "daily_bar_sources",
            "index_bars",
            "security_master_snapshots",
            "universe_membership",
            "monthly_revenue",
            "valuation_snapshots",
            "annual_company_fundamentals",
            "corporate_actions",
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
            "source_payloads",
        ):
            _ensure_column(conn, table, "available_date", "TEXT")
        _ensure_column(conn, "universe_membership", "published_at", "TEXT")
        for table in (
            "period_bars",
            "daily_bars",
            "daily_bar_sources",
            "index_bars",
            "security_master_snapshots",
            "universe_membership",
            "quarterly_company_fundamentals",
            "monthly_revenue",
            "valuation_snapshots",
            "annual_company_fundamentals",
            "corporate_actions",
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
        ):
            _ensure_column(
                conn,
                table,
                "availability_precision",
                "TEXT NOT NULL DEFAULT 'unknown'",
            )
        for table in (
            "period_bars",
            "daily_bars",
            "daily_bar_sources",
            "index_bars",
            "security_master_snapshots",
            "universe_membership",
            "quarterly_company_fundamentals",
            "monthly_revenue",
            "valuation_snapshots",
            "annual_company_fundamentals",
            "corporate_actions",
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
        ):
            _ensure_column(conn, table, "data_gap_reason", "TEXT")
        _ensure_column(conn, "source_payloads", "storage_mode", "TEXT NOT NULL DEFAULT 'inline'")
        _ensure_column(conn, "source_payloads", "storage_uri", "TEXT")
        _ensure_column(conn, "source_payloads", "byte_size", "INTEGER")
        _ensure_column(conn, "source_payloads", "content_encoding", "TEXT NOT NULL DEFAULT 'utf-8'")
        _ensure_column(conn, "adjusted_bars", "derivation_input_sha256", "TEXT")
        _ensure_column(
            conn,
            "source_payloads",
            "availability_precision",
            "TEXT NOT NULL DEFAULT 'unknown'",
        )
        _canonicalize_monthly_revenue_periods(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quarterly_pit_available "
            "ON quarterly_company_fundamentals(market, symbol, effective_date, available_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_annual_pit_available "
            "ON annual_company_fundamentals(market, symbol, fiscal_year, available_date)"
        )
        conn.execute(
            "UPDATE source_payloads SET byte_size = length(CAST(raw_payload_json AS BLOB)) "
            "WHERE byte_size IS NULL"
        )
        conn.execute(
            "UPDATE source_payloads SET storage_mode = 'inline' "
            "WHERE storage_mode IS NULL OR storage_mode = ''"
        )
        now = _now_iso()
        catalog = (
            ("daily_bars", "D", "daily_bars", "Verified official daily OHLCV bars"),
            ("period_bars", "W/M/Q/Y", "period_bars", "Bars derived from verified daily bars"),
            ("index_bars", "D", "index_bars", "Verified benchmark/index bars"),
            ("security_master", "snapshot", "security_master_snapshots", "Security identity and classification snapshots"),
            ("monthly_revenue", "M", "monthly_revenue", "Monthly revenue snapshots"),
            ("valuation_snapshots", "D", "valuation_snapshots", "Daily valuation snapshots"),
            ("quarterly_fundamentals", "Q", "quarterly_company_fundamentals", "Quarterly company fundamentals"),
            ("annual_fundamentals", "Y", "annual_company_fundamentals", "Annual company fundamentals"),
            ("corporate_actions", "event", "corporate_actions", "Dividends, splits and ex-rights events"),
            ("financial_facts", "Q/Y", "financial_fact_observations", "Point-in-time financial facts and revisions"),
            ("market_sessions", "D", "market_sessions", "Exchange session calendar"),
            ("security_trading_status", "D", "security_trading_status", "Suspension, disposal and tradability status"),
            ("adjustment_factors", "event", "adjustment_factors", "Validated corporate-action adjustment factors"),
            ("adjusted_bars", "D", "adjusted_bars", "Rebuildable adjusted price series"),
            ("security_lifecycle", "event", "security_lifecycle", "Listing, delisting and identity lifecycle"),
            ("benchmark_membership", "event", "benchmark_membership", "Point-in-time benchmark constituents"),
            ("daily_market_stats", "D", "daily_market_stats", "Trade value, turnover and market-cap statistics"),
            ("institutional_flows", "D", "institutional_flows", "Foreign, trust and dealer flows"),
            ("margin_short_snapshots", "D", "margin_short_snapshots", "Margin, short and securities-borrowing snapshots"),
            ("market_events", "event", "market_events", "Announcements, meetings and market events"),
        )
        for dataset_key, frequency, canonical_table, description in catalog:
            conn.execute(
                """
                INSERT INTO market_data_dataset_catalog(
                    dataset_key, frequency, canonical_table, description,
                    source_policy, point_in_time_required, active, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'official_or_auditable_public', 1, 1, ?, ?)
                ON CONFLICT(dataset_key) DO UPDATE SET
                    frequency=excluded.frequency,
                    canonical_table=excluded.canonical_table,
                    description=excluded.description,
                    updated_at=excluded.updated_at
                """,
                (dataset_key, frequency, canonical_table, description, now, now),
            )
        sources = (
            ("twse.stock_day.primary", "TWSE STOCK_DAY rwd", "TWSE", "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY", 10, 1, "primary daily history"),
            ("twse.stock_day.exchange_report", "TWSE STOCK_DAY exchangeReport", "TWSE", "https://www.twse.com.tw/exchangeReport/STOCK_DAY", 20, 1, "daily history fallback"),
            ("twse.basics", "TWSE OpenAPI basics", "TWSE", "https://openapi.twse.com.tw/v1/opendata/t187ap03_L", 10, 1, "security master"),
            ("twse.revenue", "TWSE OpenAPI revenue", "TWSE", "https://openapi.twse.com.tw/v1/opendata/t187ap05_L", 10, 1, "monthly revenue"),
            ("twse.bwibbu", "TWSE BWIBBU", "TWSE", "https://www.twse.com.tw/exchangeReport/BWIBBU_d", 20, 1, "valuation snapshot"),
            ("twse.fmtqik", "TWSE FMTQIK", "TWSE", "https://www.twse.com.tw/exchangeReport/FMTQIK", 10, 1, "TAIEX history"),
            ("twse.eps", "TWSE OpenAPI EPS", "TWSE", "https://openapi.twse.com.tw/v1/opendata/t187ap14_L", 10, 1, "EPS/fundamental supplement"),
            ("tpex.trading_stock", "TPEx tradingStock", "TPEx", "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock", 10, 1, "daily history"),
            ("tpex.daily_quotes", "TPEx daily quotes", "TPEx", "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php", 30, 1, "bounded bulk daily fallback"),
            ("tpex.basics", "TPEx OpenAPI basics", "TPEx", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", 10, 1, "security master"),
            ("tpex.revenue", "TPEx OpenAPI revenue", "TPEx", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O", 10, 1, "monthly revenue"),
            ("tpex.pe_query", "TPEx PE query", "TPEx", "https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate", 20, 1, "valuation snapshot"),
            ("tpex.eps", "TPEx OpenAPI EPS", "TPEx", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", 10, 1, "EPS/fundamental supplement"),
            ("twse.market_sessions", "TWSE market sessions", "TWSE", "https://www.twse.com.tw/zh/trading/holiday.html", 20, 1, "trading calendar"),
            ("tpex.market_sessions", "TPEx market sessions", "TPEx", "https://www.tpex.org.tw/zh-tw/announce/market/holiday", 20, 1, "trading calendar"),
            ("twse.corporate_actions", "TWSE corporate actions", "TWSE", "https://openapi.twse.com.tw/", 20, 1, "dividend and ex-right events"),
            ("tpex.corporate_actions", "TPEx corporate actions", "TPEx", "https://www.tpex.org.tw/openapi/", 20, 1, "dividend and ex-right events"),
            ("twse.institutional_flows", "TWSE institutional flows", "TWSE", "https://www.twse.com.tw/zh/trading/foreign-investor/mi-index.html", 30, 1, "institutional trading"),
            ("tpex.institutional_flows", "TPEx institutional flows", "TPEx", "https://www.tpex.org.tw/zh-tw/market/trading/foreign", 30, 1, "institutional trading"),
            ("twse.margin_short", "TWSE margin and short", "TWSE", "https://www.twse.com.tw/zh/trading/margin/mi-margn.html", 30, 1, "margin and short data"),
            ("tpex.margin_short", "TPEx margin and short", "TPEx", "https://www.tpex.org.tw/zh-tw/market/trading/margin", 30, 1, "margin and short data"),
            ("mops.events", "MOPS announcements", "TWSE/TPEx", "https://mops.twse.com.tw/", 50, 0, "planned PIT event adapter"),
            ("mops.public", "MOPS public data", "TWSE/TPEx", "https://mops.twse.com.tw/", 50, 0, "planned PIT financial/event adapter"),
        )
        for endpoint, name, market, base_url, priority, active, notes in sources:
            conn.execute(
                """
                INSERT INTO market_data_source_registry(
                    source_endpoint, source_name, market, base_url, priority,
                    active, supports_redirect, notes, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(source_endpoint) DO UPDATE SET
                    source_name=excluded.source_name, market=excluded.market,
                    base_url=excluded.base_url, priority=excluded.priority,
                    active=excluded.active, notes=excluded.notes,
                    updated_at=excluded.updated_at
                """,
                (endpoint, name, market, base_url, priority, active, notes, now, now),
            )
        _backfill_quality_issues_from_legacy(conn)
        _meta(conn, "market_data_schema_version", str(MARKET_DATA_SCHEMA_VERSION))
        _meta(conn, "daily_bar_schema_version", "1")
        _meta(conn, "quarterly_schema_version", "2")
        conn.commit()
    return db_path


def _ready_market_data_db(db_path: Path) -> Path:
    """Return a schema-ready path without replaying DDL on every row write."""

    path = Path(db_path)
    if not path.exists():
        return init_market_data_db(path)
    try:
        with closing(sqlite3.connect(path)) as conn:
            exists = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type='table'
                  AND name IN (
                    'market_data_sync_runs',
                    'market_data_quality_issues',
                    'financial_fact_observations',
                    'market_data_source_links'
                  )
                GROUP BY 1
                HAVING COUNT(*) = 4
                """
            ).fetchone()
            version_row = conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'market_data_schema_version'"
            ).fetchone() if exists is not None else None
    except sqlite3.DatabaseError:
        exists = None
        version_row = None
    if exists is None:
        return init_market_data_db(path)
    try:
        version = int(str(version_row[0])) if version_row is not None else 0
    except (TypeError, ValueError):
        version = 0
    if version > MARKET_DATA_SCHEMA_VERSION:
        raise RuntimeError(
            f"market data schema {version} is newer than supported {MARKET_DATA_SCHEMA_VERSION}"
        )
    if version < MARKET_DATA_SCHEMA_VERSION:
        return init_market_data_db(path)
    # v4 databases created before the Gregorian-period migration may already
    # be at the current schema version. Run this one-time data correction
    # without replaying the full DDL on every canonical row write.
    with closing(_connect(path)) as conn:
        marker = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'monthly_revenue_period_normalization'"
        ).fetchone()
        if marker is None or str(marker[0]) != "v1":
            _canonicalize_monthly_revenue_periods(conn)
            _meta(conn, "monthly_revenue_period_normalization", "v1")
            conn.commit()
    return path


def _iso_value(value: date | str | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, date) else str(value)


def _normalize_revenue_month(value: str) -> str:
    """Store revenue periods canonically as Gregorian ``YYYY-MM``."""

    raw = str(value or "").strip()
    digits = re.sub(r"[^0-9]", "", raw)
    try:
        if len(digits) == 5:
            year, month = int(digits[:3]) + 1911, int(digits[3:])
        elif len(digits) >= 6:
            year, month = int(digits[:4]), int(digits[4:6])
        else:
            raise ValueError
        date(year, month, 1)
    except ValueError as exc:
        raise ValueError(f"invalid revenue_month: {value}") from exc
    return f"{year:04d}-{month:02d}"


def _financial_fact_identity(
    *,
    market: str,
    symbol: str,
    fact_code: str,
    fiscal_period: str,
    unit: str,
    consolidation: str,
    dimension_json: str,
    revision_id: str,
) -> str:
    identity = "|".join(
        [
            market.strip(),
            symbol.strip(),
            fact_code.strip(),
            fiscal_period.strip(),
            unit.strip(),
            consolidation.strip(),
            dimension_json,
            revision_id.strip(),
        ]
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def link_source_payload(
    db_path: Path,
    *,
    dataset_key: str,
    record_identity: str,
    payload_id: str,
    linked_at: str | None = None,
) -> None:
    """Associate a canonical record with its immutable raw payload."""

    db_path = Path(db_path)
    with closing(_connect(_ready_market_data_db(db_path))) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO market_data_source_links(
                dataset_key, record_identity, payload_id, linked_at
            ) VALUES(?, ?, ?, ?)
            """,
            (dataset_key, record_identity, payload_id, linked_at or _now_iso()),
        )
        conn.commit()


def upsert_financial_fact(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    fact_code: str,
    fiscal_period: str,
    value: float | None,
    unit: str,
    consolidation: str,
    effective_date: date | str,
    available_date: date | str,
    published_at: str | None = None,
    revision_id: str | None = None,
    revision_sequence: int = 1,
    dimension_json: str | dict[str, Any] | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    source_endpoint: str = "unknown",
    source_url: str = "unknown",
    fetched_at: str | None = None,
    validation_status: str = "verified",
    data_gap_reason: str | None = None,
    availability_precision: str = "unknown",
    raw_payload_json: str | dict[str, Any] | None = None,
) -> str:
    """Idempotently store one point-in-time financial observation."""

    db_path = _ready_market_data_db(Path(db_path))
    if isinstance(dimension_json, dict):
        dimension_text = json.dumps(dimension_json, ensure_ascii=False, sort_keys=True)
    else:
        dimension_text = dimension_json or "{}"
    try:
        parsed_dimension = json.loads(dimension_text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("dimension_json must be valid JSON") from exc
    if not isinstance(parsed_dimension, dict):
        raise ValueError("dimension_json must encode a JSON object")
    effective_text = _iso_value(effective_date)
    available_text = _iso_value(available_date)
    if not effective_text or not available_text:
        raise ValueError("financial facts require effective_date and available_date")
    try:
        date.fromisoformat(str(effective_text)[:10])
        date.fromisoformat(str(available_text)[:10])
    except ValueError as exc:
        raise ValueError("financial fact dates must be ISO dates") from exc
    if value is not None and not isinstance(value, (int, float)):
        raise ValueError("financial fact value must be numeric or null")
    if int(revision_sequence) < 1:
        raise ValueError("revision_sequence must be positive")
    if isinstance(raw_payload_json, dict):
        raw_payload_text = json.dumps(
            raw_payload_json,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    else:
        raw_payload_text = raw_payload_json
    normalized_revision = revision_id or source_payload_sha256 or "revision-1"
    fact_id = _financial_fact_identity(
        market=market,
        symbol=symbol,
        fact_code=fact_code,
        fiscal_period=fiscal_period,
        unit=unit,
        consolidation=consolidation,
        dimension_json=dimension_text,
        revision_id=normalized_revision,
    )
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO financial_fact_observations(
                fact_id, market, symbol, fact_code, fiscal_period, value, unit,
                consolidation, dimension_json, effective_date, available_date,
                published_at, revision_id, revision_sequence, source_payload_id,
                source_payload_sha256, source_endpoint, source_url, fetched_at,
                validation_status, data_gap_reason, availability_precision, raw_payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fact_id) DO UPDATE SET
                value=excluded.value,
                published_at=excluded.published_at,
                revision_sequence=excluded.revision_sequence,
                source_payload_id=excluded.source_payload_id,
                source_payload_sha256=excluded.source_payload_sha256,
                source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url,
                fetched_at=excluded.fetched_at,
                validation_status=excluded.validation_status,
                data_gap_reason=excluded.data_gap_reason,
                availability_precision=excluded.availability_precision,
                raw_payload_json=excluded.raw_payload_json
            """,
            (
                fact_id,
                market.strip(),
                symbol.strip(),
                fact_code.strip(),
                fiscal_period.strip(),
                value,
                unit.strip(),
                consolidation.strip(),
                dimension_text,
                effective_text,
                available_text,
                published_at,
                normalized_revision,
                int(revision_sequence),
                source_payload_id,
                source_payload_sha256,
                source_endpoint,
                source_url,
                fetched_at or _now_iso(),
                validation_status,
                data_gap_reason,
                availability_precision or "unknown",
                raw_payload_text,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="financial_facts",
            record_identity=fact_id,
            payload_id=source_payload_id,
        )
    return fact_id


def query_financial_facts_as_of(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    observation_date: date,
    information_cutoff: date,
    fact_code: str | None = None,
) -> list[dict[str, Any]]:
    """Return the latest verified revision known at an information cutoff."""

    conditions = [
        "market = ?",
        "symbol = ?",
        "effective_date <= ?",
        "((available_date IS NOT NULL AND available_date != '' AND substr(available_date, 1, 10) <= ?) "
        "OR (published_at IS NOT NULL AND substr(published_at, 1, 10) <= ?))",
        "validation_status = 'verified'",
        "COALESCE(availability_precision, 'unknown') != 'retrieval_date'",
    ]
    params: list[Any] = [
        market,
        symbol,
        observation_date.isoformat(),
        information_cutoff.isoformat(),
        information_cutoff.isoformat(),
    ]
    if fact_code:
        conditions.append("fact_code = ?")
        params.append(fact_code)
    with closing(_read_only_connect(Path(db_path))) as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM financial_fact_observations WHERE "
                + " AND ".join(conditions),
                params,
            ).fetchall()
        ]

    selected: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row["fact_code"]),
            str(row["fiscal_period"]),
            str(row["unit"]),
            str(row["consolidation"]),
            str(row.get("dimension_json") or "{}"),
        )
        existing = selected.get(key)
        candidate_key = (
            str(row.get("available_date") or ""),
            str(row.get("published_at") or ""),
            int(row.get("revision_sequence") or 0),
            str(row.get("revision_id") or ""),
        )
        if existing is None:
            selected[key] = row
            continue
        existing_key = (
            str(existing.get("available_date") or ""),
            str(existing.get("published_at") or ""),
            int(existing.get("revision_sequence") or 0),
            str(existing.get("revision_id") or ""),
        )
        if candidate_key > existing_key:
            selected[key] = row
    return sorted(
        selected.values(),
        key=lambda row: (
            str(row.get("effective_date") or ""),
            str(row.get("fact_code") or ""),
            str(row.get("fiscal_period") or ""),
        ),
    )


def query_market_data_as_of(
    db_path: Path,
    *,
    dataset: str,
    market: str | None,
    symbol: str | None,
    observation_date: date,
    information_cutoff: date,
) -> list[dict[str, Any]]:
    """Read a canonical dataset under the common point-in-time contract.

    A row is eligible only when its effective/observation date is no later
    than ``observation_date`` and its known/published date is no later than
    ``information_cutoff``.  Datasets without an available/published date are
    intentionally excluded here; they may remain useful for live descriptive
    reports, but cannot be presented as look-ahead-safe research input.
    """

    if dataset == "financial_facts":
        if not market or not symbol:
            raise ValueError("financial_facts PIT query requires market and symbol")
        return query_financial_facts_as_of(
            db_path,
            market=market,
            symbol=symbol,
            observation_date=observation_date,
            information_cutoff=information_cutoff,
        )
    table_by_dataset = {
        "daily_bars": ("daily_bars", "effective_date", "available_date"),
        "monthly_revenue": ("monthly_revenue", "revenue_month", "available_date"),
        "valuation_snapshots": ("valuation_snapshots", "trade_date", "available_date"),
    }
    if dataset not in table_by_dataset:
        raise ValueError(f"dataset has no PIT query contract: {dataset}")
    table, effective_column, available_column = table_by_dataset[dataset]
    conditions = [
        "validation_status = 'verified'" if table != "daily_bars" else "data_status = 'verified'",
        f"(({available_column} IS NOT NULL AND {available_column} != '' "
        f"AND substr({available_column}, 1, 10) <= ?) "
        f"OR (published_at IS NOT NULL AND substr(published_at, 1, 10) <= ?))",
    ]
    params: list[Any] = [information_cutoff.isoformat(), information_cutoff.isoformat()]
    if table == "daily_bars":
        conditions.append(f"{effective_column} <= ?")
        params.append(observation_date.isoformat())
    elif table == "valuation_snapshots":
        conditions.append(f"{effective_column} <= ?")
        params.append(observation_date.isoformat())
    if table == "monthly_revenue":
        # A retrieval date proves only that the payload was seen locally; it
        # does not prove when the issuer/exchange published the observation.
        # Such rows remain available for descriptive work but are not PIT-safe.
        conditions.append("COALESCE(availability_precision, 'unknown') != 'retrieval_date'")
    else:
        # TWSE/TPEx payloads commonly store ROC months such as ``11507``;
        # normalize them after retrieval instead of relying on lexical SQL
        # comparison between ROC and Gregorian strings.
        pass
    if market:
        conditions.append("market = ?")
        params.append(market)
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    with closing(_read_only_connect(Path(db_path))) as conn:
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE " + " AND ".join(conditions)
            + f" ORDER BY {effective_column}, available_date",
            params,
        ).fetchall()
    output = [dict(row) for row in rows]
    if table == "monthly_revenue":
        cutoff = (observation_date.year, observation_date.month)
        filtered: list[dict[str, Any]] = []
        for row in output:
            raw_month = str(row.get("revenue_month") or "").strip()
            digits = re.sub(r"[^0-9]", "", raw_month)
            try:
                if len(digits) == 5:
                    month_key = (int(digits[:3]) + 1911, int(digits[3:]))
                elif len(digits) >= 6:
                    month_key = (int(digits[:4]), int(digits[4:6]))
                else:
                    continue
            except ValueError:
                continue
            if month_key <= cutoff:
                filtered.append(row)
        return filtered
    return output


def _upsert_research_row(
    db_path: Path,
    *,
    table: str,
    key_columns: tuple[str, ...],
    values: dict[str, Any],
) -> None:
    """Upsert a row in one of the schema-v4 research tables.

    Table and column names are supplied only by module code, never by a
    caller-facing SQL string.  Keeping this helper private gives the public
    adapters small, typed-ish contracts while preserving one idempotent write
    path for all provenance-bearing datasets.
    """

    db_path = _ready_market_data_db(Path(db_path))
    prepared_values = dict(values)
    raw_payload = prepared_values.get("raw_payload_json")
    if isinstance(raw_payload, (dict, list)):
        prepared_values["raw_payload_json"] = json.dumps(
            raw_payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    columns = tuple(prepared_values)
    names = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f'"{column}" = excluded."{column}"'
        for column in columns
        if column not in key_columns
    )
    conflict = ", ".join(f'"{column}"' for column in key_columns)
    statement = (
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) '
        f'ON CONFLICT ({conflict}) DO UPDATE SET {updates}'
    )
    with closing(_connect(db_path)) as conn:
        conn.execute(statement, tuple(prepared_values[column] for column in columns))
        conn.commit()
    payload_id = values.get("source_payload_id")
    if payload_id:
        record_identity = "|".join(str(values[column]) for column in key_columns)
        link_source_payload(
            db_path,
            dataset_key=table,
            record_identity=record_identity,
            payload_id=str(payload_id),
        )


def _research_common_values(
    *,
    source_endpoint: str,
    source_url: str,
    source_payload_id: str | None,
    source_payload_sha256: str | None,
    fetched_at: str | None,
    published_at: str | None,
    validation_status: str,
    available_date: date | str | None = None,
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "source_endpoint": source_endpoint,
        "source_url": source_url,
        "source_payload_id": source_payload_id,
        "source_payload_sha256": source_payload_sha256,
        "fetched_at": fetched_at or _now_iso(),
        "available_date": _iso_value(available_date),
        "published_at": published_at,
        "validation_status": validation_status,
        "availability_precision": availability_precision or "unknown",
        "data_gap_reason": data_gap_reason,
    }


def upsert_market_session(
    db_path: Path,
    *,
    market: str,
    trade_date: date | str,
    is_open: bool,
    source_endpoint: str,
    source_url: str,
    session_type: str = "regular",
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="market_sessions",
        key_columns=("market", "trade_date"),
        values={
            "market": market,
            "trade_date": _iso_value(trade_date),
            "is_open": int(bool(is_open)),
            "session_type": session_type,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_security_trading_status(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    effective_date: date | str,
    status: str,
    source_endpoint: str,
    source_url: str,
    reason: str | None = None,
    limit_up: float | None = None,
    limit_down: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="security_trading_status",
        key_columns=("market", "symbol", "effective_date"),
        values={
            "market": market,
            "symbol": symbol,
            "effective_date": _iso_value(effective_date),
            "status": status,
            "reason": reason,
            "limit_up": limit_up,
            "limit_down": limit_down,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_adjustment_factor(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    effective_date: date | str,
    price_factor: float,
    action_hash: str,
    source_endpoint: str,
    source_url: str,
    cash_dividend: float = 0.0,
    stock_dividend_ratio: float | None = None,
    split_factor: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    if float(price_factor) <= 0:
        raise ValueError("price_factor must be positive")
    _upsert_research_row(
        db_path,
        table="adjustment_factors",
        key_columns=("market", "symbol", "effective_date"),
        values={
            "market": market,
            "symbol": symbol,
            "effective_date": _iso_value(effective_date),
            "price_factor": float(price_factor),
            "cash_dividend": float(cash_dividend),
            "stock_dividend_ratio": stock_dividend_ratio,
            "split_factor": split_factor,
            "action_hash": action_hash,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def get_adjusted_bars(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    price_mode: str = "total_return_backward",
    as_of: date | str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    db_path = Path(db_path)
    conditions = [
        "market = ?",
        "symbol = ?",
        "price_mode = ?",
        "data_status = 'derived'",
    ]
    params: list[Any] = [market, symbol, price_mode]
    if as_of is not None:
        conditions.append("trade_date <= ?")
        params.append(_iso_value(as_of))
    query = (
        "SELECT * FROM adjusted_bars WHERE "
        + " AND ".join(conditions)
        + " ORDER BY trade_date DESC"
    )
    if limit is not None:
        query += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_read_only_connect(db_path)) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in reversed(rows)]


def rebuild_adjusted_bars(
    db_path: Path,
    *,
    market: str | None = None,
    symbol: str | None = None,
    symbols: Iterable[tuple[str, str]] | None = None,
    price_mode: str = "total_return_backward",
    derivation_version: str = "daily-bars-actions-v1",
) -> dict[str, Any]:
    """Rebuild backward-adjusted bars from raw verified bars and factors.

    ``adjustment_factors.price_factor`` is an event factor.  An event dated
    ``D`` is applied to raw bars strictly before ``D``; bars on and after the
    ex-date retain factor 1 for that event.  The raw ``daily_bars`` table is
    never modified, so the derived series is reproducible from its inputs.
    """

    db_path = _ready_market_data_db(Path(db_path))
    bar_conditions = ["data_status = 'verified'"]
    factor_conditions: list[str] = []
    bar_params: list[Any] = []
    factor_params: list[Any] = []
    scoped_symbols = [
        (str(item[0]).strip(), str(item[1]).strip())
        for item in (symbols or [])
        if len(item) >= 2 and str(item[0]).strip() and str(item[1]).strip()
    ]
    if scoped_symbols:
        scope_sql = " OR ".join("(market = ? AND symbol = ?)" for _ in scoped_symbols)
        bar_conditions.append(f"({scope_sql})")
        factor_conditions.append(f"({scope_sql})")
        for scoped_market, scoped_symbol in scoped_symbols:
            bar_params.extend((scoped_market, scoped_symbol))
            factor_params.extend((scoped_market, scoped_symbol))
    if market:
        bar_conditions.append("market = ?")
        factor_conditions.append("market = ?")
        bar_params.append(market)
        factor_params.append(market)
    if symbol:
        bar_conditions.append("symbol = ?")
        factor_conditions.append("symbol = ?")
        bar_params.append(symbol)
        factor_params.append(symbol)
    with closing(_connect(db_path)) as conn:
        bars = conn.execute(
            "SELECT market, symbol, trade_date, open, high, low, close, volume "
            "FROM daily_bars WHERE " + " AND ".join(bar_conditions) +
            " ORDER BY market, symbol, trade_date",
            bar_params,
        ).fetchall()
        factors = conn.execute(
            "SELECT market, symbol, effective_date, price_factor "
            "FROM adjustment_factors WHERE validation_status = 'verified'"
            + ((" AND " + " AND ".join(factor_conditions)) if factor_conditions else "")
            + " ORDER BY market, symbol, effective_date",
            factor_params,
        ).fetchall()
        factor_map: dict[tuple[str, str], list[tuple[str, float]]] = {}
        for row in factors:
            factor_map.setdefault((str(row["market"]), str(row["symbol"])), []).append(
                (str(row["effective_date"]), float(row["price_factor"]))
            )
        input_hash = hashlib.sha256(
            json.dumps(
                {
                    "price_mode": price_mode,
                    "derivation_version": derivation_version,
                    "bars": [
                        {
                            "market": str(row["market"]),
                            "symbol": str(row["symbol"]),
                            "trade_date": str(row["trade_date"]),
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row["volume"]),
                        }
                        for row in bars
                    ],
                    "factors": [
                        {
                            "market": str(row["market"]),
                            "symbol": str(row["symbol"]),
                            "effective_date": str(row["effective_date"]),
                            "price_factor": float(row["price_factor"]),
                        }
                        for row in factors
                    ],
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        generated_at = _now_iso()
        latest_by_symbol: dict[tuple[str, str], str] = {}
        derived_rows: list[dict[str, Any]] = []
        for row in bars:
            key = (str(row["market"]), str(row["symbol"]))
            latest_by_symbol[key] = max(latest_by_symbol.get(key, ""), str(row["trade_date"]))
        for row in bars:
            key = (str(row["market"]), str(row["symbol"]))
            trade_date = str(row["trade_date"])
            factor = 1.0
            for effective_date, event_factor in factor_map.get(key, []):
                if trade_date < effective_date:
                    factor *= event_factor
            derived_rows.append(
                {
                    "market": key[0],
                    "symbol": key[1],
                    "trade_date": trade_date,
                    "price_mode": price_mode,
                    "open": float(row["open"]) * factor,
                    "high": float(row["high"]) * factor,
                    "low": float(row["low"]) * factor,
                    "close": float(row["close"]) * factor,
                    "volume": float(row["volume"]) / factor,
                    "adjustment_factor": factor,
                    "source_latest_trade_date": latest_by_symbol[key],
                    "available_date": latest_by_symbol[key],
                }
            )
            conn.execute(
                """
                INSERT INTO adjusted_bars(
                    market, symbol, trade_date, price_mode, open, high, low, close,
                    volume, adjustment_factor, derivation_version,
                    source_latest_trade_date, generated_at, available_date, data_status,
                    derivation_input_sha256
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'derived', ?)
                ON CONFLICT(market, symbol, trade_date, price_mode) DO UPDATE SET
                    open=excluded.open, high=excluded.high, low=excluded.low,
                    close=excluded.close, volume=excluded.volume,
                    adjustment_factor=excluded.adjustment_factor,
                    derivation_version=excluded.derivation_version,
                    source_latest_trade_date=excluded.source_latest_trade_date,
                    generated_at=excluded.generated_at,
                    available_date=excluded.available_date,
                    data_status=excluded.data_status,
                    derivation_input_sha256=excluded.derivation_input_sha256
                """,
                (
                    row["market"],
                    row["symbol"],
                    trade_date,
                    price_mode,
                    float(row["open"]) * factor,
                    float(row["high"]) * factor,
                    float(row["low"]) * factor,
                    float(row["close"]) * factor,
                    float(row["volume"]) / factor,
                    factor,
                    derivation_version,
                    latest_by_symbol[key],
                    generated_at,
                    latest_by_symbol[key],
                    input_hash,
                ),
            )
        conn.commit()
    series_hash = hashlib.sha256(
        json.dumps(derived_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "rows_read": len(bars),
        "bars_upserted": len(bars),
        "symbols": len(latest_by_symbol),
        "price_mode": price_mode,
        "derivation_version": derivation_version,
        "derivation_input_sha256": input_hash,
        "derived_series_sha256": series_hash,
    }


def upsert_security_lifecycle(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    effective_from: date | str,
    status: str,
    source_endpoint: str,
    source_url: str,
    effective_to: date | str | None = None,
    name: str | None = None,
    industry: str | None = None,
    listing_date: date | str | None = None,
    delisting_date: date | str | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="security_lifecycle",
        key_columns=("market", "symbol", "effective_from"),
        values={
            "market": market,
            "symbol": symbol,
            "effective_from": _iso_value(effective_from),
            "effective_to": _iso_value(effective_to),
            "status": status,
            "name": name,
            "industry": industry,
            "listing_date": _iso_value(listing_date),
            "delisting_date": _iso_value(delisting_date),
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_benchmark_membership(
    db_path: Path,
    *,
    benchmark_code: str,
    symbol: str,
    effective_from: date | str,
    source_endpoint: str,
    source_url: str,
    market: str | None = None,
    effective_to: date | str | None = None,
    weight: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="benchmark_membership",
        key_columns=("benchmark_code", "symbol", "effective_from"),
        values={
            "benchmark_code": benchmark_code,
            "market": market,
            "symbol": symbol,
            "effective_from": _iso_value(effective_from),
            "effective_to": _iso_value(effective_to),
            "weight": weight,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_daily_market_stats(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    trade_date: date | str,
    source_endpoint: str,
    source_url: str,
    trade_value: float | None = None,
    transaction_count: int | None = None,
    turnover_rate: float | None = None,
    market_cap: float | None = None,
    shares_outstanding: float | None = None,
    free_float_shares: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="daily_market_stats",
        key_columns=("market", "symbol", "trade_date"),
        values={
            "market": market,
            "symbol": symbol,
            "trade_date": _iso_value(trade_date),
            "trade_value": trade_value,
            "transaction_count": transaction_count,
            "turnover_rate": turnover_rate,
            "market_cap": market_cap,
            "shares_outstanding": shares_outstanding,
            "free_float_shares": free_float_shares,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_institutional_flow(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    trade_date: date | str,
    source_endpoint: str,
    source_url: str,
    foreign_buy: float | None = None,
    foreign_sell: float | None = None,
    foreign_net: float | None = None,
    trust_buy: float | None = None,
    trust_sell: float | None = None,
    trust_net: float | None = None,
    dealer_buy: float | None = None,
    dealer_sell: float | None = None,
    dealer_net: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="institutional_flows",
        key_columns=("market", "symbol", "trade_date"),
        values={
            "market": market,
            "symbol": symbol,
            "trade_date": _iso_value(trade_date),
            "foreign_buy": foreign_buy,
            "foreign_sell": foreign_sell,
            "foreign_net": foreign_net,
            "trust_buy": trust_buy,
            "trust_sell": trust_sell,
            "trust_net": trust_net,
            "dealer_buy": dealer_buy,
            "dealer_sell": dealer_sell,
            "dealer_net": dealer_net,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_margin_short_snapshot(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    trade_date: date | str,
    source_endpoint: str,
    source_url: str,
    margin_buy: float | None = None,
    margin_sell: float | None = None,
    margin_balance: float | None = None,
    short_buy: float | None = None,
    short_sell: float | None = None,
    short_balance: float | None = None,
    sbl_balance: float | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    _upsert_research_row(
        db_path,
        table="margin_short_snapshots",
        key_columns=("market", "symbol", "trade_date"),
        values={
            "market": market,
            "symbol": symbol,
            "trade_date": _iso_value(trade_date),
            "margin_buy": margin_buy,
            "margin_sell": margin_sell,
            "margin_balance": margin_balance,
            "short_buy": short_buy,
            "short_sell": short_sell,
            "short_balance": short_balance,
            "sbl_balance": sbl_balance,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
        },
    )


def upsert_market_event(
    db_path: Path,
    *,
    event_id: str,
    market: str,
    event_type: str,
    source_endpoint: str,
    source_url: str,
    symbol: str | None = None,
    effective_date: date | str | None = None,
    announced_at: str | None = None,
    published_at: str | None = None,
    title: str | None = None,
    source_payload_id: str | None = None,
    source_payload_sha256: str | None = None,
    fetched_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
    raw_payload_json: str | dict[str, Any] | None = None,
) -> None:
    if isinstance(raw_payload_json, dict):
        raw_payload_json = json.dumps(raw_payload_json, ensure_ascii=False, sort_keys=True)
    _upsert_research_row(
        db_path,
        table="market_events",
        key_columns=("event_id",),
        values={
            "event_id": event_id,
            "market": market,
            "symbol": symbol,
            "event_type": event_type,
            "effective_date": _iso_value(effective_date),
            "announced_at": announced_at,
            "title": title,
            **_research_common_values(
                source_endpoint=source_endpoint,
                source_url=source_url,
                source_payload_id=source_payload_id,
                source_payload_sha256=source_payload_sha256,
                fetched_at=fetched_at,
                published_at=published_at,
                available_date=available_date,
                validation_status=validation_status,
                availability_precision=availability_precision,
                data_gap_reason=data_gap_reason,
            ),
            "raw_payload_json": raw_payload_json,
        },
    )


def get_latest_security_master(
    db_path: Path,
    *,
    as_of: date,
    symbols: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    db_path = Path(db_path)
    conditions = ["effective_date <= ?", "validation_status = 'verified'"]
    params: list[Any] = [as_of.isoformat()]
    symbol_list = [str(item).strip() for item in symbols or [] if str(item).strip()]
    if symbol_list:
        placeholders = ", ".join("?" for _ in symbol_list)
        conditions.append(f"symbol IN ({placeholders})")
        params.extend(symbol_list)
    with closing(_read_only_connect(db_path)) as conn:
        rows = conn.execute(
            "SELECT * FROM security_master_snapshots WHERE "
            + " AND ".join(conditions)
            + " ORDER BY symbol, effective_date DESC",
            params,
        ).fetchall()
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        output.setdefault(str(row["symbol"]), dict(row))
    return output


def get_latest_monthly_revenue(
    db_path: Path,
    *,
    as_of: date,
    symbols: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    db_path = Path(db_path)
    conditions = [
        "validation_status = 'verified'",
        "available_date IS NOT NULL",
        "available_date <= ?",
    ]
    params: list[Any] = [as_of.isoformat()]
    symbol_list = [str(item).strip() for item in symbols or [] if str(item).strip()]
    if symbol_list:
        placeholders = ", ".join("?" for _ in symbol_list)
        conditions.append(f"symbol IN ({placeholders})")
        params.extend(symbol_list)
    with closing(_read_only_connect(db_path)) as conn:
        rows = conn.execute(
            "SELECT * FROM monthly_revenue WHERE "
            + " AND ".join(conditions)
            + " ORDER BY symbol, revenue_month DESC, available_date DESC",
            params,
        ).fetchall()
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        output.setdefault(str(row["symbol"]), dict(row))
    return output


def get_monthly_revenue_history(
    db_path: Path,
    *,
    market: str | None = None,
    symbol: str | None = None,
    as_of: date | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Read verified monthly revenue rows without mutating the database.

    ``as_of`` is an information cutoff, not an observation-date filter: a row
    is eligible only when its explicit ``available_date`` is known and no
    later than the cutoff.  Rows imported from legacy stores with no
    availability date stay out of this PIT-oriented history API.
    """

    conditions = [
        "validation_status = 'verified'",
        "available_date IS NOT NULL",
    ]
    params: list[Any] = []
    if market is not None:
        conditions.append("market = ?")
        params.append(str(market))
    if symbol is not None:
        conditions.append("symbol = ?")
        params.append(str(symbol))
    if as_of is not None:
        conditions.append("available_date <= ?")
        params.append(as_of.isoformat())
    statement = (
        "SELECT * FROM monthly_revenue WHERE "
        + " AND ".join(conditions)
        + " ORDER BY market, symbol, revenue_month"
    )
    if limit is not None:
        statement += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_read_only_connect(Path(db_path))) as conn:
        rows = conn.execute(statement, params).fetchall()
    return [dict(row) for row in rows]


def get_valuation_snapshot_as_of(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    as_of: date,
    max_backtrack_days: int = 20,
) -> dict[str, Any] | None:
    db_path = Path(db_path)
    earliest = as_of.toordinal() - max(int(max_backtrack_days), 0)
    earliest_date = date.fromordinal(earliest).isoformat()
    with closing(_read_only_connect(db_path)) as conn:
        row = conn.execute(
            """
            SELECT * FROM valuation_snapshots
            WHERE market = ? AND symbol = ?
              AND trade_date BETWEEN ? AND ?
              AND validation_status = 'verified'
              AND (available_date IS NULL OR available_date <= ?)
            ORDER BY trade_date DESC, COALESCE(available_date, '') DESC
            LIMIT 1
            """,
            (market, symbol, earliest_date, as_of.isoformat(), as_of.isoformat()),
        ).fetchone()
    return dict(row) if row is not None else None


def get_valuation_history(
    db_path: Path,
    *,
    market: str | None = None,
    symbol: str | None = None,
    as_of: date | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Read verified valuation snapshots using their real observation date."""

    conditions = ["validation_status = 'verified'"]
    params: list[Any] = []
    if market is not None:
        conditions.append("market = ?")
        params.append(str(market))
    if symbol is not None:
        conditions.append("symbol = ?")
        params.append(str(symbol))
    if as_of is not None:
        conditions.append("trade_date <= ?")
        params.append(as_of.isoformat())
    statement = (
        "SELECT * FROM valuation_snapshots WHERE "
        + " AND ".join(conditions)
        + " ORDER BY market, symbol, trade_date"
    )
    if limit is not None:
        statement += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_read_only_connect(Path(db_path))) as conn:
        rows = conn.execute(statement, params).fetchall()
    return [dict(row) for row in rows]


def get_financial_facts_history(
    db_path: Path,
    *,
    market: str | None = None,
    symbol: str | None = None,
    information_cutoff: date | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Read verified facts, retaining every revision for replay/audit."""

    conditions = ["validation_status = 'verified'"]
    params: list[Any] = []
    if market is not None:
        conditions.append("market = ?")
        params.append(str(market))
    if symbol is not None:
        conditions.append("symbol = ?")
        params.append(str(symbol))
    if information_cutoff is not None:
        conditions.append("available_date <= ?")
        params.append(information_cutoff.isoformat())
    statement = (
        "SELECT * FROM financial_fact_observations WHERE "
        + " AND ".join(conditions)
        + " ORDER BY market, symbol, fact_code, fiscal_period, available_date, revision_sequence"
    )
    if limit is not None:
        statement += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_read_only_connect(Path(db_path))) as conn:
        rows = conn.execute(statement, params).fetchall()
    return [dict(row) for row in rows]


def _copy_table_rows(
    conn: sqlite3.Connection,
    source_schema: str,
    table: str,
) -> int:
    target_columns = _columns(conn, table)
    source_columns = _columns(conn, table, source_schema)
    common = [column for column in target_columns if column in source_columns]
    if not common:
        return 0
    names = ", ".join(f'"{column}"' for column in common)
    before = conn.total_changes
    conn.execute(
        f'INSERT OR IGNORE INTO "{table}" ({names}) '
        f'SELECT {names} FROM "{source_schema}"."{table}"'
    )
    return int(conn.total_changes - before)


def migrate_legacy_databases(
    db_path: Path,
    *,
    daily_source: Path | None = None,
    quarterly_source: Path | None = None,
) -> dict[str, Any]:
    """Idempotently copy legacy stores into the canonical database.

    Source files are never removed.  INSERT OR IGNORE preserves an already
    selected canonical source while allowing a later retry to fill missing
    rows.
    """

    db_path = init_market_data_db(Path(db_path))
    sources = [("legacy_daily", daily_source), ("legacy_quarterly", quarterly_source)]
    summary: dict[str, Any] = {"database_path": str(db_path), "sources": {}, "inserted_rows": 0}
    tables = [
        "daily_bars",
        "daily_bar_sources",
        "daily_bar_sync_state",
        "daily_bar_import_runs",
        "daily_bar_import_issues",
        "quarterly_company_fundamentals",
        "quarterly_symbol_latest",
        "quarterly_refresh_runs",
        "quarterly_backfill_queue",
        "quarterly_backfill_runs",
        "schema_meta",
    ]
    with closing(_connect(db_path)) as conn:
        for alias, source in sources:
            if source is None:
                continue
            source = Path(source)
            if not source.exists() or source.resolve() == db_path.resolve():
                summary["sources"][alias] = {"path": str(source), "exists": source.exists(), "inserted": {}}
                continue
            fingerprint = _legacy_source_fingerprint(source)
            marker = conn.execute(
                "SELECT value FROM schema_meta WHERE key = ?",
                (f"legacy_migration_{alias}_fingerprint",),
            ).fetchone()
            if marker is not None and str(marker[0]) == fingerprint:
                summary["sources"][alias] = {
                    "path": str(source),
                    "exists": True,
                    "skipped": True,
                    "reason": "source_fingerprint_unchanged",
                    "fingerprint": fingerprint,
                    "inserted": {},
                }
                continue
            conn.execute(f"ATTACH DATABASE ? AS {alias}", (str(source),))
            inserted: dict[str, int] = {}
            try:
                for table in tables:
                    if not _columns(conn, table) or not _columns(conn, table, alias):
                        continue
                    inserted[table] = _copy_table_rows(conn, alias, table)
            finally:
                # SQLite cannot detach a database while the INSERT transaction
                # that read it is still open.
                conn.commit()
                conn.execute(f"DETACH DATABASE {alias}")
            _meta(conn, f"legacy_migration_{alias}_fingerprint", fingerprint)
            summary["sources"][alias] = {
                "path": str(source),
                "exists": True,
                "skipped": False,
                "fingerprint": fingerprint,
                "inserted": inserted,
            }
            summary["inserted_rows"] += sum(inserted.values())
        _meta(conn, "legacy_migration_v1", _now_iso())
        conn.commit()
    # Legacy issue rows may predate the fingerprint column; re-run the daily
    # schema upgrader after copying so their identities are populated too.
    init_daily_db(db_path)
    return summary


def backfill_sync_state_from_canonical(db_path: Path) -> dict[str, Any]:
    """Reconstruct incremental checkpoints from rows already in canonical tables.

    A reconstructed checkpoint is deliberately labelled ``migrated`` rather
    than ``verified``: it describes the stored range, but does not make a
    historical import a fresh current-day source verification.
    """

    db_path = init_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        marker = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'market_data_sync_state_backfill_v4'"
        ).fetchone()
        if marker is not None:
            return {"status": "already_backfilled", "rows_inserted": 0}

        now = _now_iso()
        statements = (
            (
                "daily_bars",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'daily_bars', market, symbol, '', MIN(trade_date), MAX(trade_date),
                       COUNT(*), NULL, 'migrated', NULL, ?
                FROM daily_bars
                GROUP BY market, symbol
                """,
            ),
            (
                "index_bars",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'index_bars', '', index_code, '', MIN(trade_date), MAX(trade_date),
                       COUNT(*), NULL, 'migrated', NULL, ?
                FROM index_bars
                GROUP BY index_code
                """,
            ),
            (
                "period_bars",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'period_bars', market, symbol, frequency,
                       MIN(period_start), MAX(period_end), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM period_bars
                GROUP BY market, symbol, frequency
                """,
            ),
            (
                "security_master",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'security_master', market, symbol, '', MIN(effective_date),
                       MAX(effective_date), COUNT(*), NULL, 'migrated', NULL, ?
                FROM security_master_snapshots
                GROUP BY market, symbol
                """,
            ),
            (
                "monthly_revenue",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'monthly_revenue', market, symbol, '',
                       MIN(COALESCE(NULLIF(available_date, ''), revenue_month || '-01')),
                       MAX(COALESCE(NULLIF(available_date, ''), revenue_month || '-01')),
                       COUNT(*), NULL, 'migrated', NULL, ?
                FROM monthly_revenue
                GROUP BY market, symbol
                """,
            ),
            (
                "valuation_snapshots",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'valuation_snapshots', market, symbol, '', MIN(trade_date),
                       MAX(trade_date), COUNT(*), NULL, 'migrated', NULL, ?
                FROM valuation_snapshots
                GROUP BY market, symbol
                """,
            ),
            (
                "quarterly_fundamentals",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'quarterly_fundamentals', market, symbol,
                       COALESCE(dataset_key, ''), MIN(as_of_date), MAX(as_of_date),
                       COUNT(*), NULL, 'migrated', NULL, ?
                FROM quarterly_company_fundamentals
                GROUP BY market, symbol, COALESCE(dataset_key, '')
                """,
            ),
            (
                "annual_fundamentals",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'annual_fundamentals', market, symbol, fiscal_year,
                       MIN(available_date), MAX(available_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM annual_company_fundamentals
                GROUP BY market, symbol, fiscal_year
                """,
            ),
            (
                "corporate_actions",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'corporate_actions', market, symbol, action_type,
                       MIN(action_date), MAX(action_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM corporate_actions
                GROUP BY market, symbol, action_type
                """,
            ),
            (
                "financial_facts",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'financial_facts', market, symbol, fact_code,
                       MIN(effective_date), MAX(effective_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM financial_fact_observations
                GROUP BY market, symbol, fact_code
                """,
            ),
            (
                "market_sessions",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'market_sessions', market, '', '', MIN(trade_date),
                       MAX(trade_date), COUNT(*), NULL, 'migrated', NULL, ?
                FROM market_sessions
                GROUP BY market
                """,
            ),
            (
                "security_trading_status",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'security_trading_status', market, symbol, '',
                       MIN(effective_date), MAX(effective_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM security_trading_status
                GROUP BY market, symbol
                """,
            ),
            (
                "adjustment_factors",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'adjustment_factors', market, symbol, '',
                       MIN(effective_date), MAX(effective_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM adjustment_factors
                GROUP BY market, symbol
                """,
            ),
            (
                "adjusted_bars",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'adjusted_bars', market, symbol, price_mode,
                       MIN(trade_date), MAX(trade_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM adjusted_bars
                GROUP BY market, symbol, price_mode
                """,
            ),
            (
                "security_lifecycle",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'security_lifecycle', market, symbol, '',
                       MIN(effective_from), MAX(COALESCE(effective_to, effective_from)),
                       COUNT(*), NULL, 'migrated', NULL, ?
                FROM security_lifecycle
                GROUP BY market, symbol
                """,
            ),
            (
                "benchmark_membership",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'benchmark_membership', COALESCE(market, ''), symbol,
                       benchmark_code, MIN(effective_from),
                       MAX(COALESCE(effective_to, effective_from)), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM benchmark_membership
                GROUP BY COALESCE(market, ''), symbol, benchmark_code
                """,
            ),
            (
                "daily_market_stats",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'daily_market_stats', market, symbol, '',
                       MIN(trade_date), MAX(trade_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM daily_market_stats
                GROUP BY market, symbol
                """,
            ),
            (
                "institutional_flows",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'institutional_flows', market, symbol, '',
                       MIN(trade_date), MAX(trade_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM institutional_flows
                GROUP BY market, symbol
                """,
            ),
            (
                "margin_short_snapshots",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'margin_short_snapshots', market, symbol, '',
                       MIN(trade_date), MAX(trade_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM margin_short_snapshots
                GROUP BY market, symbol
                """,
            ),
            (
                "market_events",
                """
                INSERT OR IGNORE INTO market_data_sync_state(
                    dataset_key, market, symbol, partition_key,
                    first_effective_date, last_effective_date,
                    verified_row_count, last_verified_at, last_status,
                    last_error, updated_at
                )
                SELECT 'market_events', market, COALESCE(symbol, ''), event_type,
                       MIN(effective_date), MAX(effective_date), COUNT(*), NULL,
                       'migrated', NULL, ?
                FROM market_events
                GROUP BY market, COALESCE(symbol, ''), event_type
                """,
            ),
            (
                "partition_state_migrated",
                """
                INSERT OR IGNORE INTO market_data_partition_state(
                    dataset_key, market, symbol, partition_key,
                    requested_from, requested_to, request_method, request_url,
                    first_effective_date, last_effective_date, row_count, status,
                    last_verified_at, gap_reason, updated_at
                )
                SELECT 'daily_bars', market, symbol, substr(trade_date, 1, 7),
                       MIN(trade_date), MAX(trade_date), 'migrated',
                       'migration://daily_bars', MIN(trade_date), MAX(trade_date),
                       COUNT(*), 'migrated', NULL, 'legacy_import', ?
                FROM daily_bars
                WHERE data_status = 'verified'
                GROUP BY market, symbol, substr(trade_date, 1, 7)
                """,
            ),
            (
                "partition_state_revenue_migrated",
                """
                INSERT OR IGNORE INTO market_data_partition_state(
                    dataset_key, market, symbol, partition_key,
                    requested_from, requested_to, request_method, request_url,
                    first_effective_date, last_effective_date, row_count, status,
                    last_verified_at, gap_reason, updated_at
                )
                SELECT 'monthly_revenue', market, symbol, revenue_month,
                       revenue_month || '-01', revenue_month || '-01', 'migration',
                       'migration://monthly_revenue', revenue_month || '-01',
                       revenue_month || '-01', COUNT(*), 'migrated', NULL,
                       'legacy_import', ?
                FROM monthly_revenue
                GROUP BY market, symbol, revenue_month
                """,
            ),
            (
                "partition_state_valuation_migrated",
                """
                INSERT OR IGNORE INTO market_data_partition_state(
                    dataset_key, market, symbol, partition_key,
                    requested_from, requested_to, request_method, request_url,
                    first_effective_date, last_effective_date, row_count, status,
                    last_verified_at, gap_reason, updated_at
                )
                SELECT 'valuation_snapshots', market, symbol, substr(trade_date, 1, 7),
                       MIN(trade_date), MAX(trade_date), 'migration',
                       'migration://valuation_snapshots', MIN(trade_date),
                       MAX(trade_date), COUNT(*), 'migrated', NULL,
                       'legacy_import', ?
                FROM valuation_snapshots
                GROUP BY market, symbol, substr(trade_date, 1, 7)
                """,
            ),
        )
        inserted = 0
        by_dataset: dict[str, int] = {}
        for dataset_key, statement in statements:
            before = conn.total_changes
            conn.execute(statement, (now,))
            delta = int(conn.total_changes - before)
            by_dataset[dataset_key] = delta
            inserted += delta
        _meta(conn, "market_data_sync_state_backfill_v1", now)
        _meta(conn, "market_data_sync_state_backfill_v2", now)
        _meta(conn, "market_data_sync_state_backfill_v3", now)
        _meta(conn, "market_data_sync_state_backfill_v4", now)
        conn.commit()
    return {"status": "backfilled", "rows_inserted": inserted, "by_dataset": by_dataset}


def ensure_market_data_db(
    db_path: Path,
    *,
    daily_source: Path | None = None,
    quarterly_source: Path | None = None,
) -> dict[str, Any]:
    """Initialise the canonical DB and replay available legacy migrations."""

    result = migrate_legacy_databases(
        db_path,
        daily_source=daily_source,
        quarterly_source=quarterly_source,
    )
    # A full period rebuild is needed once after legacy import (or when a
    # canonical DB has no derived rows).  Normal provider startup skips that
    # scan; the daily provider rebuilds only the affected symbol after an
    # incremental append.
    should_rebuild_periods = False
    with closing(sqlite3.connect(Path(db_path))) as conn:
        daily_count = int(conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0])
        period_count = int(conn.execute("SELECT COUNT(*) FROM period_bars").fetchone()[0])
        marker = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'period_derivation_initialized_v1'"
        ).fetchone()
        should_rebuild_periods = bool(daily_count and (period_count == 0 or marker is None or result.get("inserted_rows", 0)))
    result["period_bars"] = (
        rebuild_period_bars(Path(db_path))
        if should_rebuild_periods
        else {"status": "skipped", "reason": "derived_periods_current"}
    )
    if should_rebuild_periods:
        with closing(_connect(Path(db_path))) as conn:
            _meta(conn, "period_derivation_initialized_v1", _now_iso())
            conn.commit()
    result["sync_state"] = backfill_sync_state_from_canonical(Path(db_path))
    result["integrity"] = database_integrity(Path(db_path))
    return result


def _period_identity(day: date, frequency: str) -> tuple[str, date, date]:
    if frequency == "W":
        iso = day.isocalendar()
        period_key = f"{iso.year}-W{iso.week:02d}"
        start = day.fromisocalendar(iso.year, iso.week, 1)
        end = day.fromisocalendar(iso.year, iso.week, 7)
        return period_key, start, end
    if frequency == "M":
        period_key = f"{day.year:04d}-{day.month:02d}"
        start = date(day.year, day.month, 1)
        next_month = date(day.year + (day.month == 12), 1 if day.month == 12 else day.month + 1, 1)
        return period_key, start, next_month.fromordinal(next_month.toordinal() - 1)
    if frequency == "Q":
        quarter = ((day.month - 1) // 3) + 1
        start_month = (quarter - 1) * 3 + 1
        start = date(day.year, start_month, 1)
        next_quarter = date(day.year + (quarter == 4), 1 if quarter == 4 else start_month + 3, 1)
        return f"{day.year:04d}-Q{quarter}", start, next_quarter.fromordinal(next_quarter.toordinal() - 1)
    if frequency == "Y":
        return str(day.year), date(day.year, 1, 1), date(day.year, 12, 31)
    raise ValueError(f"unsupported period frequency: {frequency}")


def rebuild_period_bars(
    db_path: Path,
    *,
    market: str | None = None,
    symbol: str | None = None,
    symbols: Iterable[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Derive weekly/monthly/quarterly/yearly price bars from verified daily bars."""

    db_path = init_market_data_db(Path(db_path))
    conditions = ["data_status = 'verified'"]
    params: list[Any] = []
    scoped_symbols = [
        (str(item[0]).strip(), str(item[1]).strip())
        for item in (symbols or [])
        if len(item) >= 2 and str(item[0]).strip() and str(item[1]).strip()
    ]
    if scoped_symbols:
        conditions.append(
            "(" + " OR ".join("(market = ? AND symbol = ?)" for _ in scoped_symbols) + ")"
        )
        for scoped_market, scoped_symbol in scoped_symbols:
            params.extend((scoped_market, scoped_symbol))
    if market:
        conditions.append("market = ?")
        params.append(market)
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(
            "SELECT market, symbol, trade_date, open, high, low, close, volume "
            "FROM daily_bars WHERE " + " AND ".join(conditions) + " ORDER BY market, symbol, trade_date",
            params,
        ).fetchall()
        if not rows:
            return {"rows_read": 0, "period_rows_upserted": 0, "symbols": 0}
        grouped: dict[tuple[str, str, str, str], list[sqlite3.Row]] = {}
        for row in rows:
            day = date.fromisoformat(str(row["trade_date"]))
            for frequency in ("W", "M", "Q", "Y"):
                period_key, _, _ = _period_identity(day, frequency)
                grouped.setdefault((str(row["market"]), str(row["symbol"]), frequency, period_key), []).append(row)
        generated_at = _now_iso()
        for (row_market, row_symbol, frequency, period_key), period_rows in grouped.items():
            first = period_rows[0]
            last = period_rows[-1]
            first_day = date.fromisoformat(str(first["trade_date"]))
            last_day = date.fromisoformat(str(last["trade_date"]))
            _, period_start, period_end = _period_identity(first_day, frequency)
            conn.execute(
                """
                INSERT INTO period_bars(
                    market, symbol, frequency, period_key, period_start, period_end,
                    open, high, low, close, volume, trading_day_count,
                    source_latest_trade_date, derivation_version, generated_at, available_date, data_status,
                    availability_precision, data_gap_reason
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'derived',
                    'derived_from_daily', NULL)
                ON CONFLICT(market, symbol, frequency, period_key) DO UPDATE SET
                    period_start=excluded.period_start,
                    period_end=excluded.period_end,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    trading_day_count=excluded.trading_day_count,
                    source_latest_trade_date=excluded.source_latest_trade_date,
                    derivation_version=excluded.derivation_version,
                    generated_at=excluded.generated_at,
                    available_date=excluded.available_date,
                    data_status=excluded.data_status,
                    availability_precision=excluded.availability_precision,
                    data_gap_reason=excluded.data_gap_reason
                """,
                (
                    row_market,
                    row_symbol,
                    frequency,
                    period_key,
                    period_start.isoformat(),
                    period_end.isoformat(),
                    float(first["open"]),
                    max(float(item["high"]) for item in period_rows),
                    min(float(item["low"]) for item in period_rows),
                    float(last["close"]),
                    sum(float(item["volume"]) for item in period_rows),
                    len(period_rows),
                    last_day.isoformat(),
                    PERIOD_DERIVATION_VERSION,
                    generated_at,
                    last_day.isoformat(),
                ),
            )
        conn.commit()
    return {
        "rows_read": len(rows),
        "period_rows_upserted": len(grouped),
        "symbols": len({(str(row["market"]), str(row["symbol"])) for row in rows}),
    }


def record_source_payload(
    db_path: Path,
    *,
    dataset_key: str,
    request_method: str,
    source_endpoint: str,
    source_url: str,
    payload: Any,
    fetched_at: str | None = None,
    effective_date: str | None = None,
    available_date: str | None = None,
    published_at: str | None = None,
    cache_file: str | None = None,
    validation_status: str = "unvalidated",
    validation_error: str | None = None,
    availability_precision: str = "unknown",
    request_body_sha256: str | None = None,
    raw_storage_root: Path | None = None,
    inline_limit_bytes: int = SOURCE_PAYLOAD_INLINE_LIMIT_BYTES,
) -> str:
    db_path = _ready_market_data_db(Path(db_path))
    force_external = False
    content_encoding = "utf-8"
    if isinstance(payload, (bytes, bytearray)):
        raw_bytes = bytes(payload)
        raw = ""
        force_external = True
        content_encoding = "binary"
    elif isinstance(payload, str):
        raw = payload
        raw_bytes = raw.encode("utf-8")
        try:
            json.loads(payload)
        except json.JSONDecodeError:
            force_external = True
        else:
            content_encoding = "utf-8"
    else:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        raw_bytes = raw.encode("utf-8")
    payload_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    payload_id = hashlib.sha256(
        f"{dataset_key}|{request_method}|{source_url}|{payload_sha256}".encode("utf-8")
    ).hexdigest()
    storage_mode = "inline"
    storage_uri: str | None = None
    stored_raw = raw
    if force_external or len(raw_bytes) > max(int(inline_limit_bytes), 0):
        storage_mode = "external"
        storage_root = Path(raw_storage_root) if raw_storage_root else Path(db_path).parent / "raw_payloads"
        suffix = ".bin" if content_encoding == "binary" else ".payload"
        target = storage_root / _safe_dataset_path(dataset_key) / f"{payload_sha256}{suffix}"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            fd, temp_name = tempfile.mkstemp(prefix=f"{payload_sha256}.", suffix=".tmp", dir=target.parent)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(raw_bytes)
                os.replace(temp_name, target)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
        storage_uri = str(target)
        stored_raw = json.dumps(
            {
                "storage_mode": storage_mode,
                "storage_uri": storage_uri,
                "payload_sha256": payload_sha256,
                "byte_size": len(raw_bytes),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO source_payloads(
                payload_id, dataset_key, request_method, source_endpoint, source_url,
                request_body_sha256, payload_sha256, effective_date, published_at,
                available_date, fetched_at, cache_file, validation_status, validation_error, raw_payload_json,
                storage_mode, storage_uri, byte_size, content_encoding, availability_precision
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(payload_id) DO UPDATE SET
                source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url,
                effective_date=excluded.effective_date,
                available_date=excluded.available_date,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                cache_file=excluded.cache_file,
                validation_status=excluded.validation_status,
                validation_error=excluded.validation_error,
                raw_payload_json=excluded.raw_payload_json,
                storage_mode=excluded.storage_mode,
                storage_uri=excluded.storage_uri,
                byte_size=excluded.byte_size,
                content_encoding=excluded.content_encoding,
                availability_precision=excluded.availability_precision
            """,
            (
                payload_id,
                dataset_key,
                request_method.upper(),
                source_endpoint,
                source_url,
                request_body_sha256,
                payload_sha256,
                effective_date,
                available_date,
                published_at,
                fetched_at or _now_iso(),
                cache_file,
                validation_status,
                validation_error,
                stored_raw,
                storage_mode,
                storage_uri,
                len(raw_bytes),
                content_encoding,
                str(availability_precision or "unknown"),
            ),
        )
        conn.commit()
    return payload_id


def update_source_payload_validation(
    db_path: Path,
    *,
    payload_id: str,
    validation_status: str,
    validation_error: str | None = None,
) -> None:
    """Finalize the validation state of an immutable payload envelope."""

    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        cursor = conn.execute(
            """
            UPDATE source_payloads
            SET validation_status = ?, validation_error = ?
            WHERE payload_id = ?
            """,
            (validation_status, validation_error, payload_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"unknown source payload: {payload_id}")
        conn.commit()


def upsert_index_bars(db_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    db_path = _ready_market_data_db(Path(db_path))
    count = 0
    with closing(_connect(db_path)) as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO index_bars(
                index_code, trade_date, close, change_points, source_endpoint,
                source_url, source_payload_sha256, source_payload_id, fetched_at,
                    available_date, published_at, data_status, availability_precision, data_gap_reason
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(index_code, trade_date) DO UPDATE SET
                    close=excluded.close,
                    change_points=excluded.change_points,
                    source_endpoint=excluded.source_endpoint,
                    source_url=excluded.source_url,
                    source_payload_sha256=excluded.source_payload_sha256,
                    source_payload_id=excluded.source_payload_id,
                    fetched_at=excluded.fetched_at,
                    available_date=excluded.available_date,
                    published_at=excluded.published_at,
                    data_status=excluded.data_status,
                    availability_precision=excluded.availability_precision,
                    data_gap_reason=excluded.data_gap_reason
                """,
                (
                    str(row["index_code"]),
                    str(row["trade_date"]),
                    float(row["close"]),
                    row.get("change_points"),
                    str(row.get("source_endpoint") or ""),
                    str(row.get("source_url") or ""),
                    str(row.get("source_payload_sha256") or ""),
                    row.get("source_payload_id"),
                    str(row.get("fetched_at") or _now_iso()),
                    row.get("available_date"),
                    row.get("published_at"),
                    str(row.get("data_status") or "verified"),
                    str(row.get("availability_precision") or "unknown"),
                    row.get("data_gap_reason"),
                ),
            )
            if row.get("source_payload_id"):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO market_data_source_links(
                        dataset_key, record_identity, payload_id, linked_at
                    ) VALUES('index_bars', ?, ?, ?)
                    """,
                    (
                        f"{row['index_code']}|{row['trade_date']}",
                        str(row["source_payload_id"]),
                        _now_iso(),
                    ),
                )
            count += 1
        conn.commit()
    return count


def get_index_bars(
    db_path: Path,
    *,
    index_code: str,
    as_of: date | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    db_path = _ready_market_data_db(Path(db_path))
    conditions = ["index_code = ?", "data_status = 'verified'"]
    params: list[Any] = [index_code]
    if as_of is not None:
        conditions.append("trade_date <= ?")
        params.append(as_of.isoformat())
    query = "SELECT * FROM index_bars WHERE " + " AND ".join(conditions) + " ORDER BY trade_date DESC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in reversed(rows)]


def upsert_security_master(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    effective_date: date,
    name: str | None,
    industry: str | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO security_master_snapshots(
                market, symbol, effective_date, name, industry, source_endpoint,
                source_url, source_payload_sha256, source_payload_id, fetched_at,
                available_date, published_at, validation_status, availability_precision, data_gap_reason
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market, symbol, effective_date) DO UPDATE SET
                name=excluded.name, industry=excluded.industry,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256,
                source_payload_id=excluded.source_payload_id,
                fetched_at=excluded.fetched_at, published_at=excluded.published_at,
                available_date=excluded.available_date,
                validation_status=excluded.validation_status,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason
            """,
            (
                market,
                symbol,
                effective_date.isoformat(),
                name,
                industry,
                source_endpoint,
                source_url,
                source_payload_sha256,
                source_payload_id,
                fetched_at or _now_iso(),
                available_date.isoformat() if isinstance(available_date, date) else available_date,
                published_at,
                validation_status,
                availability_precision or "unknown",
                data_gap_reason,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="security_master",
            record_identity=f"{market}|{symbol}|{_iso_value(effective_date)}",
            payload_id=source_payload_id,
        )


def upsert_monthly_revenue(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    revenue_month: str,
    monthly_revenue: float | None,
    revenue_mom: float | None,
    revenue_yoy: float | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    availability_precision: str = "unknown",
    validation_status: str = "verified",
    data_gap_reason: str | None = None,
) -> None:
    revenue_month = _normalize_revenue_month(revenue_month)
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO monthly_revenue(
                market, symbol, revenue_month, monthly_revenue, revenue_mom,
                revenue_yoy, source_endpoint, source_url, source_payload_sha256,
                source_payload_id, fetched_at, published_at, available_date,
                validation_status, availability_precision, data_gap_reason
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market, symbol, revenue_month) DO UPDATE SET
                monthly_revenue=excluded.monthly_revenue, revenue_mom=excluded.revenue_mom,
                revenue_yoy=excluded.revenue_yoy, source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url, source_payload_sha256=excluded.source_payload_sha256,
                source_payload_id=excluded.source_payload_id,
                fetched_at=excluded.fetched_at, published_at=excluded.published_at,
                available_date=excluded.available_date,
                validation_status=excluded.validation_status,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason
            """,
            (
                market,
                symbol,
                revenue_month,
                monthly_revenue,
                revenue_mom,
                revenue_yoy,
                source_endpoint,
                source_url,
                source_payload_sha256,
                source_payload_id,
                fetched_at or _now_iso(),
                published_at,
                available_date.isoformat() if isinstance(available_date, date) else available_date,
                validation_status,
                availability_precision or "unknown",
                data_gap_reason,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="monthly_revenue",
            record_identity=f"{market}|{symbol}|{revenue_month}",
            payload_id=source_payload_id,
        )


def upsert_valuation_snapshot(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    trade_date: date,
    pe: float | None,
    pb: float | None,
    dividend_yield: float | None,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    availability_precision: str = "unknown",
    validation_status: str = "verified",
    data_gap_reason: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO valuation_snapshots(
                market, symbol, trade_date, pe, pb, dividend_yield,
                source_endpoint, source_url, source_payload_sha256, source_payload_id,
                fetched_at, published_at, available_date, validation_status,
                availability_precision, data_gap_reason
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market, symbol, trade_date) DO UPDATE SET
                pe=excluded.pe, pb=excluded.pb, dividend_yield=excluded.dividend_yield,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256, fetched_at=excluded.fetched_at,
                source_payload_id=excluded.source_payload_id,
                published_at=excluded.published_at, available_date=excluded.available_date,
                validation_status=excluded.validation_status,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason
            """,
            (
                market,
                symbol,
                trade_date.isoformat(),
                pe,
                pb,
                dividend_yield,
                source_endpoint,
                source_url,
                source_payload_sha256,
                source_payload_id,
                fetched_at or _now_iso(),
                published_at,
                available_date.isoformat() if isinstance(available_date, date) else available_date,
                validation_status,
                availability_precision or "unknown",
                data_gap_reason,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="valuation_snapshots",
            record_identity=f"{market}|{symbol}|{_iso_value(trade_date)}",
            payload_id=source_payload_id,
        )


def upsert_annual_company_fundamental(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    fiscal_year: str,
    available_date: date,
    source_endpoint: str,
    source_url: str,
    revenue: float | None = None,
    gross_profit: float | None = None,
    net_income: float | None = None,
    equity: float | None = None,
    eps: float | None = None,
    roe: float | None = None,
    published_at: str | None = None,
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    fetched_at: str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
    raw_payload_json: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO annual_company_fundamentals(
                market, symbol, fiscal_year, available_date, published_at,
                revenue, gross_profit, net_income, equity, eps, roe,
                source_endpoint, source_url, source_payload_sha256, source_payload_id,
                fetched_at, validation_status, availability_precision, data_gap_reason,
                raw_payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market, symbol, fiscal_year, available_date) DO UPDATE SET
                published_at=excluded.published_at, revenue=excluded.revenue,
                gross_profit=excluded.gross_profit, net_income=excluded.net_income,
                equity=excluded.equity, eps=excluded.eps, roe=excluded.roe,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256,
                source_payload_id=excluded.source_payload_id,
                fetched_at=excluded.fetched_at, validation_status=excluded.validation_status,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason,
                raw_payload_json=excluded.raw_payload_json
            """,
            (
                market,
                symbol,
                fiscal_year,
                available_date.isoformat(),
                published_at,
                revenue,
                gross_profit,
                net_income,
                equity,
                eps,
                roe,
                source_endpoint,
                source_url,
                source_payload_sha256,
                source_payload_id,
                fetched_at or _now_iso(),
                validation_status,
                availability_precision or "unknown",
                data_gap_reason,
                raw_payload_json,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="annual_fundamentals",
            record_identity=f"{market}|{symbol}|{fiscal_year}|{_iso_value(available_date)}",
            payload_id=source_payload_id,
        )


def upsert_universe_membership(
    db_path: Path,
    *,
    theme: str,
    symbol: str,
    market: str,
    universe_mode: str,
    effective_from: date,
    source: str = "curated_theme_library",
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    effective_to: date | None = None,
    available_date: date | str | None = None,
    published_at: str | None = None,
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO universe_membership(
                theme, symbol, market, universe_mode, effective_from, effective_to,
                source, source_payload_sha256, source_payload_id, recorded_at,
                available_date, published_at, availability_precision, data_gap_reason
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme, symbol, market, universe_mode, effective_from) DO UPDATE SET
                effective_to=excluded.effective_to,
                source=excluded.source,
                source_payload_sha256=excluded.source_payload_sha256,
                source_payload_id=excluded.source_payload_id,
                recorded_at=excluded.recorded_at,
                available_date=excluded.available_date,
                published_at=excluded.published_at,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason
            """,
            (
                theme,
                symbol,
                market,
                universe_mode,
                effective_from.isoformat(),
                effective_to.isoformat() if effective_to else None,
                source,
                source_payload_sha256,
                source_payload_id,
                _now_iso(),
                available_date.isoformat() if isinstance(available_date, date) else available_date,
                published_at,
                availability_precision or "unknown",
                data_gap_reason,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="universe_membership",
            record_identity=f"{theme}|{symbol}|{market}|{universe_mode}|{_iso_value(effective_from)}",
            payload_id=source_payload_id,
        )


def record_sync_run(
    db_path: Path,
    *,
    run_id: str,
    as_of_date: date,
    themes: list[str],
    started_at: str,
    finished_at: str,
    status: str,
    summary: dict[str, Any],
    issues: Iterable[dict[str, Any]] = (),
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO market_data_sync_runs(
                run_id, as_of_date, themes_json, database_path, started_at,
                finished_at, status, summary_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                finished_at=excluded.finished_at,
                status=excluded.status,
                summary_json=excluded.summary_json
            """,
            (
                run_id,
                as_of_date.isoformat(),
                json.dumps(themes, ensure_ascii=False),
                str(db_path),
                started_at,
                finished_at,
                status,
                json.dumps(summary, ensure_ascii=False, default=str),
            ),
        )
        for issue in issues:
            dataset_key = str(issue.get("dataset_key") or "daily_bars")
            market = issue.get("market")
            symbol = issue.get("symbol")
            effective_date = issue.get("effective_date") or issue.get("trade_date")
            issue_type = str(issue.get("issue_type") or "sync_failed")
            detail = str(issue.get("detail") or "")
            fingerprint = hashlib.sha256(
                "|".join(
                    str(value or "").strip()
                    for value in (dataset_key, market, symbol, effective_date, issue_type, detail)
                ).encode("utf-8")
            ).hexdigest()
            exists = conn.execute(
                "SELECT 1 FROM market_data_sync_issues WHERE fingerprint = ? LIMIT 1",
                (fingerprint,),
            ).fetchone()
            if exists is None:
                conn.execute(
                    """
                    INSERT INTO market_data_sync_issues(
                        run_id, dataset_key, market, symbol, trade_date, issue_type,
                        detail, fingerprint, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        dataset_key,
                        market,
                        symbol,
                        effective_date,
                        issue_type,
                        detail,
                        fingerprint,
                        finished_at,
                    ),
                )

            quality = conn.execute(
                "SELECT issue_id FROM market_data_quality_issues WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()
            if quality is None:
                cursor = conn.execute(
                    """
                    INSERT INTO market_data_quality_issues(
                        dataset_key, market, symbol, effective_date, issue_type, detail,
                        fingerprint, first_seen_at, last_seen_at, occurrence_count,
                        latest_run_id, status
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 'open')
                    """,
                    (
                        dataset_key,
                        market,
                        symbol,
                        effective_date,
                        issue_type,
                        detail,
                        fingerprint,
                        finished_at,
                        finished_at,
                        run_id,
                    ),
                )
                quality_issue_id = int(cursor.lastrowid)
            else:
                quality_issue_id = int(quality["issue_id"])
                conn.execute(
                    """
                    UPDATE market_data_quality_issues
                    SET last_seen_at = ?, occurrence_count = occurrence_count + 1,
                        latest_run_id = ?, detail = ?, status = 'open'
                    WHERE issue_id = ?
                    """,
                    (finished_at, run_id, detail, quality_issue_id),
                )
            conn.execute(
                """
                INSERT INTO market_data_quality_issue_occurrences(
                    issue_id, run_id, observed_at, detail
                ) VALUES(?, ?, ?, ?)
                """,
                (quality_issue_id, run_id, finished_at, detail),
            )
        conn.commit()


def record_fetch_attempt(
    db_path: Path,
    *,
    dataset_key: str,
    request_method: str,
    request_url: str,
    started_at: str,
    finished_at: str | None = None,
    run_id: str | None = None,
    market: str | None = None,
    symbol: str | None = None,
    requested_from: date | str | None = None,
    requested_to: date | str | None = None,
    final_url: str | None = None,
    redirect_chain: list[str] | None = None,
    http_status: int | None = None,
    fallback_level: int = 0,
    cache_status: str | None = None,
    payload_sha256: str | None = None,
    status: str = "unknown",
    error: str | None = None,
) -> int:
    """Persist one observable source/cache attempt for later diagnosis."""

    db_path = Path(db_path)

    def _date_text(value: date | str | None) -> str | None:
        return value.isoformat() if isinstance(value, date) else value

    with closing(_connect(db_path)) as conn:
        cursor = conn.execute(
            """
            INSERT INTO market_data_fetch_attempts(
                run_id, dataset_key, market, symbol, requested_from, requested_to,
                request_method, request_url, final_url, redirect_chain_json,
                http_status, fallback_level, cache_status, payload_sha256,
                started_at, finished_at, status, error
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                dataset_key,
                market,
                symbol,
                _date_text(requested_from),
                _date_text(requested_to),
                request_method.upper(),
                request_url,
                final_url,
                json.dumps(redirect_chain or [], ensure_ascii=False),
                http_status,
                int(fallback_level),
                cache_status,
                payload_sha256,
                started_at,
                finished_at,
                status,
                error,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def record_sync_item(
    db_path: Path,
    *,
    run_id: str,
    dataset_key: str,
    started_at: str,
    finished_at: str,
    status: str,
    market: str | None = None,
    symbol: str | None = None,
    requested_from: date | str | None = None,
    requested_to: date | str | None = None,
    expected_trade_date: date | str | None = None,
    actual_latest_trade_date: date | str | None = None,
    expected_row_count: int | None = None,
    actual_row_count: int | None = None,
    cache_status: str | None = None,
    error: str | None = None,
) -> int:
    """Record the outcome of one dataset/symbol sync unit."""

    db_path = Path(db_path)

    def _date_text(value: date | str | None) -> str | None:
        return value.isoformat() if isinstance(value, date) else value

    with closing(_connect(db_path)) as conn:
        cursor = conn.execute(
            """
            INSERT INTO market_data_sync_items(
                run_id, dataset_key, market, symbol, requested_from, requested_to,
                expected_trade_date, actual_latest_trade_date, expected_row_count,
                actual_row_count, cache_status, status, error, started_at, finished_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, dataset_key, market, symbol, requested_from, requested_to)
            DO UPDATE SET
                expected_trade_date=excluded.expected_trade_date,
                actual_latest_trade_date=excluded.actual_latest_trade_date,
                expected_row_count=excluded.expected_row_count,
                actual_row_count=excluded.actual_row_count,
                cache_status=excluded.cache_status,
                status=excluded.status,
                error=excluded.error,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at
            """,
            (
                run_id,
                dataset_key,
                market or "",
                symbol or "",
                _date_text(requested_from),
                _date_text(requested_to),
                _date_text(expected_trade_date),
                _date_text(actual_latest_trade_date),
                expected_row_count,
                actual_row_count,
                cache_status,
                status,
                error,
                started_at,
                finished_at,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def upsert_dataset_sync_state(
    db_path: Path,
    *,
    dataset_key: str,
    last_status: str,
    market: str | None = None,
    symbol: str | None = None,
    partition_key: str = "",
    first_effective_date: date | str | None = None,
    last_effective_date: date | str | None = None,
    verified_row_count: int = 0,
    last_verified_at: str | None = None,
    last_error: str | None = None,
) -> None:
    """Maintain a dataset-level incremental checkpoint independently of raw bars."""

    if last_status not in VALID_SYNC_STATUSES:
        raise ValueError(f"unsupported sync status: {last_status}")

    db_path = Path(db_path)

    def _date_text(value: date | str | None) -> str | None:
        return value.isoformat() if isinstance(value, date) else value

    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO market_data_sync_state(
                dataset_key, market, symbol, partition_key, first_effective_date,
                last_effective_date, verified_row_count, last_verified_at,
                last_status, last_error, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_key, market, symbol, partition_key) DO UPDATE SET
                first_effective_date=excluded.first_effective_date,
                last_effective_date=excluded.last_effective_date,
                verified_row_count=excluded.verified_row_count,
                last_verified_at=excluded.last_verified_at,
                last_status=excluded.last_status,
                last_error=excluded.last_error,
                updated_at=excluded.updated_at
            """,
            (
                dataset_key,
                market or "",
                symbol or "",
                partition_key,
                _date_text(first_effective_date),
                _date_text(last_effective_date),
                int(verified_row_count),
                last_verified_at,
                last_status,
                last_error,
                _now_iso(),
            ),
        )
        conn.commit()


_PARTITION_STATUSES = {
    "migrated",
    "verified",
    "partial",
    "quarantined",
    "failed",
    "not_implemented",
}


def upsert_partition_state(
    db_path: Path,
    *,
    dataset_key: str,
    partition_key: str,
    request_method: str,
    request_url: str,
    market: str | None = None,
    symbol: str | None = None,
    requested_from: date | str | None = None,
    requested_to: date | str | None = None,
    request_body_sha256: str | None = None,
    payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    first_effective_date: date | str | None = None,
    last_effective_date: date | str | None = None,
    row_count: int = 0,
    status: str = "verified",
    last_verified_at: str | None = None,
    last_run_id: str | None = None,
    last_completeness_run_id: str | None = None,
    gap_reason: str | None = None,
    retry_after: str | None = None,
) -> None:
    """Persist the verified state of one fetch partition.

    A partition is the unit of idempotent network work.  The request range and
    request-body hash are retained alongside the payload hash so a subsequent
    run can decide that a fetch is already verified without re-querying the
    source.
    """

    if status not in _PARTITION_STATUSES:
        raise ValueError(f"unsupported partition status: {status}")
    db_path = _ready_market_data_db(Path(db_path))
    iso = lambda value: value.isoformat() if isinstance(value, date) else value
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO market_data_partition_state(
                dataset_key, market, symbol, partition_key, requested_from,
                requested_to, request_method, request_url, request_body_sha256,
                payload_sha256, source_payload_id, first_effective_date,
                last_effective_date, row_count, status, last_verified_at,
                last_run_id, last_completeness_run_id, gap_reason, retry_after,
                updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_key, market, symbol, partition_key) DO UPDATE SET
                requested_from=excluded.requested_from,
                requested_to=excluded.requested_to,
                request_method=excluded.request_method,
                request_url=excluded.request_url,
                request_body_sha256=excluded.request_body_sha256,
                payload_sha256=excluded.payload_sha256,
                source_payload_id=excluded.source_payload_id,
                first_effective_date=excluded.first_effective_date,
                last_effective_date=excluded.last_effective_date,
                row_count=excluded.row_count,
                status=excluded.status,
                last_verified_at=excluded.last_verified_at,
                last_run_id=excluded.last_run_id,
                last_completeness_run_id=excluded.last_completeness_run_id,
                gap_reason=excluded.gap_reason,
                retry_after=excluded.retry_after,
                updated_at=excluded.updated_at
            """,
            (
                str(dataset_key),
                str(market or ""),
                str(symbol or ""),
                str(partition_key),
                iso(requested_from),
                iso(requested_to),
                str(request_method or "GET").upper(),
                str(request_url),
                request_body_sha256,
                payload_sha256,
                source_payload_id,
                iso(first_effective_date),
                iso(last_effective_date),
                max(int(row_count), 0),
                status,
                last_verified_at,
                last_run_id,
                last_completeness_run_id,
                gap_reason,
                retry_after,
                _now_iso(),
            ),
        )
        conn.commit()


def get_partition_state(
    db_path: Path,
    *,
    dataset_key: str,
    partition_key: str,
    market: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any] | None:
    """Read one partition checkpoint without initializing or mutating SQLite."""

    with closing(_read_only_connect(Path(db_path))) as conn:
        row = conn.execute(
            """
            SELECT * FROM market_data_partition_state
            WHERE dataset_key = ? AND market = ? AND symbol = ? AND partition_key = ?
            """,
            (dataset_key, str(market or ""), str(symbol or ""), partition_key),
        ).fetchone()
    return dict(row) if row is not None else None


def partition_is_verified(
    db_path: Path,
    *,
    dataset_key: str,
    partition_key: str,
    requested_from: date | str | None = None,
    requested_to: date | str | None = None,
    request_body_sha256: str | None = None,
    market: str | None = None,
    symbol: str | None = None,
) -> bool:
    """Return whether an exact request partition is already verified."""

    state = get_partition_state(
        db_path,
        dataset_key=dataset_key,
        market=market,
        symbol=symbol,
        partition_key=partition_key,
    )
    if not state or state.get("status") != "verified":
        return False
    if not state.get("last_verified_at") or not state.get("payload_sha256"):
        return False
    iso = lambda value: value.isoformat() if isinstance(value, date) else value
    return (
        state.get("requested_from") == iso(requested_from)
        and state.get("requested_to") == iso(requested_to)
        and state.get("request_body_sha256") == request_body_sha256
    )


def record_data_gap(
    db_path: Path,
    *,
    dataset_key: str,
    partition_key: str,
    reason: str,
    detail: str,
    market: str | None = None,
    symbol: str | None = None,
    run_id: str | None = None,
    retry_after: str | None = None,
    status: str = "open",
    observed_at: str | None = None,
) -> None:
    """Upsert one explainable gap without duplicating repeated observations."""

    if not str(reason).strip():
        raise ValueError("data gap reason cannot be empty")
    observed = observed_at or _now_iso()
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO market_data_gap_ledger(
                dataset_key, market, symbol, partition_key, reason, detail,
                first_seen_at, last_seen_at, occurrence_count, latest_run_id,
                retry_after, status
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(dataset_key, market, symbol, partition_key) DO UPDATE SET
                reason=excluded.reason,
                detail=excluded.detail,
                last_seen_at=excluded.last_seen_at,
                occurrence_count=market_data_gap_ledger.occurrence_count + 1,
                latest_run_id=excluded.latest_run_id,
                retry_after=excluded.retry_after,
                status=excluded.status
            """,
            (
                dataset_key,
                str(market or ""),
                str(symbol or ""),
                partition_key,
                reason,
                detail,
                observed,
                observed,
                run_id,
                retry_after,
                status,
            ),
        )
        conn.commit()


def begin_completeness_run(
    db_path: Path,
    *,
    dataset_key: str,
    run_id: str | None = None,
    expected_rows: int | None = None,
    expected_partitions: int | None = None,
    started_at: str | None = None,
    completeness_run_id: str | None = None,
) -> str:
    """Start a completeness measurement linked to a sync run when available."""

    db_path = _ready_market_data_db(Path(db_path))
    identifier = completeness_run_id or f"completeness-{uuid.uuid4().hex}"
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO market_data_completeness_runs(
                completeness_run_id, run_id, dataset_key, started_at, status,
                expected_rows, expected_partitions
            ) VALUES(?, ?, ?, ?, 'running', ?, ?)
            """,
            (
                identifier,
                run_id,
                dataset_key,
                started_at or _now_iso(),
                expected_rows,
                expected_partitions,
            ),
        )
        conn.commit()
    return identifier


def finish_completeness_run(
    db_path: Path,
    *,
    completeness_run_id: str,
    status: str,
    actual_rows: int | None = None,
    actual_partitions: int | None = None,
    missing_partitions: Iterable[str] = (),
    summary: dict[str, Any] | None = None,
    finished_at: str | None = None,
) -> None:
    """Close a completeness run with explicit missing partition evidence."""

    if status not in _PARTITION_STATUSES | {"running"}:
        raise ValueError(f"unsupported completeness status: {status}")
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        cursor = conn.execute(
            """
            UPDATE market_data_completeness_runs
            SET finished_at = ?, status = ?, actual_rows = ?, actual_partitions = ?,
                missing_partitions_json = ?, summary_json = ?
            WHERE completeness_run_id = ?
            """,
            (
                finished_at or _now_iso(),
                status,
                actual_rows,
                actual_partitions,
                json.dumps(sorted({str(item) for item in missing_partitions}), ensure_ascii=False),
                json.dumps(summary or {}, ensure_ascii=False, default=str),
                completeness_run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"unknown completeness run: {completeness_run_id}")
        conn.commit()


def upsert_corporate_action(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    action_date: date,
    action_type: str,
    source_endpoint: str,
    source_url: str,
    source_payload_sha256: str | None = None,
    source_payload_id: str | None = None,
    ex_date: date | None = None,
    record_date: date | None = None,
    payment_date: date | None = None,
    ratio: float | None = None,
    cash_amount: float | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
    validation_status: str = "verified",
    availability_precision: str = "unknown",
    data_gap_reason: str | None = None,
    raw_payload_json: str | None = None,
) -> str:
    db_path = _ready_market_data_db(Path(db_path))
    if isinstance(raw_payload_json, (dict, list)):
        raw_payload_text = json.dumps(
            raw_payload_json,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
    else:
        raw_payload_text = raw_payload_json
    action_id = hashlib.sha256(
        "|".join(
            [
                market,
                symbol,
                action_date.isoformat(),
                action_type,
            ]
        ).encode("utf-8")
    ).hexdigest()
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO corporate_actions(
                action_id, market, symbol, action_date, action_type, ex_date,
                record_date, payment_date, ratio, cash_amount, source_endpoint,
                source_url, source_payload_sha256, source_payload_id, fetched_at,
                available_date, published_at, validation_status, availability_precision, data_gap_reason,
                raw_payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(action_id) DO UPDATE SET
                ex_date=excluded.ex_date, record_date=excluded.record_date,
                payment_date=excluded.payment_date, ratio=excluded.ratio,
                cash_amount=excluded.cash_amount, source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url, source_payload_sha256=excluded.source_payload_sha256,
                source_payload_id=excluded.source_payload_id,
                fetched_at=excluded.fetched_at, available_date=excluded.available_date,
                published_at=excluded.published_at,
                validation_status=excluded.validation_status,
                availability_precision=excluded.availability_precision,
                data_gap_reason=excluded.data_gap_reason,
                raw_payload_json=excluded.raw_payload_json
            """,
            (
                action_id,
                market,
                symbol,
                action_date.isoformat(),
                action_type,
                ex_date.isoformat() if ex_date else None,
                record_date.isoformat() if record_date else None,
                payment_date.isoformat() if payment_date else None,
                ratio,
                cash_amount,
                source_endpoint,
                source_url,
                source_payload_sha256,
                source_payload_id,
                fetched_at or _now_iso(),
                available_date.isoformat() if isinstance(available_date, date) else available_date,
                published_at,
                validation_status,
                availability_precision or "unknown",
                data_gap_reason,
                raw_payload_text,
            ),
        )
        conn.commit()
    if source_payload_id:
        link_source_payload(
            db_path,
            dataset_key="corporate_actions",
            record_identity=action_id,
            payload_id=source_payload_id,
        )
    return action_id


def database_integrity(db_path: Path) -> dict[str, Any]:
    db_path = init_market_data_db(Path(db_path))
    daily = daily_database_integrity(db_path)
    with closing(_connect(db_path)) as conn:
        table_counts: dict[str, int] = {}
        for table in (
            "period_bars",
            "index_bars",
            "security_master_snapshots",
            "universe_membership",
            "monthly_revenue",
            "valuation_snapshots",
            "annual_company_fundamentals",
            "corporate_actions",
            "source_payloads",
            "market_data_sync_runs",
            "market_data_sync_issues",
            "market_data_dataset_catalog",
            "market_data_source_registry",
            "market_data_fetch_attempts",
            "market_data_sync_items",
            "market_data_sync_state",
            "market_data_partition_state",
            "market_data_gap_ledger",
            "market_data_completeness_runs",
            "market_data_quality_issues",
            "market_data_quality_issue_occurrences",
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
        ):
            table_counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        max_trade_date = conn.execute("SELECT MAX(trade_date) FROM daily_bars").fetchone()[0]
        max_index_date = conn.execute("SELECT MAX(trade_date) FROM index_bars").fetchone()[0]
        sqlite_integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        foreign_key_violations = [
            dict(row)
            for row in conn.execute("PRAGMA foreign_key_check").fetchall()
        ]
        external_payload_missing = [
            str(row[0])
            for row in conn.execute(
                "SELECT storage_uri FROM source_payloads "
                "WHERE storage_mode = 'external' AND storage_uri IS NOT NULL"
            ).fetchall()
            if not Path(str(row[0])).exists()
        ]
        payload_hash_mismatches: list[str] = []
        for row in conn.execute(
            "SELECT payload_id, storage_mode, storage_uri, payload_sha256, raw_payload_json "
            "FROM source_payloads"
        ).fetchall():
            payload_id = str(row["payload_id"])
            if str(row["storage_mode"] or "inline") == "external":
                storage_uri = str(row["storage_uri"] or "")
                if not storage_uri or not Path(storage_uri).exists():
                    continue
                actual_hash = hashlib.sha256(Path(storage_uri).read_bytes()).hexdigest()
            else:
                actual_hash = hashlib.sha256(
                    str(row["raw_payload_json"] or "").encode("utf-8")
                ).hexdigest()
            if actual_hash != str(row["payload_sha256"] or ""):
                payload_hash_mismatches.append(payload_id)
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
        meta_rows = conn.execute(
            "SELECT key, value FROM schema_meta WHERE key LIKE '%schema_version' OR key LIKE '%migration%'"
        ).fetchall()
    ok = (
        bool(daily.get("ok"))
        and sqlite_integrity == "ok"
        and not foreign_key_violations
        and not external_payload_missing
        and not payload_hash_mismatches
        and not source_link_orphans
    )
    return {
        "database_path": str(db_path),
        "ok": ok,
        "daily": daily,
        "table_counts": table_counts,
        "max_daily_trade_date": max_trade_date,
        "max_index_trade_date": max_index_date,
        "sqlite_integrity": sqlite_integrity,
        "foreign_key_violations": foreign_key_violations,
        "external_payload_missing": external_payload_missing,
        "payload_hash_mismatches": payload_hash_mismatches,
        "source_link_orphans": source_link_orphans,
        "source_payload_integrity": {
            "status": (
                "verified"
                if not external_payload_missing
                and not payload_hash_mismatches
                and not source_link_orphans
                else "failed"
            ),
            "external_payload_missing": external_payload_missing,
            "hash_mismatches": payload_hash_mismatches,
            "source_link_orphans": source_link_orphans,
        },
        "schema_meta": {str(row["key"]): str(row["value"]) for row in meta_rows},
    }
