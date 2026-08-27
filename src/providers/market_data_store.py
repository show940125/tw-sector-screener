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
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.providers.daily_bar_store import database_integrity as daily_database_integrity
from src.providers.daily_bar_store import init_db as init_daily_db
from src.providers.quarterly_store import init_db as init_quarterly_db


MARKET_DATA_SCHEMA_VERSION = 2
PERIOD_DERIVATION_VERSION = "daily-bars-period-v1"
SOURCE_PAYLOAD_INLINE_LIMIT_BYTES = 10 * 1024 * 1024


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


def _safe_dataset_path(dataset_key: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(dataset_key).strip())
    return value or "unknown"


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
                data_status TEXT NOT NULL DEFAULT 'derived',
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
            CREATE INDEX IF NOT EXISTS idx_quality_issue_dataset_status
            ON market_data_quality_issues(dataset_key, status, last_seen_at);
            CREATE INDEX IF NOT EXISTS idx_quality_occurrence_issue_date
            ON market_data_quality_issue_occurrences(issue_id, observed_at);
            """
        )
        # These columns are additive migrations for databases created by the
        # v1 implementation.  Keeping the legacy raw_payload_json column
        # non-null lets old readers continue to work while large payloads are
        # stored in the external raw store with a compact descriptor in that
        # column.
        _ensure_column(conn, "monthly_revenue", "available_date", "TEXT")
        _ensure_column(conn, "valuation_snapshots", "available_date", "TEXT")
        _ensure_column(conn, "source_payloads", "storage_mode", "TEXT NOT NULL DEFAULT 'inline'")
        _ensure_column(conn, "source_payloads", "storage_uri", "TEXT")
        _ensure_column(conn, "source_payloads", "byte_size", "INTEGER")
        _ensure_column(conn, "source_payloads", "content_encoding", "TEXT NOT NULL DEFAULT 'utf-8'")
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
                  AND name IN ('market_data_sync_runs', 'market_data_quality_issues')
                GROUP BY 1
                HAVING COUNT(*) = 2
                """
            ).fetchone()
    except sqlite3.DatabaseError:
        exists = None
    if exists is None:
        return init_market_data_db(path)
    return path


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
    summary: dict[str, Any] = {"database_path": str(db_path), "sources": {}}
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
            summary["sources"][alias] = {"path": str(source), "exists": True, "inserted": inserted}
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
            "SELECT value FROM schema_meta WHERE key = 'market_data_sync_state_backfill_v1'"
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
    # Backfill period bars after a migration only when daily rows exist.  The
    # operation is idempotent and also repairs a partially completed prior run.
    result["period_bars"] = rebuild_period_bars(Path(db_path))
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
) -> dict[str, Any]:
    """Derive weekly/monthly/quarterly/yearly price bars from verified daily bars."""

    db_path = init_market_data_db(Path(db_path))
    conditions = ["data_status = 'verified'"]
    params: list[Any] = []
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
                    source_latest_trade_date, derivation_version, generated_at, data_status
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'derived')
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
                    data_status=excluded.data_status
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
    published_at: str | None = None,
    cache_file: str | None = None,
    validation_status: str = "unvalidated",
    validation_error: str | None = None,
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
                fetched_at, cache_file, validation_status, validation_error, raw_payload_json,
                storage_mode, storage_uri, byte_size, content_encoding
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(payload_id) DO UPDATE SET
                source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url,
                effective_date=excluded.effective_date,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                cache_file=excluded.cache_file,
                validation_status=excluded.validation_status,
                validation_error=excluded.validation_error,
                raw_payload_json=excluded.raw_payload_json,
                storage_mode=excluded.storage_mode,
                storage_uri=excluded.storage_uri,
                byte_size=excluded.byte_size,
                content_encoding=excluded.content_encoding
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
            ),
        )
        conn.commit()
    return payload_id


def upsert_index_bars(db_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    db_path = _ready_market_data_db(Path(db_path))
    count = 0
    with closing(_connect(db_path)) as conn:
        for row in rows:
            conn.execute(
                """
                INSERT INTO index_bars(
                    index_code, trade_date, close, change_points, source_endpoint,
                    source_url, source_payload_sha256, fetched_at, published_at, data_status
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(index_code, trade_date) DO UPDATE SET
                    close=excluded.close,
                    change_points=excluded.change_points,
                    source_endpoint=excluded.source_endpoint,
                    source_url=excluded.source_url,
                    source_payload_sha256=excluded.source_payload_sha256,
                    fetched_at=excluded.fetched_at,
                    published_at=excluded.published_at,
                    data_status=excluded.data_status
                """,
                (
                    str(row["index_code"]),
                    str(row["trade_date"]),
                    float(row["close"]),
                    row.get("change_points"),
                    str(row.get("source_endpoint") or ""),
                    str(row.get("source_url") or ""),
                    str(row.get("source_payload_sha256") or ""),
                    str(row.get("fetched_at") or _now_iso()),
                    row.get("published_at"),
                    str(row.get("data_status") or "verified"),
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
    source_payload_sha256: str | None,
    fetched_at: str | None = None,
    published_at: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO security_master_snapshots(
                market, symbol, effective_date, name, industry, source_endpoint,
                source_url, source_payload_sha256, fetched_at, published_at, validation_status
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
            ON CONFLICT(market, symbol, effective_date) DO UPDATE SET
                name=excluded.name, industry=excluded.industry,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256,
                fetched_at=excluded.fetched_at, published_at=excluded.published_at,
                validation_status=excluded.validation_status
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
                fetched_at or _now_iso(),
                published_at,
            ),
        )
        conn.commit()


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
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO monthly_revenue(
                market, symbol, revenue_month, monthly_revenue, revenue_mom,
                revenue_yoy, source_endpoint, source_url, source_payload_sha256,
                fetched_at, published_at, available_date, validation_status
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
            ON CONFLICT(market, symbol, revenue_month) DO UPDATE SET
                monthly_revenue=excluded.monthly_revenue, revenue_mom=excluded.revenue_mom,
                revenue_yoy=excluded.revenue_yoy, source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url, source_payload_sha256=excluded.source_payload_sha256,
                fetched_at=excluded.fetched_at, published_at=excluded.published_at,
                available_date=excluded.available_date,
                validation_status=excluded.validation_status
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
                fetched_at or _now_iso(),
                published_at,
                available_date.isoformat() if isinstance(available_date, date) else available_date,
            ),
        )
        conn.commit()


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
    fetched_at: str | None = None,
    published_at: str | None = None,
    available_date: date | str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO valuation_snapshots(
                market, symbol, trade_date, pe, pb, dividend_yield,
                source_endpoint, source_url, source_payload_sha256, fetched_at,
                published_at, available_date, validation_status
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
            ON CONFLICT(market, symbol, trade_date) DO UPDATE SET
                pe=excluded.pe, pb=excluded.pb, dividend_yield=excluded.dividend_yield,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256, fetched_at=excluded.fetched_at,
                published_at=excluded.published_at, available_date=excluded.available_date,
                validation_status=excluded.validation_status
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
                fetched_at or _now_iso(),
                published_at,
                available_date.isoformat() if isinstance(available_date, date) else available_date,
            ),
        )
        conn.commit()


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
    fetched_at: str | None = None,
    raw_payload_json: str | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO annual_company_fundamentals(
                market, symbol, fiscal_year, available_date, published_at,
                revenue, gross_profit, net_income, equity, eps, roe,
                source_endpoint, source_url, source_payload_sha256, fetched_at,
                validation_status, raw_payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified', ?)
            ON CONFLICT(market, symbol, fiscal_year, available_date) DO UPDATE SET
                published_at=excluded.published_at, revenue=excluded.revenue,
                gross_profit=excluded.gross_profit, net_income=excluded.net_income,
                equity=excluded.equity, eps=excluded.eps, roe=excluded.roe,
                source_endpoint=excluded.source_endpoint, source_url=excluded.source_url,
                source_payload_sha256=excluded.source_payload_sha256,
                fetched_at=excluded.fetched_at, validation_status=excluded.validation_status,
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
                fetched_at or _now_iso(),
                raw_payload_json,
            ),
        )
        conn.commit()


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
    effective_to: date | None = None,
) -> None:
    db_path = _ready_market_data_db(Path(db_path))
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO universe_membership(
                theme, symbol, market, universe_mode, effective_from, effective_to,
                source, source_payload_sha256, recorded_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme, symbol, market, universe_mode, effective_from) DO UPDATE SET
                effective_to=excluded.effective_to,
                source=excluded.source,
                source_payload_sha256=excluded.source_payload_sha256,
                recorded_at=excluded.recorded_at
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
                _now_iso(),
            ),
        )
        conn.commit()


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
    ex_date: date | None = None,
    record_date: date | None = None,
    payment_date: date | None = None,
    ratio: float | None = None,
    cash_amount: float | None = None,
    fetched_at: str | None = None,
    published_at: str | None = None,
    raw_payload_json: str | None = None,
) -> str:
    db_path = _ready_market_data_db(Path(db_path))
    action_id = hashlib.sha256(
        "|".join(
            [
                market,
                symbol,
                action_date.isoformat(),
                action_type,
                str(source_payload_sha256 or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO corporate_actions(
                action_id, market, symbol, action_date, action_type, ex_date,
                record_date, payment_date, ratio, cash_amount, source_endpoint,
                source_url, source_payload_sha256, fetched_at, published_at,
                validation_status, raw_payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified', ?)
            ON CONFLICT(action_id) DO UPDATE SET
                ex_date=excluded.ex_date, record_date=excluded.record_date,
                payment_date=excluded.payment_date, ratio=excluded.ratio,
                cash_amount=excluded.cash_amount, source_endpoint=excluded.source_endpoint,
                source_url=excluded.source_url, source_payload_sha256=excluded.source_payload_sha256,
                fetched_at=excluded.fetched_at, published_at=excluded.published_at,
                validation_status=excluded.validation_status, raw_payload_json=excluded.raw_payload_json
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
                fetched_at or _now_iso(),
                published_at,
                raw_payload_json,
            ),
        )
        conn.commit()
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
            "market_data_quality_issues",
            "market_data_quality_issue_occurrences",
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
        meta_rows = conn.execute(
            "SELECT key, value FROM schema_meta WHERE key LIKE '%schema_version' OR key LIKE '%migration%'"
        ).fetchall()
    ok = bool(daily.get("ok")) and sqlite_integrity == "ok" and not foreign_key_violations and not external_payload_missing
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
        "schema_meta": {str(row["key"]): str(row["value"]) for row in meta_rows},
    }
