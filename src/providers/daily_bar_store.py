from __future__ import annotations

import json
import hashlib
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class VerifiedDailyBar:
    market: str
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_endpoint: str
    source_url: str
    source_cache_file: str
    source_payload_sha256: str
    source_fetched_at: str
    source_priority: int = 100
    effective_date: date | None = None
    published_at: str | None = None


@dataclass
class StoreImportStats:
    source_payloads_matched: int = 0
    source_payloads_valid: int = 0
    source_payloads_invalid: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    rows_skipped_after_cutoff: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    duplicate_rows: int = 0
    issues: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        self.issues = self.issues or []

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_payloads_matched": self.source_payloads_matched,
            "source_payloads_valid": self.source_payloads_valid,
            "source_payloads_invalid": self.source_payloads_invalid,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "rows_skipped_after_cutoff": self.rows_skipped_after_cutoff,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "duplicate_rows": self.duplicate_rows,
            "issue_count": len(self.issues or []),
            "issues": list(self.issues or []),
        }


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _issue_fingerprint(issue: dict[str, Any]) -> str:
    """Stable identity for the same rejected payload across import runs."""

    parts = [
        str(issue.get("cache_file") or "").strip(),
        str(issue.get("market") or "").strip(),
        str(issue.get("symbol") or "").strip(),
        str(issue.get("requested_month") or "").strip(),
        str(issue.get("issue_type") or "unknown").strip(),
        str(issue.get("detail") or "").strip(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def init_db(db_path: Path) -> None:
    with closing(_connect(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_bars (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_cache_file TEXT NOT NULL,
                source_payload_sha256 TEXT NOT NULL,
                source_fetched_at TEXT NOT NULL,
                source_priority INTEGER NOT NULL DEFAULT 100,
                imported_at TEXT NOT NULL,
                data_status TEXT NOT NULL DEFAULT 'verified',
                effective_date TEXT,
                published_at TEXT,
                PRIMARY KEY (market, symbol, trade_date)
            );

            CREATE TABLE IF NOT EXISTS daily_bar_sources (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_cache_file TEXT NOT NULL,
                source_payload_sha256 TEXT NOT NULL,
                source_fetched_at TEXT NOT NULL,
                source_priority INTEGER NOT NULL,
                validation_status TEXT NOT NULL,
                validation_error TEXT,
                imported_at TEXT NOT NULL,
                effective_date TEXT,
                published_at TEXT,
                PRIMARY KEY (
                    market, symbol, trade_date, source_url, source_payload_sha256
                )
            );

            CREATE TABLE IF NOT EXISTS daily_bar_sync_state (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                first_verified_trade_date TEXT,
                last_verified_trade_date TEXT,
                verified_bar_count INTEGER NOT NULL DEFAULT 0,
                last_imported_at TEXT NOT NULL,
                last_status TEXT NOT NULL,
                last_error TEXT,
                last_current_day_verified_date TEXT,
                last_current_day_verified_at TEXT,
                PRIMARY KEY (market, symbol)
            );

            CREATE TABLE IF NOT EXISTS daily_bar_import_runs (
                run_id TEXT PRIMARY KEY,
                source_cache_dir TEXT NOT NULL,
                database_path TEXT NOT NULL,
                themes_json TEXT NOT NULL,
                symbol_count INTEGER NOT NULL,
                start_month TEXT NOT NULL,
                end_month TEXT NOT NULL,
                max_trade_date TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS daily_bar_import_issues (
                issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                cache_file TEXT,
                market TEXT,
                symbol TEXT,
                requested_month TEXT,
                issue_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                issue_fingerprint TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES daily_bar_import_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_bars_symbol_date
            ON daily_bars(market, symbol, trade_date);

            CREATE INDEX IF NOT EXISTS idx_daily_bars_trade_date
            ON daily_bars(trade_date);

            CREATE INDEX IF NOT EXISTS idx_daily_bar_sources_symbol_date
            ON daily_bar_sources(market, symbol, trade_date);

            CREATE INDEX IF NOT EXISTS idx_daily_bar_issues_run
            ON daily_bar_import_issues(run_id);
            """
        )
        issue_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(daily_bar_import_issues)").fetchall()
        }
        if "issue_fingerprint" not in issue_columns:
            conn.execute("ALTER TABLE daily_bar_import_issues ADD COLUMN issue_fingerprint TEXT")
        for table in ("daily_bars", "daily_bar_sources"):
            columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "effective_date" not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN effective_date TEXT")
            if "published_at" not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN published_at TEXT")
        sync_state_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(daily_bar_sync_state)").fetchall()
        }
        if "last_current_day_verified_date" not in sync_state_columns:
            conn.execute(
                "ALTER TABLE daily_bar_sync_state ADD COLUMN last_current_day_verified_date TEXT"
            )
        if "last_current_day_verified_at" not in sync_state_columns:
            conn.execute(
                "ALTER TABLE daily_bar_sync_state ADD COLUMN last_current_day_verified_at TEXT"
            )
        conn.execute(
            "UPDATE daily_bars SET effective_date = trade_date "
            "WHERE effective_date IS NULL OR effective_date = ''"
        )
        conn.execute(
            "UPDATE daily_bar_sources SET effective_date = trade_date "
            "WHERE effective_date IS NULL OR effective_date = ''"
        )
        legacy_issues = conn.execute(
            "SELECT issue_id, cache_file, market, symbol, requested_month, issue_type, detail "
            "FROM daily_bar_import_issues WHERE issue_fingerprint IS NULL OR issue_fingerprint = ''"
        ).fetchall()
        for row in legacy_issues:
            fingerprint = _issue_fingerprint(
                {
                    "cache_file": row["cache_file"],
                    "market": row["market"],
                    "symbol": row["symbol"],
                    "requested_month": row["requested_month"],
                    "issue_type": row["issue_type"],
                    "detail": row["detail"],
                }
            )
            conn.execute(
                "UPDATE daily_bar_import_issues SET issue_fingerprint = ? WHERE issue_id = ?",
                (fingerprint, row["issue_id"]),
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_bar_issues_fingerprint "
            "ON daily_bar_import_issues(issue_fingerprint)"
        )
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        conn.commit()


def _bar_params(bar: VerifiedDailyBar, imported_at: str) -> dict[str, Any]:
    return {
        "market": bar.market,
        "symbol": bar.symbol,
        "trade_date": bar.trade_date.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "source_endpoint": bar.source_endpoint,
        "source_url": bar.source_url,
        "source_cache_file": bar.source_cache_file,
        "source_payload_sha256": bar.source_payload_sha256,
        "source_fetched_at": bar.source_fetched_at,
        "source_priority": bar.source_priority,
        "imported_at": imported_at,
        "effective_date": (bar.effective_date or bar.trade_date).isoformat(),
        "published_at": bar.published_at,
    }


def _is_better_source(candidate: sqlite3.Row, existing: sqlite3.Row) -> bool:
    candidate_priority = int(candidate["source_priority"])
    existing_priority = int(existing["source_priority"])
    if candidate_priority != existing_priority:
        return candidate_priority < existing_priority
    return str(candidate["source_fetched_at"]) > str(existing["source_fetched_at"])


def _record_source(conn: sqlite3.Connection, bar: VerifiedDailyBar, imported_at: str) -> None:
    params = _bar_params(bar, imported_at)
    conn.execute(
        """
        INSERT INTO daily_bar_sources(
            market, symbol, trade_date, source_endpoint, source_url,
            source_cache_file, source_payload_sha256, source_fetched_at,
            source_priority, validation_status, validation_error, imported_at,
            effective_date, published_at
        ) VALUES(
            :market, :symbol, :trade_date, :source_endpoint, :source_url,
            :source_cache_file, :source_payload_sha256, :source_fetched_at,
            :source_priority, 'verified', NULL, :imported_at,
            :effective_date, :published_at
        )
        ON CONFLICT(
            market, symbol, trade_date, source_url, source_payload_sha256
        ) DO UPDATE SET
            source_endpoint=excluded.source_endpoint,
            source_cache_file=excluded.source_cache_file,
            source_fetched_at=excluded.source_fetched_at,
            source_priority=excluded.source_priority,
            validation_status=excluded.validation_status,
            validation_error=excluded.validation_error,
            imported_at=excluded.imported_at,
            effective_date=excluded.effective_date,
            published_at=excluded.published_at
        """,
        params,
    )


def _upsert_bar(
    conn: sqlite3.Connection,
    bar: VerifiedDailyBar,
    imported_at: str,
) -> str:
    params = _bar_params(bar, imported_at)
    _record_source(conn, bar, imported_at)
    existing = conn.execute(
        """
        SELECT source_priority, source_fetched_at
        FROM daily_bars
        WHERE market = ? AND symbol = ? AND trade_date = ?
        """,
        (bar.market, bar.symbol, bar.trade_date.isoformat()),
    ).fetchone()
    if existing is not None:
        candidate = {"source_priority": bar.source_priority, "source_fetched_at": bar.source_fetched_at}
        if not _is_better_source(candidate, existing):
            return "duplicate"
        conn.execute(
            """
            UPDATE daily_bars SET
                open=:open, high=:high, low=:low, close=:close, volume=:volume,
                source_endpoint=:source_endpoint, source_url=:source_url,
                source_cache_file=:source_cache_file,
                source_payload_sha256=:source_payload_sha256,
                source_fetched_at=:source_fetched_at,
                source_priority=:source_priority, imported_at=:imported_at,
                data_status='verified', effective_date=:effective_date,
                published_at=:published_at
            WHERE market=:market AND symbol=:symbol AND trade_date=:trade_date
            """,
            params,
        )
        return "updated"
    conn.execute(
        """
        INSERT INTO daily_bars(
            market, symbol, trade_date, open, high, low, close, volume,
            source_endpoint, source_url, source_cache_file,
            source_payload_sha256, source_fetched_at, source_priority,
            imported_at, data_status, effective_date, published_at
        ) VALUES(
            :market, :symbol, :trade_date, :open, :high, :low, :close, :volume,
            :source_endpoint, :source_url, :source_cache_file,
            :source_payload_sha256, :source_fetched_at, :source_priority,
            :imported_at, 'verified', :effective_date, :published_at
        )
        """,
        params,
    )
    return "inserted"


def _refresh_sync_state(conn: sqlite3.Connection, imported_at: str) -> None:
    conn.execute(
        """
        INSERT INTO daily_bar_sync_state(
            market, symbol, first_verified_trade_date, last_verified_trade_date,
            verified_bar_count, last_imported_at, last_status, last_error
        )
        SELECT
            market, symbol, MIN(trade_date), MAX(trade_date), COUNT(*),
            ?, 'ok', NULL
        FROM daily_bars
        GROUP BY market, symbol
        ON CONFLICT(market, symbol) DO UPDATE SET
            first_verified_trade_date=excluded.first_verified_trade_date,
            last_verified_trade_date=excluded.last_verified_trade_date,
            verified_bar_count=excluded.verified_bar_count,
            last_imported_at=excluded.last_imported_at,
            last_status=excluded.last_status,
            last_error=excluded.last_error
        """,
        (imported_at,),
    )


def import_verified_bars(
    db_path: Path,
    bars: Iterable[VerifiedDailyBar],
    *,
    imported_at: str | None = None,
) -> StoreImportStats:
    init_db(db_path)
    imported_at = imported_at or datetime.now().astimezone().isoformat()
    stats = StoreImportStats()
    with closing(_connect(db_path)) as conn:
        try:
            conn.execute("BEGIN")
            for bar in bars:
                stats.valid_rows += 1
                outcome = _upsert_bar(conn, bar, imported_at)
                if outcome == "inserted":
                    stats.inserted_rows += 1
                elif outcome == "updated":
                    stats.updated_rows += 1
                else:
                    stats.duplicate_rows += 1
            _refresh_sync_state(conn, imported_at)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return stats


def record_import_run(
    db_path: Path,
    *,
    run_id: str,
    source_cache_dir: Path,
    themes: list[str],
    symbol_count: int,
    start_month: str,
    end_month: str,
    max_trade_date: str | None,
    started_at: str,
    finished_at: str,
    status: str,
    summary: dict[str, Any],
    issues: Iterable[dict[str, Any]] = (),
) -> None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO daily_bar_import_runs(
                run_id, source_cache_dir, database_path, themes_json, symbol_count,
                start_month, end_month, max_trade_date, started_at, finished_at,
                status, summary_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                str(source_cache_dir),
                str(db_path),
                json.dumps(themes, ensure_ascii=False),
                symbol_count,
                start_month,
                end_month,
                max_trade_date,
                started_at,
                finished_at,
                status,
                json.dumps(summary, ensure_ascii=False),
            ),
        )
        for issue in issues:
            fingerprint = _issue_fingerprint(issue)
            exists = conn.execute(
                "SELECT 1 FROM daily_bar_import_issues WHERE issue_fingerprint = ? LIMIT 1",
                (fingerprint,),
            ).fetchone()
            if exists is not None:
                continue
            conn.execute(
                """
                INSERT INTO daily_bar_import_issues(
                    run_id, cache_file, market, symbol, requested_month,
                    issue_type, detail, issue_fingerprint, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    issue.get("cache_file"),
                    issue.get("market"),
                    issue.get("symbol"),
                    issue.get("requested_month"),
                    str(issue.get("issue_type") or "unknown"),
                    str(issue.get("detail") or ""),
                    fingerprint,
                    finished_at,
                ),
            )
        conn.commit()


def get_bars(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    as_of: date | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    init_db(db_path)
    conditions = ["market = ?", "symbol = ?", "data_status = 'verified'"]
    params: list[Any] = [market, symbol]
    if as_of is not None:
        conditions.append("trade_date <= ?")
        params.append(as_of.isoformat())
    query = (
            "SELECT market, symbol, trade_date, open, high, low, close, volume, "
            "source_endpoint, source_url, source_cache_file, source_payload_sha256, "
            "source_fetched_at, source_priority, imported_at, effective_date, published_at "
        "FROM daily_bars WHERE "
        + " AND ".join(conditions)
        + " ORDER BY trade_date DESC"
    )
    if limit is not None:
        query += " LIMIT ?"
        params.append(max(int(limit), 0))
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(query, params).fetchall()
    output: list[dict[str, Any]] = []
    for row in reversed(rows):
        item = {key: row[key] for key in row.keys()}
        item["date"] = date.fromisoformat(str(item.pop("trade_date")))
        output.append(item)
    return output


def get_sync_state(db_path: Path, *, market: str, symbol: str) -> dict[str, Any] | None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        row = conn.execute(
            "SELECT * FROM daily_bar_sync_state WHERE market = ? AND symbol = ?",
            (market, symbol),
        ).fetchone()
    return {key: row[key] for key in row.keys()} if row is not None else None


def mark_current_day_verified(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    trade_date: date,
    verified_at: str | None = None,
) -> None:
    """Record an explicit provider verification of a current-day bar.

    Cache imports intentionally do not set this marker.  A DB-first read may
    use a current-day row without network access only after this marker has
    been set by a validated provider response.
    """

    init_db(db_path)
    verified_at = verified_at or datetime.now().astimezone().isoformat()
    with closing(_connect(db_path)) as conn:
        existing = conn.execute(
            "SELECT 1 FROM daily_bar_sync_state WHERE market = ? AND symbol = ?",
            (market, symbol),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO daily_bar_sync_state(
                    market, symbol, verified_bar_count, last_imported_at,
                    last_status, last_current_day_verified_date,
                    last_current_day_verified_at
                ) VALUES(?, ?, 0, ?, 'current_day_verified', ?, ?)
                """,
                (market, symbol, verified_at, trade_date.isoformat(), verified_at),
            )
        else:
            conn.execute(
                """
                UPDATE daily_bar_sync_state
                SET last_current_day_verified_date = ?,
                    last_current_day_verified_at = ?
                WHERE market = ? AND symbol = ?
                """,
                (trade_date.isoformat(), verified_at, market, symbol),
            )
        conn.commit()


def is_current_day_verified(db_path: Path, *, market: str, symbol: str, trade_date: date) -> bool:
    """Return whether the exact date was explicitly verified by the provider."""

    state = get_sync_state(db_path, market=market, symbol=symbol)
    return bool(
        state
        and str(state.get("last_current_day_verified_date") or "") == trade_date.isoformat()
    )


def database_integrity(db_path: Path) -> dict[str, Any]:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        row_count = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
        source_count = conn.execute("SELECT COUNT(*) FROM daily_bar_sources").fetchone()[0]
        issue_count = conn.execute("SELECT COUNT(*) FROM daily_bar_import_issues").fetchone()[0]
        unique_issue_count = conn.execute(
            "SELECT COUNT(DISTINCT issue_fingerprint) FROM daily_bar_import_issues "
            "WHERE issue_fingerprint IS NOT NULL AND issue_fingerprint != ''"
        ).fetchone()[0]
        symbol_count = conn.execute(
            "SELECT COUNT(*) FROM daily_bar_sync_state WHERE verified_bar_count > 0"
        ).fetchone()[0]
        current_day_verified_count = conn.execute(
            """
            SELECT COUNT(*) FROM daily_bar_sync_state
            WHERE last_current_day_verified_date IS NOT NULL
            """
        ).fetchone()[0]
    return {
        "integrity_check": integrity,
        "foreign_key_error_count": len(foreign_keys),
        "daily_bar_count": int(row_count),
        "daily_bar_source_count": int(source_count),
        "daily_bar_issue_count": int(issue_count),
        "daily_bar_unique_issue_count": int(unique_issue_count),
        "symbol_count": int(symbol_count),
        "current_day_verified_symbol_count": int(current_day_verified_count),
        "ok": integrity == "ok" and not foreign_keys,
    }
