from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _parse_iso_date(value: str | None) -> date:
    if not value:
        return date.today()
    return date.fromisoformat(value[:10])


def _recent_periods(as_of_date: str, count: int) -> list[str]:
    cursor = _parse_iso_date(as_of_date)
    roc_year = cursor.year - 1911
    quarter = ((cursor.month - 1) // 3) + 1
    periods: list[str] = []
    for _ in range(max(count, 0)):
        periods.append(f"{roc_year}Q{quarter}")
        quarter -= 1
        if quarter == 0:
            quarter = 4
            roc_year -= 1
    return periods


def _period_sequence_from(anchor_period: str, count: int) -> list[str]:
    year_part, quarter_part = anchor_period.split("Q", 1)
    roc_year = int(year_part)
    quarter = int(quarter_part)
    periods: list[str] = []
    for _ in range(max(count, 0)):
        periods.append(f"{roc_year}Q{quarter}")
        quarter -= 1
        if quarter == 0:
            quarter = 4
            roc_year -= 1
    return periods


def init_db(db_path: Path) -> None:
    with closing(_connect(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS quarterly_company_fundamentals (
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                period TEXT NOT NULL,
                dataset_key TEXT NOT NULL,
                source TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                gross_margin REAL,
                eps REAL,
                roe REAL,
                revenue REAL,
                gross_profit REAL,
                net_income REAL,
                equity REAL,
                fetch_status TEXT NOT NULL,
                missing_reason TEXT,
                raw_payload_json TEXT,
                PRIMARY KEY (symbol, market, period, fetched_at)
            );

            CREATE TABLE IF NOT EXISTS quarterly_symbol_latest (
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                period TEXT NOT NULL,
                latest_fetched_at TEXT NOT NULL,
                PRIMARY KEY (symbol, market, period)
            );

            CREATE TABLE IF NOT EXISTS quarterly_refresh_runs (
                run_id TEXT PRIMARY KEY,
                as_of_date TEXT NOT NULL,
                theme_mode TEXT NOT NULL,
                themes_json TEXT NOT NULL,
                symbol_count INTEGER NOT NULL,
                current_complete_pct REAL NOT NULL,
                previous_complete_pct REAL NOT NULL,
                warnings_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS quarterly_backfill_queue (
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                period TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                next_retry_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, market, period)
            );

            CREATE TABLE IF NOT EXISTS quarterly_backfill_runs (
                run_id TEXT PRIMARY KEY,
                trigger_type TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                target_periods_json TEXT NOT NULL,
                queued_count INTEGER NOT NULL,
                completed_count INTEGER NOT NULL DEFAULT 0,
                unavailable_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol_period
            ON quarterly_company_fundamentals(symbol, period);

            CREATE INDEX IF NOT EXISTS idx_fundamentals_market_period
            ON quarterly_company_fundamentals(market, period);

            CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_status
            ON quarterly_company_fundamentals(fetch_status);

            CREATE INDEX IF NOT EXISTS idx_latest_period
            ON quarterly_symbol_latest(period);

            CREATE INDEX IF NOT EXISTS idx_backfill_queue_status_priority
            ON quarterly_backfill_queue(status, priority, updated_at);
            """
        )
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        conn.commit()


def insert_fundamental_snapshot(db_path: Path, snapshot: dict[str, Any]) -> None:
    init_db(db_path)
    params = {
        "symbol": str(snapshot["symbol"]).strip(),
        "market": str(snapshot["market"]).strip(),
        "period": str(snapshot["period"]).strip(),
        "dataset_key": str(snapshot.get("dataset_key") or "").strip(),
        "source": str(snapshot.get("source") or "").strip(),
        "fetched_at": str(snapshot["fetched_at"]).strip(),
        "as_of_date": str(snapshot["as_of_date"]).strip(),
        "gross_margin": snapshot.get("gross_margin"),
        "eps": snapshot.get("eps"),
        "roe": snapshot.get("roe"),
        "revenue": snapshot.get("revenue"),
        "gross_profit": snapshot.get("gross_profit"),
        "net_income": snapshot.get("net_income"),
        "equity": snapshot.get("equity"),
        "fetch_status": str(snapshot.get("fetch_status") or "unavailable").strip(),
        "missing_reason": snapshot.get("missing_reason"),
        "raw_payload_json": snapshot.get("raw_payload_json") or "{}",
    }
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO quarterly_company_fundamentals (
                symbol, market, period, dataset_key, source, fetched_at, as_of_date,
                gross_margin, eps, roe, revenue, gross_profit, net_income, equity,
                fetch_status, missing_reason, raw_payload_json
            ) VALUES (
                :symbol, :market, :period, :dataset_key, :source, :fetched_at, :as_of_date,
                :gross_margin, :eps, :roe, :revenue, :gross_profit, :net_income, :equity,
                :fetch_status, :missing_reason, :raw_payload_json
            )
            ON CONFLICT(symbol, market, period, fetched_at) DO UPDATE SET
                dataset_key=excluded.dataset_key,
                source=excluded.source,
                as_of_date=excluded.as_of_date,
                gross_margin=excluded.gross_margin,
                eps=excluded.eps,
                roe=excluded.roe,
                revenue=excluded.revenue,
                gross_profit=excluded.gross_profit,
                net_income=excluded.net_income,
                equity=excluded.equity,
                fetch_status=excluded.fetch_status,
                missing_reason=excluded.missing_reason,
                raw_payload_json=excluded.raw_payload_json
            """,
            params,
        )
        latest_valid = conn.execute(
            """
            SELECT fetched_at
            FROM quarterly_company_fundamentals
            WHERE symbol = ?
              AND market = ?
              AND period = ?
              AND fetch_status IN ('ok', 'partial', 'unavailable')
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (params["symbol"], params["market"], params["period"]),
        ).fetchone()
        latest_any = conn.execute(
            """
            SELECT fetched_at
            FROM quarterly_company_fundamentals
            WHERE symbol = ?
              AND market = ?
              AND period = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (params["symbol"], params["market"], params["period"]),
        ).fetchone()
        latest = latest_valid or latest_any
        if latest is not None:
            conn.execute(
                """
                INSERT INTO quarterly_symbol_latest(symbol, market, period, latest_fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, market, period) DO UPDATE SET
                    latest_fetched_at=excluded.latest_fetched_at
                """,
                (params["symbol"], params["market"], params["period"], latest["fetched_at"]),
            )
        conn.commit()


def upsert_refresh_run(db_path: Path, payload: dict[str, Any]) -> None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO quarterly_refresh_runs(
                run_id, as_of_date, theme_mode, themes_json, symbol_count,
                current_complete_pct, previous_complete_pct, warnings_json, created_at
            ) VALUES (
                :run_id, :as_of_date, :theme_mode, :themes_json, :symbol_count,
                :current_complete_pct, :previous_complete_pct, :warnings_json, :created_at
            )
            ON CONFLICT(run_id) DO UPDATE SET
                as_of_date=excluded.as_of_date,
                theme_mode=excluded.theme_mode,
                themes_json=excluded.themes_json,
                symbol_count=excluded.symbol_count,
                current_complete_pct=excluded.current_complete_pct,
                previous_complete_pct=excluded.previous_complete_pct,
                warnings_json=excluded.warnings_json,
                created_at=excluded.created_at
            """,
            {
                "run_id": payload["run_id"],
                "as_of_date": payload["as_of_date"],
                "theme_mode": payload["theme_mode"],
                "themes_json": payload["themes_json"],
                "symbol_count": payload["symbol_count"],
                "current_complete_pct": payload["current_complete_pct"],
                "previous_complete_pct": payload["previous_complete_pct"],
                "warnings_json": payload.get("warnings_json") or "[]",
                "created_at": payload["created_at"],
            },
        )
        conn.commit()


def get_latest_periods(
    db_path: Path,
    symbol: str,
    market: str,
    periods: int = 2,
    as_of_date: str | None = None,
    fetched_at_lte: str | None = None,
) -> list[dict[str, Any]]:
    init_db(db_path)
    conditions = ["symbol = ?", "market = ?"]
    params: list[Any] = [symbol, market]
    if as_of_date:
        conditions.append("as_of_date <= ?")
        params.append(as_of_date)
    if fetched_at_lte:
        conditions.append("fetched_at <= ?")
        params.append(fetched_at_lte)
    where_clause = " AND ".join(conditions)
    query = f"""
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY period
                    ORDER BY
                        CASE fetch_status
                            WHEN 'ok' THEN 0
                            WHEN 'partial' THEN 1
                            WHEN 'unavailable' THEN 2
                            ELSE 3
                        END,
                        fetched_at DESC
                ) AS rn
            FROM quarterly_company_fundamentals
            WHERE {where_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY period DESC
        LIMIT ?
    """
    params.append(periods)
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(query, params).fetchall()
    return [_row_to_dict(row) or {} for row in rows]


def get_period_rows(
    db_path: Path,
    symbol: str,
    market: str,
    periods: list[str],
    as_of_date: str | None = None,
) -> list[dict[str, Any]]:
    init_db(db_path)
    if not periods:
        return []
    with closing(_connect(db_path)) as conn:
        rows = [_load_period_row(conn, symbol, market, period, as_of_date) for period in periods]
    return [row for row in rows if row]


def _load_period_row(conn: sqlite3.Connection, symbol: str, market: str, period: str, as_of_date: str | None) -> dict[str, Any] | None:
    conditions = ["symbol = ?", "market = ?", "period = ?"]
    params: list[Any] = [symbol, market, period]
    if as_of_date:
        conditions.append("as_of_date <= ?")
        params.append(as_of_date)
    row = conn.execute(
        f"""
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY period
                    ORDER BY
                        CASE fetch_status
                            WHEN 'ok' THEN 0
                            WHEN 'partial' THEN 1
                            WHEN 'unavailable' THEN 2
                            ELSE 3
                        END,
                        fetched_at DESC
                ) AS rn
            FROM quarterly_company_fundamentals
            WHERE {" AND ".join(conditions)}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        LIMIT 1
        """,
        params,
    ).fetchone()
    return _row_to_dict(row)


def get_quality_history_depth(
    db_path: Path,
    symbol: str,
    market: str,
    as_of_date: str,
    history_depth: int = 8,
) -> dict[str, Any]:
    init_db(db_path)
    target_periods = _recent_periods(as_of_date, history_depth)
    complete_periods: list[str] = []
    available_periods: list[str] = []
    missing_periods: list[str] = []
    with closing(_connect(db_path)) as conn:
        latest_row = conn.execute(
            """
            SELECT period
            FROM quarterly_company_fundamentals
            WHERE symbol = ? AND market = ? AND as_of_date <= ?
            ORDER BY period DESC, fetched_at DESC
            LIMIT 1
            """,
            (symbol, market, as_of_date),
        ).fetchone()
        if latest_row and latest_row["period"]:
            target_periods = _period_sequence_from(str(latest_row["period"]), history_depth)
        for period in target_periods:
            row = _load_period_row(conn, symbol, market, period, as_of_date)
            if row is None:
                missing_periods.append(period)
                continue
            available_periods.append(period)
            if all(isinstance(row.get(key), (int, float)) for key in ["gross_margin", "eps", "roe"]):
                complete_periods.append(period)
            else:
                missing_periods.append(period)
    complete_count = len(complete_periods)
    return {
        "history_depth_target": history_depth,
        "target_periods": target_periods,
        "available_periods": available_periods,
        "complete_periods": complete_periods,
        "missing_periods": missing_periods,
        "complete_period_count": complete_count,
        "complete_pct": round((complete_count / history_depth) * 100.0, 2) if history_depth else 0.0,
    }


def summarize_coverage(
    db_path: Path,
    symbols: list[tuple[str, str]],
    periods_required: int = 2,
    as_of_date: str | None = None,
    top_n: int = 3,
    history_depth: int = 8,
    anchor_period: str | None = None,
) -> dict[str, Any]:
    universe_count = len(symbols)
    if universe_count == 0:
        return {
            "universe_count": 0,
            "current_complete_count": 0,
            "current_complete_pct": 0.0,
            "previous_complete_count": 0,
            "previous_complete_pct": 0.0,
            "history_complete_count": 0,
            "history_complete_pct": 0.0,
            "ok_count": 0,
            "unavailable_count": 0,
            "partial_count": 0,
            "fetch_failed_count": 0,
            "top_candidate_gap_count": 0,
            "top_candidate_gaps": [],
        }

    def _is_complete(row: dict[str, Any] | None) -> bool:
        if not row:
            return False
        return all(isinstance(row.get(key), (int, float)) for key in ["gross_margin", "eps", "roe"])

    current_complete_count = 0
    previous_complete_count = 0
    history_complete_count = 0
    status_counts = {"ok": 0, "unavailable": 0, "partial": 0, "fetch_failed": 0}
    top_candidate_gaps: list[dict[str, Any]] = []
    anchor_periods = _period_sequence_from(anchor_period, max(periods_required, history_depth)) if anchor_period else []

    for index, (symbol, market) in enumerate(symbols, start=1):
        if anchor_periods:
            target_periods = anchor_periods
            periods = get_period_rows(
                db_path,
                symbol=symbol,
                market=market,
                periods=target_periods[:periods_required],
                as_of_date=as_of_date,
            )
            current = periods[0] if periods else None
            previous = periods[1] if len(periods) > 1 else None
            history_complete = 0
            for period in target_periods[:history_depth]:
                row = get_period_rows(db_path, symbol=symbol, market=market, periods=[period], as_of_date=as_of_date)
                item = row[0] if row else None
                if _is_complete(item):
                    history_complete += 1
        else:
            periods = get_latest_periods(
                db_path,
                symbol=symbol,
                market=market,
                periods=periods_required,
                as_of_date=as_of_date,
            )
            current = periods[0] if periods else None
            previous = periods[1] if len(periods) > 1 else None
            history = get_quality_history_depth(
                db_path,
                symbol,
                market,
                as_of_date or date.today().isoformat(),
                history_depth=history_depth,
            )
            history_complete = history["complete_period_count"]
        current_complete = _is_complete(current)
        previous_complete = _is_complete(previous)
        if current_complete:
            current_complete_count += 1
        if previous_complete:
            previous_complete_count += 1
        if history_complete >= history_depth:
            history_complete_count += 1
        status = str((current or {}).get("fetch_status") or "fetch_failed")
        if status not in status_counts:
            status = "fetch_failed"
        status_counts[status] += 1
        if index <= top_n and (not current_complete or not previous_complete):
            top_candidate_gaps.append(
                {
                    "rank": index,
                    "symbol": symbol,
                    "quality_fetch_status": (current or {}).get("fetch_status") or "unavailable",
                    "quality_missing_reason": (
                        (current or {}).get("missing_reason")
                        or ("previous_period_unavailable" if current_complete and not previous_complete else "unavailable")
                    ),
                }
            )

    return {
        "universe_count": universe_count,
        "current_complete_count": current_complete_count,
        "current_complete_pct": round((current_complete_count / universe_count) * 100.0, 2),
        "previous_complete_count": previous_complete_count,
        "previous_complete_pct": round((previous_complete_count / universe_count) * 100.0, 2),
        "history_complete_count": history_complete_count,
        "history_complete_pct": round((history_complete_count / universe_count) * 100.0, 2),
        "ok_count": status_counts["ok"],
        "unavailable_count": status_counts["unavailable"],
        "partial_count": status_counts["partial"],
        "fetch_failed_count": status_counts["fetch_failed"],
        "top_candidate_gap_count": len(top_candidate_gaps),
        "top_candidate_gaps": top_candidate_gaps,
    }


def _has_valid_snapshot(conn: sqlite3.Connection, symbol: str, market: str, period: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM quarterly_company_fundamentals
        WHERE symbol = ?
          AND market = ?
          AND period = ?
          AND fetch_status IN ('ok', 'partial')
        LIMIT 1
        """,
        (symbol, market, period),
    ).fetchone()
    return row is not None


def enqueue_backfill_targets(
    db_path: Path,
    symbols: list[tuple[str, str]],
    periods: list[str],
    priority: int = 100,
    source_hint: str = "manual",
) -> int:
    init_db(db_path)
    now_iso = datetime.now().replace(microsecond=0).isoformat()
    queued_count = 0
    seen: set[tuple[str, str, str]] = set()
    with closing(_connect(db_path)) as conn:
        for symbol, market in symbols:
            for period in periods:
                key = (symbol, market, period)
                if key in seen:
                    continue
                seen.add(key)
                if _has_valid_snapshot(conn, symbol, market, period):
                    continue
                existing = conn.execute(
                    """
                    SELECT status
                    FROM quarterly_backfill_queue
                    WHERE symbol = ? AND market = ? AND period = ?
                    """,
                    (symbol, market, period),
                ).fetchone()
                if existing and existing["status"] == "done":
                    continue
                conn.execute(
                    """
                    INSERT INTO quarterly_backfill_queue(
                        symbol, market, period, priority, status, source,
                        attempt_count, last_attempt_at, next_retry_at, last_error, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', ?, 0, NULL, NULL, NULL, ?)
                    ON CONFLICT(symbol, market, period) DO UPDATE SET
                        priority = excluded.priority,
                        status = CASE
                            WHEN quarterly_backfill_queue.status = 'done' THEN quarterly_backfill_queue.status
                            ELSE 'pending'
                        END,
                        source = excluded.source,
                        updated_at = excluded.updated_at
                    """,
                    (symbol, market, period, priority, source_hint, now_iso),
                )
                if not existing or existing["status"] != "done":
                    queued_count += 1
        conn.commit()
    return queued_count


def claim_backfill_batch(db_path: Path, limit: int, now_iso: str) -> list[dict[str, Any]]:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM quarterly_backfill_queue
            WHERE status = 'pending'
               OR (status IN ('failed', 'unavailable') AND (next_retry_at IS NULL OR next_retry_at <= ?))
            ORDER BY priority ASC, updated_at ASC
            LIMIT ?
            """,
            (now_iso, limit),
        ).fetchall()
    return [_row_to_dict(row) or {} for row in rows]


def mark_backfill_result(
    db_path: Path,
    symbol: str,
    market: str,
    period: str,
    status: str,
    error: str | None,
    attempted_at: str,
) -> None:
    init_db(db_path)
    next_retry_at = attempted_at if status in {"failed", "unavailable"} else None
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            UPDATE quarterly_backfill_queue
            SET status = ?,
                attempt_count = attempt_count + 1,
                last_attempt_at = ?,
                next_retry_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE symbol = ? AND market = ? AND period = ?
            """,
            (status, attempted_at, next_retry_at, error, attempted_at, symbol, market, period),
        )
        conn.commit()


def create_backfill_run(
    db_path: Path,
    trigger_type: str,
    as_of_date: str,
    scope_json: str,
    target_periods_json: str,
    queued_count: int,
    started_at: str,
) -> str:
    init_db(db_path)
    run_id = f"backfill-{trigger_type}-{uuid.uuid4().hex[:8]}"
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO quarterly_backfill_runs(
                run_id, trigger_type, as_of_date, scope_json, target_periods_json,
                queued_count, completed_count, unavailable_count, failed_count,
                started_at, finished_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, NULL, 'running')
            """,
            (run_id, trigger_type, as_of_date, scope_json, target_periods_json, queued_count, started_at),
        )
        conn.commit()
    return run_id


def finish_backfill_run(
    db_path: Path,
    run_id: str,
    completed_count: int,
    unavailable_count: int,
    failed_count: int,
    finished_at: str,
    status: str,
) -> None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        conn.execute(
            """
            UPDATE quarterly_backfill_runs
            SET completed_count = ?,
                unavailable_count = ?,
                failed_count = ?,
                finished_at = ?,
                status = ?
            WHERE run_id = ?
            """,
            (completed_count, unavailable_count, failed_count, finished_at, status, run_id),
        )
        conn.commit()


def get_refresh_run(db_path: Path, run_id: str) -> dict[str, Any] | None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        row = conn.execute("SELECT * FROM quarterly_refresh_runs WHERE run_id = ?", (run_id,)).fetchone()
    payload = _row_to_dict(row)
    if payload is None:
        return None
    if payload.get("themes_json"):
        payload["themes_json"] = json.loads(str(payload["themes_json"]))
    if payload.get("warnings_json"):
        payload["warnings_json"] = json.loads(str(payload["warnings_json"]))
    return payload


def get_backfill_run(db_path: Path, run_id: str) -> dict[str, Any] | None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        row = conn.execute("SELECT * FROM quarterly_backfill_runs WHERE run_id = ?", (run_id,)).fetchone()
    payload = _row_to_dict(row)
    if payload is None:
        return None
    if payload.get("scope_json"):
        payload["scope_json"] = json.loads(str(payload["scope_json"]))
    if payload.get("target_periods_json"):
        payload["target_periods_json"] = json.loads(str(payload["target_periods_json"]))
    return payload


def get_latest_refresh_run(db_path: Path, theme: str, theme_mode: str) -> dict[str, Any] | None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM quarterly_refresh_runs
            WHERE theme_mode = ?
            ORDER BY created_at DESC
            LIMIT 30
            """,
            (theme_mode,),
        ).fetchall()
    for row in rows:
        payload = _row_to_dict(row) or {}
        themes = json.loads(str(payload.get("themes_json") or "[]"))
        if theme in themes:
            payload["themes_json"] = themes
            payload["warnings_json"] = json.loads(str(payload.get("warnings_json") or "[]"))
            return payload
    return None
