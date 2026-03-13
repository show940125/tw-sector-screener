from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


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

            CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol_period
            ON quarterly_company_fundamentals(symbol, period);

            CREATE INDEX IF NOT EXISTS idx_fundamentals_market_period
            ON quarterly_company_fundamentals(market, period);

            CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_status
            ON quarterly_company_fundamentals(fetch_status);

            CREATE INDEX IF NOT EXISTS idx_latest_period
            ON quarterly_symbol_latest(period);
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


def summarize_coverage(
    db_path: Path,
    symbols: list[tuple[str, str]],
    periods_required: int = 2,
    as_of_date: str | None = None,
    top_n: int = 3,
) -> dict[str, Any]:
    universe_count = len(symbols)
    if universe_count == 0:
        return {
            "universe_count": 0,
            "current_complete_count": 0,
            "current_complete_pct": 0.0,
            "previous_complete_count": 0,
            "previous_complete_pct": 0.0,
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
    status_counts = {"ok": 0, "unavailable": 0, "partial": 0, "fetch_failed": 0}
    top_candidate_gaps: list[dict[str, Any]] = []

    for index, (symbol, market) in enumerate(symbols, start=1):
        periods = get_latest_periods(
            db_path,
            symbol=symbol,
            market=market,
            periods=periods_required,
            as_of_date=as_of_date,
        )
        current = periods[0] if periods else None
        previous = periods[1] if len(periods) > 1 else None
        current_complete = _is_complete(current)
        previous_complete = _is_complete(previous)
        if current_complete:
            current_complete_count += 1
        if previous_complete:
            previous_complete_count += 1
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
        "ok_count": status_counts["ok"],
        "unavailable_count": status_counts["unavailable"],
        "partial_count": status_counts["partial"],
        "fetch_failed_count": status_counts["fetch_failed"],
        "top_candidate_gap_count": len(top_candidate_gaps),
        "top_candidate_gaps": top_candidate_gaps,
    }


def get_refresh_run(db_path: Path, run_id: str) -> dict[str, Any] | None:
    init_db(db_path)
    with closing(_connect(db_path)) as conn:
        row = conn.execute(
            "SELECT * FROM quarterly_refresh_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    payload = _row_to_dict(row)
    if payload is None:
        return None
    if payload.get("themes_json"):
        payload["themes_json"] = json.loads(str(payload["themes_json"]))
    if payload.get("warnings_json"):
        payload["warnings_json"] = json.loads(str(payload["warnings_json"]))
    return payload
