from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS portfolio_states (
  run_id TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  portfolio_id TEXT NOT NULL,
  equity REAL NOT NULL,
  cash REAL NOT NULL,
  holdings_value REAL NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (run_id, trade_date, portfolio_id)
);
CREATE TABLE IF NOT EXISTS orders (
  run_id TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  portfolio_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trades (
  run_id TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  portfolio_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  fill_price REAL NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily_equity (
  run_id TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  portfolio_id TEXT NOT NULL,
  equity REAL NOT NULL,
  cash REAL NOT NULL,
  unsettled_cash REAL NOT NULL,
  holdings_value REAL NOT NULL,
  return_pct REAL NOT NULL,
  drawdown_pct REAL NOT NULL,
  PRIMARY KEY (run_id, trade_date, portfolio_id)
);
CREATE TABLE IF NOT EXISTS analysis_refs (
  run_id TEXT NOT NULL,
  analysis_date TEXT NOT NULL,
  execution_date TEXT NOT NULL,
  path TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (run_id, analysis_date, execution_date)
);
"""


class SimulationStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def save_run(self, run_id: str, payload: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO runs(run_id, payload_json) VALUES (?, ?)",
            (run_id, json.dumps(payload, ensure_ascii=False, sort_keys=True)),
        )
        self.conn.commit()

    def save_state(self, run_id: str, trade_date: str, portfolio: dict[str, Any], metrics: dict[str, float]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO portfolio_states(run_id, trade_date, portfolio_id, equity, cash, holdings_value, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                trade_date,
                portfolio["portfolio_id"],
                metrics["equity"],
                metrics["cash"],
                metrics["holdings_value"],
                json.dumps(portfolio, ensure_ascii=False, sort_keys=True),
            ),
        )

    def latest_states(self, run_id: str) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT portfolio_id, payload_json
            FROM portfolio_states
            WHERE run_id = ?
              AND trade_date = (SELECT MAX(trade_date) FROM portfolio_states WHERE run_id = ?)
            """,
            (run_id, run_id),
        ).fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    def latest_states_before(self, run_id: str, trade_date: str) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT portfolio_id, payload_json
            FROM portfolio_states
            WHERE run_id = ?
              AND trade_date = (
                SELECT MAX(trade_date)
                FROM portfolio_states
                WHERE run_id = ?
                  AND trade_date < ?
              )
            """,
            (run_id, run_id, trade_date),
        ).fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    def daily_equity_rows(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT trade_date, portfolio_id, equity, cash, unsettled_cash, holdings_value, return_pct, drawdown_pct
            FROM daily_equity
            WHERE run_id = ?
            ORDER BY trade_date, portfolio_id
            """,
            (run_id,),
        ).fetchall()
        headers = ["trade_date", "portfolio_id", "equity", "cash", "unsettled_cash", "holdings_value", "return_pct", "drawdown_pct"]
        return [dict(zip(headers, row, strict=True)) for row in rows]

    def clear_execution_activity(self, run_id: str, trade_date: str) -> None:
        self.conn.execute("DELETE FROM orders WHERE run_id = ? AND trade_date = ?", (run_id, trade_date))
        self.conn.execute("DELETE FROM trades WHERE run_id = ? AND trade_date = ?", (run_id, trade_date))

    def save_orders(self, run_id: str, trade_date: str, orders: list[dict[str, Any]]) -> None:
        for order in orders:
            self.conn.execute(
                "INSERT INTO orders(run_id, trade_date, portfolio_id, symbol, side, status, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    trade_date,
                    order.get("portfolio_id"),
                    order.get("symbol"),
                    order.get("side"),
                    order.get("status"),
                    json.dumps(order, ensure_ascii=False, sort_keys=True),
                ),
            )

    def save_trades(self, run_id: str, trade_date: str, trades: list[dict[str, Any]]) -> None:
        for trade in trades:
            self.conn.execute(
                "INSERT INTO trades(run_id, trade_date, portfolio_id, symbol, side, quantity, fill_price, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    trade_date,
                    trade.get("portfolio_id"),
                    trade.get("symbol"),
                    trade.get("side"),
                    trade.get("quantity"),
                    trade.get("fill_price"),
                    json.dumps(trade, ensure_ascii=False, sort_keys=True),
                ),
            )

    def save_daily_equity(self, run_id: str, trade_date: str, portfolio_id: str, metrics: dict[str, float], initial_cash: float, peak: float) -> None:
        return_pct = ((metrics["equity"] / initial_cash) - 1.0) * 100.0 if initial_cash else 0.0
        drawdown_pct = ((metrics["equity"] / peak) - 1.0) * 100.0 if peak else 0.0
        self.conn.execute(
            """
            INSERT OR REPLACE INTO daily_equity(run_id, trade_date, portfolio_id, equity, cash, unsettled_cash, holdings_value, return_pct, drawdown_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                trade_date,
                portfolio_id,
                metrics["equity"],
                metrics["cash"],
                metrics["unsettled_cash"],
                metrics["holdings_value"],
                round(return_pct, 4),
                round(drawdown_pct, 4),
            ),
        )

    def save_analysis_ref(self, run_id: str, analysis_date: str, execution_date: str, path: Path, row_count: int) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO analysis_refs(run_id, analysis_date, execution_date, path, row_count) VALUES (?, ?, ?, ?, ?)",
            (run_id, analysis_date, execution_date, str(path), row_count),
        )

    def commit(self) -> None:
        self.conn.commit()


def write_daily_equity_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["trade_date", "portfolio_id", "equity", "cash", "unsettled_cash", "holdings_value", "return_pct", "drawdown_pct"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in headers})
