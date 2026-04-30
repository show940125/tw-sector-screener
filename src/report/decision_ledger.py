from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
  id INTEGER PRIMARY KEY,
  skill_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  theme TEXT,
  as_of TEXT NOT NULL,
  recommendation TEXT NOT NULL,
  confidence_score REAL NOT NULL,
  risk_score REAL NOT NULL,
  action_view TEXT,
  idea_score REAL,
  rank_score REAL,
  close_price REAL,
  target_low REAL,
  target_base REAL,
  target_high REAL,
  invalidation_json TEXT NOT NULL,
  evidence_refs_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outcomes (
  decision_id INTEGER NOT NULL,
  horizon_days INTEGER NOT NULL,
  return_pct REAL,
  benchmark_return_pct REAL,
  excess_return_pct REAL,
  max_drawdown_pct REAL,
  checked_at TEXT NOT NULL,
  PRIMARY KEY (decision_id, horizon_days)
);
"""


def write_decision(ledger_path: Path, skill_name: str, payload: dict[str, Any], theme: str | None = None) -> int:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    target = payload.get("target_range") or {}
    conn = sqlite3.connect(ledger_path)
    try:
        conn.executescript(SCHEMA)
        cursor = conn.execute(
            """
            INSERT INTO decisions (
              skill_name, symbol, theme, as_of, recommendation, confidence_score, risk_score,
              action_view, idea_score, rank_score, close_price, target_low, target_base,
              target_high, invalidation_json, evidence_refs_json, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                skill_name,
                str(payload.get("symbol") or ""),
                theme,
                str(payload.get("as_of") or ""),
                str(payload.get("recommendation") or "持有"),
                float(payload.get("confidence_score") or 0.0),
                float(payload.get("risk_score") or 0.0),
                str(payload.get("action_view") or ""),
                payload.get("idea_score"),
                payload.get("rank_score"),
                payload.get("close_price"),
                target.get("low"),
                target.get("base"),
                target.get("high"),
                json.dumps(payload.get("invalidation_conditions") or [], ensure_ascii=False),
                json.dumps(payload.get("evidence_refs") or [], ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()
