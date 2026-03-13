import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.quarterly_store import (
    get_latest_periods,
    get_refresh_run,
    init_db,
    insert_fundamental_snapshot,
    summarize_coverage,
    upsert_refresh_run,
)


class QuarterlyStoreTests(unittest.TestCase):
    def test_latest_periods_prefers_latest_valid_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quarterly.sqlite"
            init_db(db_path)
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T08:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 39.0,
                    "eps": 4.9,
                    "roe": 18.8,
                    "revenue": 1000.0,
                    "gross_profit": 390.0,
                    "net_income": 94.0,
                    "equity": 2000.0,
                    "fetch_status": "partial",
                    "missing_reason": "partial_metrics",
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 40.0,
                    "eps": 5.0,
                    "roe": 20.0,
                    "revenue": 1000.0,
                    "gross_profit": 400.0,
                    "net_income": 100.0,
                    "equity": 2000.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "113Q3",
                    "dataset_key": "twse_ci",
                    "source": "snapshot",
                    "fetched_at": "2026-03-11T09:00:00",
                    "as_of_date": "2026-03-11",
                    "gross_margin": 38.0,
                    "eps": 4.2,
                    "roe": 16.84,
                    "revenue": 1000.0,
                    "gross_profit": 380.0,
                    "net_income": 80.0,
                    "equity": 1900.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )

            periods = get_latest_periods(db_path, "2330", "TWSE", periods=2)

        self.assertEqual([item["period"] for item in periods], ["114Q4", "113Q3"])
        self.assertEqual(periods[0]["fetch_status"], "ok")
        self.assertEqual(periods[0]["fetched_at"], "2026-03-12T09:00:00")

    def test_fetch_failed_snapshot_does_not_override_latest_valid_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quarterly.sqlite"
            init_db(db_path)
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 40.0,
                    "eps": 5.0,
                    "roe": 20.0,
                    "revenue": 1000.0,
                    "gross_profit": 400.0,
                    "net_income": 100.0,
                    "equity": 2000.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T10:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "fetch_failed",
                    "missing_reason": "fetch_failed",
                    "raw_payload_json": "{}",
                },
            )

            periods = get_latest_periods(db_path, "2330", "TWSE", periods=1)

        self.assertEqual(periods[0]["fetch_status"], "ok")
        self.assertEqual(periods[0]["fetched_at"], "2026-03-12T09:00:00")

    def test_summarize_coverage_uses_point_in_time_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quarterly.sqlite"
            init_db(db_path)
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 40.0,
                    "eps": 5.0,
                    "roe": 20.0,
                    "revenue": 1000.0,
                    "gross_profit": 400.0,
                    "net_income": 100.0,
                    "equity": 2000.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "113Q3",
                    "dataset_key": "twse_ci",
                    "source": "snapshot",
                    "fetched_at": "2026-03-11T09:00:00",
                    "as_of_date": "2026-03-11",
                    "gross_margin": 38.0,
                    "eps": 4.2,
                    "roe": 16.84,
                    "revenue": 1000.0,
                    "gross_profit": 380.0,
                    "net_income": 80.0,
                    "equity": 1900.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                db_path,
                {
                    "symbol": "2317",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "twse_openapi",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 8.0,
                    "eps": 3.5,
                    "roe": 9.0,
                    "revenue": 2000.0,
                    "gross_profit": 160.0,
                    "net_income": 45.0,
                    "equity": 2000.0,
                    "fetch_status": "partial",
                    "missing_reason": "previous_period_unavailable",
                    "raw_payload_json": "{}",
                },
            )
            upsert_refresh_run(
                db_path,
                {
                    "run_id": "run-1",
                    "as_of_date": date(2026, 3, 12).isoformat(),
                    "theme_mode": "strict",
                    "themes_json": '["AI"]',
                    "symbol_count": 2,
                    "current_complete_pct": 100.0,
                    "previous_complete_pct": 50.0,
                    "warnings_json": "[]",
                    "created_at": "2026-03-12T10:00:00",
                },
            )

            summary = summarize_coverage(db_path, [("2330", "TWSE"), ("2317", "TWSE")], periods_required=2)
            refresh_run = get_refresh_run(db_path, "run-1")

        self.assertEqual(summary["universe_count"], 2)
        self.assertEqual(summary["current_complete_count"], 2)
        self.assertEqual(summary["previous_complete_count"], 1)
        self.assertEqual(summary["current_complete_pct"], 100.0)
        self.assertEqual(summary["previous_complete_pct"], 50.0)
        self.assertIsNotNone(refresh_run)
        self.assertEqual(refresh_run["symbol_count"], 2)
        self.assertEqual(refresh_run["run_id"], "run-1")

    def test_init_db_creates_expected_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quarterly.sqlite"
            init_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                tables = {
                    row[0]
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
            finally:
                conn.close()

        self.assertIn("quarterly_company_fundamentals", tables)
        self.assertIn("quarterly_symbol_latest", tables)
        self.assertIn("quarterly_refresh_runs", tables)
        self.assertIn("schema_meta", tables)


if __name__ == "__main__":
    unittest.main()
