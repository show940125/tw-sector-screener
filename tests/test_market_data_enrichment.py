from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.market_data_adapters import AdapterRegistry
from src.providers.market_data_store import (
    backfill_sync_state_from_canonical,
    begin_completeness_run,
    finish_completeness_run,
    get_partition_state,
    init_market_data_db,
    partition_is_verified,
    record_data_gap,
    upsert_partition_state,
)
from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars


class MarketDataEnrichmentFoundationTests(unittest.TestCase):
    def test_adapter_registry_rejects_incomplete_contract(self) -> None:
        class IncompleteAdapter:
            dataset_key = "incomplete"

        with self.assertRaisesRegex(TypeError, "does not implement contract"):
            AdapterRegistry([IncompleteAdapter()])

    def test_schema_v4_contains_partition_gap_and_completeness_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            init_market_data_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                schema_version = conn.execute(
                    "SELECT value FROM schema_meta WHERE key = 'market_data_schema_version'"
                ).fetchone()[0]
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                payload_columns = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(source_payloads)")
                }
                fact_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(financial_fact_observations)"
                    )
                }
                quarterly_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(quarterly_company_fundamentals)"
                    )
                }
                period_columns = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(period_bars)")
                }
            finally:
                conn.close()

            self.assertEqual(schema_version, "4")
            self.assertTrue(
                {
                    "market_data_partition_state",
                    "market_data_gap_ledger",
                    "market_data_completeness_runs",
                }.issubset(tables)
            )
            self.assertIn("availability_precision", payload_columns)
            self.assertIn("availability_precision", fact_columns)
            self.assertIn("availability_precision", quarterly_columns)
            self.assertIn("availability_precision", period_columns)

    def test_partition_checkpoint_is_idempotent_and_requires_refetch_after_range_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            init_market_data_db(db_path)
            values = dict(
                db_path=db_path,
                dataset_key="daily_bars",
                market="TWSE",
                symbol="2330",
                partition_key="2026-08",
                requested_from=date(2026, 8, 1),
                requested_to=date(2026, 8, 31),
                request_method="GET",
                request_url="https://example.test/daily?date=20260801",
                request_body_sha256="request-v1",
                payload_sha256="payload-v1",
                first_effective_date=date(2026, 8, 3),
                last_effective_date=date(2026, 8, 26),
                row_count=18,
                status="verified",
                last_verified_at="2026-08-27T08:00:00+08:00",
                last_run_id="run-1",
            )
            upsert_partition_state(**values)
            upsert_partition_state(**values)

            state = get_partition_state(
                db_path,
                dataset_key="daily_bars",
                market="TWSE",
                symbol="2330",
                partition_key="2026-08",
            )
            self.assertIsNotNone(state)
            self.assertEqual(state["payload_sha256"], "payload-v1")
            self.assertTrue(
                partition_is_verified(
                    db_path,
                    dataset_key="daily_bars",
                    market="TWSE",
                    symbol="2330",
                    partition_key="2026-08",
                    requested_from=date(2026, 8, 1),
                    requested_to=date(2026, 8, 31),
                    request_body_sha256="request-v1",
                )
            )
            self.assertFalse(
                partition_is_verified(
                    db_path,
                    dataset_key="daily_bars",
                    market="TWSE",
                    symbol="2330",
                    partition_key="2026-08",
                    requested_from=date(2026, 8, 1),
                    requested_to=date(2026, 9, 1),
                    request_body_sha256="request-v1",
                )
            )
            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_partition_state"
                    ).fetchone()[0],
                    1,
                )
            finally:
                conn.close()

    def test_gap_ledger_and_completeness_run_are_idempotent_and_traceable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            init_market_data_db(db_path)
            record_data_gap(
                db_path,
                dataset_key="monthly_revenue",
                market="TWSE",
                symbol="2330",
                partition_key="2021-01",
                reason="source_limit",
                detail="official archive did not return a payload",
                run_id="run-1",
            )
            record_data_gap(
                db_path,
                dataset_key="monthly_revenue",
                market="TWSE",
                symbol="2330",
                partition_key="2021-01",
                reason="source_limit",
                detail="official archive did not return a payload",
                run_id="run-2",
            )
            completeness_id = begin_completeness_run(
                db_path,
                run_id="run-2",
                dataset_key="monthly_revenue",
                expected_rows=60,
                expected_partitions=60,
            )
            finish_completeness_run(
                db_path,
                completeness_run_id=completeness_id,
                status="partial",
                actual_rows=59,
                actual_partitions=59,
                missing_partitions=["2021-01"],
                summary={"source_limit": 1},
            )
            conn = sqlite3.connect(db_path)
            try:
                gap = conn.execute(
                    "SELECT occurrence_count, status, latest_run_id "
                    "FROM market_data_gap_ledger"
                ).fetchone()
                run = conn.execute(
                    "SELECT status, actual_rows, missing_partitions_json "
                    "FROM market_data_completeness_runs"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(tuple(gap), (2, "open", "run-2"))
            self.assertEqual(run[0], "partial")
            self.assertEqual(run[1], 59)
            self.assertIn("2021-01", run[2])

    def test_v4_backfill_runs_even_when_v3_marker_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            bar = VerifiedDailyBar(
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000.0,
                source_endpoint="twse.stock_day",
                source_url="https://example.test/stock-day",
                source_cache_file="fixture.json",
                source_payload_sha256="fixture-hash",
                source_fetched_at="2026-08-27T00:00:00+08:00",
            )
            import_verified_bars(db_path, [bar])
            init_market_data_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO schema_meta(key, value) "
                    "VALUES('market_data_sync_state_backfill_v3', 'old-run')"
                )
                conn.commit()
            finally:
                conn.close()

            result = backfill_sync_state_from_canonical(db_path)
            state = get_partition_state(
                db_path,
                dataset_key="daily_bars",
                market="TWSE",
                symbol="2330",
                partition_key="2026-08",
            )
            self.assertEqual(result["status"], "backfilled")
            self.assertIsNotNone(state)
            self.assertEqual(state["status"], "migrated")


if __name__ == "__main__":
    unittest.main()
