from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars, init_db
from src.providers.market_data_store import (
    database_integrity,
    ensure_market_data_db,
    record_source_payload,
    record_sync_run,
)
from src.providers.quarterly_store import init_db as init_quarterly_db
from src.providers.quarterly_store import insert_fundamental_snapshot


def _bar(day: date, close: float) -> VerifiedDailyBar:
    return VerifiedDailyBar(
        market="TWSE",
        symbol="2330",
        trade_date=day,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=1000,
        source_endpoint="test",
        source_url="https://example.test/stock-day",
        source_cache_file="test.json",
        source_payload_sha256=f"hash-{day.isoformat()}",
        source_fetched_at="2026-01-10T00:00:00+08:00",
    )


class MarketDataStoreTests(unittest.TestCase):
    def test_unified_db_migrates_legacy_daily_and_quarterly_rows_and_derives_periods(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daily_source = root / "daily_bars.sqlite"
            quarterly_source = root / "quarterly_fundamentals.sqlite"
            target = root / "market_data.sqlite"
            init_db(daily_source)
            import_verified_bars(
                daily_source,
                [_bar(date(2026, 1, 2), 101), _bar(date(2026, 1, 5), 103)],
            )
            init_quarterly_db(quarterly_source)
            insert_fundamental_snapshot(
                quarterly_source,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "115Q1",
                    "dataset_key": "test",
                    "source": "test",
                    "fetched_at": "2026-05-01T00:00:00+08:00",
                    "as_of_date": "2026-05-01",
                    "gross_margin": 50,
                    "eps": 10,
                    "roe": 20,
                    "revenue": 100,
                    "gross_profit": 50,
                    "net_income": 20,
                    "equity": 200,
                    "fetch_status": "ok",
                    "raw_payload_json": "{}",
                },
            )

            result = ensure_market_data_db(
                target,
                daily_source=daily_source,
                quarterly_source=quarterly_source,
            )

            self.assertEqual(result["integrity"]["ok"], True)
            conn = sqlite3.connect(target)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0], 2)
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM quarterly_company_fundamentals").fetchone()[0], 1
                )
                self.assertGreaterEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_sync_state "
                        "WHERE dataset_key IN ('daily_bars', 'period_bars', 'quarterly_fundamentals')"
                    ).fetchone()[0],
                    6,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT last_status FROM market_data_sync_state "
                        "WHERE dataset_key='daily_bars' AND market='TWSE' AND symbol='2330'"
                    ).fetchone()[0],
                    "migrated",
                )
                frequencies = {
                    row[0]
                    for row in conn.execute("SELECT DISTINCT frequency FROM period_bars").fetchall()
                }
                self.assertEqual(frequencies, {"W", "M", "Q", "Y"})
                monthly = conn.execute(
                    "SELECT close, volume, trading_day_count FROM period_bars "
                    "WHERE frequency='M' AND period_key='2026-01'"
                ).fetchone()
                self.assertEqual(tuple(monthly), (103.0, 2000.0, 2))
            finally:
                conn.close()

            second = ensure_market_data_db(
                target,
                daily_source=daily_source,
                quarterly_source=quarterly_source,
            )
            self.assertEqual(second["integrity"]["daily"]["daily_bar_count"], 2)
            self.assertEqual(second["period_bars"]["status"], "skipped")
            self.assertTrue(
                second["sources"]["legacy_daily"]["skipped"]
            )

    def test_sync_issue_fingerprint_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            issue = {
                "dataset_key": "daily_bars",
                "market": "TWSE",
                "symbol": "2330",
                "trade_date": "2026-08-26",
                "issue_type": "current_day_failure",
                "detail": "missing",
            }
            record_sync_run(
                db_path,
                run_id="run-1",
                as_of_date=date(2026, 8, 26),
                themes=["AI"],
                started_at="2026-08-26T16:30:00+08:00",
                finished_at="2026-08-26T16:31:00+08:00",
                status="failed",
                summary={},
                issues=[issue],
            )
            record_sync_run(
                db_path,
                run_id="run-2",
                as_of_date=date(2026, 8, 26),
                themes=["AI"],
                started_at="2026-08-26T16:32:00+08:00",
                finished_at="2026-08-26T16:33:00+08:00",
                status="failed",
                summary={},
                issues=[issue],
            )
            integrity = database_integrity(db_path)
            self.assertEqual(integrity["table_counts"]["market_data_sync_runs"], 2)
            self.assertEqual(integrity["table_counts"]["market_data_sync_issues"], 1)
            self.assertEqual(integrity["table_counts"]["market_data_quality_issues"], 1)
            self.assertEqual(integrity["table_counts"]["market_data_quality_issue_occurrences"], 2)

    def test_large_source_payload_is_externalised_with_hash_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "market_data.sqlite"
            payload_id = record_source_payload(
                db_path,
                dataset_key="test.large",
                request_method="GET",
                source_endpoint="test.large",
                source_url="https://example.test/large",
                payload={"body": "x" * 128},
                inline_limit_bytes=16,
                raw_storage_root=root / "raw",
            )
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT storage_mode, storage_uri, byte_size, raw_payload_json "
                    "FROM source_payloads WHERE payload_id = ?",
                    (payload_id,),
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(row[0], "external")
            self.assertTrue(Path(row[1]).exists())
            self.assertGreater(row[2], 16)
            descriptor = json.loads(row[3])
            self.assertEqual(descriptor["storage_mode"], "external")

    def test_non_json_payload_is_externalised_as_raw_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "market_data.sqlite"
            record_source_payload(
                db_path,
                dataset_key="test.binary",
                request_method="GET",
                source_endpoint="test.binary",
                source_url="https://example.test/binary",
                payload=b"not-json\x00payload",
                raw_storage_root=root / "raw",
            )
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT storage_mode, storage_uri, content_encoding FROM source_payloads"
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(tuple(row[:1]), ("external",))
            self.assertEqual(row[2], "binary")
            self.assertEqual(Path(row[1]).read_bytes(), b"not-json\x00payload")


if __name__ == "__main__":
    unittest.main()
