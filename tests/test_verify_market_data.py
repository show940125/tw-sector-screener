from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts.verify_market_data import verify_database
from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars
from src.providers.market_data_store import init_market_data_db, upsert_index_bars


def _bars(count: int) -> list[VerifiedDailyBar]:
    start = date(2025, 8, 1)
    return [
        VerifiedDailyBar(
            market="TWSE",
            symbol="2330",
            trade_date=start + timedelta(days=index),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1000.0,
            source_endpoint="test",
            source_url="https://example.test/daily",
            source_cache_file="test.json",
            source_payload_sha256=f"hash-{index}",
            source_fetched_at="2026-08-26T00:00:00+08:00",
        )
        for index in range(count)
    ]


class VerifyMarketDataTests(unittest.TestCase):
    def test_verification_is_read_only_and_checks_lookback_and_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            init_market_data_db(db_path)
            import_verified_bars(db_path, _bars(253))
            upsert_index_bars(
                db_path,
                [
                    {
                        "index_code": "TAIEX",
                        "trade_date": date(2025, 8, 1) + timedelta(days=index),
                        "close": 10000.0,
                        "change_points": 1.0,
                        "source_endpoint": "test",
                        "source_url": "https://example.test/index",
                        "source_payload_sha256": f"index-{index}",
                    }
                    for index in range(253)
                ],
            )
            before = db_path.stat().st_mtime_ns
            with patch("scripts.verify_market_data._candidate_symbols", return_value=["2330"]):
                result = verify_database(
                    db_path,
                    themes=["AI"],
                    as_of=date(2026, 8, 29),
                    lookback=253,
                )
            self.assertEqual(result["status"], "complete")
            self.assertTrue(result["read_only"])
            self.assertEqual(result["themes_result"]["AI"]["verified_count"], 1)
            self.assertEqual(result["benchmark_result"]["status"], "verified")
            self.assertEqual(db_path.stat().st_mtime_ns, before)
            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(count, 253)

    def test_verification_fails_closed_when_weekday_current_day_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            init_market_data_db(db_path)
            import_verified_bars(db_path, _bars(253))
            upsert_index_bars(
                db_path,
                [
                    {
                        "index_code": "TAIEX",
                        "trade_date": date(2025, 8, 1) + timedelta(days=index),
                        "close": 10000.0,
                        "source_endpoint": "test",
                        "source_url": "https://example.test/index",
                        "source_payload_sha256": f"index-{index}",
                    }
                    for index in range(253)
                ],
            )
            with patch("scripts.verify_market_data._candidate_symbols", return_value=["2330"]):
                result = verify_database(
                    db_path,
                    themes=["AI"],
                    as_of=date(2026, 8, 28),
                    lookback=253,
                )
            self.assertEqual(result["status"], "failed")
            self.assertIn("current_day_missing", result["themes_result"]["AI"]["candidates"][0]["errors"][0])
            self.assertEqual(result["benchmark_result"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
