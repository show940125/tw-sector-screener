from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.daily_bar_cache_importer import (
    _cache_request_specs,
    collect_verified_cached_bars,
    import_market_cache,
)
from src.providers.daily_bar_store import (
    VerifiedDailyBar,
    database_integrity,
    get_bars,
    init_db,
    import_verified_bars,
)


class DailyBarStoreTests(unittest.TestCase):
    def test_schema_and_source_preference_preserve_all_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "daily_bars.sqlite"
            init_db(db_path)
            primary = VerifiedDailyBar(
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 1, 2),
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                volume=1000.0,
                source_endpoint="twse.stock_day.primary",
                source_url="https://primary",
                source_cache_file="primary.json",
                source_payload_sha256="primary-hash",
                source_fetched_at="2026-01-03T00:00:00+08:00",
                source_priority=10,
            )
            fallback = VerifiedDailyBar(
                **{
                    **primary.__dict__,
                    "source_endpoint": "twse.stock_day.fallback",
                    "source_url": "https://fallback",
                    "source_cache_file": "fallback.json",
                    "source_payload_sha256": "fallback-hash",
                    "source_priority": 20,
                }
            )
            stats = import_verified_bars(db_path, [fallback, primary])
            rows = get_bars(db_path, market="TWSE", symbol="2330")
            self.assertEqual(stats.inserted_rows, 1)
            self.assertEqual(stats.updated_rows, 1)
            self.assertEqual(stats.duplicate_rows, 0)
            self.assertEqual(rows[0]["source_endpoint"], "twse.stock_day.primary")
            self.assertEqual(database_integrity(db_path)["ok"], True)

    def test_cache_collector_maps_hash_and_rejects_invalid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            spec = _cache_request_specs("2330", "TWSE", date(2026, 1, 1))[0]
            payload = {
                "stat": "OK",
                "fields": ["日期", "成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價"],
                "data": [
                    ["115/01/02", "1,000", "100,000", "100", "102", "99", "101"],
                    ["115/01/03", "--", "--", "--", "--", "--", "--"],
                ],
            }
            (cache_dir / spec.cache_name).write_text(json.dumps(payload), encoding="utf-8")
            bars, stats, issues, symbols = collect_verified_cached_bars(
                cache_dir,
                themes=["半導體"],
                start_month=date(2026, 1, 1),
                end_month=date(2026, 1, 1),
                max_trade_date=date(2026, 1, 2),
            )
            self.assertIn("2330", symbols)
            self.assertEqual(stats.source_payloads_matched, 1)
            self.assertEqual(stats.source_payloads_valid, 1)
            self.assertEqual(len(bars), 1)
            self.assertEqual(bars[0].trade_date, date(2026, 1, 2))
            self.assertEqual(stats.invalid_rows, 1)
            self.assertEqual(issues[0]["issue_type"], "invalid_ohlcv")

    def test_import_manifest_marks_historical_import_not_current_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            cache_dir.mkdir()
            db_path = Path(tmp) / "daily_bars.sqlite"
            manifest_path = Path(tmp) / "manifest.json"
            spec = _cache_request_specs("2330", "TWSE", date(2026, 1, 1))[0]
            payload = {
                "stat": "OK",
                "fields": ["日期", "成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價"],
                "data": [["115/01/02", "1,000", "100,000", "100", "102", "99", "101"]],
            }
            (cache_dir / spec.cache_name).write_text(json.dumps(payload), encoding="utf-8")
            summary = import_market_cache(
                cache_dir=cache_dir,
                database_path=db_path,
                themes=["半導體"],
                start_month=date(2026, 1, 1),
                end_month=date(2026, 1, 1),
                max_trade_date=date(2026, 1, 2),
                manifest_path=manifest_path,
            )
            self.assertEqual(summary["status"], "complete")
            self.assertTrue(summary["historical_import_only"])
            self.assertFalse(summary["current_day_verified"])
            self.assertTrue(manifest_path.exists())
            self.assertFalse(json.loads(manifest_path.read_text(encoding="utf-8"))["current_day_verified"])


if __name__ == "__main__":
    unittest.main()
