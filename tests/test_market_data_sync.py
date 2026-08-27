from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import sync_market_data as sync
from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars


class _FakeSyncProvider:
    def __init__(self, **kwargs: object) -> None:
        self.market_data_db_path = Path(str(kwargs["market_database_path"]))
        self.cache_dir = Path(str(kwargs["cache_dir"]))
        self.failed_symbol: str | None = None

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 253):
        if symbol == self.failed_symbol:
            raise RuntimeError("mock current-day failure")
        return [
            {
                "date": as_of - timedelta(days=lookback - 1 - index),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
            }
            for index in range(lookback)
        ]

    def get_taiex_series(self, as_of: date, lookback: int = 253):
        return [
            {"date": as_of - timedelta(days=lookback - 1 - index), "close": 10000.0, "change_points": 1.0}
            for index in range(lookback)
        ]

    def get_market_data_diagnostics(self):
        return {"redirect_308_unresolved_count": 0}

    def _load_basics(self):
        return {}


class MarketDataSyncTests(unittest.TestCase):
    def test_market_mapping_uses_canonical_state_for_exchange_exceptions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            bar = VerifiedDailyBar(
                market="TPEx",
                symbol="3260",
                trade_date=date(2026, 8, 26),
                open=1,
                high=2,
                low=1,
                close=2,
                volume=1,
                source_endpoint="test",
                source_url="https://test",
                source_cache_file="test.json",
                source_payload_sha256="test",
                source_fetched_at="2026-08-26T00:00:00+08:00",
            )
            import_verified_bars(db_path, [bar])
            self.assertEqual(sync._market_for_symbol("3260", db_path), "TPEx")

    def test_sync_manifest_passes_only_when_all_coverage_candidates_and_taiex_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(sync, "TwMarketProvider", _FakeSyncProvider):
                outputs = sync.run(
                    themes=["AI", "半導體"],
                    as_of=date(2026, 8, 26),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "complete")
            self.assertEqual(payload["coverage_count"], 54)
            self.assertEqual(payload["daily_success_count"], 54)
            self.assertEqual(payload["taiex"]["latest_trade_date"], "2026-08-26")
            self.assertTrue(outputs["markdown"].read_text(encoding="utf-8"))

    def test_dry_run_does_not_create_or_mutate_canonical_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(sync, "TwMarketProvider", side_effect=AssertionError("provider must not be built")):
                outputs = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 27),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    dry_run=True,
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "planned")
            self.assertTrue(payload["dry_run"])
            self.assertFalse((output_root / "cache" / "market" / "market_data.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
