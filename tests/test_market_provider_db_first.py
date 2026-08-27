from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from src.providers.daily_bar_store import (
    VerifiedDailyBar,
    import_verified_bars,
    mark_current_day_verified,
)
from src.providers.market_data_store import upsert_index_bars
from src.providers.tw_market_provider import MarketDataFetchError, TwMarketProvider


def _daily_bars(end: date, count: int = 253) -> list[VerifiedDailyBar]:
    start = end - timedelta(days=count - 1)
    return [
        VerifiedDailyBar(
            market="TWSE",
            symbol="2330",
            trade_date=start + timedelta(days=index),
            open=100 + index,
            high=101 + index,
            low=99 + index,
            close=100.5 + index,
            volume=1000,
            source_endpoint="seed",
            source_url="https://seed",
            source_cache_file="seed.json",
            source_payload_sha256=f"seed-{index}",
            source_fetched_at="2026-08-26T00:00:00+08:00",
        )
        for index in range(count)
    ]


class MarketProviderDbFirstTests(unittest.TestCase):
    def test_complete_current_day_history_is_a_db_hit_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            import_verified_bars(db_path, _daily_bars(date(2026, 8, 26)))
            mark_current_day_verified(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
            )
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            with patch.object(provider, "_fetch_twse_month_bars", side_effect=AssertionError("network")):
                bars = provider.get_ohlcv("2330", "TWSE", date(2026, 8, 26), lookback=253)
            self.assertEqual(len(bars), 253)
            self.assertEqual(bars[-1]["date"], date(2026, 8, 26))
            self.assertEqual(provider.get_market_data_diagnostics()["db_hit_count"], 1)
            self.assertEqual(provider.get_market_data_diagnostics()["incremental_fetch_count"], 0)

    def test_history_hit_fetches_only_current_month_and_appends_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            import_verified_bars(db_path, _daily_bars(date(2026, 8, 21)))
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            current = _daily_bars(date(2026, 8, 26), 1)[0]
            current = VerifiedDailyBar(**{**current.__dict__, "source_endpoint": "incremental"})
            with patch.object(provider, "_fetch_twse_month_bars", return_value=[current]) as fetch:
                bars = provider.get_ohlcv("2330", "TWSE", date(2026, 8, 26), lookback=253)
            fetch.assert_called_once()
            self.assertEqual(len(bars), 253)
            self.assertEqual(bars[-1]["date"], date(2026, 8, 26))
            diagnostics = provider.get_market_data_diagnostics()
            self.assertEqual(diagnostics["db_missing_count"], 1)
            self.assertGreaterEqual(diagnostics["db_write_count"], 1)

    def test_current_day_gap_fails_closed_even_when_old_history_is_long_enough(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            import_verified_bars(db_path, _daily_bars(date(2026, 8, 21)))
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            with patch.object(provider, "_fetch_twse_month_bars", return_value=[]):
                with self.assertRaises(MarketDataFetchError) as context:
                    provider.get_ohlcv("2330", "TWSE", date(2026, 8, 26), lookback=253)
            self.assertIn("當日資料缺口", str(context.exception))

    def test_imported_current_day_row_is_not_accepted_without_explicit_verification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            import_verified_bars(db_path, _daily_bars(date(2026, 8, 26)))
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            current = _daily_bars(date(2026, 8, 26), 1)[0]
            with patch.object(provider, "_fetch_twse_month_bars", return_value=[current]) as fetch:
                bars = provider.get_ohlcv("2330", "TWSE", date(2026, 8, 26), lookback=253)
            fetch.assert_called_once()
            self.assertEqual(bars[-1]["date"], date(2026, 8, 26))

    def test_taiex_history_is_a_db_hit_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            rows = [
                {
                    "index_code": "TAIEX",
                    "trade_date": (date(2026, 8, 26) - timedelta(days=index)).isoformat(),
                    "close": 10000 + index,
                    "change_points": 1.0,
                    "source_endpoint": "seed",
                    "source_url": "https://seed",
                    "source_payload_sha256": f"index-{index}",
                    "fetched_at": "2026-08-26T00:00:00+08:00",
                }
                for index in range(253)
            ]
            upsert_index_bars(db_path, rows)
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            with patch.object(provider, "_get_json", side_effect=AssertionError("network")):
                series = provider.get_taiex_series(date(2026, 8, 26), lookback=253)
            self.assertEqual(len(series), 253)
            self.assertEqual(series[-1]["date"], date(2026, 8, 26))
            self.assertEqual(provider.get_market_data_diagnostics()["db_hit_count"], 1)

    def test_explicit_from_date_expands_db_read_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            import_verified_bars(
                db_path,
                [
                    VerifiedDailyBar(
                        market="TWSE",
                        symbol="2330",
                        trade_date=day,
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.0,
                        volume=1000.0,
                        source_endpoint="seed",
                        source_url="https://seed",
                        source_cache_file="seed.json",
                        source_payload_sha256=f"seed-{day}",
                        source_fetched_at="2026-01-10T00:00:00+08:00",
                    )
                    for day in (date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5))
                ]
            )
            mark_current_day_verified(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 1, 5),
            )
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            with patch.object(provider, "_fetch_twse_month_bars", side_effect=AssertionError("network")):
                bars = provider.get_ohlcv(
                    "2330",
                    "TWSE",
                    date(2026, 1, 5),
                    lookback=1,
                    from_date=date(2026, 1, 1),
                )
            self.assertEqual([item["date"] for item in bars], [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5)])


if __name__ == "__main__":
    unittest.main()
