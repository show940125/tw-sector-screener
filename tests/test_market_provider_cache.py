import unittest
from datetime import date
from urllib.request import Request

from src.providers.tw_market_provider import TWSE_STOCK_DAY_URL, TwMarketProvider


class MarketProviderCacheTests(unittest.TestCase):
    def test_current_month_incremental_stock_day_cache_is_stale_when_tail_lags_today(self) -> None:
        provider = TwMarketProvider()
        req = Request(f"{TWSE_STOCK_DAY_URL}?response=json&date=20260501&stockNo=2356")
        payload = {"stat": "OK", "data": [["115/05/04", "1", "1", "1", "1", "1", "1"]]}

        self.assertTrue(provider._cached_incremental_payload_is_stale(req, payload, today=date(2026, 5, 7)))

    def test_current_month_incremental_stock_day_cache_is_fresh_when_tail_reaches_today(self) -> None:
        provider = TwMarketProvider()
        req = Request(f"{TWSE_STOCK_DAY_URL}?response=json&date=20260501&stockNo=2356")
        payload = {"stat": "OK", "data": [["115/05/07", "1", "1", "1", "1", "1", "1"]]}

        self.assertFalse(provider._cached_incremental_payload_is_stale(req, payload, today=date(2026, 5, 7)))

    def test_historical_month_incremental_stock_day_cache_can_remain_fresh(self) -> None:
        provider = TwMarketProvider()
        req = Request(f"{TWSE_STOCK_DAY_URL}?response=json&date=20260401&stockNo=2356")
        payload = {"stat": "OK", "data": [["115/04/30", "1", "1", "1", "1", "1", "1"]]}

        self.assertFalse(provider._cached_incremental_payload_is_stale(req, payload, today=date(2026, 5, 7)))


if __name__ == "__main__":
    unittest.main()
