import json
import tempfile
import unittest
from datetime import date
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from unittest.mock import patch

from src.providers.tw_market_provider import (
    TPEX_DAILY_QUOTES_URL,
    TPEX_TRADING_STOCK_URL,
    TWSE_STOCK_DAY_PRIMARY_URL,
    TWSE_STOCK_DAY_URL,
    MarketDataFetchError,
    TwMarketProvider,
)


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self) -> bytes:
        return self.payload


def _redirect_error(url: str, code: int, location: str) -> HTTPError:
    headers = Message()
    headers["Location"] = location
    return HTTPError(url, code, "Permanent Redirect", headers, BytesIO())


class MarketProviderRedirectTests(unittest.TestCase):
    def _provider(self) -> TwMarketProvider:
        return TwMarketProvider(cache_dir=Path(tempfile.mkdtemp()))

    def test_308_is_followed_with_post_method_and_body_preserved(self) -> None:
        provider = self._provider()
        original = Request(
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock",
            data=b"code=6223&date=2026%2F08%2F01&response=json",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        redirected = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock/"
        observed: list[Request] = []

        def fake_open(req: Request, *, insecure: bool = False):
            observed.append(req)
            if len(observed) == 1:
                raise _redirect_error(req.full_url, 308, redirected)
            return _Response({"stat": "ok"})

        with patch.object(provider, "_open_request", side_effect=fake_open):
            payload, final_url = provider._read_json_request(original)

        self.assertEqual(payload, {"stat": "ok"})
        self.assertEqual(final_url, redirected)
        self.assertEqual(observed[1].get_method(), "POST")
        self.assertEqual(observed[1].data, original.data)
        self.assertEqual(provider.get_market_data_diagnostics()["redirect_308_count"], 1)

    def test_redirect_loop_is_rejected_before_unbounded_retry(self) -> None:
        provider = self._provider()
        request = Request("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY")

        def fake_open(req: Request, *, insecure: bool = False):
            raise _redirect_error(req.full_url, 308, req.full_url)

        with patch.object(provider, "_open_request", side_effect=fake_open):
            with self.assertRaises(ValueError) as context:
                provider._read_json_request(request)

        self.assertIn("redirect loop", str(context.exception))
        self.assertEqual(provider.get_market_data_diagnostics()["redirect_failure_count"], 1)

    def test_endpoint_fallback_returns_second_valid_payload(self) -> None:
        provider = self._provider()
        requests = [
            provider._build_get_request(TWSE_STOCK_DAY_PRIMARY_URL, {"date": "20260801", "stockNo": "2330"}),
            provider._build_get_request(TWSE_STOCK_DAY_URL, {"date": "20260801", "stockNo": "2330"}),
        ]
        payload = {"stat": "OK", "data": []}

        def fake_load_json(request: Request, **kwargs: object):
            if request.full_url.startswith(TWSE_STOCK_DAY_PRIMARY_URL):
                provider._market_data_stats.redirect_308_count += 1
                raise RuntimeError("primary failed")
            return payload

        with patch.object(provider, "_load_json", side_effect=fake_load_json):
            result = provider._load_json_candidates(
                requests,
                endpoint_label="twse.stock_day",
                validator=provider._valid_twse_stock_day_payload,
            )

        self.assertEqual(result, payload)
        stats = provider.get_market_data_diagnostics()
        self.assertEqual(stats["fallback_success_count"], 1)
        self.assertEqual(stats["endpoint_fallback_successes"]["www.twse.com.tw/exchangeReport/STOCK_DAY"], 1)
        self.assertEqual(stats["redirect_308_recovered_count"], 1)
        self.assertEqual(stats["redirect_308_unresolved_count"], 0)

    def test_all_endpoint_candidates_raise_market_data_fetch_error(self) -> None:
        provider = self._provider()
        requests = [
            provider._build_get_request(TWSE_STOCK_DAY_PRIMARY_URL, {"date": "20260801", "stockNo": "2330"}),
            provider._build_get_request(TWSE_STOCK_DAY_URL, {"date": "20260801", "stockNo": "2330"}),
        ]
        with patch.object(provider, "_load_json", side_effect=RuntimeError("unavailable")):
            with self.assertRaises(MarketDataFetchError) as context:
                provider._load_json_candidates(requests, endpoint_label="twse.stock_day")

        self.assertEqual(len(context.exception.attempts), 2)
        self.assertIn("所有來源失敗", str(context.exception))

    def test_tpex_bulk_daily_fallback_parses_ohlcv(self) -> None:
        provider = self._provider()
        payload = {
            "tables": [
                {
                    "date": "115/08/26",
                    "fields": ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "最高 ", "最低", "成交股數  "],
                    "data": [["6223", "旺矽", "2,630.00", "+115.00", "2,500.00", "2,670.00", "2,485.00", "4,430"]],
                }
            ]
        }
        with patch.object(provider, "_load_json_candidates", return_value=payload) as fetch:
            mapping = provider._get_tpex_daily_quotes_for_day(date(2026, 8, 26))

        self.assertEqual(mapping["6223"]["close"], 2630.0)
        self.assertEqual(mapping["6223"]["open"], 2500.0)
        self.assertEqual(mapping["6223"]["volume"], 4430.0)
        fetch.assert_called_once()
        request = fetch.call_args.args[0][0]
        self.assertTrue(request.full_url.startswith(TPEX_DAILY_QUOTES_URL))

    def test_current_bulk_cache_is_stale_when_payload_date_lags_today(self) -> None:
        provider = self._provider()
        request = provider._build_get_request(
            TPEX_DAILY_QUOTES_URL,
            {"l": "zh-tw", "d": "115/08/26", "se": "EW", "o": "json"},
        )
        payload = {"date": "115/08/25", "tables": []}
        self.assertTrue(provider._cached_incremental_payload_is_stale(request, payload, today=date(2026, 8, 26)))

    def test_network_fetch_is_recorded_with_payload_hash_and_final_source(self) -> None:
        provider = self._provider()
        request = Request("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY")
        with patch.object(provider, "_open_request", return_value=_Response({"stat": "OK", "data": []})):
            result = provider._load_json(request, endpoint_label="twse.stock_day", use_cache=False)
        self.assertEqual(result["stat"], "OK")
        import sqlite3

        conn = sqlite3.connect(provider.market_data_db_path)
        try:
            row = conn.execute(
                "SELECT status, final_url, payload_sha256, cache_status "
                "FROM market_data_fetch_attempts ORDER BY attempt_id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], "network_success")
        self.assertEqual(row[1], request.full_url)
        self.assertTrue(row[2])
        self.assertEqual(row[3], "network")
        self.assertEqual(
            provider.get_market_data_store_diagnostics()["source_payload_integrity"]["hash_mismatches"],
            [],
        )


if __name__ == "__main__":
    unittest.main()
