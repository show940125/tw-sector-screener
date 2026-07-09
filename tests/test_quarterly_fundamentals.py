import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.providers.quarterly_store import init_db, insert_fundamental_snapshot
from src.providers.tw_market_provider import TwMarketProvider


class QuarterlyFundamentalsTests(unittest.TestCase):
    def test_get_quarterly_fundamentals_uses_current_openapi_and_sqlite_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = TwMarketProvider(timeout=0.1, cache_dir=Path(tmp))
            init_db(provider.quarterly_store_path)
            insert_fundamental_snapshot(
                provider.quarterly_store_path,
                {
                    "symbol": "2330",
                    "market": "TWSE",
                    "period": "114Q3",
                    "dataset_key": "twse_ci",
                    "source": "sqlite_seed",
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

            def fake_get_json(url: str, params=None):
                if "t187ap06_L_ci" in url:
                    return [{"公司代號": "2330", "營業收入": "1000", "營業毛利（毛損）淨額": "400"}]
                if "t187ap07_L_ci" in url:
                    return [{"公司代號": "2330", "歸屬於母公司業主之權益合計": "2000"}]
                if "t187ap14_L" in url:
                    return [{"公司代號": "2330", "基本每股盈餘(元)": "5.00", "稅後淨利": "100", "年度": "114", "季別": "4"}]
                return []

            with patch.object(provider, "_get_json", side_effect=fake_get_json):
                result = provider.get_quarterly_fundamentals("2330", "TWSE", date(2026, 3, 12))

        self.assertAlmostEqual(result["gross_margin_latest"], 40.0)
        self.assertAlmostEqual(result["gross_margin_prev"], 38.0)
        self.assertAlmostEqual(result["eps_latest"], 5.0)
        self.assertAlmostEqual(result["eps_prev"], 4.2)
        self.assertAlmostEqual(result["roe_latest"], 20.0)
        self.assertAlmostEqual(result["roe_prev"], 16.84, places=2)
        self.assertEqual(result["quality_fetch_status"], "ok")
        self.assertEqual(result["quality_periods_used"], ["114Q4", "114Q3"])
        self.assertIn("sqlite", result["quality_data_source"])

    def test_get_quarterly_fundamentals_marks_fetch_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = TwMarketProvider(timeout=0.1, cache_dir=Path(tmp))
            with patch.object(provider, "_get_json", side_effect=RuntimeError("boom")):
                result = provider.get_quarterly_fundamentals("2330", "TWSE", date(2026, 3, 12))

        self.assertEqual(result["quality_fetch_status"], "fetch_failed")
        self.assertEqual(result["quality_missing_reason"], "fetch_failed")
        self.assertIn("quality:fetch_failed", result["data_quality_flags"])

    def test_get_quarterly_fundamentals_refreshes_stale_unavailable_current_period(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = TwMarketProvider(timeout=0.1, cache_dir=Path(tmp))
            init_db(provider.quarterly_store_path)
            for period, status in [("115Q1", "unavailable"), ("114Q4", "ok")]:
                insert_fundamental_snapshot(
                    provider.quarterly_store_path,
                    {
                        "symbol": "2451",
                        "market": "TWSE",
                        "period": period,
                        "dataset_key": "twse_unknown" if status == "unavailable" else "twse_ci",
                        "source": "seed",
                        "fetched_at": "2026-05-05T09:00:00",
                        "as_of_date": "2026-05-05",
                        "gross_margin": None if status == "unavailable" else 45.0,
                        "eps": None if status == "unavailable" else 12.0,
                        "roe": None if status == "unavailable" else 80.0,
                        "revenue": None,
                        "gross_profit": None,
                        "net_income": None,
                        "equity": None,
                        "fetch_status": status,
                        "missing_reason": "unavailable" if status == "unavailable" else None,
                        "raw_payload_json": "{}",
                    },
                )

            def fake_get_json(url: str, params=None):
                if "t187ap06_L_ci" in url:
                    return [{"公司代號": "2451", "營業收入": "1000", "營業毛利（毛損）淨額": "400"}]
                if "t187ap07_L_ci" in url:
                    return [{"公司代號": "2451", "歸屬於母公司業主之權益合計": "2000"}]
                if "t187ap14_L" in url:
                    return [{"公司代號": "2451", "基本每股盈餘(元)": "5.00", "稅後淨利": "100", "年度": "115", "季別": "1"}]
                return []

            with patch.object(provider, "_latest_reported_period", return_value="115Q1"), patch.object(
                provider, "_get_json", side_effect=fake_get_json
            ):
                result = provider.get_quarterly_fundamentals("2451", "TWSE", date(2026, 5, 29))

        self.assertEqual(result["quality_fetch_status"], "ok")
        self.assertEqual(result["quality_periods_used"], ["115Q1", "114Q4"])
        self.assertAlmostEqual(result["gross_margin_latest"], 40.0)
        self.assertAlmostEqual(result["eps_latest"], 5.0)
        self.assertAlmostEqual(result["roe_latest"], 20.0)

    def test_get_quarterly_fundamentals_accepts_tpex_eps_without_unit_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = TwMarketProvider(timeout=0.1, cache_dir=Path(tmp))

            def fake_get_json(url: str, params=None):
                if "t187ap06_O_ci" in url:
                    return [{"SecuritiesCompanyCode": "6147", "營業收入": "1000", "營業毛利（毛損）淨額": "250"}]
                if "t187ap07_O_ci" in url:
                    return [{"SecuritiesCompanyCode": "6147", "歸屬於母公司業主之權益合計": "2000"}]
                if "t187ap14_O" in url:
                    return [{"SecuritiesCompanyCode": "6147", "基本每股盈餘": "0.59", "稅後淨利": "100", "Year": "115", "季別": "1"}]
                return []

            with patch.object(provider, "_latest_reported_period", return_value="115Q1"), patch.object(
                provider, "_get_json", side_effect=fake_get_json
            ):
                result = provider.get_quarterly_fundamentals("6147", "TPEx", date(2026, 5, 29))

        self.assertEqual(result["quality_fetch_status"], "ok")
        self.assertAlmostEqual(result["eps_latest"], 0.59)
        self.assertNotIn("quality:partial_current_metrics", result["data_quality_flags"])


if __name__ == "__main__":
    unittest.main()
