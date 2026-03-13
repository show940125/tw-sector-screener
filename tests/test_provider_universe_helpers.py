import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.providers.quarterly_store import init_db, insert_fundamental_snapshot
from src.providers.tw_market_provider import TwMarketProvider


class ProviderUniverseHelpersTests(unittest.TestCase):
    def test_load_all_universe_filters_and_sorts(self) -> None:
        provider = TwMarketProvider(timeout=0.1)
        basics = {
            "1111": {"symbol": "1111", "name": "甲公司", "industry": "水泥", "market": "TWSE"},
            "2222": {"symbol": "2222", "name": "乙公司", "industry": "食品", "market": "TPEx"},
            "3333": {"symbol": "3333", "name": "丙公司", "industry": "", "market": "TWSE"},
        }
        revenue = {
            "1111": {"industry": "水泥工業", "monthly_revenue": 300.0, "revenue_yoy": "10", "revenue_mom": "2"},
            "2222": {"industry": "食品工業", "monthly_revenue": 100.0, "revenue_yoy": "-5", "revenue_mom": "1"},
            "3333": {"industry": "", "monthly_revenue": 500.0, "revenue_yoy": "3", "revenue_mom": "0"},
        }
        with patch.object(provider, "_load_basics", return_value=basics), patch.object(
            provider, "_load_latest_revenue_map", return_value=revenue
        ):
            rows = provider.load_all_universe(min_monthly_revenue=200)

        self.assertEqual([x["symbol"] for x in rows], ["3333", "1111"])
        self.assertEqual(rows[0]["industry"], "未分類")
        self.assertEqual(rows[1]["industry"], "水泥工業")

    def test_load_industry_universes_groups_by_industry(self) -> None:
        provider = TwMarketProvider(timeout=0.1)
        mocked = [
            {"symbol": "1111", "name": "甲", "market": "TWSE", "industry": "A", "monthly_revenue": 10},
            {"symbol": "2222", "name": "乙", "market": "TWSE", "industry": "A", "monthly_revenue": 9},
            {"symbol": "3333", "name": "丙", "market": "TWSE", "industry": "B", "monthly_revenue": 8},
        ]
        with patch.object(provider, "load_all_universe", return_value=mocked):
            buckets = provider.load_industry_universes(min_count=2)

        self.assertIn("A", buckets)
        self.assertNotIn("B", buckets)
        self.assertEqual(len(buckets["A"]), 2)

    def test_load_theme_universe_uses_shared_universe(self) -> None:
        provider = TwMarketProvider(timeout=0.1)
        mocked = [
            {
                "symbol": "2408",
                "name": "南亞科",
                "market": "TWSE",
                "industry": "半導體業",
                "monthly_revenue": 100.0,
                "revenue_yoy": 5.0,
                "revenue_mom": 3.0,
            },
            {
                "symbol": "9999",
                "name": "假公司",
                "market": "TWSE",
                "industry": "航運業",
                "monthly_revenue": 100.0,
                "revenue_yoy": 5.0,
                "revenue_mom": 3.0,
            },
        ]
        with patch.object(provider, "load_all_universe", return_value=mocked):
            rows = provider.load_theme_universe("記憶體")
        self.assertEqual([x["symbol"] for x in rows], ["2408"])

    def test_summarize_quality_coverage_counts_current_and_previous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            provider = TwMarketProvider(timeout=0.1, cache_dir=Path(tmp))
            init_db(provider.quarterly_store_path)
            insert_fundamental_snapshot(
                provider.quarterly_store_path,
                {
                    "symbol": "A",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "seed",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 10.0,
                    "eps": 1.0,
                    "roe": 5.0,
                    "revenue": 100.0,
                    "gross_profit": 10.0,
                    "net_income": 5.0,
                    "equity": 100.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                provider.quarterly_store_path,
                {
                    "symbol": "A",
                    "market": "TWSE",
                    "period": "114Q3",
                    "dataset_key": "twse_ci",
                    "source": "seed",
                    "fetched_at": "2026-03-11T09:00:00",
                    "as_of_date": "2026-03-11",
                    "gross_margin": 9.0,
                    "eps": 0.9,
                    "roe": 4.0,
                    "revenue": 100.0,
                    "gross_profit": 9.0,
                    "net_income": 4.0,
                    "equity": 100.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                provider.quarterly_store_path,
                {
                    "symbol": "B",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "seed",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": 10.0,
                    "eps": 1.0,
                    "roe": 5.0,
                    "revenue": 100.0,
                    "gross_profit": 10.0,
                    "net_income": 5.0,
                    "equity": 100.0,
                    "fetch_status": "ok",
                    "missing_reason": None,
                    "raw_payload_json": "{}",
                },
            )
            insert_fundamental_snapshot(
                provider.quarterly_store_path,
                {
                    "symbol": "C",
                    "market": "TWSE",
                    "period": "114Q4",
                    "dataset_key": "twse_ci",
                    "source": "seed",
                    "fetched_at": "2026-03-12T09:00:00",
                    "as_of_date": "2026-03-12",
                    "gross_margin": None,
                    "eps": None,
                    "roe": None,
                    "revenue": None,
                    "gross_profit": None,
                    "net_income": None,
                    "equity": None,
                    "fetch_status": "unavailable",
                    "missing_reason": "unavailable",
                    "raw_payload_json": "{}",
                },
            )

            summary = provider.summarize_quality_coverage(
                [{"symbol": "A", "market": "TWSE"}, {"symbol": "B", "market": "TWSE"}, {"symbol": "C", "market": "TWSE"}],
                top_n=2,
            )

        self.assertEqual(summary["universe_count"], 3)
        self.assertEqual(summary["current_complete_count"], 2)
        self.assertEqual(summary["previous_complete_count"], 1)
        self.assertEqual(summary["current_complete_pct"], 66.67)
        self.assertEqual(summary["top_candidate_gap_count"], 1)


if __name__ == "__main__":
    unittest.main()
