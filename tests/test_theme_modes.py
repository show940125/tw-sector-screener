import unittest
from unittest.mock import patch

from src.providers.tw_market_provider import TwMarketProvider
from src.themes import available_themes, theme_rule


class ThemeModeTests(unittest.TestCase):
    def test_available_themes_include_curated_subthemes(self) -> None:
        themes = available_themes()
        self.assertIn("AI infra", themes)
        self.assertIn("AI server/ODM", themes)
        self.assertIn("memory", themes)
        self.assertIn("foundry", themes)
        self.assertIn("IC design", themes)

    def test_ai_strict_excludes_proxy_names_but_broad_keeps_them(self) -> None:
        provider = TwMarketProvider(timeout=0.1)
        mocked = [
            {
                "symbol": "2412",
                "name": "中華電",
                "market": "TWSE",
                "industry": "通信網路業",
                "monthly_revenue": 100.0,
                "revenue_yoy": 1.0,
                "revenue_mom": 1.0,
            },
            {
                "symbol": "2382",
                "name": "廣達",
                "market": "TWSE",
                "industry": "電腦及週邊設備業",
                "monthly_revenue": 100.0,
                "revenue_yoy": 1.0,
                "revenue_mom": 1.0,
            },
        ]
        with patch.object(provider, "load_all_universe", return_value=mocked):
            strict_rows = provider.load_theme_universe("AI", theme_mode="strict")
            broad_rows = provider.load_theme_universe("AI", theme_mode="broad")

        self.assertEqual([row["symbol"] for row in strict_rows], ["2382"])
        self.assertEqual([row["symbol"] for row in broad_rows], ["2412", "2382"])

    def test_ai_coverage_is_larger_than_core_and_has_buckets(self) -> None:
        core = theme_rule("AI", universe_mode="core")
        coverage = theme_rule("AI", universe_mode="coverage")

        self.assertEqual(len(core["symbols"]), 9)
        self.assertGreater(len(coverage["symbols"]), len(core["symbols"]))
        self.assertIn("2330", coverage["bucket_map"])
        self.assertIn("semiconductor", coverage["bucket_map"]["2330"])
        self.assertEqual(core["universe_mode"], "core")
        self.assertEqual(coverage["universe_mode"], "coverage")

    def test_deprecated_strict_maps_to_core(self) -> None:
        strict = theme_rule("AI", theme_mode="strict")
        core = theme_rule("AI", universe_mode="core")

        self.assertEqual(strict["universe_mode"], "core")
        self.assertEqual(strict["symbols"], core["symbols"])

    def test_coverage_candidates_include_bucket_metadata(self) -> None:
        provider = TwMarketProvider(timeout=0.1)
        symbols = theme_rule("AI", universe_mode="coverage")["symbols"][:4]
        mocked = [
            {
                "symbol": symbol,
                "name": symbol,
                "market": "TWSE",
                "industry": "半導體業",
                "monthly_revenue": 100.0,
                "revenue_yoy": 1.0,
                "revenue_mom": 1.0,
            }
            for symbol in symbols
        ]
        with patch.object(provider, "load_all_universe", return_value=mocked):
            rows = provider.load_theme_universe("AI", universe_mode="coverage")

        self.assertEqual(len(rows), len(symbols))
        for row in rows:
            self.assertIn("primary_bucket", row)
            self.assertTrue(row["theme_buckets"])
            self.assertEqual(row["universe_mode"], "coverage")


if __name__ == "__main__":
    unittest.main()
