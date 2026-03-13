import unittest
from unittest.mock import patch

from src.providers.tw_market_provider import TwMarketProvider
from src.themes import available_themes


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


if __name__ == "__main__":
    unittest.main()
