import unittest

from src.providers.tw_market_provider import TwMarketProvider
from src.themes import theme_rule


class ThemeMatchingTests(unittest.TestCase):
    def test_memory_theme_excludes_tsmc(self) -> None:
        provider = TwMarketProvider()
        rule = theme_rule("記憶體")
        self.assertFalse(provider._theme_match("2330", "台積電", "半導體業", rule))

    def test_memory_theme_includes_memory_names(self) -> None:
        provider = TwMarketProvider()
        rule = theme_rule("記憶體")
        self.assertTrue(provider._theme_match("2408", "南亞科", "半導體業", rule))
        self.assertTrue(provider._theme_match("2344", "華邦電", "半導體業", rule))


if __name__ == "__main__":
    unittest.main()
