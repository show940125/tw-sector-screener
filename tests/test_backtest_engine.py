import unittest
from datetime import date

from src.analysis.backtest import run_cross_sectional_backtest


class BacktestEngineTests(unittest.TestCase):
    def test_backtest_returns_deterministic_metrics(self) -> None:
        benchmark_series = [
            {"date": date(2026, 1, 1), "close": 100.0},
            {"date": date(2026, 1, 8), "close": 101.0},
            {"date": date(2026, 1, 15), "close": 102.0},
            {"date": date(2026, 1, 22), "close": 103.0},
        ]
        snapshots = [
            {
                "rebalance_date": date(2026, 1, 1),
                "rows": [
                    {"symbol": "A", "close": 100.0, "score": 80.0, "price_factor_score": 82.0, "fundamental_factor_score": 55.0, "quality_factor_score": 65.0},
                    {"symbol": "B", "close": 100.0, "score": 60.0, "price_factor_score": 58.0, "fundamental_factor_score": 52.0, "quality_factor_score": 45.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 8),
                "rows": [
                    {"symbol": "A", "close": 110.0, "score": 78.0, "price_factor_score": 79.0, "fundamental_factor_score": 56.0, "quality_factor_score": 66.0},
                    {"symbol": "B", "close": 90.0, "score": 65.0, "price_factor_score": 61.0, "fundamental_factor_score": 53.0, "quality_factor_score": 48.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 15),
                "rows": [
                    {"symbol": "A", "close": 121.0, "score": 76.0, "price_factor_score": 77.0, "fundamental_factor_score": 57.0, "quality_factor_score": 66.0},
                    {"symbol": "B", "close": 95.0, "score": 68.0, "price_factor_score": 63.0, "fundamental_factor_score": 54.0, "quality_factor_score": 49.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 22),
                "rows": [
                    {"symbol": "A", "close": 133.1, "score": 75.0, "price_factor_score": 76.0, "fundamental_factor_score": 57.0, "quality_factor_score": 67.0},
                    {"symbol": "B", "close": 97.0, "score": 70.0, "price_factor_score": 65.0, "fundamental_factor_score": 54.0, "quality_factor_score": 50.0},
                ],
            },
        ]

        result = run_cross_sectional_backtest(
            snapshots=snapshots,
            benchmark_series=benchmark_series,
            top_n=1,
            cost_bps=10,
            factor_groups={
                "price": ["price_factor_score"],
                "fundamental": ["fundamental_factor_score"],
                "quality": ["quality_factor_score"],
            },
        )

        self.assertEqual(result["rebalance_count"], 3)
        self.assertGreater(result["strategy_total_return_pct"], result["benchmark_total_return_pct"])
        self.assertGreaterEqual(result["hit_rate"], 1.0)
        self.assertIn("cost_adjusted_return_pct", result)
        self.assertIn("max_drawdown_pct", result)
        self.assertIn("factor_sleeves", result)
        self.assertIn("price", result["factor_sleeves"])
        self.assertIn("factor_attribution", result)


if __name__ == "__main__":
    unittest.main()
