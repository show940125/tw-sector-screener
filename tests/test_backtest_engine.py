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
                    {"symbol": "A", "close": 100.0, "score": 80.0},
                    {"symbol": "B", "close": 100.0, "score": 60.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 8),
                "rows": [
                    {"symbol": "A", "close": 110.0, "score": 78.0},
                    {"symbol": "B", "close": 90.0, "score": 65.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 15),
                "rows": [
                    {"symbol": "A", "close": 121.0, "score": 76.0},
                    {"symbol": "B", "close": 95.0, "score": 68.0},
                ],
            },
            {
                "rebalance_date": date(2026, 1, 22),
                "rows": [
                    {"symbol": "A", "close": 133.1, "score": 75.0},
                    {"symbol": "B", "close": 97.0, "score": 70.0},
                ],
            },
        ]

        result = run_cross_sectional_backtest(
            snapshots=snapshots,
            benchmark_series=benchmark_series,
            top_n=1,
            cost_bps=10,
        )

        self.assertEqual(result["rebalance_count"], 3)
        self.assertGreater(result["strategy_total_return_pct"], result["benchmark_total_return_pct"])
        self.assertGreaterEqual(result["hit_rate"], 1.0)
        self.assertIn("cost_adjusted_return_pct", result)
        self.assertIn("max_drawdown_pct", result)


if __name__ == "__main__":
    unittest.main()
