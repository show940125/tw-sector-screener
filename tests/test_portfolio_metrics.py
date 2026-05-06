import unittest
from datetime import date

from src.analysis.portfolio_metrics import calculate_portfolio_diagnostics


class PortfolioMetricsTests(unittest.TestCase):
    def test_empty_and_short_series_return_stable_contract(self) -> None:
        empty = calculate_portfolio_diagnostics([])
        short = calculate_portfolio_diagnostics([{"date": date(2026, 1, 1), "equity": 100.0}])

        self.assertEqual(empty["status"], "insufficient_data")
        self.assertEqual(short["status"], "insufficient_data")
        for payload in [empty, short]:
            self.assertIn("var95_pct", payload)
            self.assertIn("rolling_sharpe", payload)
            self.assertEqual(payload["rolling_drawdown"], [])

    def test_extreme_returns_and_mismatched_benchmark_are_finite(self) -> None:
        diagnostics = calculate_portfolio_diagnostics(
            [
                {"date": date(2026, 1, 1), "equity": 100.0},
                {"date": date(2026, 1, 2), "equity": 80.0},
                {"date": date(2026, 1, 3), "equity": 120.0},
                {"date": date(2026, 1, 4), "equity": 90.0},
                {"date": date(2026, 1, 5), "equity": 130.0},
            ],
            benchmark_series=[
                {"date": date(2026, 1, 1), "equity": 100.0},
                {"date": date(2026, 1, 2), "equity": 101.0},
            ],
        )

        self.assertEqual(diagnostics["status"], "ok")
        self.assertLess(diagnostics["var95_pct"], 0.0)
        self.assertLess(diagnostics["cvar95_pct"], 0.0)
        self.assertGreaterEqual(diagnostics["ulcer_index"], 0.0)
        self.assertIn("omega_ratio", diagnostics)
        self.assertEqual(diagnostics["benchmark_status"], "length_mismatch")
        self.assertEqual(diagnostics["tracking_error_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
