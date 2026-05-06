import unittest
from datetime import date, timedelta

from src.analysis.stock_risk_metrics import calculate_stock_risk_metrics


def series(values: list[float]) -> list[dict]:
    start = date(2026, 1, 1)
    return [{"date": start + timedelta(days=i), "close": value} for i, value in enumerate(values)]


class StockRiskMetricsTests(unittest.TestCase):
    def test_empty_and_short_series_are_stable(self) -> None:
        for payload in (calculate_stock_risk_metrics([]), calculate_stock_risk_metrics(series([100.0]))):
            self.assertEqual(payload["status"], "insufficient_data")
            self.assertEqual(payload["sharpe_ratio"], 0.0)
            self.assertEqual(payload["risk_adjusted_score"], 0.0)

    def test_uptrend_has_positive_risk_adjusted_metrics(self) -> None:
        payload = calculate_stock_risk_metrics(series([100, 102, 104, 106, 109, 111, 114]))
        self.assertEqual(payload["status"], "ok")
        self.assertGreater(payload["annualized_return_pct"], 0.0)
        self.assertGreater(payload["sharpe_ratio"], 0.0)
        self.assertGreater(payload["sortino_ratio"], 0.0)
        self.assertGreater(payload["risk_adjusted_score"], 50.0)

    def test_drawdown_is_captured_without_nan(self) -> None:
        payload = calculate_stock_risk_metrics(series([100, 120, 80, 90, 70, 95]))
        self.assertEqual(payload["status"], "ok")
        self.assertLess(payload["max_drawdown_pct"], 0.0)
        for key in ["sharpe_ratio", "sortino_ratio", "calmar_ratio", "return_to_drawdown", "risk_adjusted_score"]:
            self.assertIsInstance(payload[key], float)

    def test_zero_volatility_does_not_raise(self) -> None:
        payload = calculate_stock_risk_metrics(series([100, 100, 100, 100]))
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["annualized_volatility_pct"], 0.0)
        self.assertEqual(payload["sharpe_ratio"], 0.0)


if __name__ == "__main__":
    unittest.main()
