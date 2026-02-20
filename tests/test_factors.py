import unittest

from src.analysis.factors import percentile_rank, position_plan


class FactorTests(unittest.TestCase):
    def test_percentile_rank(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0]
        self.assertAlmostEqual(percentile_rank(30.0, values), 75.0)
        self.assertAlmostEqual(percentile_rank(10.0, values), 25.0)

    def test_position_plan_contains_risk_formula(self) -> None:
        plan = position_plan(score=78.0, close=120.0, atr14=4.0, volatility20=28.0)
        self.assertIn("max_position_pct", plan)
        self.assertIn("risk_budget_pct", plan)
        self.assertIn("share_formula", plan)
        self.assertGreater(plan["max_position_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
