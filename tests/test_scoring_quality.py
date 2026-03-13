import unittest

from src.analysis.scoring import score_candidates


class ScoringQualityTests(unittest.TestCase):
    def test_missing_factors_reduce_confidence_instead_of_silent_neutral_fill(self) -> None:
        ranked = score_candidates(
            [
                {
                    "symbol": "A",
                    "trend_score": 82.0,
                    "momentum_score": 75.0,
                    "value_score": None,
                    "fundamental_score": None,
                    "risk_control_score": 61.0,
                },
                {
                    "symbol": "B",
                    "trend_score": 75.0,
                    "momentum_score": 68.0,
                    "value_score": 54.0,
                    "fundamental_score": 52.0,
                    "risk_control_score": 61.0,
                },
            ]
        )

        missing_heavy = next(row for row in ranked if row["symbol"] == "A")
        complete = next(row for row in ranked if row["symbol"] == "B")

        self.assertEqual(missing_heavy["missing_factor_count"], 2)
        self.assertLess(missing_heavy["confidence_score"], complete["confidence_score"])
        self.assertIn("missing:value_score", missing_heavy["data_quality_flags"])
        self.assertIn("factor_breakdown", missing_heavy)
        self.assertIn("idea_score", missing_heavy)
        self.assertIn("factor_coverage_confidence", missing_heavy)
        self.assertIn("data_freshness_confidence", missing_heavy)


if __name__ == "__main__":
    unittest.main()
