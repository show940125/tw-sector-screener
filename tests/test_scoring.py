import unittest

from src.analysis.scoring import score_candidates


class ScoringTests(unittest.TestCase):
    def test_score_candidates_sorted(self) -> None:
        rows = [
            {
                "symbol": "A",
                "trend_score": 85.0,
                "momentum_score": 80.0,
                "value_score": 60.0,
                "fundamental_score": 70.0,
                "risk_control_score": 65.0,
            },
            {
                "symbol": "B",
                "trend_score": 55.0,
                "momentum_score": 50.0,
                "value_score": 90.0,
                "fundamental_score": 45.0,
                "risk_control_score": 80.0,
            },
        ]
        ranked = score_candidates(rows)
        self.assertEqual(ranked[0]["symbol"], "A")
        self.assertGreater(ranked[0]["total_score"], ranked[1]["total_score"])


if __name__ == "__main__":
    unittest.main()
