import unittest

from src.analysis.actions import build_action_view


class ActionViewTests(unittest.TestCase):
    def test_action_view_contains_add_and_trim_triggers(self) -> None:
        action = build_action_view(
            idea_score=79.0,
            confidence_score=88.0,
            close=100.0,
            atr14=4.0,
            volatility20=22.0,
            rel_to_taiex_20d=5.5,
            rel_to_sector_20d=4.0,
        )

        self.assertEqual(action["action"], "Overweight")
        self.assertIn("why_now", action)
        self.assertIn("why_not", action)
        self.assertIn("add_trigger", action)
        self.assertIn("trim_trigger", action)
        self.assertIn("entry_range", action)


if __name__ == "__main__":
    unittest.main()
