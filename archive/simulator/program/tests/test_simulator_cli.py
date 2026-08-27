import unittest
from datetime import date

from scripts.tw_sector_investment_simulator import _default_run_id, _parse_date


class SimulatorCliTests(unittest.TestCase):
    def test_parse_date_accepts_today(self) -> None:
        self.assertEqual(_parse_date("today"), date.today())
        self.assertEqual(_parse_date("2026-04-29"), date(2026, 4, 29))

    def test_daily_default_run_id_is_stable(self) -> None:
        first = _default_run_id("daily", ["AI", "半導體"], date(2026, 4, 29), date(2026, 4, 29))
        second = _default_run_id("daily", ["AI", "半導體"], date(2026, 4, 30), date(2026, 4, 30))
        self.assertEqual(first, "daily-AI-半導體")
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
