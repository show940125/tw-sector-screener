import unittest
from datetime import date

from src.report.render_markdown import build_report_filename, render_report


class ReportContractTests(unittest.TestCase):
    def test_sections(self) -> None:
        content = render_report(
            {
                "theme": "半導體",
                "as_of": date(2026, 2, 20),
                "summary": "半導體族群動能仍在，但波動偏高，建議分批佈局。",
                "method": [
                    "Momentum: 63/126 日報酬分位",
                    "Value: PE/PB/殖利率分位",
                ],
                "picks": [
                    {
                        "rank": 1,
                        "symbol": "2330",
                        "name": "台積電",
                        "market": "TWSE",
                        "total_score": 82.3,
                        "close": 1200.0,
                        "reasons": ["趨勢強", "估值中位偏低"],
                        "position": {
                            "max_position_pct": 9.0,
                            "initial_position_pct": 3.6,
                            "risk_budget_pct": 0.6,
                            "stop_price": 1150.0,
                            "share_formula": "可買股數 = (資金 x 單筆風險%) / (進場價 - 停損價)",
                        },
                    }
                ],
                "risks": ["題材輪動速度快，追高風險上升"],
                "sources": ["TWSE", "TPEx"],
            }
        )
        self.assertIn("# 台股類股選股報告", content)
        self.assertIn("## 方法與共識", content)
        self.assertIn("## 候選清單", content)
        self.assertIn("## 倉位建議", content)
        self.assertIn("## 風險提示", content)

    def test_filename(self) -> None:
        filename = build_report_filename("半導體", date(2026, 2, 20))
        self.assertEqual(filename, "sector-report-半導體-20260220.md")


if __name__ == "__main__":
    unittest.main()
