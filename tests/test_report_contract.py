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
                "universe_overview": {
                    "universe_mode": "coverage",
                    "universe_size_before_limit": 30,
                    "ranked_count": 20,
                    "bucket_counts": {"semiconductor": 10},
                },
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
                "buying_ranking": [],
                "actionable_queue": [
                    {
                        "list_rank": 1,
                        "symbol": "2330",
                        "name": "台積電",
                        "primary_bucket": "semiconductor",
                        "buyability_score": 63.0,
                        "actionability_score": 70.0,
                        "risk_adjusted_score": 64.0,
                        "stock_risk_metrics": {
                            "sharpe_ratio": 0.8,
                            "sortino_ratio": 1.0,
                            "max_drawdown_pct": -12.0,
                            "annualized_volatility_pct": 24.0,
                        },
                        "idea_score": 72.0,
                        "confidence_score": 80.0,
                        "risk_score": 70.0,
                        "recommendation": "持有",
                        "decision_tier": "near_buy",
                        "next_action": "等待 risk_score 降到 65 以下再轉正式買進。",
                        "why_not_buy_now": "risk_score 70.0 > 65",
                        "action_view": {"action": "Neutral"},
                    }
                ],
                "watchlist_candidates": [],
                "research_list": [],
                "risks": ["題材輪動速度快，追高風險上升"],
                "sources": ["TWSE", "TPEx"],
            }
        )
        self.assertIn("# 台股類股選股報告", content)
        self.assertIn("## 方法與共識", content)
        self.assertIn("## Coverage Universe", content)
        self.assertIn("## Buying Ranking / 買進優先序", content)
        self.assertIn("## Actionable Queue / 可行動候選隊列", content)
        self.assertIn("Buying Tier", content)
        self.assertIn("Sharpe", content)
        self.assertIn("## Watchlist / 追蹤與處理清單", content)
        self.assertIn("## Research List / 題材研究清單", content)
        self.assertIn("## 倉位建議", content)
        self.assertIn("## 風險提示", content)

    def test_filename(self) -> None:
        filename = build_report_filename("半導體", date(2026, 2, 20))
        self.assertEqual(filename, "sector-report-半導體-20260220.md")


if __name__ == "__main__":
    unittest.main()
