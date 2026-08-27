import unittest

from src.report.render_validation_markdown import render_validation_markdown


class ValidationMarkdownTests(unittest.TestCase):
    def test_renders_metrics_and_research_guardrail(self) -> None:
        content = render_validation_markdown(
            "AI",
            "2026-07-15",
            {
                "mode": "validation_report_v3",
                "base_mode": "factor_aware_cross_sectional_v2",
                "window": "1y",
                "rebalance": "monthly",
                "cost_bps": 10,
                "metrics": {
                    "strategy_total_return_pct": 10.0,
                    "benchmark_total_return_pct": 3.0,
                    "excess_return_pct": 7.0,
                    "max_drawdown_pct": -2.0,
                    "annualized_volatility_pct": 12.0,
                    "hit_rate": 0.6,
                    "turnover_pct": 20.0,
                    "rebalance_count": 11,
                },
                "limitations": ["snapshot limitation"],
                "candidate_tracking": {"candidate_count": 1, "mode": "current_top_n_individual_history_v1", "rows": [{"rank": 1, "symbol": "2330", "name": "台積電", "return_20d_pct": 3.0, "annualized_volatility_pct": 20.0, "max_drawdown_pct": -5.0, "data_start": "2025-01-01", "data_end": "2026-01-01", "data_gaps": []}]},
            },
        )
        self.assertIn("# AI 驗證回測解讀", content)
        self.assertIn("7.00%", content)
        self.assertIn("不構成交易指令", content)
        self.assertIn("snapshot limitation", content)
        self.assertIn("本日候選 Top 1 個股歷史追蹤", content)
        self.assertIn("2330 台積電", content)
        self.assertIn("2025-01-01 to 2026-01-01", content)
        self.assertIn("資料缺口", content)

    def test_allows_missing_optional_metrics(self) -> None:
        content = render_validation_markdown("AI", "2026-07-15", {"mode": "validation_report_v3", "metrics": {}})
        self.assertIn("validation_report_v3", content)
        self.assertIn("N/A", content)


if __name__ == "__main__":
    unittest.main()
