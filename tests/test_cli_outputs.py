import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import tw_sector_screener as cli


class _FakeProvider:
    def __init__(self, timeout: float = 0.1, **_: object) -> None:
        self.timeout = timeout
        self.quarterly_store_path = Path("C:/tmp/quarterly_fundamentals.sqlite")

    def load_theme_universe(self, theme: str, min_monthly_revenue: float = 0.0, theme_mode: str = "strict"):
        return [
            {
                "symbol": "2330",
                "name": "台積電",
                "market": "TWSE",
                "industry": "半導體業",
                "monthly_revenue": 1000.0,
                "revenue_yoy": 22.0,
                "revenue_mom": 4.0,
                "revenue_yoy_prev": 18.0,
                "revenue_mom_prev": 1.5,
            },
            {
                "symbol": "2382",
                "name": "廣達",
                "market": "TWSE",
                "industry": "電腦及週邊設備業",
                "monthly_revenue": 900.0,
                "revenue_yoy": 16.0,
                "revenue_mom": 2.0,
                "revenue_yoy_prev": 15.0,
                "revenue_mom_prev": 1.5,
            },
        ]

    def get_taiex_series(self, as_of: date, lookback: int = 252):
        start = as_of - timedelta(days=lookback + 5)
        series = []
        close = 100.0
        for i in range(lookback + 5):
            close += 0.5
            series.append({"date": start + timedelta(days=i), "close": close, "change_points": 1.0})
        return series[-lookback:]

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 252):
        start = as_of - timedelta(days=lookback + 5)
        series = []
        close = 100.0 if symbol == "2330" else 80.0
        step = 1.2 if symbol == "2330" else 0.8
        for i in range(lookback + 5):
            close += step
            series.append(
                {
                    "date": start + timedelta(days=i),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 100000 + (i * 100),
                }
            )
        return series[-lookback:]

    def get_latest_valuation(self, symbol: str, market: str, as_of: date, max_backtrack_days: int = 20):
        if symbol == "2330":
            return {"pe": 20.0, "pb": 5.0, "dividend_yield": 1.5}
        return {"pe": 18.0, "pb": 4.0, "dividend_yield": 1.0}

    def get_quarterly_fundamentals(self, symbol: str, market: str, as_of: date):
        if symbol == "2330":
            return {
                "gross_margin_latest": 54.0,
                "gross_margin_prev": 52.0,
                "eps_latest": 14.0,
                "eps_prev": 12.0,
                "roe_latest": 28.0,
                "roe_prev": 26.0,
                "quality_fetch_status": "ok",
                "quality_missing_reason": None,
                "quality_data_source": "mock",
                "quality_periods_used": ["114Q4", "114Q3"],
                "data_quality_flags": [],
            }
        return {
            "gross_margin_latest": 15.0,
            "gross_margin_prev": 14.0,
            "eps_latest": 4.0,
            "eps_prev": 3.8,
            "roe_latest": 12.0,
            "roe_prev": 11.0,
            "quality_fetch_status": "ok",
            "quality_missing_reason": None,
            "quality_data_source": "mock",
            "quality_periods_used": ["114Q4", "114Q3"],
            "data_quality_flags": [],
        }

    def summarize_quality_coverage(self, rows, top_n: int = 3):
        return {
            "universe_count": len(rows),
            "current_complete_count": len(rows),
            "current_complete_pct": 100.0,
            "previous_complete_count": len(rows),
            "previous_complete_pct": 100.0,
            "ok_count": len(rows),
            "unavailable_count": 0,
            "partial_count": 0,
            "fetch_failed_count": 0,
            "top_candidate_gap_count": 0,
            "top_candidate_gaps": [],
        }


class CliOutputTests(unittest.TestCase):
    def test_run_writes_markdown_json_csv_audit_and_watchlist_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            coverage_path = output_dir / "coverage-list.txt"
            coverage_path.write_text("2330\n2382\n", encoding="utf-8")

            with patch.object(cli, "TwMarketProvider", _FakeProvider):
                outputs = cli.run(
                    theme="AI",
                    as_of=date(2026, 3, 12),
                    top_n=2,
                    universe_limit=10,
                    min_monthly_revenue=0.0,
                    lookback=130,
                    timeout=0.1,
                    output_root=output_dir,
                    theme_mode="strict",
                    benchmark="TAIEX",
                    output_formats={"md", "json", "csv"},
                    config_path=None,
                    coverage_list_path=coverage_path,
                    run_backtest=True,
                    rebalance="monthly",
                    cost_bps=10,
                    validation_window="1y",
                )

            self.assertTrue(outputs["md"].exists())
            self.assertTrue(outputs["json"].exists())
            self.assertTrue(outputs["csv"].exists())
            self.assertTrue(outputs["audit"].exists())
            self.assertTrue(outputs["watchlist"].exists())
            self.assertTrue(outputs["backtest"].exists())
            self.assertEqual(outputs["md"].parent, output_dir / "reports" / "20260312" / "AI")
            self.assertEqual(outputs["audit"].parent, output_dir / "audit" / "20260312")
            self.assertEqual(outputs["watchlist"].parent, output_dir / "watchlists" / "AI")
            self.assertEqual(outputs["backtest"].parent, output_dir / "backtests" / "AI")

            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertIn("picks", payload)
            self.assertIn("audit", payload)
            self.assertIn("action_view", payload["picks"][0])
            self.assertIn("confidence_score", payload["picks"][0])
            self.assertIn("quality_data_source", payload["picks"][0])
            self.assertIn("quality_periods_used", payload["picks"][0])
            self.assertIn("validation_summary", payload)
            self.assertEqual(payload["validation_summary"]["mode"], "factor_aware_cross_sectional_v2")
            self.assertIn("windows", payload["validation_summary"])
            self.assertIn("quality_coverage_summary", payload["sector_overview"])

            audit = json.loads(outputs["audit"].read_text(encoding="utf-8"))
            self.assertEqual(audit["output_root"], str(output_dir))
            self.assertIn("backtest_config", audit)
            self.assertIn("quality_coverage_summary", audit)
            self.assertIn("quarterly_store_path", audit)
            self.assertEqual(audit["quality_period_requirement"], 2)

            watchlist = json.loads(outputs["watchlist"].read_text(encoding="utf-8"))
            self.assertIn("rating_change_reason", watchlist["rows"][0])
            self.assertIn("event_risk_state", watchlist["rows"][0])


if __name__ == "__main__":
    unittest.main()
