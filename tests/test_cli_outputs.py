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
        self._decision = {
            "mode": "auto",
            "decision": "sync-repair",
            "history_depth_target": 8,
            "history_complete_pct": 25.0,
            "backfill_enqueued": True,
            "backfill_run_id": "backfill-auto-1",
            "repair_refreshed_symbols": ["2330"],
            "refresh_run_id": "refresh-auto-1",
        }

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

    def summarize_quality_coverage(self, rows, top_n: int = 3, history_depth: int = 8, as_of=None):
        return {
            "universe_count": len(rows),
            "current_complete_count": len(rows),
            "current_complete_pct": 100.0,
            "previous_complete_count": len(rows),
            "previous_complete_pct": 100.0,
            "history_complete_count": 0,
            "history_complete_pct": 0.0,
            "ok_count": len(rows),
            "unavailable_count": 0,
            "partial_count": 0,
            "fetch_failed_count": 0,
            "top_candidate_gap_count": 0,
            "top_candidate_gaps": [],
        }

    def run_quality_update_check(
        self,
        theme: str,
        universe,
        as_of: date,
        mode: str = "auto",
        budget_sec: float = 3.0,
        history_depth: int = 8,
        top_n: int = 3,
        theme_mode: str = "strict",
    ):
        payload = dict(self._decision)
        payload["mode"] = mode
        if mode == "skip":
            payload["decision"] = "skipped"
            payload["backfill_enqueued"] = False
            payload["backfill_run_id"] = None
            payload["repair_refreshed_symbols"] = []
        elif mode == "force":
            payload["decision"] = "forced-sync-repair"
        return payload


class CliOutputTests(unittest.TestCase):
    def test_cli_default_top_n_is_20(self) -> None:
        with patch("sys.argv", ["tw_sector_screener.py", "--theme", "AI"]):
            args = cli.parse_args()
        self.assertEqual(args.top_n, 20)

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
                    universe_mode="coverage",
                    benchmark="TAIEX",
                    output_formats={"md", "json", "csv"},
                    config_path=None,
                    coverage_list_path=coverage_path,
                    run_backtest=True,
                    rebalance="monthly",
                    cost_bps=10,
                    validation_window="1y",
                    quality_update_mode="auto",
                    quality_update_budget_sec=3.0,
                    quality_history_depth=8,
                )

            self.assertTrue(outputs["md"].exists())
            self.assertTrue(outputs["json"].exists())
            self.assertTrue(outputs["csv"].exists())
            self.assertTrue(outputs["audit"].exists())
            self.assertTrue(outputs["watchlist"].exists())
            self.assertTrue(outputs["backtest"].exists())
            self.assertTrue(outputs["decisions"].exists())
            self.assertEqual(outputs["md"].parent, output_dir / "reports" / "20260312" / "AI")
            self.assertEqual(outputs["audit"].parent, output_dir / "audit" / "20260312")
            self.assertEqual(outputs["watchlist"].parent, output_dir / "watchlists" / "AI")
            self.assertEqual(outputs["backtest"].parent, output_dir / "backtests" / "AI")

            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertIn("picks", payload)
            self.assertIn("buying_ranking", payload)
            self.assertIn("actionable_queue", payload)
            self.assertIn("watchlist_candidates", payload)
            self.assertIn("research_list", payload)
            self.assertIn("audit", payload)
            self.assertIn("action_view", payload["picks"][0])
            self.assertIn("recommendation", payload["picks"][0])
            self.assertIn("risk_score", payload["picks"][0])
            self.assertIn("recommendation_detail", payload["picks"][0])
            self.assertIn("confidence_score", payload["picks"][0])
            self.assertIn("quality_data_source", payload["picks"][0])
            self.assertIn("quality_periods_used", payload["picks"][0])
            self.assertIn("theme_buckets", payload["picks"][0])
            self.assertIn("primary_bucket", payload["picks"][0])
            self.assertIn("decision_tier", payload["picks"][0])
            self.assertIn("actionability_score", payload["picks"][0])
            self.assertIn("buying_tier", payload["picks"][0])
            self.assertIn("stock_risk_metrics", payload["picks"][0])
            self.assertIn("risk_adjusted_score", payload["picks"][0])
            self.assertIn("stock_risk_metrics", payload["buying_ranking"][0])
            self.assertIn("buying_tier", payload["buying_ranking"][0])
            self.assertIn("validation_summary", payload)
            self.assertEqual(payload["validation_summary"]["mode"], "validation_report_v3")
            self.assertEqual(payload["validation_summary"]["base_mode"], "factor_aware_cross_sectional_v2")
            self.assertIn("windows", payload["validation_summary"])
            self.assertIn("portfolio_diagnostics", payload["validation_summary"]["metrics"])
            self.assertIn("macro_regime_overlay", payload["picks"][0])
            self.assertIn("quality_coverage_summary", payload["sector_overview"])
            self.assertIn("selection_pool_count", payload["sector_overview"])
            self.assertEqual(payload["sector_overview"]["research_display_limit"], 2)
            self.assertEqual(payload["sector_overview"]["buying_display_limit"], 2)

            audit = json.loads(outputs["audit"].read_text(encoding="utf-8"))
            self.assertEqual(audit["output_root"], str(output_dir))
            self.assertEqual(audit["universe_mode"], "coverage")
            self.assertIn("universe_size_before_limit", audit)
            self.assertIn("selection_pool_count", audit)
            self.assertIn("universe_limit_applied", audit)
            self.assertIn("theme_mode is deprecated", " / ".join(audit["warnings"]))
            self.assertIn("backtest_config", audit)
            self.assertIn("recommendation_policy_version", audit)
            self.assertIn("recommendation_distribution", audit)
            self.assertEqual(audit["ranking_policy_version"], "tw-three-list-v1")
            self.assertEqual(audit["action_queue_policy_version"], "tw-actionable-queue-v1")
            self.assertEqual(audit["stock_risk_metrics_version"], "stock-risk-v1")
            self.assertEqual(audit["research_display_limit"], 2)
            self.assertEqual(audit["buying_display_limit"], 2)
            self.assertEqual(audit["actionable_display_limit"], 2)
            self.assertEqual(audit["watchlist_display_limit"], 2)
            self.assertIn("decision_tier_distribution", audit)
            self.assertEqual(audit["buying_gate_policy_version"], "tw-buying-gate-v2")
            self.assertIn("buying_tier_distribution", audit)
            self.assertIn("near_buy_count", audit)
            self.assertIn("list_counts", audit)
            self.assertEqual(audit["connector_contract_version"], "supplementary-json-contract-v1")
            self.assertIn("supplementary_connectors", audit)
            self.assertIn("quality_coverage_summary", audit)
            self.assertIn("quarterly_store_path", audit)
            self.assertEqual(audit["quality_period_requirement"], 2)
            self.assertEqual(audit["quality_update_mode"], "auto")
            self.assertEqual(audit["quality_update_decision"], "sync-repair")
            self.assertTrue(audit["backfill_enqueued"])
            self.assertEqual(audit["backfill_run_id"], "backfill-auto-1")

            watchlist = json.loads(outputs["watchlist"].read_text(encoding="utf-8"))
            self.assertIn("rating_change_reason", watchlist["rows"][0])
            self.assertIn("event_risk_state", watchlist["rows"][0])
            self.assertIn("recommendation", watchlist["rows"][0])
            self.assertIn("recommendation_delta", watchlist["rows"][0])
            self.assertIn("action_required", watchlist["rows"][0])

            csv_text = outputs["csv"].read_text(encoding="utf-8-sig")
            self.assertIn("risk_adjusted_score", csv_text)
            self.assertIn("buying_tier", csv_text)
            self.assertIn("sharpe_ratio", csv_text)

    def test_run_supports_skip_update_mode_without_enqueue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
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
                    output_formats={"json"},
                    config_path=None,
                    coverage_list_path=None,
                    run_backtest=False,
                    rebalance="monthly",
                    cost_bps=10,
                    validation_window="1y",
                    quality_update_mode="skip",
                    quality_update_budget_sec=1.0,
                    quality_history_depth=8,
                )

            audit = json.loads(outputs["audit"].read_text(encoding="utf-8"))
            self.assertEqual(audit["quality_update_mode"], "skip")
            self.assertEqual(audit["quality_update_decision"], "skipped")
            self.assertFalse(audit["backfill_enqueued"])


if __name__ == "__main__":
    unittest.main()
