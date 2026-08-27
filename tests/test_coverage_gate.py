import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import tw_sector_screener as cli
from src.analysis.coverage_gate import CoverageGateError, evaluate_coverage_gate


try:
    from test_cli_outputs import _FakeProvider
except ImportError:
    from tests.test_cli_outputs import _FakeProvider


class _CoverageProvider(_FakeProvider):
    def __init__(self, fail_symbols: set[str] | None = None, count: int = 3, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.fail_symbols = fail_symbols or set()
        self.count = count

    def load_theme_universe(self, theme: str, min_monthly_revenue: float = 0.0, theme_mode: str = "strict"):
        return [
            {
                "symbol": str(2300 + index),
                "name": f"測試股{index}",
                "market": "TWSE",
                "industry": "半導體業",
                "monthly_revenue": 1000.0 - index,
                "revenue_yoy": 20.0,
                "revenue_mom": 4.0,
                "revenue_yoy_prev": 18.0,
                "revenue_mom_prev": 1.5,
            }
            for index in range(self.count)
        ]

    def get_ohlcv(self, symbol: str, market: str, as_of, lookback: int = 252):
        if symbol in self.fail_symbols:
            raise RuntimeError("mock daily data failure")
        return super().get_ohlcv(symbol, market, as_of, lookback)


class CoverageGateTests(unittest.TestCase):
    def test_short_ranked_pool_fails_even_when_it_has_thirty_rows_requirement(self) -> None:
        result = evaluate_coverage_gate(
            coverage_count=54,
            attempted_count=54,
            ranked_count=17,
            top_n=30,
            missing_candidates=[{"symbol": "6669", "reason": "HTTP 308"}],
        )

        self.assertFalse(result.passed)
        self.assertIn("daily_data_missing", result.reason_codes)
        self.assertIn("ranked_below_top_n", result.reason_codes)
        self.assertEqual(result.as_dict()["missing_candidates"][0]["symbol"], "6669")

    def test_complete_coverage_with_at_least_top_n_passes(self) -> None:
        result = evaluate_coverage_gate(
            coverage_count=54,
            attempted_count=54,
            ranked_count=54,
            top_n=30,
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.reason_codes, ())

    def test_missing_benchmark_fails_even_with_complete_candidate_coverage(self) -> None:
        result = evaluate_coverage_gate(
            coverage_count=54,
            attempted_count=54,
            ranked_count=54,
            top_n=30,
            benchmark_valid=False,
        )

        self.assertFalse(result.passed)
        self.assertIn("benchmark_data_missing", result.reason_codes)
        self.assertFalse(result.as_dict()["benchmark_valid"])

    def test_run_writes_diagnostic_artifacts_then_raises_without_decision_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            provider = _CoverageProvider(fail_symbols={"2301"}, count=3)
            with patch.object(cli, "TwMarketProvider", lambda **_: provider):
                with self.assertRaises(CoverageGateError) as context:
                    cli.run(
                        theme="AI",
                        as_of=__import__("datetime").date(2026, 3, 12),
                        top_n=3,
                        universe_limit=10,
                        min_monthly_revenue=0.0,
                        lookback=130,
                        timeout=0.1,
                        output_root=output_root,
                        universe_mode="coverage",
                        benchmark="TAIEX",
                        output_formats={"md", "json", "csv"},
                        run_backtest=True,
                        quality_update_mode="skip",
                        quality_update_budget_sec=1.0,
                        quality_history_depth=8,
                    )

            artifacts = context.exception.artifacts
            for key in ("md", "json", "csv", "audit"):
                self.assertTrue(artifacts[key].exists(), key)
            self.assertNotIn("backtest", artifacts)
            self.assertFalse((output_root / "decisions" / "decision-ledger.sqlite").exists())
            report = json.loads(artifacts["json"].read_text(encoding="utf-8"))
            audit = json.loads(artifacts["audit"].read_text(encoding="utf-8"))
            self.assertEqual(report["report_status"], "failed")
            self.assertEqual(report["ranking_status"], "diagnostic_only")
            self.assertFalse(report["coverage_gate"]["passed"])
            self.assertEqual(audit["daily_data_failures"][0]["symbol"], "2301")
            with artifacts["csv"].open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["report_status"] == "failed" for row in rows))
            self.assertTrue(all(row["ranking_valid"] == "False" for row in rows))

    def test_run_returns_complete_artifacts_for_full_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            provider = _CoverageProvider(count=3)
            with patch.object(cli, "TwMarketProvider", lambda **_: provider):
                outputs = cli.run(
                    theme="AI",
                    as_of=__import__("datetime").date(2026, 3, 12),
                    top_n=3,
                    universe_limit=10,
                    min_monthly_revenue=0.0,
                    lookback=130,
                    timeout=0.1,
                    output_root=output_root,
                    universe_mode="coverage",
                    benchmark="TAIEX",
                    output_formats={"md", "json", "csv"},
                    run_backtest=False,
                    quality_update_mode="skip",
                    quality_update_budget_sec=1.0,
                    quality_history_depth=8,
                )

            report = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(report["report_status"], "complete")
            self.assertTrue(report["coverage_gate"]["passed"])
            self.assertTrue(report["ranking_valid"])
            with outputs["csv"].open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 3)
            self.assertTrue(all(row["ranking_valid"] == "True" for row in rows))
            self.assertTrue(outputs["watchlist"].exists())


if __name__ == "__main__":
    unittest.main()
