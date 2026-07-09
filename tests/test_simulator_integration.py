import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from src.simulator.dashboard import render_dashboard
from src.simulator.engine import SimulatorConfig, run_simulation


class _FakeProvider:
    def get_taiex_series(self, as_of: date, lookback: int = 252):
        start = date(2026, 4, 27)
        return [{"date": start + timedelta(days=i), "close": 100 + i} for i in range(5)]

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
        base = 100.0 + (int(symbol[-1]) * 2)
        rows = []
        for i in range(5):
            d = date(2026, 4, 27) + timedelta(days=i)
            close = base + i
            rows.append({"date": d, "open": close - 1, "high": close + 2, "low": close - 2, "close": close, "volume": 1000})
        return [row for row in rows if row["date"] <= as_of][-lookback:]


class _ClosedMayFirstProvider(_FakeProvider):
    def get_taiex_series(self, as_of: date, lookback: int = 252):
        return [row for row in super().get_taiex_series(as_of, lookback) if row["date"] < date(2026, 5, 1)]

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
        return [row for row in super().get_ohlcv(symbol, market, as_of, lookback) if row["date"] < date(2026, 5, 1)]


class _StaleTaiexProvider(_FakeProvider):
    def get_taiex_series(self, as_of: date, lookback: int = 252):
        return [row for row in super().get_taiex_series(as_of, lookback) if row["date"] <= date(2026, 4, 30)]

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
        base = 100.0 + (int(symbol[-1]) * 2)
        rows = []
        cursor = date(2026, 4, 27)
        while cursor <= date(2026, 5, 4):
            if cursor.weekday() < 5 and cursor != date(2026, 5, 1):
                close = base + (cursor - date(2026, 4, 27)).days
                rows.append({"date": cursor, "open": close - 1, "high": close + 2, "low": close - 2, "close": close, "volume": 1000})
            cursor += timedelta(days=1)
        return [row for row in rows if row["date"] <= as_of][-lookback:]


class _StaleTaiexNoOhlcvProvider(_StaleTaiexProvider):
    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
        return [row for row in super().get_ohlcv(symbol, market, as_of, lookback) if row["date"] <= date(2026, 4, 30)]


def _fake_runner(**kwargs):
    output_root = Path(kwargs["output_root"])
    theme = kwargs["theme"]
    as_of = kwargs["as_of"]
    date_tag = as_of.strftime("%Y%m%d")
    path = output_root / "fake-screener" / theme / f"{date_tag}.json"
    md_path = output_root / "fake-screener" / theme / f"{date_tag}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    offset = 0 if theme == "AI" else 5
    picks = []
    for i in range(1, 7):
        recommendation = "買入" if i in {1, 2, 3} else "持有"
        if i == 6:
            recommendation = "賣出"
        picks.append(
            {
                "symbol": f"230{i + offset}",
                "name": f"測試{i}",
                "market": "TWSE",
                "rank": i,
                "close": 100.0 + i,
                "idea_score": 80.0 - i,
                "rank_score": 80.0 - i,
                "recommendation": recommendation,
                "confidence_score": 88.0,
                "risk_score": 30.0 + i,
                "stock_risk_metrics": {
                    "risk_adjusted_score": 60.0 + i,
                    "sharpe_ratio": 0.5 + (i / 10),
                    "sortino_ratio": 0.7 + (i / 10),
                    "max_drawdown_pct": -10.0 - i,
                    "annualized_volatility_pct": 20.0 + i,
                },
                "target_range": {"low": 200.0, "base": 105.0, "high": 110.0, "basis": "mixed"},
                "data_quality_flags": [],
                "volatility20": 20.0,
                "action_view": {"action": "Neutral"},
                "recommendation_detail": {"evidence_refs": ["trend_score"]},
            }
        )
    path.write_text(
        json.dumps(
            {
                "picks": picks,
                "buying_ranking": [{**row, "list_rank": idx, "list_type": "buying_ranking"} for idx, row in enumerate(picks[:3], start=1)],
                "actionable_queue": [{**row, "list_rank": idx, "list_type": "actionable_queue", "next_action": "wait"} for idx, row in enumerate(picks[3:5], start=1)],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    md_path.write_text(
        "\n".join(
            [
                "# 台股類股選股報告",
                "",
                f"- 主題：`{theme}`",
                f"- 截止日：`{as_of.isoformat()}`",
                "",
                "## 摘要",
                "fake daily report",
                "",
                "## Buying Ranking / 買進優先序",
                "| 排名 | 代碼 |",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": path, "md": md_path}


class SimulatorIntegrationTests(unittest.TestCase):
    def test_run_simulation_writes_outputs_and_shared_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SimulatorConfig(
                themes=["AI", "半導體"],
                theme_mode="strict",
                start_date=date(2026, 4, 28),
                end_date=date(2026, 5, 1),
                initial_cash=1_000_000,
                top_n=10,
                recommendation_mode="deterministic",
                analysis_cache="refresh",
                output_root=Path(tmp),
                run_id="test-run",
            )
            outputs = run_simulation(config, screener_runner=_fake_runner, provider=_FakeProvider())
            for value in outputs.values():
                paths = value if isinstance(value, list) else [value]
                for path in paths:
                    self.assertTrue(path.exists(), path)
            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            self.assertEqual(len(summary["portfolio_summaries"]), 3)
            for item in summary["portfolio_summaries"]:
                self.assertIn("portfolio_diagnostics", item)
                self.assertIn("var95_pct", item["portfolio_diagnostics"])
            self.assertEqual(len(summary["latest_analysis"]), 10)
            self.assertIn("buying_ranking", summary)
            self.assertIn("actionable_queue", summary)
            self.assertIn("stock_risk_metrics", summary["latest_analysis"][0])
            self.assertIn("risk_adjusted_score", summary["latest_analysis"][0])
            self.assertIn("sharpe_ratio", summary["latest_analysis"][0])
            self.assertTrue((Path(tmp) / "simulations" / "test-run" / "analysis" / "20260430" / "merged-top30.json").exists())
            self.assertTrue((Path(tmp) / "simulations" / "test-run" / "analysis" / "20260430" / "daily-analysis-manifest.json").exists())
            self.assertEqual(len(outputs["analysis_reports"]), 2)
            for report_path in outputs["analysis_reports"]:
                self.assertTrue(report_path.exists())
                self.assertIn("台股類股選股報告", report_path.read_text(encoding="utf-8"))
            dashboard = outputs["dashboard"].read_text(encoding="utf-8")
            self.assertIn("aggressive", dashboard)
            self.assertIn("Buying Ranking", dashboard)
            self.assertIn("Actionable Queue", dashboard)

    def test_daily_closed_market_carries_forward_latest_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = dict(
                themes=["AI", "半導體"],
                theme_mode="strict",
                initial_cash=1_000_000,
                top_n=10,
                recommendation_mode="deterministic",
                analysis_cache="refresh",
                output_root=Path(tmp),
                run_id="daily-AI-半導體",
                mode="daily",
            )
            run_simulation(
                SimulatorConfig(start_date=date(2026, 4, 30), end_date=date(2026, 4, 30), **base),
                screener_runner=_fake_runner,
                provider=_ClosedMayFirstProvider(),
            )

            outputs = run_simulation(
                SimulatorConfig(start_date=date(2026, 5, 1), end_date=date(2026, 5, 1), **base),
                screener_runner=_fake_runner,
                provider=_ClosedMayFirstProvider(),
            )

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            aggressive = next(item for item in summary["portfolio_summaries"] if item["portfolio_id"] == "aggressive")
            self.assertLess(aggressive["cash"], 1_000_000)
            self.assertGreater(aggressive["holdings_value"], 0)
            equity_dates = {row["trade_date"] for row in summary["daily_equity"]}
            self.assertEqual(equity_dates, {"2026-04-30", "2026-05-01"})
            self.assertEqual(len(summary["daily_equity"]), 6)
            self.assertFalse(summary["market_status"]["is_trading_day"])
            self.assertIn("勞動節休市", summary["market_status"]["note"])
            manifest = json.loads(outputs["analysis_manifest"].read_text(encoding="utf-8"))
            self.assertEqual(manifest["cache_status"], "carry_forward")
            self.assertEqual(manifest["carry_forward_as_of"], "2026-05-01")
            self.assertEqual(Path(outputs["analysis_manifest"]).parts[-2:], ("20260501", "daily-analysis-manifest.json"))
            self.assertEqual(len(outputs["analysis_reports"]), 2)
            self.assertIn("勞動節休市", outputs["dashboard"].read_text(encoding="utf-8"))

    def test_reuse_snapshot_rebuilds_missing_markdown_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SimulatorConfig(
                themes=["AI", "半導體"],
                theme_mode="strict",
                start_date=date(2026, 5, 4),
                end_date=date(2026, 5, 4),
                initial_cash=1_000_000,
                top_n=10,
                recommendation_mode="deterministic",
                analysis_cache="refresh",
                output_root=Path(tmp),
                run_id="daily-AI-半導體",
                mode="daily",
            )
            first = run_simulation(config, screener_runner=_fake_runner, provider=_StaleTaiexProvider())
            for report_path in first["analysis_reports"]:
                report_path.unlink()

            second = run_simulation(
                SimulatorConfig(
                    themes=["AI", "半導體"],
                    theme_mode="strict",
                    start_date=date(2026, 5, 4),
                    end_date=date(2026, 5, 4),
                    initial_cash=1_000_000,
                    top_n=10,
                    recommendation_mode="deterministic",
                    analysis_cache="reuse",
                    output_root=Path(tmp),
                    run_id="daily-AI-半導體",
                    mode="daily",
                ),
                screener_runner=_fake_runner,
                provider=_StaleTaiexProvider(),
            )

            manifest = json.loads(second["analysis_manifest"].read_text(encoding="utf-8"))
            self.assertEqual(manifest["cache_status"], "rebuilt_missing_reports")
            for report_path in second["analysis_reports"]:
                self.assertTrue(report_path.exists())

    def test_daily_stale_taiex_uses_calendar_weekday_for_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outputs = run_simulation(
                SimulatorConfig(
                    themes=["AI", "半導體"],
                    theme_mode="strict",
                    start_date=date(2026, 5, 4),
                    end_date=date(2026, 5, 4),
                    initial_cash=1_000_000,
                    top_n=10,
                    recommendation_mode="deterministic",
                    analysis_cache="refresh",
                    output_root=Path(tmp),
                    run_id="daily-AI-半導體",
                    mode="daily",
                ),
                screener_runner=_fake_runner,
                provider=_StaleTaiexProvider(),
            )

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            self.assertTrue(summary["market_status"]["is_trading_day"])
            self.assertEqual(summary["market_status"]["source"], "TWSE FMTQIK + OHLCV cross-check")
            self.assertIn("2026-05-04", summary["market_status"]["fallback_dates"])
            self.assertTrue(summary["market_status"]["warnings"])
            self.assertIn("2026-05-04", {row["trade_date"] for row in summary["daily_equity"]})
            self.assertGreater(len(summary["orders"]), 0)

    def test_daily_same_day_analysis_uses_same_day_manifest_and_missing_exact_execution_data(self) -> None:
        class _MissingCandidateExecutionProvider(_StaleTaiexProvider):
            def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
                rows = super().get_ohlcv(symbol, market, as_of, lookback)
                if symbol in {"2301", "2302", "2303", "2306", "2307", "2308"}:
                    return [row for row in rows if row["date"] < date(2026, 5, 4)]
                return rows

        with tempfile.TemporaryDirectory() as tmp:
            outputs = run_simulation(
                SimulatorConfig(
                    themes=["AI", "半導體"],
                    theme_mode="strict",
                    start_date=date(2026, 5, 4),
                    end_date=date(2026, 5, 4),
                    initial_cash=1_000_000,
                    top_n=10,
                    recommendation_mode="deterministic",
                    analysis_cache="refresh",
                    output_root=Path(tmp),
                    run_id="daily-AI-半導體",
                    mode="daily",
                    daily_analysis_mode="same-day",
                ),
                screener_runner=_fake_runner,
                provider=_MissingCandidateExecutionProvider(),
            )

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            manifest = json.loads(outputs["analysis_manifest"].read_text(encoding="utf-8"))
            self.assertEqual(manifest["analysis_date"], "2026-05-04")
            self.assertEqual(manifest["execution_date"], "2026-05-05")
            self.assertTrue(summary["market_status"]["warnings"])
            self.assertIn("execution_price_proxy", summary["market_status"])
            self.assertTrue(all(order["status"] == "filled" for order in summary["orders"]))
            self.assertGreater(summary["trade_count"], 0)

    def test_daily_rerun_replaces_orders_and_trades_for_same_execution_date(self) -> None:
        class _MissingCandidateExecutionProvider(_StaleTaiexProvider):
            def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 10):
                rows = super().get_ohlcv(symbol, market, as_of, lookback)
                if symbol in {"2301", "2302", "2303", "2306", "2307", "2308"}:
                    return [row for row in rows if row["date"] < date(2026, 5, 4)]
                return rows

        with tempfile.TemporaryDirectory() as tmp:
            base = dict(
                themes=["AI", "半導體"],
                theme_mode="strict",
                start_date=date(2026, 5, 4),
                end_date=date(2026, 5, 4),
                initial_cash=1_000_000,
                top_n=10,
                recommendation_mode="deterministic",
                analysis_cache="refresh",
                output_root=Path(tmp),
                run_id="daily-AI-半導體",
                mode="daily",
                daily_analysis_mode="same-day",
            )
            first = run_simulation(SimulatorConfig(**base), screener_runner=_fake_runner, provider=_StaleTaiexProvider())
            first_summary = json.loads(first["summary"].read_text(encoding="utf-8"))
            self.assertGreater(len(first_summary["orders"]), 0)

            second = run_simulation(SimulatorConfig(**base), screener_runner=_fake_runner, provider=_MissingCandidateExecutionProvider())
            second_summary = json.loads(second["summary"].read_text(encoding="utf-8"))

            import sqlite3

            conn = sqlite3.connect(Path(tmp) / "simulations" / "daily-AI-半導體" / "simulator.sqlite")
            try:
                orders_count = conn.execute("SELECT COUNT(*) FROM orders WHERE run_id = ? AND trade_date = ?", ("daily-AI-半導體", "2026-05-04")).fetchone()[0]
                trades_count = conn.execute("SELECT COUNT(*) FROM trades WHERE run_id = ? AND trade_date = ?", ("daily-AI-半導體", "2026-05-04")).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(orders_count, len(second_summary["orders"]))
            self.assertEqual(trades_count, second_summary["trade_count"])
            csv_rows = second["daily_equity"].read_text(encoding="utf-8-sig").splitlines()[1:]
            csv_keys = [tuple(row.split(",")[:2]) for row in csv_rows]
            self.assertEqual(len(csv_keys), len(set(csv_keys)))
            self.assertEqual(len(csv_keys), len(second_summary["daily_equity"]))

    def test_daily_stale_taiex_without_ohlcv_does_not_mark_weekday_trading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = dict(
                themes=["AI", "半導體"],
                theme_mode="strict",
                initial_cash=1_000_000,
                top_n=10,
                recommendation_mode="deterministic",
                analysis_cache="refresh",
                output_root=Path(tmp),
                run_id="daily-AI-半導體",
                mode="daily",
            )
            run_simulation(
                SimulatorConfig(start_date=date(2026, 4, 30), end_date=date(2026, 4, 30), **base),
                screener_runner=_fake_runner,
                provider=_StaleTaiexNoOhlcvProvider(),
            )

            outputs = run_simulation(
                SimulatorConfig(start_date=date(2026, 5, 4), end_date=date(2026, 5, 4), **base),
                screener_runner=_fake_runner,
                provider=_StaleTaiexNoOhlcvProvider(),
            )

            summary = json.loads(outputs["summary"].read_text(encoding="utf-8"))
            self.assertFalse(summary["market_status"]["is_trading_day"])
            self.assertEqual(summary["market_status"]["source"], "TWSE FMTQIK")
            self.assertNotIn("fallback_dates", summary["market_status"])
            self.assertIn("2026-05-04", {row["trade_date"] for row in summary["daily_equity"]})
            self.assertEqual(summary["orders"], [])

    def test_dashboard_contains_three_portfolios(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = render_dashboard(
                Path(tmp) / "dashboard.html",
                {
                    "run_id": "demo",
                    "themes": ["AI", "半導體"],
                    "start_date": "2026-04-28",
                    "end_date": "2026-04-29",
                    "initial_cash": 1_000_000,
                    "portfolio_summaries": [
                        {"portfolio_id": "aggressive", "name": "激進型", "equity": 1, "return_pct": 0, "max_drawdown_pct": 0, "cash": 1, "holdings_value": 0},
                        {"portfolio_id": "balanced", "name": "穩健型", "equity": 1, "return_pct": 0, "max_drawdown_pct": 0, "cash": 1, "holdings_value": 0},
                        {"portfolio_id": "conservative", "name": "保守型", "equity": 1, "return_pct": 0, "max_drawdown_pct": 0, "cash": 1, "holdings_value": 0},
                    ],
                    "daily_equity": [],
                    "positions": [],
                    "orders": [],
                    "latest_analysis": [],
                },
            )
            html = path.read_text(encoding="utf-8")
            self.assertIn("激進型", html)
            self.assertIn("穩健型", html)
            self.assertIn("保守型", html)


if __name__ == "__main__":
    unittest.main()
