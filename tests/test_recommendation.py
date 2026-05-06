import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.analysis.recommendation import build_sector_recommendation
from src.report.decision_ledger import write_decision


class SectorRecommendationTests(unittest.TestCase):
    def test_recommendation_contract(self) -> None:
        rec = build_sector_recommendation(
            {
                "symbol": "2330",
                "as_of": "2026-03-12",
                "close": 800.0,
                "atr14": 15.0,
                "idea_score": 78.0,
                "confidence_score": 82.0,
                "trend_score": 75.0,
                "momentum_score": 72.0,
                "value_score": 55.0,
                "fundamental_score": 66.0,
                "quality_score": 70.0,
                "benchmark_score": 68.0,
                "risk_control_score": 78.0,
                "volatility20": 22.0,
                "data_quality_flags": [],
            }
        )
        self.assertIn(rec["recommendation"], {"買入", "持有", "賣出"})
        self.assertIn("risk_score", rec)
        self.assertIn("target_range", rec)
        self.assertIn("position_note", rec)
        self.assertIn("evidence_refs", rec)

    def test_quality_blocker_prevents_buy(self) -> None:
        rec = build_sector_recommendation(
            {
                "symbol": "9999",
                "as_of": "2026-03-12",
                "close": 50.0,
                "idea_score": 88.0,
                "confidence_score": 90.0,
                "trend_score": 80.0,
                "momentum_score": 80.0,
                "value_score": 80.0,
                "fundamental_score": 80.0,
                "quality_score": 80.0,
                "benchmark_score": 80.0,
                "risk_control_score": 80.0,
                "volatility20": 20.0,
                "data_quality_flags": ["quality:fetch_failed"],
            }
        )
        self.assertNotEqual(rec["recommendation"], "買入")

    def test_macro_overlay_raises_risk_without_upgrading_recommendation(self) -> None:
        base_row = {
            "symbol": "2330",
            "as_of": "2026-03-12",
            "close": 800.0,
            "idea_score": 78.0,
            "confidence_score": 82.0,
            "trend_score": 75.0,
            "momentum_score": 72.0,
            "value_score": 55.0,
            "fundamental_score": 66.0,
            "quality_score": 70.0,
            "benchmark_score": 68.0,
            "risk_control_score": 78.0,
            "volatility20": 22.0,
            "data_quality_flags": [],
        }
        base = build_sector_recommendation(base_row)
        stressed = build_sector_recommendation(
            {
                **base_row,
                "macro_regime_overlay": {
                    "risk_adjustment": 20.0,
                    "evidence_refs": ["macro_regime_overlay"],
                    "rank_signal": False,
                    "risk_level": "elevated",
                },
            }
        )

        self.assertGreater(stressed["risk_score"], base["risk_score"])
        self.assertIn("macro_regime_overlay", stressed["evidence_refs"])
        self.assertFalse(stressed["macro_regime_overlay"]["rank_signal"])
        self.assertNotEqual(stressed["recommendation"], "買入")

    def test_decision_ledger(self) -> None:
        payload = {
            "symbol": "2330",
            "as_of": "2026-03-12",
            "recommendation": "買入",
            "confidence_score": 80.0,
            "risk_score": 35.0,
            "action_view": "研究型加碼",
            "target_range": {"low": 790.0, "base": 820.0, "high": 850.0},
            "invalidation_conditions": ["相對題材轉弱"],
            "evidence_refs": ["trend_score"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "decision-ledger.sqlite"
            write_decision(path, "tw-sector-screener", payload, theme="AI")
            conn = sqlite3.connect(path)
            try:
                row = conn.execute("SELECT theme, recommendation, evidence_refs_json FROM decisions").fetchone()
            finally:
                conn.close()
        self.assertEqual(row[0], "AI")
        self.assertEqual(row[1], "買入")
        self.assertEqual(json.loads(row[2]), ["trend_score"])


if __name__ == "__main__":
    unittest.main()
