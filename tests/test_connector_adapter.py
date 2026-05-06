import unittest
from datetime import date

from src.connectors.contract import build_connector_result, normalize_macro_overlay


class ConnectorAdapterTests(unittest.TestCase):
    def test_failure_result_has_contract_and_quality_flag(self) -> None:
        result = build_connector_result(
            success=False,
            source="fred",
            as_of=date(2026, 5, 3),
            error="network unavailable",
            license_hint="supplementary",
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["data"], {})
        self.assertEqual(result["freshness"], "unavailable")
        self.assertIn("connector:fred:failed", result["data_quality_flags"])

    def test_macro_overlay_is_supplementary_and_never_rank_signal(self) -> None:
        result = build_connector_result(
            success=True,
            source="manual-macro",
            as_of=date(2026, 5, 3),
            data={"risk_level": "elevated", "risk_adjustment": 12.0, "regime": "tight_liquidity"},
            freshness="current",
            license_hint="supplementary",
        )

        overlay = normalize_macro_overlay(result)

        self.assertEqual(overlay["tier"], "supplementary")
        self.assertEqual(overlay["risk_adjustment"], 12.0)
        self.assertFalse(overlay["rank_signal"])
        self.assertIn("macro_regime_overlay", overlay["evidence_refs"])


if __name__ == "__main__":
    unittest.main()
