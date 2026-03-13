import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import backfill_quarterly_history as cli


class _FakeBackfillProvider:
    def __init__(self, timeout: float = 0.1, **_: object) -> None:
        self.timeout = timeout
        self.quarterly_store_path = Path("C:/tmp/quarterly_fundamentals.sqlite")

    def backfill_quarterly_history(
        self,
        as_of,
        themes,
        theme_mode="strict",
        periods=8,
        only_missing=True,
        limit_symbols=None,
        batch_size=20,
        force_retry_days=30,
        trigger_type="manual",
    ):
        return {
            "as_of": as_of.isoformat(),
            "themes": [{"theme": "AI", "symbol_count": 2, "symbols": ["2330", "2382"]}],
            "theme_mode": theme_mode,
            "periods": periods,
            "target_periods": ["114Q4", "114Q3"],
            "target_symbol_count": 2,
            "queued_count": 4,
            "completed_count": 2,
            "unavailable_count": 1,
            "failed_count": 1,
            "quarterly_store_path": str(self.quarterly_store_path),
            "backfill_run_id": "manual-backfill-1",
            "quality_coverage_summary": {
                "current_complete_pct": 100.0,
                "previous_complete_pct": 50.0,
                "history_complete_pct": 0.0,
                "top_candidate_gaps": [{"rank": 1, "symbol": "2382", "quality_missing_reason": "unavailable"}],
            },
            "unresolved_symbols": ["2382"],
            "warnings": [],
        }


class BackfillCliTests(unittest.TestCase):
    def test_main_writes_json_and_markdown_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(cli, "TwMarketProvider", _FakeBackfillProvider):
                exit_code = cli.main(
                    [
                        "--as-of",
                        "2026-03-12",
                        "--themes",
                        "AI",
                        "--output-root",
                        str(output_root),
                    ]
                )

            self.assertEqual(exit_code, 0)
            json_path = output_root / "audit" / "20260312" / "quarterly-backfill-20260312.json"
            md_path = output_root / "audit" / "20260312" / "quarterly-backfill-20260312.md"
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["backfill_run_id"], "manual-backfill-1")
            self.assertEqual(payload["queued_count"], 4)


if __name__ == "__main__":
    unittest.main()
