from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.backup_market_data import backup_database
from src.providers.market_data_store import init_market_data_db, record_source_payload


class MarketDataBackupTests(unittest.TestCase):
    def test_backup_manifest_requires_integrity_payload_and_logical_parity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "market_data.sqlite"
            output_dir = root / "backups"
            init_market_data_db(database)
            payload = {"data": [["2330", "100"]], "fields": ["symbol", "close"]}
            payload_id = record_source_payload(
                database,
                dataset_key="daily_bars",
                request_method="GET",
                source_endpoint="fixture.daily",
                source_url="https://example.test/daily",
                effective_date="2026-08-26",
                payload=payload,
            )

            outputs = backup_database(
                database,
                output_dir=output_dir,
                label="pre-migration",
            )
            manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))

            self.assertEqual(manifest["status"], "complete")
            self.assertTrue(manifest["logical_parity"])
            self.assertIn("source_sha256", manifest)
            self.assertIn("backup_sha256", manifest)
            self.assertEqual(
                manifest["source_preflight"]["source_payload_integrity"]["status"],
                "verified",
            )
            self.assertTrue(Path(outputs["database"]).exists())
            connection = sqlite3.connect(outputs["database"])
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM source_payloads WHERE payload_id = ?",
                        (payload_id,),
                    ).fetchone()[0],
                    1,
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
