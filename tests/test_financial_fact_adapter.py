from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.enrichment_adapters import (
    HistoricalFinancialFactsAdapter,
    parse_financial_facts_payload,
)
from src.providers.market_data_adapters import AdapterContext, FetchRequest, FetchResult
from src.providers.market_data_store import query_financial_facts_as_of, upsert_financial_fact


class FinancialFactAdapterTests(unittest.TestCase):
    def test_parser_keeps_revision_and_point_in_time_fields(self) -> None:
        rows = parse_financial_facts_payload(
            [
                {
                    "公司代號": "2330",
                    "fact_code": "revenue",
                    "fiscal_period": "115Q1",
                    "value": "100,000",
                    "unit": "TWD",
                    "consolidation": "consolidated",
                    "effective_date": "2026-03-31",
                    "available_date": "2026-05-10",
                    "published_at": "2026-05-10T08:00:00+08:00",
                    "revision_id": "r1",
                    "revision_sequence": "1",
                }
            ],
            market="TWSE",
            source_endpoint="test.facts",
            source_url="https://example.test/facts",
            source_payload_sha256="facts-1",
        )
        self.assertEqual(rows[0]["symbol"], "2330")
        self.assertEqual(rows[0]["value"], 100000.0)
        self.assertEqual(rows[0]["effective_date"], date(2026, 3, 31))
        self.assertEqual(rows[0]["available_date"], date(2026, 5, 10))
        self.assertEqual(rows[0]["revision_sequence"], 1)

    def test_adapter_upsert_replays_latest_revision_at_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            context = AdapterContext(
                as_of=date(2026, 8, 27),
                database_path=str(db_path),
                run_id="facts-run",
                requested_from=date(2026, 1, 1),
                requested_to=date(2026, 6, 30),
                options={
                    "fetcher": lambda request: FetchResult(
                        status="fetched",
                        payload=[
                            {
                                "公司代號": "2330",
                                "fact_code": "revenue",
                                "fiscal_period": "115Q1",
                                "value": "100",
                                "unit": "TWD",
                                "consolidation": "consolidated",
                                "effective_date": "2026-03-31",
                                "available_date": "2026-05-10",
                                "revision_id": "r1",
                            },
                            {
                                "公司代號": "2330",
                                "fact_code": "revenue",
                                "fiscal_period": "115Q1",
                                "value": "110",
                                "unit": "TWD",
                                "consolidation": "consolidated",
                                "effective_date": "2026-03-31",
                                "available_date": "2026-07-01",
                                "revision_id": "r2",
                            },
                        ],
                        request=request,
                        payload_sha256="facts-2",
                    )
                },
            )
            request = FetchRequest(
                dataset_key="financial_facts",
                market="TWSE",
                symbol=None,
                requested_from=date(2026, 1, 1),
                requested_to=date(2026, 6, 30),
                method="GET",
                url="https://example.test/facts",
            )
            adapter = HistoricalFinancialFactsAdapter()
            result = adapter.fetch_range(request, context)
            parsed = adapter.parse(result, context)
            self.assertEqual(adapter.validate(parsed, context).status, "verified")
            self.assertEqual(adapter.upsert(parsed, context), 2)
            before = query_financial_facts_as_of(
                db_path,
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 6, 30),
                information_cutoff=date(2026, 6, 30),
                fact_code="revenue",
            )
            after = query_financial_facts_as_of(
                db_path,
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 8, 1),
                information_cutoff=date(2026, 8, 1),
                fact_code="revenue",
            )
            self.assertEqual(before[0]["value"], 100.0)
            self.assertEqual(after[0]["value"], 110.0)

    def test_upsert_serializes_raw_payload_dates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            upsert_financial_fact(
                db_path,
                market="TWSE",
                symbol="6415",
                fact_code="eps",
                fiscal_period="2026Q1",
                value=1.98,
                unit="TWD_per_share",
                consolidation="consolidated",
                effective_date=date(2026, 3, 31),
                available_date=date(2026, 8, 27),
                revision_id="date-payload",
                raw_payload_json={"effective_date": date(2026, 3, 31)},
            )
            import sqlite3

            conn = sqlite3.connect(db_path)
            try:
                raw = conn.execute(
                    "SELECT raw_payload_json FROM financial_fact_observations"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertIn('"effective_date": "2026-03-31"', raw)


if __name__ == "__main__":
    unittest.main()
