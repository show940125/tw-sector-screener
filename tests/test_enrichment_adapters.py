from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.providers.enrichment_adapters import (
    CorporateActionsAdapter,
    HistoricalMonthlyRevenueAdapter,
    HistoricalValuationAdapter,
    MarketSessionsAdapter,
    parse_corporate_actions_payload,
    parse_market_sessions_payload,
    parse_mops_company_revenue_payload,
    parse_monthly_revenue_payload,
    parse_mops_revenue_html,
    parse_twse_company_financial_payload,
    parse_valuation_payload,
)
from src.providers.market_data_adapters import AdapterContext, FetchRequest, FetchResult


class EnrichmentAdapterTests(unittest.TestCase):
    def test_mops_company_json_revenue_parser_reads_yoy_not_cumulative_percent(self) -> None:
        rows = parse_mops_company_revenue_payload(
            {
                "code": 200,
                "result": {
                    "companyId": "5347",
                    "yymm": "11407",
                    "data": [
                        ["本月", "3,612,796"],
                        ["去年同期", "3,556,803"],
                        ["增減金額", "55,993"],
                        ["增減百分比", "1.57"],
                        ["本年累計", "27,261,448"],
                        ["去年累計", "24,254,733"],
                        ["增減金額", "3,006,715"],
                        ["增減百分比", "12.40"],
                    ],
                },
            },
            market="TPEx",
            symbol="5347",
            available_date=date(2026, 8, 27),
            source_endpoint="mops.t05st10_ifrs",
            source_url="https://mops.twse.com.tw/mops/api/t05st10_ifrs",
        )
        self.assertEqual(rows[0]["revenue_month"], "2025-07")
        self.assertEqual(rows[0]["monthly_revenue"], 3612796.0)
        self.assertIsNone(rows[0]["revenue_mom"])
        self.assertEqual(rows[0]["revenue_yoy"], 1.57)

    def test_twse_company_financial_chart_parser_recomputes_monthly_rates(self) -> None:
        payload = {
            "info": {"status": "success", "data": {"code": "2330"}},
            "chart": {
                "revenue": {
                    "categories": [
                        "202506", "202507", "202508", "202509", "202510", "202511",
                        "202512", "202601", "202602", "202603", "202604", "202605", "202606",
                    ],
                    "series": [{"name": "月營收", "data": [100, 110, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 120]}],
                },
                "pe": {"categories": [], "series": [{"name": "本益比", "data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]}]},
                "pb": {"categories": [], "series": [{"name": "股價淨值比", "data": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]}]},
            },
        }
        rows = parse_twse_company_financial_payload(
            payload,
            market="TWSE",
            symbol="2330",
            available_date=date(2026, 8, 27),
            source_endpoint="twse.IIH.company.financial",
            source_url="https://www.twse.com.tw/rwd/zh/IIH/company/financial?code=2330",
        )
        self.assertEqual(len(rows), 13)
        self.assertAlmostEqual(rows[1]["revenue_mom"], 10.0)
        self.assertIsNone(rows[1]["revenue_yoy"])
        self.assertAlmostEqual(rows[12]["revenue_mom"], 20.0)
        self.assertAlmostEqual(rows[12]["revenue_yoy"], 20.0)
        self.assertEqual(rows[12]["pe"], 13.0)

    def test_company_revenue_parsers_fail_closed_on_wrong_shape(self) -> None:
        with self.assertRaises(ValueError):
            parse_mops_company_revenue_payload(
                {"code": 500, "message": "bad"},
                market="TPEx",
                symbol="5347",
                available_date=date(2026, 8, 27),
                source_endpoint="mops.t05st10_ifrs",
                source_url="https://mops.twse.com.tw/mops/api/t05st10_ifrs",
            )
        with self.assertRaises(ValueError):
            parse_twse_company_financial_payload(
                {"info": {"status": "error"}},
                market="TWSE",
                symbol="2330",
                available_date=date(2026, 8, 27),
                source_endpoint="twse.IIH.company.financial",
                source_url="https://www.twse.com.tw/rwd/zh/IIH/company/financial?code=2330",
            )

    def test_monthly_revenue_payload_normalizes_roc_month_and_numeric_fields(self) -> None:
        rows = parse_monthly_revenue_payload(
            [
                {
                    "公司代號": "2330",
                    "資料年月": "11507",
                    "營業收入-當月營收": "1,234,567",
                    "營業收入-上月比較增減(%)": "12.5",
                    "營業收入-去年同月增減(%)": "-3.2",
                }
            ],
            market="TWSE",
            available_date=date(2026, 8, 10),
            source_endpoint="test.revenue",
            source_url="https://example.test/revenue",
            source_payload_sha256="payload-1",
        )
        self.assertEqual(rows, [
            {
                "market": "TWSE",
                "symbol": "2330",
                "revenue_month": "2026-07",
                "monthly_revenue": 1234567.0,
                "revenue_mom": 12.5,
                "revenue_yoy": -3.2,
                "available_date": date(2026, 8, 10),
                "published_at": None,
                "source_endpoint": "test.revenue",
                "source_url": "https://example.test/revenue",
                "source_payload_sha256": "payload-1",
            }
        ])

    def test_valuation_payload_supports_twse_fields_and_keeps_missing_metric_null(self) -> None:
        rows = parse_valuation_payload(
            {
                "stat": "OK",
                "fields": ["證券代號", "本益比", "股價淨值比", "殖利率(%)"],
                "data": [["2330", "24.5", "", "2.1"]],
            },
            market="TWSE",
            trade_date=date(2026, 8, 7),
            source_endpoint="test.valuation",
            source_url="https://example.test/valuation",
            source_payload_sha256="payload-2",
        )
        self.assertEqual(rows[0]["symbol"], "2330")
        self.assertEqual(rows[0]["trade_date"], date(2026, 8, 7))
        self.assertEqual(rows[0]["pe"], 24.5)
        self.assertIsNone(rows[0]["pb"])
        self.assertEqual(rows[0]["dividend_yield"], 2.1)

    def test_mops_html_revenue_parser_requires_expected_table_columns(self) -> None:
        html = """
        <table><tr><th>公司代號</th><th>營業收入-當月營收</th>
        <th>營業收入-上月比較增減(%)</th></tr>
        <tr><td>2330</td><td>1,000</td><td>5.0</td></tr></table>
        """
        rows = parse_mops_revenue_html(
            html,
            market="TWSE",
            revenue_month="11507",
            available_date=date(2026, 8, 10),
            source_endpoint="mops.revenue",
            source_url="https://mops.example/revenue",
            source_payload_sha256="html-payload",
        )
        self.assertEqual(rows[0]["revenue_month"], "2026-07")
        self.assertEqual(rows[0]["monthly_revenue"], 1000.0)
        self.assertEqual(rows[0]["revenue_mom"], 5.0)
        self.assertEqual(
            parse_mops_revenue_html(
                "<table><tr><th>名稱</th></tr><tr><td>bad</td></tr></table>",
                market="TWSE",
                revenue_month="11507",
                available_date=date(2026, 8, 10),
                source_endpoint="mops.revenue",
                source_url="https://mops.example/revenue",
            ),
            [],
        )

    def test_adapter_contract_fetches_parses_validates_and_upserts_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            context = AdapterContext(
                as_of=date(2026, 8, 27),
                database_path=str(db_path),
                run_id="run-1",
                requested_from=date(2026, 7, 1),
                requested_to=date(2026, 7, 31),
                options={
                    "fetcher": lambda request: FetchResult(
                        status="fetched",
                        payload=[
                            {
                                "公司代號": "2330",
                                "資料年月": "11507",
                                "營業收入-當月營收": "100",
                            }
                        ],
                        request=request,
                        payload_sha256="payload-3",
                    ),
                    "available_date": date(2026, 8, 10),
                    "source_endpoint": "test.revenue",
                    "source_url": "https://example.test/revenue",
                },
            )
            request = FetchRequest(
                dataset_key="monthly_revenue",
                market="TWSE",
                symbol=None,
                requested_from=date(2026, 7, 1),
                requested_to=date(2026, 7, 31),
                method="GET",
                url="https://example.test/revenue?month=11507",
            )
            adapter = HistoricalMonthlyRevenueAdapter()
            result = adapter.fetch_range(request, context)
            parsed = adapter.parse(result, context)
            validation = adapter.validate(parsed, context)
            inserted = adapter.upsert(parsed, context)
            self.assertEqual(validation.status, "verified")
            self.assertEqual(inserted, 1)
            self.assertEqual(adapter.partition_key(request), "2026-07")
            self.assertEqual(adapter.identity_key(parsed[0]), "TWSE|2330|2026-07")

    def test_corporate_action_parser_is_idempotent_by_event_identity(self) -> None:
        payload = [
            {
                "公司代號": "2330",
                "事件日期": "2026/08/01",
                "事件類型": "cash_dividend",
                "除權息日": "2026/08/05",
                "現金股利": "2.5",
            }
        ]
        rows = parse_corporate_actions_payload(
            payload,
            market="TWSE",
            source_endpoint="test.actions",
            source_url="https://example.test/actions",
            source_payload_sha256="actions-1",
        )
        changed_payload_rows = parse_corporate_actions_payload(
            [{**payload[0], "備註": "source wording changed"}],
            market="TWSE",
            source_endpoint="test.actions",
            source_url="https://example.test/actions",
            source_payload_sha256="actions-2",
        )
        self.assertEqual(rows[0]["action_date"], date(2026, 8, 1))
        self.assertEqual(rows[0]["cash_amount"], 2.5)
        action = CorporateActionsAdapter()
        self.assertEqual(action.identity_key(rows[0]), action.identity_key(changed_payload_rows[0]))

    def test_market_sessions_parser_does_not_infer_open_from_missing_flag(self) -> None:
        rows = parse_market_sessions_payload(
            [{"日期": "2026/08/03"}],
            market="TWSE",
            source_endpoint="test.sessions",
            source_url="https://example.test/sessions",
        )
        self.assertEqual(rows[0]["trade_date"], date(2026, 8, 3))
        self.assertFalse(rows[0]["is_open"])
        session = MarketSessionsAdapter()
        self.assertEqual(session.identity_key(rows[0]), "TWSE|2026-08-03")

    def test_market_sessions_parser_accepts_official_date_and_description_fields(self) -> None:
        rows = parse_market_sessions_payload(
            [
                {"Date": "1150102", "Description": "國曆新年開始交易。"},
                {"Date": "1150101", "Description": "依規定放假1日。"},
            ],
            market="TWSE",
            source_endpoint="twse.holidaySchedule",
            source_url="https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule",
        )
        self.assertEqual([row["trade_date"] for row in rows], [date(2026, 1, 2), date(2026, 1, 1)])
        self.assertTrue(rows[0]["is_open"])
        self.assertFalse(rows[1]["is_open"])

    def test_research_upserts_serialize_raw_dates_and_dicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            context = AdapterContext(
                as_of=date(2026, 8, 27),
                database_path=str(db_path),
                run_id="research-run",
                options={
                    "source_endpoint": "test.actions",
                    "source_url": "https://example.test/actions",
                    "available_date": date(2026, 8, 27),
                    "validation_status": "verified",
                },
            )
            request = FetchRequest(
                dataset_key="corporate_actions",
                market="TWSE",
                symbol=None,
                requested_from=date(2026, 8, 27),
                requested_to=date(2026, 8, 27),
                method="GET",
                url="https://example.test/actions",
            )
            result = FetchResult(
                status="fetched",
                payload=[
                    {
                        "Code": "2330",
                        "Date": "1150827",
                        "Exdividend": "息",
                        "CashDividend": "1.0",
                    }
                ],
                request=request,
                payload_sha256="actions-payload",
            )
            adapter = CorporateActionsAdapter()
            parsed = adapter.parse(result, context)
            self.assertEqual(adapter.upsert(parsed, context), 1)
            import sqlite3

            conn = sqlite3.connect(db_path)
            try:
                raw = conn.execute(
                    "SELECT raw_payload_json FROM corporate_actions"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertIn('"Code": "2330"', raw)
            self.assertIn('"Date": "1150827"', raw)


if __name__ == "__main__":
    unittest.main()
