from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import sync_market_data as sync
from src.providers.market_data_adapters import dataset_specs, unimplemented_dataset_keys
from src.providers.market_data_store import (
    database_integrity,
    ensure_market_data_db,
    get_adjusted_bars,
    get_latest_monthly_revenue,
    get_latest_security_master,
    get_valuation_snapshot_as_of,
    query_financial_facts_as_of,
    query_market_data_as_of,
    rebuild_adjusted_bars,
    rebuild_period_bars,
    upsert_adjustment_factor,
    upsert_daily_market_stats,
    upsert_institutional_flow,
    upsert_margin_short_snapshot,
    upsert_market_event,
    upsert_market_session,
    upsert_security_lifecycle,
    upsert_security_trading_status,
    upsert_benchmark_membership,
    upsert_annual_company_fundamental,
    upsert_corporate_action,
    upsert_financial_fact,
    upsert_monthly_revenue,
    upsert_security_master,
    upsert_valuation_snapshot,
)
from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars
from src.providers.tw_market_provider import TwMarketProvider


class _SyncProvider:
    def __init__(self, **kwargs: object) -> None:
        self.market_data_db_path = Path(str(kwargs["market_database_path"]))
        self.cache_dir = Path(str(kwargs["cache_dir"]))

    def get_market_data_diagnostics(self) -> dict[str, int]:
        return {"redirect_308_unresolved_count": 0}


class _RangeSyncProvider:
    requested_ranges: list[tuple[date, date | None]] = []

    def __init__(self, **kwargs: object) -> None:
        self.market_data_db_path = Path(str(kwargs["market_database_path"]))
        self.cache_dir = Path(str(kwargs["cache_dir"]))

    def get_ohlcv(
        self,
        symbol: str,
        market: str,
        as_of: date,
        lookback: int = 253,
        from_date: date | None = None,
    ) -> list[dict[str, object]]:
        self.requested_ranges.append((as_of, from_date))
        return [
            {
                "date": as_of - timedelta(days=lookback - 1 - index),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
            }
            for index in range(lookback)
        ]

    def get_market_data_diagnostics(self) -> dict[str, int]:
        return {"redirect_308_unresolved_count": 0}


class MarketDataFoundationTests(unittest.TestCase):
    def test_dataset_registry_declares_profile_and_unimplemented_state(self) -> None:
        specs = dataset_specs()
        self.assertEqual(specs["daily_bars"].profile, "daily")
        self.assertEqual(specs["annual_fundamentals"].profile, "enrichment")
        self.assertIn("market_events", specs)
        self.assertIn("market_events", unimplemented_dataset_keys(["market_events"]))
        self.assertNotIn("daily_bars", unimplemented_dataset_keys(["daily_bars"]))

    def test_existing_v2_database_is_upgraded_before_new_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            ensure_market_data_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("DROP TABLE financial_fact_observations")
                conn.execute(
                    "UPDATE schema_meta SET value='2' WHERE key='market_data_schema_version'"
                )
                conn.commit()
            finally:
                conn.close()

            upsert_financial_fact(
                db_path,
                market="TWSE",
                symbol="2330",
                fact_code="revenue",
                fiscal_period="115Q1",
                value=100.0,
                unit="TWD",
                consolidation="consolidated",
                effective_date=date(2026, 3, 31),
                available_date=date(2026, 5, 10),
            )
            conn = sqlite3.connect(db_path)
            try:
                self.assertIsNotNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='financial_fact_observations'"
                    ).fetchone()
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT value FROM schema_meta WHERE key='market_data_schema_version'"
                    ).fetchone()[0],
                    "3",
                )
            finally:
                conn.close()

    def test_canonical_schema_contains_research_and_quality_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            ensure_market_data_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            finally:
                conn.close()

            self.assertTrue(
                {
                    "financial_fact_observations",
                    "market_sessions",
                    "security_trading_status",
                    "adjustment_factors",
                    "adjusted_bars",
                    "security_lifecycle",
                    "benchmark_membership",
                    "daily_market_stats",
                    "institutional_flows",
                    "margin_short_snapshots",
                    "market_events",
                    "market_data_source_links",
                }.issubset(tables)
            )

    def test_financial_fact_query_selects_revision_known_by_information_cutoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            ensure_market_data_db(db_path)
            upsert_financial_fact(
                db_path,
                market="TWSE",
                symbol="2330",
                fact_code="revenue",
                fiscal_period="115Q1",
                value=100.0,
                unit="TWD",
                consolidation="consolidated",
                effective_date=date(2026, 3, 31),
                available_date=date(2026, 5, 10),
                published_at="2026-05-10T08:00:00+08:00",
                revision_id="r1",
                source_payload_id="payload-r1",
            )
            upsert_financial_fact(
                db_path,
                market="TWSE",
                symbol="2330",
                fact_code="revenue",
                fiscal_period="115Q1",
                value=110.0,
                unit="TWD",
                consolidation="consolidated",
                effective_date=date(2026, 3, 31),
                available_date=date(2026, 7, 1),
                published_at="2026-07-01T08:00:00+08:00",
                revision_id="r2",
                source_payload_id="payload-r2",
            )

            before_revision = query_financial_facts_as_of(
                db_path,
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 6, 30),
                information_cutoff=date(2026, 6, 30),
            )
            after_revision = query_financial_facts_as_of(
                db_path,
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 8, 1),
                information_cutoff=date(2026, 8, 1),
            )

            self.assertEqual(before_revision[0]["value"], 100.0)
            self.assertEqual(after_revision[0]["value"], 110.0)
            self.assertEqual(before_revision[0]["revision_id"], "r1")
            self.assertEqual(after_revision[0]["revision_id"], "r2")

    def test_financial_fact_query_keeps_distinct_xbrl_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            for dimension, value in (({"segment": "cloud"}, 10.0), ({"segment": "edge"}, 20.0)):
                upsert_financial_fact(
                    db_path,
                    market="TWSE",
                    symbol="2330",
                    fact_code="revenue",
                    fiscal_period="115Q1",
                    value=value,
                    unit="TWD",
                    consolidation="consolidated",
                    effective_date=date(2026, 3, 31),
                    available_date=date(2026, 5, 10),
                    revision_id="same-revision",
                    dimension_json=dimension,
                )
            rows = query_financial_facts_as_of(
                db_path,
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 6, 30),
                information_cutoff=date(2026, 6, 30),
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual(
                {json.loads(str(row["dimension_json"]))["segment"] for row in rows},
                {"cloud", "edge"},
            )

    def test_generic_pit_query_contract_rejects_future_available_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            upsert_financial_fact(
                db_path,
                market="TWSE",
                symbol="2330",
                fact_code="eps",
                fiscal_period="115Q1",
                value=5.0,
                unit="TWD",
                consolidation="consolidated",
                effective_date=date(2026, 3, 31),
                available_date=date(2026, 5, 10),
                revision_id="eps-r1",
            )
            rows = query_market_data_as_of(
                db_path,
                dataset="financial_facts",
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 6, 30),
                information_cutoff=date(2026, 5, 31),
            )
            self.assertEqual(len(rows), 1)
            upsert_monthly_revenue(
                db_path,
                market="TWSE",
                symbol="2330",
                revenue_month="11507",
                monthly_revenue=100.0,
                revenue_mom=None,
                revenue_yoy=None,
                source_endpoint="test.revenue",
                source_url="https://example.test/revenue",
                available_date=date(2026, 5, 1),
            )
            revenue_rows = query_market_data_as_of(
                db_path,
                dataset="monthly_revenue",
                market="TWSE",
                symbol="2330",
                observation_date=date(2026, 8, 31),
                information_cutoff=date(2026, 5, 31),
            )
            self.assertEqual(len(revenue_rows), 1)
            with self.assertRaises(ValueError):
                query_market_data_as_of(
                    db_path,
                    dataset="not_a_dataset",
                    market="TWSE",
                    symbol="2330",
                    observation_date=date(2026, 6, 30),
                    information_cutoff=date(2026, 5, 31),
                )

    def test_research_tables_are_idempotent_and_adjusted_bars_are_rebuildable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            bars = [
                VerifiedDailyBar(
                    market="TWSE",
                    symbol="2330",
                    trade_date=day,
                    open=100.0,
                    high=105.0,
                    low=95.0,
                    close=102.0,
                    volume=1000.0,
                    source_endpoint="test.daily",
                    source_url="https://example.test/daily",
                    source_cache_file="daily.json",
                    source_payload_sha256=f"bar-{day}",
                    source_fetched_at="2026-08-26T08:00:00+08:00",
                )
                for day in (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6))
            ]
            import_verified_bars(db_path, bars)
            upsert_adjustment_factor(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_date=date(2026, 1, 5),
                price_factor=2.0,
                action_hash="split-2",
                source_endpoint="test.action",
                source_url="https://example.test/action",
            )
            first = rebuild_adjusted_bars(db_path, market="TWSE", symbol="2330")
            second = rebuild_adjusted_bars(db_path, market="TWSE", symbol="2330")
            adjusted = get_adjusted_bars(
                db_path,
                market="TWSE",
                symbol="2330",
                price_mode="total_return_backward",
            )
            self.assertEqual(first["bars_upserted"], 3)
            self.assertEqual(second["bars_upserted"], 3)
            self.assertEqual(len(adjusted), 3)
            self.assertEqual(adjusted[0]["trade_date"], "2026-01-02")
            self.assertEqual(adjusted[0]["close"], 204.0)
            self.assertEqual(adjusted[0]["volume"], 500.0)
            self.assertEqual(adjusted[-1]["adjustment_factor"], 1.0)

    def test_period_rebuild_can_be_scoped_to_affected_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            bars: list[VerifiedDailyBar] = []
            for symbol, close in (("2330", 100.0), ("6669", 200.0)):
                bars.append(
                    VerifiedDailyBar(
                        market="TWSE",
                        symbol=symbol,
                        trade_date=date(2026, 1, 5),
                        open=close,
                        high=close + 1,
                        low=close - 1,
                        close=close,
                        volume=1000.0,
                        source_endpoint="test.daily",
                        source_url="https://example.test/daily",
                        source_cache_file="daily.json",
                        source_payload_sha256=f"bar-{symbol}",
                        source_fetched_at="2026-08-26T08:00:00+08:00",
                    )
                )
            import_verified_bars(db_path, bars)
            result = rebuild_period_bars(db_path, symbols=[("TWSE", "2330")])
            self.assertEqual(result["symbols"], 1)
            conn = sqlite3.connect(db_path)
            try:
                self.assertGreater(
                    conn.execute("SELECT COUNT(*) FROM period_bars WHERE symbol = '2330'").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM period_bars WHERE symbol = '6669'").fetchone()[0],
                    0,
                )
            finally:
                conn.close()

    def test_high_value_tables_have_pit_and_provenance_storage_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            upsert_market_session(
                db_path,
                market="TWSE",
                trade_date=date(2026, 8, 26),
                is_open=True,
                source_endpoint="test.sessions",
                source_url="https://example.test/sessions",
                source_payload_id="payload-session",
            )
            upsert_security_trading_status(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_date=date(2026, 8, 26),
                status="normal",
                source_endpoint="test.status",
                source_url="https://example.test/status",
            )
            upsert_security_lifecycle(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_from=date(1990, 1, 1),
                status="listed",
                listing_date=date(1990, 1, 1),
                source_endpoint="test.lifecycle",
                source_url="https://example.test/lifecycle",
            )
            upsert_benchmark_membership(
                db_path,
                benchmark_code="TAIEX",
                market="TWSE",
                symbol="2330",
                effective_from=date(2026, 1, 1),
                weight=0.25,
                source_endpoint="test.membership",
                source_url="https://example.test/membership",
            )
            upsert_daily_market_stats(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                trade_value=1000000.0,
                transaction_count=123,
                turnover_rate=0.5,
                source_endpoint="test.stats",
                source_url="https://example.test/stats",
            )
            upsert_institutional_flow(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                foreign_net=10.0,
                source_endpoint="test.flow",
                source_url="https://example.test/flow",
            )
            upsert_margin_short_snapshot(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                margin_balance=20.0,
                source_endpoint="test.margin",
                source_url="https://example.test/margin",
            )
            upsert_market_event(
                db_path,
                event_id="event-1",
                market="TWSE",
                symbol="2330",
                event_type="earnings_call",
                effective_date=date(2026, 8, 26),
                announced_at="2026-08-20T08:00:00+08:00",
                source_endpoint="test.events",
                source_url="https://example.test/events",
            )
            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(conn.execute("SELECT is_open FROM market_sessions").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT status FROM security_trading_status").fetchone()[0], "normal")
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM daily_market_stats").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM institutional_flows").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM margin_short_snapshots").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM market_events").fetchone()[0], 1)
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_source_links "
                        "WHERE dataset_key = 'market_sessions' AND payload_id = 'payload-session'"
                    ).fetchone()[0],
                    1,
                )
            finally:
                conn.close()

    def test_provider_metadata_and_valuation_use_canonical_db_before_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            as_of = date(2026, 8, 26)
            upsert_security_master(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_date=as_of,
                name="台積電",
                industry="半導體業",
                source_endpoint="test.basics",
                source_url="https://example.test/basics",
            )
            upsert_monthly_revenue(
                db_path,
                market="TWSE",
                symbol="2330",
                revenue_month="11507",
                monthly_revenue=100.0,
                revenue_mom=2.0,
                revenue_yoy=10.0,
                source_endpoint="test.revenue",
                source_url="https://example.test/revenue",
                available_date=as_of,
            )
            upsert_valuation_snapshot(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=as_of,
                pe=20.0,
                pb=3.0,
                dividend_yield=1.5,
                source_endpoint="test.valuation",
                source_url="https://example.test/valuation",
                available_date=as_of,
            )
            provider = TwMarketProvider(cache_dir=Path(tmp), market_database_path=db_path)
            with patch.object(provider, "_safe_get_json", side_effect=AssertionError("network")):
                basics = provider._load_basics()
                revenue = provider._load_latest_revenue_map()
            with patch.object(provider, "_get_twse_latest_valuation", side_effect=AssertionError("network")):
                valuation = provider.get_latest_valuation("2330", "TWSE", as_of)
            self.assertEqual(basics["2330"]["name"], "台積電")
            self.assertEqual(revenue["2330"]["monthly_revenue"], 100.0)
            self.assertEqual(valuation["pe"], 20.0)

    def test_canonical_read_helpers_filter_future_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            upsert_security_master(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_date=date(2026, 8, 27),
                name="future",
                industry="future",
                source_endpoint="test",
                source_url="https://example.test",
            )
            upsert_monthly_revenue(
                db_path,
                market="TWSE",
                symbol="2330",
                revenue_month="11508",
                monthly_revenue=200.0,
                revenue_mom=None,
                revenue_yoy=None,
                source_endpoint="test",
                source_url="https://example.test",
                available_date=date(2026, 8, 27),
            )
            upsert_valuation_snapshot(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 27),
                pe=1.0,
                pb=1.0,
                dividend_yield=1.0,
                source_endpoint="test",
                source_url="https://example.test",
                available_date=date(2026, 8, 27),
            )
            self.assertEqual(get_latest_security_master(db_path, as_of=date(2026, 8, 26)), {})
            self.assertEqual(get_latest_monthly_revenue(db_path, as_of=date(2026, 8, 26)), {})
            self.assertIsNone(
                get_valuation_snapshot_as_of(
                    db_path,
                    market="TWSE",
                    symbol="2330",
                    as_of=date(2026, 8, 26),
                )
            )

    def test_database_integrity_counts_new_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = database_integrity(Path(tmp) / "market_data.sqlite")
            self.assertTrue(report["ok"])
            for table in (
                "financial_fact_observations",
                "market_sessions",
                "security_trading_status",
                "adjusted_bars",
                "market_events",
            ):
                self.assertIn(table, report["table_counts"])

    def test_existing_canonical_rows_link_to_immutable_source_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            from src.providers.market_data_store import record_source_payload

            payload_id = record_source_payload(
                db_path,
                dataset_key="security_master",
                request_method="GET",
                source_endpoint="test.basics",
                source_url="https://example.test/basics",
                payload=[{"symbol": "2330"}],
                validation_status="verified",
            )
            upsert_security_master(
                db_path,
                market="TWSE",
                symbol="2330",
                effective_date=date(2026, 8, 26),
                name="台積電",
                industry="半導體業",
                source_endpoint="test.basics",
                source_url="https://example.test/basics",
                source_payload_id=payload_id,
            )
            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(
                    conn.execute(
                        "SELECT source_payload_id FROM security_master_snapshots"
                    ).fetchone()[0],
                    payload_id,
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_source_links WHERE payload_id = ?",
                        (payload_id,),
                    ).fetchone()[0],
                    1,
                )
            finally:
                conn.close()

    def test_all_legacy_compatibility_writes_accept_source_payload_link(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            from src.providers.market_data_store import record_source_payload

            payload_id = record_source_payload(
                db_path,
                dataset_key="test.enrichment",
                request_method="GET",
                source_endpoint="test.enrichment",
                source_url="https://example.test/enrichment",
                payload={"rows": []},
                validation_status="verified",
            )
            upsert_monthly_revenue(
                db_path,
                market="TWSE",
                symbol="2330",
                revenue_month="11507",
                monthly_revenue=100.0,
                revenue_mom=1.0,
                revenue_yoy=2.0,
                source_endpoint="test.enrichment",
                source_url="https://example.test/enrichment",
                source_payload_id=payload_id,
                available_date=date(2026, 8, 26),
            )
            upsert_valuation_snapshot(
                db_path,
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                pe=20.0,
                pb=3.0,
                dividend_yield=1.0,
                source_endpoint="test.enrichment",
                source_url="https://example.test/enrichment",
                source_payload_id=payload_id,
                available_date=date(2026, 8, 26),
            )
            upsert_annual_company_fundamental(
                db_path,
                market="TWSE",
                symbol="2330",
                fiscal_year="2025",
                available_date=date(2026, 3, 1),
                source_endpoint="test.enrichment",
                source_url="https://example.test/enrichment",
                source_payload_id=payload_id,
            )
            upsert_corporate_action(
                db_path,
                market="TWSE",
                symbol="2330",
                action_date=date(2026, 8, 26),
                action_type="cash_dividend",
                source_endpoint="test.enrichment",
                source_url="https://example.test/enrichment",
                source_payload_id=payload_id,
            )
            conn = sqlite3.connect(db_path)
            try:
                for table in (
                    "monthly_revenue",
                    "valuation_snapshots",
                    "annual_company_fundamentals",
                    "corporate_actions",
                ):
                    self.assertEqual(
                        conn.execute(
                            f"SELECT source_payload_id FROM {table}"
                        ).fetchone()[0],
                        payload_id,
                    )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_source_links WHERE payload_id = ?",
                        (payload_id,),
                    ).fetchone()[0],
                    4,
                )
            finally:
                conn.close()

    def test_daily_bar_store_keeps_source_payload_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            ensure_market_data_db(db_path)
            bar = VerifiedDailyBar(
                market="TWSE",
                symbol="2330",
                trade_date=date(2026, 8, 26),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=1000.0,
                source_endpoint="test.daily",
                source_url="https://example.test/daily",
                source_cache_file="daily.json",
                source_payload_sha256="payload-hash",
                source_fetched_at="2026-08-26T08:00:00+08:00",
                source_payload_id="payload-id",
            )
            import_verified_bars(db_path, [bar])
            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(
                    conn.execute(
                        "SELECT source_payload_id FROM daily_bars"
                    ).fetchone()[0],
                    "payload-id",
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM market_data_source_links WHERE payload_id = 'payload-id'"
                    ).fetchone()[0],
                    1,
                )
            finally:
                conn.close()

    def test_sync_marks_unimplemented_dataset_failed_instead_of_succeeding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(sync, "TwMarketProvider", _SyncProvider):
                outputs = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 26),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    datasets=["annual_fundamentals"],
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("annual_fundamentals", payload["not_implemented_datasets"])

    def test_enrichment_profile_passes_real_range_to_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            _RangeSyncProvider.requested_ranges = []
            with patch.object(sync, "TwMarketProvider", _RangeSyncProvider):
                outputs = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 26),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    datasets=["daily_bars"],
                    profile="enrichment",
                    from_date=date(2026, 1, 1),
                    to_date=date(2026, 2, 1),
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "complete")
            self.assertEqual(payload["profile"], "enrichment")
            self.assertEqual(payload["from_date"], "2026-01-01")
            self.assertEqual(payload["to_date"], "2026-02-01")
            self.assertTrue(_RangeSyncProvider.requested_ranges)
            self.assertEqual(_RangeSyncProvider.requested_ranges[0], (date(2026, 2, 1), date(2026, 1, 1)))

    def test_daily_profile_without_explicit_range_requests_lookback_not_single_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            _RangeSyncProvider.requested_ranges = []
            with patch.object(sync, "TwMarketProvider", _RangeSyncProvider):
                outputs = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 26),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    datasets=["daily_bars"],
                    profile="daily",
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "complete")
            self.assertIsNone(payload["from_date"])
            self.assertTrue(_RangeSyncProvider.requested_ranges)
            self.assertEqual(_RangeSyncProvider.requested_ranges[0], (date(2026, 8, 26), None))


if __name__ == "__main__":
    unittest.main()
