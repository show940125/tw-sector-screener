from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import sync_market_data as sync
from src.providers.daily_bar_store import VerifiedDailyBar, import_verified_bars
from src.providers.market_data_adapters import FetchRequest, FetchResult
from src.providers.market_data_store import ensure_market_data_db


class _FakeSyncProvider:
    def __init__(self, **kwargs: object) -> None:
        self.market_data_db_path = Path(str(kwargs["market_database_path"]))
        self.cache_dir = Path(str(kwargs["cache_dir"]))
        self.failed_symbol: str | None = None

    def get_ohlcv(self, symbol: str, market: str, as_of: date, lookback: int = 253):
        if symbol == self.failed_symbol:
            raise RuntimeError("mock current-day failure")
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

    def get_taiex_series(self, as_of: date, lookback: int = 253):
        return [
            {"date": as_of - timedelta(days=lookback - 1 - index), "close": 10000.0, "change_points": 1.0}
            for index in range(lookback)
        ]

    def get_market_data_diagnostics(self):
        return {"redirect_308_unresolved_count": 0}

    def _load_basics(self):
        return {}


class _FakeEnrichmentProvider:
    def __init__(self, **kwargs: object) -> None:
        self.market_data_db_path = Path(str(kwargs["market_database_path"]))
        self.cache_dir = Path(str(kwargs["cache_dir"]))
        ensure_market_data_db(self.market_data_db_path)
        self.calls = 0
        self._last_persisted_payload_id = None

    def get_market_data_diagnostics(self) -> dict[str, int]:
        return {"redirect_308_unresolved_count": 0, "request_count": self.calls}

    def fetch_monthly_revenue_partition(
        self, *, market: str, revenue_month: str, as_of: date, force_network: bool = False
    ) -> FetchResult:
        self.calls += 1
        month = date.fromisoformat(revenue_month + "-01")
        request = FetchRequest(
            dataset_key="monthly_revenue",
            market=market,
            symbol=None,
            requested_from=month,
            requested_to=date(2026, 7, 31),
            method="POST",
            url="https://example.test/revenue",
            body=b"month=11507",
        )
        return FetchResult(
            status="fetched",
            payload=[
                {
                    "公司代號": "2330",
                    "資料年月": "11507",
                    "營業收入-當月營收": "100",
                }
            ],
            request=request,
            final_url=request.url,
            payload_sha256="revenue-payload",
        )


class MarketDataSyncTests(unittest.TestCase):
    def test_market_mapping_uses_canonical_state_for_exchange_exceptions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            bar = VerifiedDailyBar(
                market="TPEx",
                symbol="3260",
                trade_date=date(2026, 8, 26),
                open=1,
                high=2,
                low=1,
                close=2,
                volume=1,
                source_endpoint="test",
                source_url="https://test",
                source_cache_file="test.json",
                source_payload_sha256="test",
                source_fetched_at="2026-08-26T00:00:00+08:00",
            )
            import_verified_bars(db_path, [bar])
            self.assertEqual(sync._market_for_symbol("3260", db_path), "TPEx")

    def test_sync_manifest_passes_only_when_all_coverage_candidates_and_taiex_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(sync, "TwMarketProvider", _FakeSyncProvider):
                outputs = sync.run(
                    themes=["AI", "半導體"],
                    as_of=date(2026, 8, 26),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "complete")
            self.assertEqual(payload["coverage_count"], 54)
            self.assertEqual(payload["daily_success_count"], 54)
            self.assertEqual(payload["taiex"]["latest_trade_date"], "2026-08-26")
            self.assertTrue(outputs["markdown"].read_text(encoding="utf-8"))

    def test_dry_run_does_not_create_or_mutate_canonical_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            with patch.object(sync, "TwMarketProvider", side_effect=AssertionError("provider must not be built")):
                outputs = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 27),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    dry_run=True,
                )
            payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "planned")
            self.assertTrue(payload["dry_run"])
            self.assertFalse((output_root / "cache" / "market" / "market_data.sqlite").exists())

    def test_enrichment_uses_partition_checkpoint_on_second_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            provider_instances: list[_FakeEnrichmentProvider] = []

            def provider_factory(**kwargs: object) -> _FakeEnrichmentProvider:
                provider = _FakeEnrichmentProvider(**kwargs)
                provider_instances.append(provider)
                return provider

            with patch.object(sync, "TwMarketProvider", provider_factory), patch.object(
                sync, "_coverage_symbols", return_value=["2330"]
            ), patch.object(
                sync, "theme_rule", return_value={"coverage_symbols": ["2330"]}
            ):
                first = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 27),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    database_path=output_root / "market_data.sqlite",
                    datasets=["monthly_revenue"],
                    profile="enrichment",
                    from_date=date(2026, 7, 1),
                    to_date=date(2026, 7, 31),
                )
                second = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 27),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    database_path=output_root / "market_data.sqlite",
                    datasets=["monthly_revenue"],
                    profile="enrichment",
                    from_date=date(2026, 7, 1),
                    to_date=date(2026, 7, 31),
                )
                second_payload = json.loads(second["json"].read_text(encoding="utf-8"))
                full = sync.run(
                    themes=["AI"],
                    as_of=date(2026, 8, 27),
                    universe_mode="coverage",
                    lookback=253,
                    timeout=0.1,
                    output_root=output_root,
                    database_path=output_root / "market_data.sqlite",
                    datasets=["monthly_revenue"],
                    profile="enrichment",
                    from_date=date(2026, 7, 1),
                    to_date=date(2026, 7, 31),
                    mode="full",
                )
            first_payload = json.loads(first["json"].read_text(encoding="utf-8"))
            self.assertEqual(first_payload["enrichment"]["monthly_revenue"]["status"], "complete")
            self.assertEqual(second_payload["enrichment"]["monthly_revenue"]["db_hits"], 1)
            self.assertEqual(provider_instances[0].calls, 1)
            self.assertEqual(provider_instances[1].calls, 0)
            self.assertEqual(provider_instances[2].calls, 1)
            full_payload = json.loads(full["json"].read_text(encoding="utf-8"))
            self.assertEqual(full_payload["enrichment"]["monthly_revenue"]["network_requests"], 1)


if __name__ == "__main__":
    unittest.main()
