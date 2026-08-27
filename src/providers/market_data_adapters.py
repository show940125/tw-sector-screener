from __future__ import annotations

"""Small contracts shared by future market-data dataset adapters.

The current TWSE/TPEx provider remains the production implementation for
daily bars and existing enrichment endpoints.  These dataclasses make the
next quarterly/annual/revenue/valuation/corporate-action adapters explicit
without coupling them to a particular HTTP client or vendor payload shape.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class FetchRequest:
    dataset_key: str
    market: str | None
    symbol: str | None
    requested_from: date | None
    requested_to: date | None
    method: str
    url: str
    body: bytes | None = None


@dataclass(frozen=True)
class FetchResult:
    status: str
    payload: Any | None
    request: FetchRequest
    final_url: str | None = None
    redirect_chain: tuple[str, ...] = ()
    http_status: int | None = None
    fallback_level: int = 0
    cache_status: str = "network"
    payload_sha256: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    status: str
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    row_count: int = 0
    first_effective_date: date | None = None
    last_effective_date: date | None = None


@dataclass
class AdapterContext:
    """Runtime context passed to dataset adapters during a sync run."""

    as_of: date
    database_path: str
    run_id: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetSpec:
    """Catalog entry used to prevent a requested dataset from being skipped."""

    dataset_key: str
    canonical_table: str
    frequency: str
    description: str
    implemented: bool
    profile: str
    identity_key: str = "market,symbol"
    partition_key: str = "trade_date"


DATASET_SPECS: tuple[DatasetSpec, ...] = (
    DatasetSpec("daily_bars", "daily_bars", "D", "Verified daily OHLCV", True, "daily", "market,symbol", "month"),
    DatasetSpec("index_bars", "index_bars", "D", "Benchmark/index bars", True, "daily", "index_code", "month"),
    DatasetSpec("security_master", "security_master_snapshots", "snapshot", "Security identity", True, "daily", "market,symbol", "effective_date"),
    DatasetSpec("monthly_revenue", "monthly_revenue", "M", "Monthly revenue", True, "daily", "market,symbol", "revenue_month"),
    DatasetSpec("period_bars", "period_bars", "W/M/Q/Y", "Derived period bars", True, "daily", "market,symbol,frequency", "period_key"),
    DatasetSpec("financial_facts", "financial_fact_observations", "Q/Y", "Point-in-time financial facts", False, "enrichment", "market,symbol,fact_code,fiscal_period", "revision"),
    DatasetSpec("quarterly_fundamentals", "quarterly_company_fundamentals", "Q", "Quarterly fundamentals", False, "enrichment", "market,symbol,fiscal_period", "available_date"),
    DatasetSpec("annual_fundamentals", "annual_company_fundamentals", "Y", "Annual fundamentals", False, "enrichment", "market,symbol,fiscal_year", "available_date"),
    DatasetSpec("valuation_snapshots", "valuation_snapshots", "D", "Valuation snapshots", False, "enrichment", "market,symbol", "trade_date"),
    DatasetSpec("corporate_actions", "corporate_actions", "event", "Corporate actions", False, "enrichment", "market,symbol,action_type", "action_date"),
    DatasetSpec("market_sessions", "market_sessions", "D", "Exchange sessions", False, "enrichment", "market", "trade_date"),
    DatasetSpec("security_trading_status", "security_trading_status", "D", "Security trading status", False, "enrichment", "market,symbol", "effective_date"),
    DatasetSpec("adjustment_factors", "adjustment_factors", "event", "Adjustment factors", False, "enrichment", "market,symbol", "effective_date"),
    DatasetSpec("adjusted_bars", "adjusted_bars", "D", "Derived adjusted bars", True, "daily", "market,symbol,price_mode", "trade_date"),
    DatasetSpec("security_lifecycle", "security_lifecycle", "event", "Security lifecycle", False, "enrichment", "market,symbol", "effective_from"),
    DatasetSpec("benchmark_membership", "benchmark_membership", "event", "Benchmark membership", False, "enrichment", "benchmark_code,symbol", "effective_from"),
    DatasetSpec("daily_market_stats", "daily_market_stats", "D", "Market statistics", False, "enrichment", "market,symbol", "trade_date"),
    DatasetSpec("institutional_flows", "institutional_flows", "D", "Institutional flows", False, "enrichment", "market,symbol", "trade_date"),
    DatasetSpec("margin_short_snapshots", "margin_short_snapshots", "D", "Margin and short data", False, "enrichment", "market,symbol", "trade_date"),
    DatasetSpec("market_events", "market_events", "event", "Market events", False, "enrichment", "event_id", "effective_date"),
)


def dataset_specs() -> dict[str, DatasetSpec]:
    return {spec.dataset_key: spec for spec in DATASET_SPECS}


def unimplemented_dataset_keys(dataset_keys: list[str] | tuple[str, ...]) -> list[str]:
    specs = dataset_specs()
    return sorted(
        {
            key
            for key in dataset_keys
            if key in specs and not specs[key].implemented
        }
    )


class AdapterRegistry:
    """Runtime registry that makes missing dataset adapters explicit."""

    def __init__(self, adapters: list["DatasetAdapter"] | tuple["DatasetAdapter", ...] = ()) -> None:
        self._adapters: dict[str, DatasetAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: "DatasetAdapter") -> None:
        key = str(adapter.dataset_key).strip()
        if not key:
            raise ValueError("dataset adapter key cannot be empty")
        if key in self._adapters:
            raise ValueError(f"duplicate dataset adapter: {key}")
        self._adapters[key] = adapter

    def get(self, dataset_key: str) -> "DatasetAdapter | None":
        return self._adapters.get(dataset_key)

    def require(self, dataset_key: str) -> "DatasetAdapter":
        adapter = self.get(dataset_key)
        if adapter is None:
            raise LookupError(f"no validated adapter registered for {dataset_key}")
        return adapter

    def keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))


class DatasetAdapter(Protocol):
    dataset_key: str

    def fetch_range(self, request: FetchRequest, context: AdapterContext) -> FetchResult:
        ...

    def parse(self, result: FetchResult, context: AdapterContext) -> list[dict[str, Any]]:
        ...

    def validate(
        self,
        rows: list[dict[str, Any]],
        context: AdapterContext,
    ) -> ValidationResult:
        ...

    def upsert(
        self,
        rows: list[dict[str, Any]],
        context: AdapterContext,
    ) -> int:
        ...

    def completeness_report(
        self,
        context: AdapterContext,
    ) -> dict[str, Any]:
        ...


__all__ = [
    "AdapterContext",
    "AdapterRegistry",
    "DATASET_SPECS",
    "DatasetAdapter",
    "DatasetSpec",
    "FetchRequest",
    "FetchResult",
    "ValidationResult",
    "dataset_specs",
    "unimplemented_dataset_keys",
]
