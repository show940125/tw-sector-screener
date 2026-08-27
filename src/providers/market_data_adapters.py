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


class DatasetAdapter(Protocol):
    dataset_key: str

    def fetch(self, request: FetchRequest, context: AdapterContext) -> FetchResult:
        ...

    def validate(self, result: FetchResult, context: AdapterContext) -> ValidationResult:
        ...

    def persist(
        self,
        result: FetchResult,
        validation: ValidationResult,
        context: AdapterContext,
    ) -> int:
        ...


__all__ = [
    "AdapterContext",
    "DatasetAdapter",
    "FetchRequest",
    "FetchResult",
    "ValidationResult",
]
