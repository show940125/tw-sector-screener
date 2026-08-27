from __future__ import annotations

"""Synchronise the curated coverage market data into canonical SQLite.

This command is intentionally limited to canonical market-data
synchronisation and its audit manifest.  It does not invoke downstream
execution or publication actions.
"""

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.providers.market_data_store import (
    begin_completeness_run,
    database_integrity,
    finish_completeness_run,
    get_monthly_revenue_history,
    get_partition_state,
    get_valuation_history,
    record_fetch_attempt,
    record_data_gap,
    record_sync_item,
    record_sync_run,
    rebuild_adjusted_bars,
    rebuild_period_bars,
    upsert_partition_state,
    upsert_dataset_sync_state,
    upsert_monthly_revenue,
    upsert_security_master,
    upsert_universe_membership,
)
from src.providers.market_data_adapters import (
    AdapterContext,
    FetchRequest,
    FetchResult,
    dataset_specs,
    unimplemented_dataset_keys,
    validated_adapter_registry,
)
from src.providers.daily_bar_store import get_sync_state
from src.providers.tw_market_provider import (
    TWSE_BASICS_URL,
    TWSE_REVENUE_URL,
    TPEX_BASICS_URL,
    TPEX_REVENUE_URL,
    TwMarketProvider,
)
from src.themes import theme_rule


DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"
DEFAULT_DATASETS = (
    "daily_bars",
    "index_bars",
    "security_master",
    "monthly_revenue",
    "period_bars",
)
ENRICHMENT_DEFAULT_DATASETS = (
    "monthly_revenue",
    "valuation_snapshots",
    "financial_facts",
    "corporate_actions",
    "market_sessions",
)
SUPPORTED_DATASETS = set(dataset_specs())


def _provider_get_ohlcv(
    provider: Any,
    *,
    symbol: str,
    market: str,
    as_of: date,
    lookback: int,
    from_date: date | None,
    require_current_day: bool,
) -> list[dict[str, Any]]:
    """Call new range-aware providers while retaining test-double compatibility."""

    try:
        return provider.get_ohlcv(
            symbol,
            market,
            as_of=as_of,
            lookback=lookback,
            from_date=from_date,
            require_current_day=require_current_day,
        )
    except TypeError as exc:
        if "from_date" not in str(exc) and "require_current_day" not in str(exc):
            raise
        try:
            return provider.get_ohlcv(
                symbol,
                market,
                as_of=as_of,
                lookback=lookback,
                from_date=from_date,
            )
        except TypeError as nested_exc:
            if "from_date" not in str(nested_exc):
                raise
            return provider.get_ohlcv(symbol, market, as_of=as_of, lookback=lookback)


def _market_for_symbol(symbol: str, database_path: Path | None = None) -> str:
    if database_path is not None:
        known: list[tuple[str, int]] = []
        for market in ("TWSE", "TPEx"):
            state = get_sync_state(database_path, market=market, symbol=symbol)
            if state and int(state.get("verified_bar_count") or 0) > 0:
                known.append((market, int(state.get("verified_bar_count") or 0)))
        if known:
            return max(known, key=lambda item: item[1])[0]
    return "TPEx" if symbol.startswith(("6", "8")) else "TWSE"


def _as_of(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _coverage_symbols(themes: list[str], universe_mode: str) -> list[str]:
    return sorted(
        {
            str(symbol).strip()
            for theme in themes
            for symbol in (
                theme_rule(theme, universe_mode=universe_mode).get("coverage_symbols")
                or theme_rule(theme, universe_mode=universe_mode).get("symbols")
                or []
            )
            if str(symbol).strip()
        }
    )


def _month_starts(start: date, end: date) -> list[date]:
    """Return calendar-month partitions intersecting an inclusive range."""

    cursor = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    output: list[date] = []
    while cursor <= end_month:
        output.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return output


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        next_month = date(month_start.year + 1, 1, 1)
    else:
        next_month = date(month_start.year, month_start.month + 1, 1)
    return next_month - timedelta(days=1)


def _partition_request_body_hash(request: FetchRequest) -> str | None:
    if request.body is None:
        return None
    return hashlib.sha256(request.body).hexdigest()


def _partition_checkpoint_is_reusable(
    state: dict[str, Any] | None,
    *,
    requested_from: date,
    requested_to: date,
    minimum_row_count: int = 0,
) -> bool:
    if not state or state.get("status") != "verified":
        return False
    if not state.get("payload_sha256") or not state.get("last_verified_at"):
        return False
    if int(state.get("row_count") or 0) < max(int(minimum_row_count), 0):
        return False
    return (
        str(state.get("requested_from") or "") == requested_from.isoformat()
        and str(state.get("requested_to") or "") == requested_to.isoformat()
    )


def _run_monthly_revenue_history(
    *,
    provider: Any,
    database_path: Path,
    run_id: str,
    symbols: dict[tuple[str, str], dict[str, Any]],
    from_date: date,
    to_date: date,
    as_of: date,
    mode: str,
) -> dict[str, Any]:
    """Synchronise monthly revenue per symbol and only for missing months.

    The old compatibility runner used one bulk HTML request for each
    market/month.  Production providers now expose a symbol-aware history
    method: TWSE returns a bounded chart and TPEx uses the official MOPS SPA
    JSON endpoint for one symbol/month.  This makes the SQLite rows the
    source of incremental truth and prevents a complete history from being
    rebuilt from the network on every run.
    """

    registry = validated_adapter_registry()
    adapter = registry.require("monthly_revenue")
    months = _month_starts(from_date, to_date)
    expected_periods = {month.strftime("%Y-%m") for month in months}
    symbol_items = sorted(symbols)
    expected_rows = len(symbol_items) * len(expected_periods)
    completeness_id = begin_completeness_run(
        database_path,
        dataset_key="monthly_revenue",
        run_id=run_id,
        expected_rows=expected_rows,
        expected_partitions=len(symbol_items),
    )
    started_at = datetime.now().astimezone().isoformat()
    actual_rows = 0
    actual_partitions = 0
    db_hits = 0
    network_requests = 0
    failures: list[dict[str, Any]] = []
    missing_partitions: list[str] = []
    warnings: list[dict[str, Any]] = []

    for market, symbol in symbol_items:
        item_started = datetime.now().astimezone().isoformat()
        cached_rows = get_monthly_revenue_history(
            database_path,
            market=market,
            symbol=symbol,
            as_of=as_of,
        )
        cached_by_period = {
            str(row.get("revenue_month") or ""): row
            for row in cached_rows
            if str(row.get("revenue_month") or "") in expected_periods
        }
        missing = set(expected_periods) if mode == "full" else expected_periods - set(cached_by_period)
        fetched_rows: list[dict[str, Any]] = []
        last_fetch: FetchResult | None = None
        last_source_payload_id: str | None = None
        last_error: str | None = None
        item_cache_status = "db_hit" if not missing else "network"

        if not missing:
            db_hits += 1
        else:
            try:
                if market == "TWSE":
                    before_requests = int(
                        (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                    )
                    last_fetch = provider.fetch_monthly_revenue_history_for_symbol(
                        market=market,
                        symbol=symbol,
                        requested_from=from_date,
                        requested_to=to_date,
                        as_of=as_of,
                        force_network=mode == "full",
                    )
                    after_requests = int(
                        (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                    )
                    network_requests += max(after_requests - before_requests, 0)
                    last_source_payload_id = getattr(provider, "_last_persisted_payload_id", None)
                    context = AdapterContext(
                        as_of=as_of,
                        database_path=str(database_path),
                        run_id=run_id,
                        requested_from=from_date,
                        requested_to=to_date,
                        output_root=str(database_path.parent.parent.parent),
                        options={
                            "fetcher": lambda _request, result=last_fetch: result,
                            "market": market,
                            "symbol": symbol,
                            "source_endpoint": "twse.IIH.company.financial",
                            "source_payload_id": last_source_payload_id,
                            "source_url": last_fetch.final_url or last_fetch.request.url,
                            "fetched_at": datetime.now().astimezone().isoformat(),
                            "available_date": as_of,
                            "availability_precision": "retrieval_date",
                            "validation_status": "verified",
                        },
                    )
                    parsed = adapter.parse(last_fetch, context)
                    fetched_rows = [
                        row
                        for row in parsed
                        if str(row.get("market") or "") == market
                        and str(row.get("symbol") or "") == symbol
                        and str(row.get("revenue_month") or "") in missing
                    ]
                    validation = adapter.validate(fetched_rows, context)
                    fetched_periods = {str(row.get("revenue_month") or "") for row in fetched_rows}
                    missing_after_fetch = sorted(missing - fetched_periods)
                    if validation.status != "verified" or missing_after_fetch:
                        raise ValueError(
                            "TWSE monthly revenue missing periods: "
                            + ",".join(missing_after_fetch or validation.errors)
                        )
                    adapter.upsert(fetched_rows, context)
                else:
                    # TPEx has no historical monthly-revenue bulk OpenAPI
                    # partition. Fetch only the missing symbol/month cells.
                    for period in sorted(missing):
                        period_start = datetime.strptime(period, "%Y-%m").date()
                        before_requests = int(
                            (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                        )
                        fetch_result = provider.fetch_monthly_revenue_partition_for_symbol(
                            market=market,
                            symbol=symbol,
                            revenue_month=period,
                            as_of=as_of,
                            force_network=mode == "full",
                        )
                        after_requests = int(
                            (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                        )
                        network_requests += max(after_requests - before_requests, 0)
                        last_fetch = fetch_result
                        last_source_payload_id = getattr(provider, "_last_persisted_payload_id", None)
                        context = AdapterContext(
                            as_of=as_of,
                            database_path=str(database_path),
                            run_id=run_id,
                            requested_from=period_start,
                            requested_to=_month_end(period_start),
                            output_root=str(database_path.parent.parent.parent),
                            options={
                                "fetcher": lambda _request, result=fetch_result: result,
                                "market": market,
                                "symbol": symbol,
                                "source_endpoint": "mops.t05st10_ifrs",
                                "source_payload_id": last_source_payload_id,
                                "source_url": fetch_result.final_url or fetch_result.request.url,
                                "fetched_at": datetime.now().astimezone().isoformat(),
                                "available_date": as_of,
                                "availability_precision": "retrieval_date",
                                "validation_status": "verified",
                            },
                        )
                        parsed = adapter.parse(fetch_result, context)
                        parsed = [
                            row
                            for row in parsed
                            if str(row.get("market") or "") == market
                            and str(row.get("symbol") or "") == symbol
                            and str(row.get("revenue_month") or "") == period
                        ]
                        validation = adapter.validate(parsed, context)
                        if validation.status != "verified":
                            raise ValueError("TPEx monthly revenue validation failed: " + "; ".join(validation.errors))
                        adapter.upsert(parsed, context)
                        fetched_rows.extend(parsed)
            except Exception as exc:
                last_error = str(exc)
                failures.append(
                    {
                        "market": market,
                        "symbol": symbol,
                        "reason": last_error,
                        "missing_periods": sorted(missing),
                    }
                )

        refreshed_rows = get_monthly_revenue_history(
            database_path,
            market=market,
            symbol=symbol,
            as_of=as_of,
        )
        refreshed_periods = {
            str(row.get("revenue_month") or "")
            for row in refreshed_rows
            if str(row.get("revenue_month") or "") in expected_periods
        }
        remaining = sorted(expected_periods - refreshed_periods)
        actual_rows += len(refreshed_periods)
        complete = not remaining
        if complete:
            actual_partitions += 1
        else:
            missing_partitions.extend(f"{market}:{symbol}:{period}" for period in remaining)
            for period in remaining:
                record_data_gap(
                    database_path,
                    dataset_key="monthly_revenue",
                    market=market,
                    symbol=symbol,
                    partition_key=period,
                    reason="coverage_missing" if last_error is None else "fetch_or_parse_failed",
                    detail=last_error or "verified monthly revenue row is absent",
                    run_id=run_id,
                )
        status = "verified" if complete else ("partial" if refreshed_periods else "failed")
        partition_key = f"{from_date:%Y-%m}..{to_date:%Y-%m}"
        if last_fetch is not None:
            request_for_state = last_fetch.request
            request_url = last_fetch.final_url or request_for_state.url
            request_method = request_for_state.method
            request_hash = _partition_request_body_hash(request_for_state)
            payload_hash = last_fetch.payload_sha256
        else:
            request_url = f"db://monthly_revenue/{market}/{symbol}/{partition_key}"
            request_method = "DB"
            request_hash = None
            payload_hash = None
        upsert_partition_state(
            database_path,
            dataset_key="monthly_revenue",
            market=market,
            symbol=symbol,
            partition_key=partition_key,
            request_method=request_method,
            request_url=request_url,
            requested_from=from_date,
            requested_to=to_date,
            request_body_sha256=request_hash,
            payload_sha256=payload_hash,
            source_payload_id=last_source_payload_id,
            first_effective_date=from_date,
            last_effective_date=to_date,
            row_count=len(refreshed_periods),
            status=status,
            last_verified_at=datetime.now().astimezone().isoformat() if complete else None,
            last_run_id=run_id,
            last_completeness_run_id=completeness_id,
            gap_reason=last_error if not complete else None,
        )
        record_sync_item(
            database_path,
            run_id=run_id,
            dataset_key="monthly_revenue",
            market=market,
            symbol=symbol,
            requested_from=from_date,
            requested_to=to_date,
            expected_row_count=len(expected_periods),
            actual_row_count=len(refreshed_periods),
            cache_status=item_cache_status,
            status=status,
            error=last_error or ("missing=" + ",".join(remaining) if remaining else None),
            started_at=item_started,
            finished_at=datetime.now().astimezone().isoformat(),
        )
        if fetched_rows and remaining:
            warnings.append(
                {
                    "market": market,
                    "symbol": symbol,
                    "reason": "partial_history",
                    "missing_periods": remaining,
                }
            )

    status = "complete" if actual_partitions == len(symbol_items) and not failures else "failed"
    finish_completeness_run(
        database_path,
        completeness_run_id=completeness_id,
        status="verified" if status == "complete" else "partial",
        actual_rows=actual_rows,
        actual_partitions=actual_partitions,
        missing_partitions=sorted(set(missing_partitions)),
        summary={
            "dataset_key": "monthly_revenue",
            "status": status,
            "db_hits": db_hits,
            "network_requests": network_requests,
            "failures": failures,
            "warnings": warnings,
            "started_at": started_at,
            "mode": mode,
        },
    )
    return {
        "status": status,
        "expected_partitions": len(symbol_items),
        "actual_partitions": actual_partitions,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "missing_partitions": sorted(set(missing_partitions)),
        "failures": failures,
        "warnings": warnings,
        "db_hits": db_hits,
        "network_requests": network_requests,
        "completeness_run_id": completeness_id,
    }


def _normalise_public_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    try:
        if len(digits) == 7:
            return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7]))
        if len(digits) >= 8:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _financial_fact_rows(payload: Any, market: str, coverage: set[str]) -> list[dict[str, Any]]:
    """Expand the official current-quarter summary into canonical facts."""

    raw_rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        raw_rows = [dict(row) for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        raw_rows = [dict(row) for row in payload["data"] if isinstance(row, dict)]
    rows: list[dict[str, Any]] = []
    value_fields = {
        "revenue": ("營業收入",),
        "operating_income": ("營業利益", "營業利益（損失）"),
        "net_income": ("稅後淨利", "本期淨利（淨損）", "本期稅後淨利（淨損）"),
        "eps": ("基本每股盈餘(元)", "基本每股盈餘（元）", "基本每股盈餘"),
    }
    for raw in raw_rows:
        symbol = str(
            raw.get("公司代號")
            or raw.get("SecuritiesCompanyCode")
            or raw.get("股票代號")
            or ""
        ).strip()
        if symbol not in coverage:
            continue
        year_raw = raw.get("年度") or raw.get("Year") or raw.get("年")
        season_raw = raw.get("季別") or raw.get("Season") or raw.get("季度")
        try:
            year_number = int(re.sub(r"[^0-9]", "", str(year_raw)))
            year = year_number + 1911 if year_number < 1900 else year_number
            season = int(re.sub(r"[^0-9]", "", str(season_raw)))
            if season not in {1, 2, 3, 4}:
                raise ValueError
            end_month = season * 3
            next_month = date(year + (end_month == 12), 1 if end_month == 12 else end_month + 1, 1)
            effective_date = next_month - timedelta(days=1)
        except (TypeError, ValueError):
            continue
        available_date = _normalise_public_date(raw.get("出表日期") or raw.get("Date"))
        if available_date is None:
            continue
        for fact_code, aliases in value_fields.items():
            value = None
            for alias in aliases:
                if alias in raw:
                    value = raw.get(alias)
                    break
            if value in {None, "", "--", "-"}:
                continue
            rows.append(
                {
                    "公司代號": symbol,
                    "fact_code": fact_code,
                    "fiscal_period": f"{year:04d}Q{season}",
                    "value": value,
                    "unit": "TWD_per_share" if fact_code == "eps" else "TWD_thousand",
                    "consolidation": "consolidated",
                    "dimension_json": {},
                    "effective_date": effective_date,
                    "available_date": available_date,
                    "published_at": available_date.isoformat(),
                    "revision_sequence": 1,
                    "raw_payload_json": raw,
                }
            )
    return rows


def _run_current_research_adapters(
    *,
    provider: Any,
    database_path: Path,
    run_id: str,
    symbols: dict[tuple[str, str], dict[str, Any]],
    selected_datasets: list[str],
    as_of: date,
    mode: str,
) -> dict[str, Any]:
    """Populate bounded official research snapshots with explicit coverage."""

    registry = validated_adapter_registry()
    result: dict[str, Any] = {}
    markets = sorted({market for market, _symbol in symbols})
    coverage_by_market = {
        market: {symbol for row_market, symbol in symbols if row_market == market}
        for market in markets
    }
    for dataset_key in ("financial_facts", "corporate_actions", "market_sessions"):
        if dataset_key not in selected_datasets:
            continue
        adapter = registry.require(dataset_key)
        source_markets = markets
        expected_rows = sum(len(items) * 4 for items in coverage_by_market.values()) if dataset_key == "financial_facts" else 0
        if dataset_key == "market_sessions":
            expected_rows = 1
        completeness_id = begin_completeness_run(
            database_path,
            dataset_key=dataset_key,
            run_id=run_id,
            expected_rows=expected_rows,
            expected_partitions=len(source_markets),
        )
        actual_rows = 0
        actual_partitions = 0
        failures: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        coverage_gaps: list[dict[str, Any]] = []
        db_hits = 0
        network_requests = 0
        for market in source_markets:
            coverage = coverage_by_market.get(market, set())
            item_started = datetime.now().astimezone().isoformat()
            request: FetchRequest | None = None
            fetch_result: FetchResult | None = None
            parsed: list[dict[str, Any]] = []
            missing_symbols: list[str] = []
            try:
                before = int((provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0)
                fetch_result = provider.fetch_research_dataset(
                    dataset_key=dataset_key,
                    market=market,
                    as_of=as_of,
                    force_network=mode == "full",
                )
                after = int((provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0)
                network_requests += max(after - before, 0)
                request = fetch_result.request
                source_payload_id = getattr(provider, "_last_persisted_payload_id", None)
                source_endpoint = str(
                    provider._endpoint_name(fetch_result.final_url or request.url)
                    if hasattr(provider, "_endpoint_name")
                    else dataset_key
                )
                context = AdapterContext(
                    as_of=as_of,
                    database_path=str(database_path),
                    run_id=run_id,
                    requested_from=as_of,
                    requested_to=as_of,
                    output_root=str(database_path.parent.parent.parent),
                    options={
                        "fetcher": lambda _request, result=fetch_result: result,
                        "market": market,
                        "source_endpoint": source_endpoint,
                        "source_payload_id": source_payload_id,
                        "source_url": fetch_result.final_url or request.url,
                        "fetched_at": datetime.now().astimezone().isoformat(),
                        "available_date": as_of,
                        "availability_precision": "source_published_date" if dataset_key == "financial_facts" else "retrieval_date",
                        "validation_status": "verified",
                    },
                )
                payload = fetch_result.payload
                if dataset_key == "financial_facts":
                    payload = _financial_fact_rows(payload, market, coverage)
                parsed = adapter.parse(
                    FetchResult(
                        status=fetch_result.status,
                        payload=payload,
                        request=fetch_result.request,
                        final_url=fetch_result.final_url,
                        redirect_chain=fetch_result.redirect_chain,
                        http_status=fetch_result.http_status,
                        fallback_level=fetch_result.fallback_level,
                        cache_status=fetch_result.cache_status,
                        payload_sha256=fetch_result.payload_sha256,
                    ),
                    context,
                )
                if dataset_key in {"financial_facts", "corporate_actions"}:
                    parsed = [
                        row for row in parsed
                        if str(row.get("symbol") or "") in coverage
                    ]
                if dataset_key == "financial_facts":
                    expected_symbols = coverage
                    actual_symbols = {str(row.get("symbol") or "") for row in parsed}
                    missing_symbols = sorted(expected_symbols - actual_symbols)
                    if missing_symbols:
                        gap = {
                            "market": market,
                            "reason": "source_symbol_missing_from_current_snapshot",
                            "symbols": missing_symbols,
                        }
                        coverage_gaps.append(gap)
                        warnings.append(gap)
                        for missing_symbol in missing_symbols:
                            record_data_gap(
                                database_path,
                                dataset_key=dataset_key,
                                market=market,
                                symbol=missing_symbol,
                                partition_key=as_of.isoformat(),
                                reason="source_symbol_missing_from_current_snapshot",
                                detail=(
                                    "Official current-quarter snapshot did not contain "
                                    f"coverage symbol {missing_symbol}; no value was inferred."
                                ),
                                run_id=run_id,
                            )
                if dataset_key == "corporate_actions" and not parsed:
                    # No ex-right/dividend event on the snapshot date is a
                    # valid observation, not a malformed payload.
                    warnings.append({"market": market, "reason": "no_coverage_action_in_snapshot"})
                    try:
                        from src.providers.market_data_store import update_source_payload_validation

                        if source_payload_id:
                            update_source_payload_validation(
                                database_path,
                                payload_id=str(source_payload_id),
                                validation_status="verified",
                            )
                    except Exception:
                        pass
                elif parsed:
                    validation = adapter.validate(parsed, context)
                    if validation.status != "verified":
                        raise ValueError("payload validation failed: " + "; ".join(validation.errors))
                    actual_rows += adapter.upsert(parsed, context)
                else:
                    raise ValueError("official research payload produced no valid rows")
                actual_partitions += 1
                status = "partial" if missing_symbols else "verified"
                db_hits += 1 if fetch_result.cache_status in {"fresh", "db_hit"} else 0
                record_sync_item(
                    database_path,
                    run_id=run_id,
                    dataset_key=dataset_key,
                    market=market,
                    requested_from=as_of,
                    requested_to=as_of,
                    expected_row_count=(len(coverage) * 4 if dataset_key == "financial_facts" else len(parsed)),
                    actual_row_count=len(parsed),
                    cache_status=fetch_result.cache_status,
                    status=status,
                    started_at=item_started,
                    finished_at=datetime.now().astimezone().isoformat(),
                )
            except Exception as exc:
                status = "failed"
                failure = {"market": market, "dataset_key": dataset_key, "reason": str(exc)}
                failures.append(failure)
                record_data_gap(
                    database_path,
                    dataset_key=dataset_key,
                    market=market,
                    partition_key=as_of.isoformat(),
                    reason="fetch_or_parse_failed",
                    detail=str(exc),
                    run_id=run_id,
                )
                record_sync_item(
                    database_path,
                    run_id=run_id,
                    dataset_key=dataset_key,
                    market=market,
                    requested_from=as_of,
                    requested_to=as_of,
                    expected_row_count=(len(coverage) * 4 if dataset_key == "financial_facts" else 0),
                    actual_row_count=len(parsed),
                    cache_status=fetch_result.cache_status if fetch_result else "network",
                    status=status,
                    error=str(exc),
                    started_at=item_started,
                    finished_at=datetime.now().astimezone().isoformat(),
                )
            if request is not None:
                upsert_partition_state(
                    database_path,
                    dataset_key=dataset_key,
                    market=market,
                    partition_key=as_of.isoformat(),
                    request_method=request.method,
                    request_url=fetch_result.final_url if fetch_result else request.url,
                    requested_from=as_of,
                    requested_to=as_of,
                    request_body_sha256=_partition_request_body_hash(request),
                    payload_sha256=fetch_result.payload_sha256 if fetch_result else None,
                    source_payload_id=getattr(provider, "_last_persisted_payload_id", None),
                    first_effective_date=as_of,
                    last_effective_date=as_of,
                    row_count=len(parsed),
                    status=status,
                    last_verified_at=datetime.now().astimezone().isoformat() if status == "verified" else None,
                    last_run_id=run_id,
                    last_completeness_run_id=completeness_id,
                    gap_reason=(
                        None
                        if status == "verified"
                        else (
                            "source_symbol_missing_from_current_snapshot"
                            if missing_symbols
                            else (failures[-1]["reason"] if failures else "failed")
                        )
                    ),
                )
        if actual_partitions != len(source_markets) or failures:
            dataset_status = "failed"
        elif coverage_gaps:
            dataset_status = "partial"
        else:
            dataset_status = "complete"
        finish_completeness_run(
            database_path,
            completeness_run_id=completeness_id,
            status="verified" if dataset_status == "complete" else dataset_status,
            actual_rows=actual_rows,
            actual_partitions=actual_partitions,
            missing_partitions=[item.get("market") for item in failures],
            summary={
                "dataset_key": dataset_key,
                "status": dataset_status,
                "db_hits": db_hits,
                "network_requests": network_requests,
                "failures": failures,
                "warnings": warnings,
                "coverage_gaps": coverage_gaps,
            },
        )
        result[dataset_key] = {
            "status": dataset_status,
            "expected_partitions": len(source_markets),
            "actual_partitions": actual_partitions,
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
            "failures": failures,
            "warnings": warnings,
            "coverage_gaps": coverage_gaps,
            "db_hits": db_hits,
            "network_requests": network_requests,
            "completeness_run_id": completeness_id,
        }
    return result


def _run_enrichment_adapters(
    *,
    provider: Any,
    database_path: Path,
    run_id: str,
    selected_datasets: list[str],
    symbols: dict[tuple[str, str], dict[str, Any]],
    from_date: date,
    to_date: date,
    as_of: date,
    mode: str = "incremental",
) -> dict[str, Any]:
    """Run the validated historical adapters over month partitions.

    The daily report process never enters this function.  It is deliberately
    a bounded, explicit enrichment profile: one source partition is fetched,
    parsed, validated, upserted, and checkpointed before the next partition.
    """

    registry = validated_adapter_registry()
    result: dict[str, Any] = {}
    months = _month_starts(from_date, to_date)
    markets = sorted({market for market, _symbol in symbols})
    symbols_by_market = {
        market: sorted(symbol for row_market, symbol in symbols if row_market == market)
        for market in markets
    }
    for dataset_key in ("monthly_revenue", "valuation_snapshots"):
        if dataset_key not in selected_datasets:
            continue
        if dataset_key == "monthly_revenue" and callable(
            getattr(provider, "fetch_monthly_revenue_history_for_symbol", None)
        ):
            # The production provider uses a symbol-aware DB-first runner.
            # Keep the old market/month branch below for minimal test doubles
            # and legacy providers that have not adopted the new contract.
            result[dataset_key] = _run_monthly_revenue_history(
                provider=provider,
                database_path=database_path,
                run_id=run_id,
                symbols=symbols,
                from_date=from_date,
                to_date=to_date,
                as_of=as_of,
                mode=mode,
            )
            continue
        adapter = registry.require(dataset_key)
        expected_partitions = len(markets) * len(months)
        completeness_id = begin_completeness_run(
            database_path,
            dataset_key=dataset_key,
            run_id=run_id,
            expected_rows=sum(len(items) for items in symbols_by_market.values()) * len(months),
            expected_partitions=expected_partitions,
        )
        dataset_started = datetime.now().astimezone().isoformat()
        actual_rows = 0
        actual_partitions = 0
        missing_partitions: list[str] = []
        failures: list[dict[str, Any]] = []
        checkpoint_warnings: list[dict[str, Any]] = []
        db_hits = 0
        network_requests = 0
        for market in markets:
            expected_symbols = set(symbols_by_market.get(market) or [])
            for month_start in months:
                requested_from = month_start
                requested_to = min(_month_end(month_start), to_date)
                if requested_to < from_date:
                    continue
                partition_key = month_start.strftime("%Y-%m")
                checkpoint_missing_symbols: list[str] = []
                state = get_partition_state(
                    database_path,
                    dataset_key=dataset_key,
                    market=market,
                    partition_key=partition_key,
                )
                if mode != "full" and _partition_checkpoint_is_reusable(
                    state,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    minimum_row_count=len(expected_symbols),
                ):
                    if dataset_key == "monthly_revenue":
                        rows = get_monthly_revenue_history(database_path, market=market)
                        count = sum(
                            1
                            for row in rows
                            if str(row.get("revenue_month") or "") == partition_key
                            and str(row.get("symbol") or "") in expected_symbols
                        )
                    else:
                        rows = get_valuation_history(database_path, market=market)
                        count = sum(
                            1
                            for row in rows
                            if str(row.get("trade_date") or "").startswith(partition_key)
                            and str(row.get("symbol") or "") in expected_symbols
                    )
                    if count >= len(expected_symbols):
                        db_hits += 1
                        actual_rows += count
                        actual_partitions += 1
                        continue
                    cached_symbols = {
                        str(row.get("symbol") or "")
                        for row in rows
                        if (
                            str(row.get("revenue_month") or "") == partition_key
                            if dataset_key == "monthly_revenue"
                            else str(row.get("trade_date") or "").startswith(partition_key)
                        )
                    }
                    missing_symbols = sorted(expected_symbols - cached_symbols)
                    checkpoint_missing_symbols = missing_symbols
                    checkpoint_warnings.append(
                        {
                            "market": market,
                            "partition": partition_key,
                            "reason": "checkpoint_coverage_missing_refetch",
                            "symbols": missing_symbols,
                        }
                    )

                request = FetchRequest(
                    dataset_key=dataset_key,
                    market=market,
                    symbol=None,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    method="GET" if dataset_key == "valuation_snapshots" and market == "TWSE" else "POST",
                    url=f"provider://{dataset_key}/{market}/{partition_key}",
                )
                options: dict[str, Any] = {
                    "market": market,
                    "source_endpoint": dataset_key,
                    "symbols": expected_symbols,
                    "source_payload_id": None,
                    "fetched_at": datetime.now().astimezone().isoformat(),
                    # Historical MOPS/official feeds do not always expose a
                    # publication timestamp.  Keep the retrieval cutoff
                    # explicit so the row is useful descriptively but can be
                    # excluded from formal PIT queries.
                    "available_date": as_of if dataset_key == "monthly_revenue" else None,
                }
                context = AdapterContext(
                    as_of=as_of,
                    database_path=str(database_path),
                    run_id=run_id,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    output_root=str(database_path.parent.parent.parent),
                    options=options,
                )
                item_started = datetime.now().astimezone().isoformat()
                try:
                    if dataset_key == "monthly_revenue":
                        fetcher = lambda _request, m=market, p=partition_key: provider.fetch_monthly_revenue_partition(
                            market=m,
                            revenue_month=p,
                            as_of=as_of,
                            force_network=mode == "full",
                        )
                    else:
                        fetcher = lambda _request, m=market, rf=requested_from, rt=requested_to: provider.fetch_valuation_partition(
                            market=m,
                            requested_from=rf,
                            requested_to=rt,
                            force_network=mode == "full",
                        )
                    context.options["fetcher"] = fetcher
                    before_requests = int(
                        (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                    )
                    fetch_result = adapter.fetch_range(request, context)
                    after_requests = int(
                        (provider.get_market_data_diagnostics() or {}).get("request_count", 0) or 0
                    )
                    network_requests += max(after_requests - before_requests, 0)
                    context.options["source_payload_id"] = getattr(
                        provider, "_last_persisted_payload_id", None
                    )
                    context.options["source_endpoint"] = (
                        "mops.monthly_revenue"
                        if dataset_key == "monthly_revenue"
                        else ("twse.BWIBBU_d" if market == "TWSE" else "tpex.peQryDate")
                    )
                    context.options["source_url"] = fetch_result.final_url or fetch_result.request.url
                    parsed = adapter.parse(fetch_result, context)
                    parsed = [
                        row for row in parsed
                        if str(row.get("market") or "") == market
                        and str(row.get("symbol") or "") in expected_symbols
                    ]
                    validation = adapter.validate(parsed, context)
                    actual_symbols = {str(row.get("symbol") or "") for row in parsed}
                    missing_symbols = sorted(expected_symbols - actual_symbols)
                    if missing_symbols:
                        failures.append(
                            {
                                "market": market,
                                "partition": partition_key,
                                "reason": "coverage_missing",
                                "symbols": missing_symbols,
                            }
                        )
                    if validation.status != "verified" or missing_symbols:
                        context.options["validation_status"] = "partial" if parsed else "failed"
                        context.options["data_gap_reason"] = (
                            "coverage_missing" if missing_symbols else "payload_validation_failed"
                        )
                        if parsed:
                            adapter.upsert(parsed, context)
                        status = "partial" if parsed else "failed"
                        missing_partitions.append(f"{market}:{partition_key}")
                    else:
                        context.options["validation_status"] = "verified"
                        context.options["availability_precision"] = (
                            "source_observation_date"
                            if dataset_key == "valuation_snapshots"
                            else (
                                "source_published_date"
                                if all(row.get("published_at") for row in parsed)
                                else "retrieval_date"
                            )
                        )
                        inserted = adapter.upsert(parsed, context)
                        actual_rows += inserted
                        actual_partitions += 1
                        status = "verified"
                    request_for_state = fetch_result.request
                    upsert_partition_state(
                        database_path,
                        dataset_key=dataset_key,
                        market=market,
                        partition_key=partition_key,
                        request_method=request_for_state.method,
                        request_url=fetch_result.final_url or request_for_state.url,
                        requested_from=requested_from,
                        requested_to=requested_to,
                        request_body_sha256=_partition_request_body_hash(request_for_state),
                        payload_sha256=fetch_result.payload_sha256,
                        source_payload_id=context.options.get("source_payload_id"),
                        first_effective_date=requested_from,
                        last_effective_date=requested_to,
                        row_count=len(parsed),
                        status=status,
                        last_verified_at=(
                            datetime.now().astimezone().isoformat()
                            if status == "verified"
                            else None
                        ),
                        last_run_id=run_id,
                        last_completeness_run_id=completeness_id,
                        gap_reason=context.options.get("data_gap_reason"),
                    )
                    if status != "verified":
                        record_data_gap(
                            database_path,
                            dataset_key=dataset_key,
                            market=market,
                            partition_key=partition_key,
                            reason=str(context.options.get("data_gap_reason") or "partial"),
                            detail=(
                                f"validation={validation.status}; missing={','.join(missing_symbols)}"
                            ),
                            run_id=run_id,
                        )
                    record_sync_item(
                        database_path,
                        run_id=run_id,
                        dataset_key=dataset_key,
                        market=market,
                        requested_from=requested_from,
                        requested_to=requested_to,
                        expected_row_count=len(expected_symbols),
                        actual_row_count=len(parsed),
                        cache_status=fetch_result.cache_status,
                        status=status,
                        error=(
                            ",".join(missing_symbols)
                            if missing_symbols
                            else ("; ".join(validation.errors) if validation.errors else None)
                        ),
                        started_at=item_started,
                        finished_at=datetime.now().astimezone().isoformat(),
                    )
                except Exception as exc:
                    missing_partitions.append(f"{market}:{partition_key}")
                    failure = {
                        "market": market,
                        "partition": partition_key,
                        "reason": str(exc),
                    }
                    if checkpoint_missing_symbols:
                        failure["checkpoint_missing_symbols"] = checkpoint_missing_symbols
                    failures.append(failure)
                    upsert_partition_state(
                        database_path,
                        dataset_key=dataset_key,
                        market=market,
                        partition_key=partition_key,
                        request_method=request.method,
                        request_url=request.url,
                        requested_from=requested_from,
                        requested_to=requested_to,
                        status="failed",
                        last_run_id=run_id,
                        last_completeness_run_id=completeness_id,
                        gap_reason="fetch_or_parse_failed",
                    )
                    record_data_gap(
                        database_path,
                        dataset_key=dataset_key,
                        market=market,
                        partition_key=partition_key,
                        reason="fetch_or_parse_failed",
                        detail=str(exc),
                        run_id=run_id,
                    )
                    record_sync_item(
                        database_path,
                        run_id=run_id,
                        dataset_key=dataset_key,
                        market=market,
                        requested_from=requested_from,
                        requested_to=requested_to,
                        expected_row_count=len(expected_symbols),
                        cache_status="network",
                        status="failed",
                        error=str(exc),
                        started_at=item_started,
                        finished_at=datetime.now().astimezone().isoformat(),
                    )
        status = "complete" if not failures and actual_partitions == expected_partitions else "failed"
        finish_completeness_run(
            database_path,
            completeness_run_id=completeness_id,
            status="verified" if status == "complete" else "partial",
            actual_rows=actual_rows,
            actual_partitions=actual_partitions,
            missing_partitions=missing_partitions,
            summary={
                "dataset_key": dataset_key,
                "status": status,
                "db_hits": db_hits,
                "network_requests": network_requests,
                "failures": failures,
                "warnings": checkpoint_warnings,
                "started_at": dataset_started,
            },
        )
        result[dataset_key] = {
            "status": status,
            "expected_partitions": expected_partitions,
            "actual_partitions": actual_partitions,
            "expected_rows": sum(len(items) for items in symbols_by_market.values()) * len(months),
            "actual_rows": actual_rows,
            "missing_partitions": sorted(set(missing_partitions)),
            "failures": failures,
            "warnings": checkpoint_warnings,
            "db_hits": db_hits,
            "network_requests": network_requests,
            "completeness_run_id": completeness_id,
        }
    return result


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Market data sync — {payload['as_of']}",
        "",
        f"- status: `{payload['status']}`",
        f"- database: `{payload['database_path']}`",
        f"- themes: {', '.join(payload['themes'])}",
        f"- mode: `{payload['mode']}`",
        f"- profile: `{payload.get('profile', 'daily')}`",
        f"- datasets: {', '.join(payload['datasets'])}",
        f"- dry run: `{payload['dry_run']}`",
        f"- coverage symbols: {payload['coverage_count']}",
        f"- verified daily bars: {payload['daily_success_count']}",
        f"- TAIEX: `{payload['taiex']['status']}` ({payload['taiex'].get('latest_trade_date') or 'N/A'})",
        f"- period bars: {payload.get('period_bars', {}).get('period_rows_upserted', 0)} upserted",
        f"- adjusted bars: {payload.get('adjusted_bars', {}).get('bars_upserted', 0)} upserted",
        f"- network requests: {payload.get('network_requests', 0)}",
        f"- DB hits: {payload.get('db_hits', 0)}",
        "",
        "## Range gate",
        "",
        "Daily profile requires the verified tail to equal `to_date` on a weekday. "
        "Enrichment profile validates the requested historical range. A prior bar is never used as a substitute.",
        "",
        "## Failures",
        "",
    ]
    failures = payload.get("failures") or []
    if failures:
        for item in failures:
            dataset = str(item.get("dataset_key") or "daily_bars")
            identity = str(item.get("symbol") or item.get("partition") or "run")
            market = str(item.get("market") or "")
            scope = ":".join(part for part in (dataset, market, identity) if part)
            detail = item.get("reason") or item.get("error") or "unknown failure"
            lines.append(f"- `{scope}` — {detail}")
    else:
        lines.append("- none")
    warnings = payload.get("warnings") or []
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- {warning}" for warning in warnings) if warnings else lines.append("- none")
    lines.extend(
        [
            "",
            "## Research boundary",
            "",
            "This is a canonical market-data synchronisation manifest only. It does not invoke downstream execution or publication actions.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_sync_outputs(output_root: Path, as_of: date, payload: dict[str, Any]) -> dict[str, Path]:
    audit_dir = Path(output_root) / "audit" / as_of.strftime("%Y%m%d")
    audit_dir.mkdir(parents=True, exist_ok=True)
    log_date = as_of.strftime("%Y%m%d")
    json_path = audit_dir / f"market-sync-{log_date}.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    markdown_path = audit_dir / f"market-sync-{log_date}.md"
    _write_markdown(markdown_path, payload)
    return {"json": json_path, "markdown": markdown_path}


def run(
    *,
    themes: list[str],
    as_of: date,
    universe_mode: str,
    lookback: int,
    timeout: float,
    output_root: Path,
    database_path: Path | None = None,
    datasets: list[str] | None = None,
    mode: str = "incremental",
    from_date: date | None = None,
    to_date: date | None = None,
    dry_run: bool = False,
    profile: str = "daily",
) -> dict[str, Path]:
    output_root = Path(output_root)
    database_path = Path(database_path) if database_path else output_root / "cache" / "market" / "market_data.sqlite"
    if profile not in {"daily", "enrichment"}:
        raise ValueError(f"unsupported sync profile: {profile}")
    default_datasets = DEFAULT_DATASETS if profile == "daily" else ENRICHMENT_DEFAULT_DATASETS
    selected_datasets = list(dict.fromkeys(datasets or default_datasets))
    unknown_datasets = sorted(set(selected_datasets) - SUPPORTED_DATASETS)
    if unknown_datasets:
        raise ValueError(f"unsupported datasets: {', '.join(unknown_datasets)}")
    not_implemented_datasets = unimplemented_dataset_keys(selected_datasets)
    if mode not in {"incremental", "full"}:
        raise ValueError(f"unsupported sync mode: {mode}")
    # With no explicit range, the daily profile asks the provider for its
    # normal lookback window ending at ``to_date``.  Passing ``as_of`` as an
    # artificial ``from_date`` would reduce a DB hit to one row and make the
    # 253-bar gate fail.  An explicit from-date remains a true range request.
    to_date = to_date or as_of
    if from_date is not None and from_date > to_date:
        raise ValueError("from_date must be on or before to_date")
    if to_date > as_of:
        raise ValueError("to_date cannot be later than as_of")
    if profile == "daily" and to_date != as_of:
        raise ValueError("daily profile requires to_date to equal as_of")
    log_date = as_of.strftime("%Y%m%d")
    audit_dir = output_root / "audit" / log_date
    audit_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now().astimezone().isoformat()
    run_id = f"market-sync-{as_of:%Y%m%d}-{uuid.uuid4().hex[:12]}"

    if not_implemented_datasets:
        payload = {
            "run_id": run_id,
            "status": "failed",
            "as_of": as_of.isoformat(),
            "expected_trade_date": to_date.isoformat() if profile == "daily" and to_date.weekday() < 5 else None,
            "themes": themes,
            "universe_mode": universe_mode,
            "lookback": max(lookback, 253),
            "mode": mode,
            "profile": profile,
            "datasets": selected_datasets,
            "dry_run": dry_run,
            "from_date": from_date.isoformat() if from_date else None,
            "to_date": to_date.isoformat(),
            "database_path": str(database_path),
            "coverage_count": 0,
            "daily_success_count": 0,
            "daily_failure_count": 0,
            "latest_dates": {},
            "failures": [],
            "taiex": {"status": "not_run"},
            "period_bars": {"status": "not_run"},
            "adjusted_bars": {"status": "not_run"},
            "network_requests": 0,
            "db_hits": 0,
            "fallbacks": 0,
            "missing_partitions": [],
            "source_warnings": [],
            "warnings": [],
            "errors": [
                "requested dataset has no validated adapter: "
                + ", ".join(not_implemented_datasets)
            ],
            "not_implemented_datasets": not_implemented_datasets,
            "market_data_diagnostics": {"status": "not_run"},
            "integrity": {"status": "not_run"},
            "source_policy": "official or auditable public source; unknown payloads remain raw/quarantine",
            "research_only": True,
            "started_at": started_at,
            "finished_at": datetime.now().astimezone().isoformat(),
        }
        return _write_sync_outputs(output_root, as_of, payload)

    coverage_symbols = _coverage_symbols(themes, universe_mode)

    if dry_run:
        payload = {
            "run_id": run_id,
            "status": "planned",
            "as_of": as_of.isoformat(),
            "expected_trade_date": to_date.isoformat() if profile == "daily" and to_date.weekday() < 5 else None,
            "themes": themes,
            "universe_mode": universe_mode,
            "lookback": max(lookback, 253),
            "mode": mode,
            "profile": profile,
            "datasets": selected_datasets,
            "dry_run": True,
            "from_date": from_date.isoformat() if from_date else None,
            "to_date": to_date.isoformat(),
            "database_path": str(database_path),
            "coverage_count": len(coverage_symbols),
            "daily_success_count": 0,
            "daily_failure_count": 0,
            "latest_dates": {},
            "failures": [],
            "taiex": {"status": "not_run"},
            "period_bars": {"status": "not_run"},
            "adjusted_bars": {"status": "not_run"},
            "network_requests": 0,
            "db_hits": 0,
            "fallbacks": 0,
            "missing_partitions": [],
            "source_warnings": [],
            "warnings": ["dry_run: no network request or database mutation was performed"],
            "market_data_diagnostics": {"status": "not_run"},
            "integrity": {"status": "not_run"},
            "source_policy": "official or auditable public source; unknown payloads remain raw/quarantine",
            "research_only": True,
            "started_at": started_at,
            "finished_at": datetime.now().astimezone().isoformat(),
        }
        json_path = audit_dir / f"market-sync-{log_date}.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        markdown_path = audit_dir / f"market-sync-{log_date}.md"
        _write_markdown(markdown_path, payload)
        return {"json": json_path, "markdown": markdown_path}

    provider = TwMarketProvider(
        timeout=timeout,
        cache_dir=output_root / "cache" / "market",
        market_database_path=database_path,
        sync_run_id=run_id,
    )

    record_sync_run(
        database_path,
        run_id=run_id,
        as_of_date=as_of,
        themes=themes,
        started_at=started_at,
        finished_at=started_at,
        status="running",
        summary={
            "mode": mode,
            "profile": profile,
            "datasets": selected_datasets,
            "from_date": from_date.isoformat() if from_date else None,
            "to_date": to_date.isoformat(),
        },
    )

    warnings: list[str] = []
    basics: dict[str, dict[str, Any]] = {}
    if "security_master" in selected_datasets:
        try:
            try:
                basics = provider._load_basics(
                    as_of=to_date,
                    required_symbols=set(coverage_symbols),
                )
            except TypeError as exc:
                if "required_symbols" not in str(exc) and "as_of" not in str(exc):
                    raise
                basics = provider._load_basics()
        except Exception as exc:
            warnings.append(f"security master enrichment failed: {exc}")

    symbols: dict[tuple[str, str], dict[str, Any]] = {}
    for theme in themes:
        rule = theme_rule(theme, universe_mode=universe_mode)
        for symbol in rule.get("coverage_symbols") or rule.get("symbols") or []:
            symbol = str(symbol).strip()
            if not symbol:
                continue
            basic_market = str((basics.get(symbol) or {}).get("market") or "").strip()
            market = basic_market or _market_for_symbol(symbol, database_path)
            symbols[(market, symbol)] = {
                "symbol": symbol,
                "market": market,
                "themes": sorted(
                    set(symbols.get((market, symbol), {}).get("themes") or []) | {theme}
                ),
            }
            upsert_universe_membership(
                database_path,
                theme=theme,
                symbol=symbol,
                market=market,
                universe_mode=universe_mode,
                effective_from=as_of,
            )

    failures: list[dict[str, Any]] = []
    daily_success_count = 0
    latest_dates: dict[str, str] = {}
    if "daily_bars" in selected_datasets:
        for market, symbol in sorted(symbols):
            item_started_at = datetime.now().astimezone().isoformat()
            try:
                bars = _provider_get_ohlcv(
                    provider,
                    symbol=symbol,
                    market=market,
                    as_of=to_date,
                    lookback=max(lookback, 253),
                    from_date=from_date,
                    require_current_day=profile == "daily",
                )
                if profile == "daily" and len(bars) < max(lookback, 253):
                    raise RuntimeError(f"日線不足：{len(bars)}/{max(lookback, 253)}")
                if not bars:
                    raise RuntimeError("指定區間沒有合格日線")
                if from_date is not None and bars[0]["date"] > from_date:
                    raise RuntimeError(
                        f"歷史區間缺口：expected_from={from_date.isoformat()} actual_from={bars[0]['date'].isoformat()}"
                    )
                latest = bars[-1]["date"]
                if to_date.weekday() < 5 and latest != to_date:
                    raise RuntimeError(
                        f"指定迄日資料缺口：expected={to_date.isoformat()} actual={latest.isoformat()}"
                    )
                daily_success_count += 1
                latest_dates[f"{market}:{symbol}"] = latest.isoformat()
                record_sync_item(
                    database_path,
                    run_id=run_id,
                    dataset_key="daily_bars",
                    market=market,
                    symbol=symbol,
                    requested_from=from_date,
                    requested_to=to_date,
                    expected_trade_date=to_date if profile == "daily" and to_date.weekday() < 5 else None,
                    actual_latest_trade_date=latest,
                    expected_row_count=max(lookback, 253),
                    actual_row_count=len(bars),
                    cache_status="db_first_or_network",
                    status="verified",
                    started_at=item_started_at,
                    finished_at=datetime.now().astimezone().isoformat(),
                )
                upsert_dataset_sync_state(
                    database_path,
                    dataset_key="daily_bars",
                    market=market,
                    symbol=symbol,
                    first_effective_date=bars[0]["date"],
                    last_effective_date=latest,
                    verified_row_count=len(bars),
                    last_verified_at=datetime.now().astimezone().isoformat(),
                    last_status="verified",
                )
                record_fetch_attempt(
                    database_path,
                    run_id=run_id,
                    dataset_key="daily_bars",
                    market=market,
                    symbol=symbol,
                    request_method="provider",
                    request_url="provider://tw_market_provider/get_ohlcv",
                    requested_from=from_date,
                    requested_to=to_date,
                    started_at=item_started_at,
                    finished_at=datetime.now().astimezone().isoformat(),
                    cache_status="db_first_or_network",
                    status="verified",
                )
            except Exception as exc:
                failure = {"market": market, "symbol": symbol, "reason": str(exc)}
                failures.append(failure)
                finished_item_at = datetime.now().astimezone().isoformat()
                record_sync_item(
                    database_path,
                    run_id=run_id,
                    dataset_key="daily_bars",
                    market=market,
                    symbol=symbol,
                    requested_from=from_date,
                    requested_to=to_date,
                    expected_trade_date=to_date if profile == "daily" and to_date.weekday() < 5 else None,
                    expected_row_count=max(lookback, 253),
                    cache_status="db_first_or_network",
                    status="failed",
                    error=str(exc),
                    started_at=item_started_at,
                    finished_at=finished_item_at,
                )
                record_fetch_attempt(
                    database_path,
                    run_id=run_id,
                    dataset_key="daily_bars",
                    market=market,
                    symbol=symbol,
                    request_method="provider",
                    request_url="provider://tw_market_provider/get_ohlcv",
                    requested_from=from_date,
                    requested_to=to_date,
                    started_at=item_started_at,
                    finished_at=finished_item_at,
                    cache_status="db_first_or_network",
                    status="failed",
                    error=str(exc),
                )
    else:
        warnings.append("daily_bars dataset not selected; no current-day gate was evaluated")

    taiex: dict[str, Any]
    if "index_bars" in selected_datasets:
        try:
            taiex_series = provider.get_taiex_series(as_of=to_date, lookback=max(lookback, 253))
            latest_taiex = taiex_series[-1]["date"] if taiex_series else None
            if to_date.weekday() < 5 and latest_taiex != to_date:
                raise RuntimeError(
                    f"TAIEX 指定迄日資料缺口：expected={to_date.isoformat()} actual={latest_taiex}"
                )
            taiex = {
                "status": "verified",
                "count": len(taiex_series),
                "latest_trade_date": latest_taiex.isoformat() if latest_taiex else None,
            }
        except Exception as exc:
            taiex = {"status": "failed", "reason": str(exc), "latest_trade_date": None}
    else:
        taiex = {"status": "not_selected", "latest_trade_date": None}

    enrichment_results: dict[str, Any] = {}
    if profile == "enrichment":
        enrichment_datasets = [
            dataset
            for dataset in (
                "monthly_revenue",
                "valuation_snapshots",
                "financial_facts",
                "corporate_actions",
                "market_sessions",
            )
            if dataset in selected_datasets
        ]
        if enrichment_datasets:
            # A missing --from-date means "the current calendar month".  A
            # caller asking for historical depth should always provide an
            # explicit range; this default only keeps the one-month operator
            # command useful and bounded.
            enrichment_from = from_date or date(to_date.year, to_date.month, 1)
            enrichment_results = _run_enrichment_adapters(
                provider=provider,
                database_path=database_path,
                run_id=run_id,
                selected_datasets=enrichment_datasets,
                symbols=symbols,
                from_date=enrichment_from,
                to_date=to_date,
                as_of=as_of,
                mode=mode,
            )
            current_research_datasets = [
                dataset
                for dataset in ("financial_facts", "corporate_actions", "market_sessions")
                if dataset in selected_datasets
            ]
            if current_research_datasets:
                enrichment_results.update(
                    _run_current_research_adapters(
                        provider=provider,
                        database_path=database_path,
                        run_id=run_id,
                        symbols=symbols,
                        selected_datasets=current_research_datasets,
                        as_of=as_of,
                        mode=mode,
                    )
                )
            for dataset_key, dataset_result in enrichment_results.items():
                warnings.extend(
                    f"{dataset_key}:{item.get('market')}:{item.get('partition')}:{item.get('reason')}"
                    for item in dataset_result.get("warnings") or []
                )
                if dataset_result.get("status") == "partial":
                    warnings.append(
                        f"{dataset_key}:partial coverage; see enrichment.coverage_gaps"
                    )
                elif dataset_result.get("status") == "failed":
                    failures.extend(
                        {
                            "market": item.get("market"),
                            "symbol": item.get("partition"),
                            "dataset_key": dataset_key,
                            "reason": item.get("reason") or "enrichment partition failed",
                        }
                        for item in dataset_result.get("failures") or []
                    )

    # Security master is enrichment.  It is persisted when available, but a
    # metadata outage never permits a stale daily bar to pass the current-day gate.
    try:
        if "security_master" in selected_datasets:
            for symbol, item in basics.items():
                market = str(item.get("market") or _market_for_symbol(str(symbol), database_path))
                if (market, symbol) not in symbols:
                    continue
                upsert_source = "twse.basics" if market == "TWSE" else "tpex.basics"
                upsert_url = TWSE_BASICS_URL if market == "TWSE" else TPEX_BASICS_URL
                upsert_security_master(
                    database_path,
                    market=market,
                    symbol=symbol,
                    effective_date=to_date,
                    name=item.get("name"),
                    industry=item.get("industry"),
                    source_endpoint=upsert_source,
                    source_url=upsert_url,
                    source_payload_sha256=getattr(provider, "_basics_payload_hash", None),
                    source_payload_id=(getattr(provider, "_basics_payload_ids", {}) or {}).get(market),
                    available_date=to_date,
                    availability_precision="retrieval_date",
                )
        if "monthly_revenue" in selected_datasets and profile == "daily":
            try:
                revenue_map = provider._load_latest_revenue_map(
                    as_of=to_date,
                    required_symbols=set(coverage_symbols),
                )
            except TypeError as exc:
                if "required_symbols" not in str(exc) and "as_of" not in str(exc):
                    raise
                revenue_map = provider._load_latest_revenue_map()
            for (market, symbol), _item in symbols.items():
                revenue = revenue_map.get(symbol)
                if not revenue:
                    continue
                revenue_month = str(revenue.get("revenue_month") or "").strip()
                if not revenue_month:
                    revenue_month = as_of.strftime("%Y-%m")
                revenue_url = TWSE_REVENUE_URL if market == "TWSE" else TPEX_REVENUE_URL
                from src.analysis.factors import safe_float

                upsert_monthly_revenue(
                    database_path,
                    market=market,
                    symbol=symbol,
                    revenue_month=revenue_month,
                    monthly_revenue=safe_float(revenue.get("monthly_revenue")),
                    revenue_mom=safe_float(revenue.get("revenue_mom")),
                    revenue_yoy=safe_float(revenue.get("revenue_yoy")),
                    source_endpoint="twse.revenue" if market == "TWSE" else "tpex.revenue",
                    source_url=revenue_url,
                    source_payload_sha256=getattr(provider, "_revenue_payload_hash", None),
                    source_payload_id=(getattr(provider, "_revenue_payload_ids", {}) or {}).get(market),
                    available_date=to_date,
                    availability_precision="retrieval_date",
                )
    except Exception as exc:
        warnings.append(f"security master write failed: {exc}")

    period_bars = {"status": "not_selected"}
    if "period_bars" in selected_datasets:
        period_bars = rebuild_period_bars(database_path, symbols=list(symbols))
    adjusted_bars = {"status": "not_selected"}
    if "adjusted_bars" in selected_datasets:
        adjusted_bars = rebuild_adjusted_bars(database_path, symbols=list(symbols))
    diagnostics = provider.get_market_data_diagnostics()
    source_warnings = list(warnings)
    if int(diagnostics.get("redirect_308_unresolved_count", 0) or 0):
        source_warnings.append(
            "unresolved HTTP 308: "
            + str(diagnostics.get("redirect_308_unresolved_count"))
        )
    status = "complete" if not failures and taiex.get("status") in {"verified", "not_selected"} else "failed"
    finished_at = datetime.now().astimezone().isoformat()
    payload: dict[str, Any] = {
        "run_id": run_id,
        "status": status,
        "as_of": as_of.isoformat(),
        "expected_trade_date": to_date.isoformat() if profile == "daily" and to_date.weekday() < 5 else None,
        "themes": themes,
        "universe_mode": universe_mode,
        "lookback": max(lookback, 253),
        "mode": mode,
        "profile": profile,
        "datasets": selected_datasets,
        "dry_run": False,
        "from_date": from_date.isoformat() if from_date else None,
        "to_date": to_date.isoformat(),
        "database_path": str(database_path),
        "coverage_count": len(symbols),
        "daily_success_count": daily_success_count,
        "daily_failure_count": len(failures),
        "latest_dates": latest_dates,
        "failures": failures,
        "taiex": taiex,
        "period_bars": period_bars,
        "adjusted_bars": adjusted_bars,
        "enrichment": enrichment_results,
        "warnings": warnings,
        "source_warnings": source_warnings,
        "missing_partitions": [
            {"market": item.get("market"), "symbol": item.get("symbol"), "reason": item.get("reason")}
            for item in failures
        ],
        "market_data_diagnostics": diagnostics,
        "network_requests": int(diagnostics.get("request_count", 0) or 0),
        "db_hits": int(diagnostics.get("db_hit_count", 0) or 0),
        "fallbacks": int(diagnostics.get("fallback_success_count", 0) or 0),
        "integrity": database_integrity(database_path),
        "source_policy": "official or auditable public source; unknown payloads remain raw/quarantine",
        "research_only": True,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    sync_issues = [
        {
            "dataset_key": str(failure.get("dataset_key") or "daily_bars"),
            **failure,
            "detail": failure.get("reason"),
            "issue_type": (
                "enrichment_partition_failure"
                if failure.get("dataset_key")
                else "current_day_or_history_failure"
            ),
        }
        for failure in failures
    ] + (
        [{"dataset_key": "index_bars", "issue_type": "benchmark_failure", "detail": taiex.get("reason")}]
        if "index_bars" in selected_datasets and taiex.get("status") != "verified"
        else []
    )
    record_sync_run(
        database_path,
        run_id=run_id,
        as_of_date=as_of,
        themes=themes,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        summary=payload,
        issues=sync_issues,
    )
    payload["integrity"] = database_integrity(database_path)
    json_path = audit_dir / f"market-sync-{log_date}.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    markdown_path = audit_dir / f"market-sync-{log_date}.md"
    _write_markdown(markdown_path, payload)
    return {"json": json_path, "markdown": markdown_path}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步 curated coverage 市場資料到 canonical SQLite")
    parser.add_argument("--themes", default="AI,半導體", help="逗號分隔主題")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="截止日 YYYY-MM-DD")
    parser.add_argument("--universe-mode", choices=["coverage", "core", "broad"], default="coverage")
    parser.add_argument("--lookback", type=int, default=253)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--datasets",
        default=None,
        help="逗號分隔資料集；省略時依 --profile 選 daily 或 enrichment 預設集合",
    )
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--profile", choices=["daily", "enrichment"], default="daily")
    parser.add_argument("--from-date", default=None, help="實際同步起日 YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, help="實際同步迄日 YYYY-MM-DD；daily profile 必須等於 --as-of")
    parser.add_argument("--dry-run", action="store_true", help="只計算同步範圍，不讀網路、不改 SQLite")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=None, help="canonical market_data.sqlite 路徑")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        outputs = run(
            themes=[item.strip() for item in str(args.themes).split(",") if item.strip()],
            as_of=_as_of(args.as_of),
            universe_mode=args.universe_mode,
            lookback=args.lookback,
            timeout=args.timeout,
            output_root=Path(args.output_root),
            database_path=Path(args.database) if args.database else None,
            datasets=(
                [item.strip() for item in str(args.datasets).split(",") if item.strip()]
                if args.datasets
                else None
            ),
            mode=args.mode,
            from_date=_as_of(args.from_date) if args.from_date else None,
            to_date=_as_of(args.to_date) if args.to_date else None,
            dry_run=args.dry_run,
            profile=args.profile,
        )
        for key, path in outputs.items():
            print(f"[sync-market-data] {key}: {path}")
        payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
        return 0 if payload.get("status") in {"complete", "planned"} else 1
    except Exception as exc:
        print(f"[sync-market-data] error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
