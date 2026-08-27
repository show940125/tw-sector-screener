from __future__ import annotations

"""Synchronise the curated coverage market data into canonical SQLite.

This command is intentionally limited to canonical market-data
synchronisation and its audit manifest.  It does not invoke downstream
execution or publication actions.
"""

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.providers.market_data_store import (
    database_integrity,
    record_fetch_attempt,
    record_sync_item,
    record_sync_run,
    rebuild_adjusted_bars,
    rebuild_period_bars,
    upsert_dataset_sync_state,
    upsert_monthly_revenue,
    upsert_security_master,
    upsert_universe_membership,
)
from src.providers.market_data_adapters import dataset_specs, unimplemented_dataset_keys
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
    "quarterly_fundamentals",
    "annual_fundamentals",
    "corporate_actions",
    "institutional_flows",
    "margin_short_snapshots",
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
        lines.extend(f"- `{item.get('market')}:{item.get('symbol')}` — {item.get('reason')}" for item in failures)
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
                )
        if "monthly_revenue" in selected_datasets:
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
            "dataset_key": "daily_bars",
            **failure,
            "detail": failure.get("reason"),
            "issue_type": "current_day_or_history_failure",
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
