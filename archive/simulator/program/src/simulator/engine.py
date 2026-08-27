from __future__ import annotations

import json
import shutil
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from src.providers.tw_market_provider import TwMarketProvider
from src.analysis.portfolio_metrics import calculate_portfolio_diagnostics
from src.simulator.broker import BrokerConfig, execute_orders, portfolio_value, release_settlements
from src.simulator.dashboard import render_dashboard
from src.simulator.policies import POLICIES, generate_policy_orders, make_portfolios
from src.simulator.store import SimulationStore, write_daily_equity_csv


ScreenerRunner = Callable[..., dict[str, Path]]


@dataclass
class SimulatorConfig:
    themes: list[str]
    theme_mode: str | None
    start_date: date
    end_date: date
    initial_cash: float
    top_n: int
    recommendation_mode: str
    analysis_cache: str
    output_root: Path
    run_id: str
    mode: str = "historical-plus-daily"
    universe_mode: str | None = "coverage"
    universe_limit: int = 80
    lookback: int = 252
    timeout: float = 10.0
    commission_bps: float = 14.25
    sell_tax_bps: float = 30.0
    min_commission: float = 20.0
    lot_size: int = 1
    daily_analysis_mode: str = "prior-close"


def run_simulation(
    config: SimulatorConfig,
    screener_runner: ScreenerRunner | None = None,
    provider: TwMarketProvider | None = None,
) -> dict[str, Any]:
    from scripts.tw_sector_screener import run as default_screener_runner

    runner = screener_runner or default_screener_runner
    run_dir = config.output_root / "simulations" / config.run_id
    analysis_dir = run_dir / "analysis"
    orders_dir = run_dir / "orders"
    run_dir.mkdir(parents=True, exist_ok=True)
    orders_dir.mkdir(parents=True, exist_ok=True)
    broker_config = BrokerConfig(
        commission_bps=config.commission_bps,
        sell_tax_bps=config.sell_tax_bps,
        min_commission=config.min_commission,
        lot_size=config.lot_size,
    )
    market_provider = provider or TwMarketProvider(timeout=config.timeout, cache_dir=config.output_root / "cache" / "market")
    store = SimulationStore(run_dir / "simulator.sqlite")
    all_orders: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    daily_equity: list[dict[str, Any]] = []
    latest_analysis: list[dict[str, Any]] = []
    market_status: dict[str, Any] = {"is_trading_day": True, "note": "", "source": "unknown", "warnings": []}
    peaks = {portfolio_id: config.initial_cash for portfolio_id in POLICIES}
    try:
        store.save_run(
            config.run_id,
            {
                "themes": config.themes,
                "mode": config.mode,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "initial_cash": config.initial_cash,
                "top_n": config.top_n,
                "recommendation_mode": config.recommendation_mode,
                "universe_mode": config.universe_mode,
            },
        )
        portfolios = store.latest_states(config.run_id) if config.mode == "daily" else {}
        if not portfolios:
            portfolios = make_portfolios(config.initial_cash)

        trading_dates, market_status = _trading_dates(market_provider, config.start_date, config.end_date)
        same_day_snapshot: tuple[Path, list[dict[str, Any]], Path] | None = None
        if config.mode == "daily" and config.daily_analysis_mode == "same-day" and config.end_date in trading_dates:
            same_day_snapshot = load_or_build_snapshot(config, config.end_date, runner, analysis_dir)
        pairs = _analysis_execution_pairs(trading_dates, config.start_date)
        if config.mode == "daily" and pairs:
            portfolios = store.latest_states_before(config.run_id, pairs[0][1].isoformat()) or make_portfolios(config.initial_cash)
        if not pairs and config.mode == "daily":
            market_status = _daily_market_status(config, trading_dates, market_status)
            daily_equity = _carry_forward_daily_equity(config, store, portfolios, peaks, market_status)
            store.commit()
        for analysis_date, execution_date in pairs:
            snapshot_path, rows, analysis_manifest = load_or_build_snapshot(config, analysis_date, runner, analysis_dir)
            latest_analysis = rows
            execution_tag = execution_date.isoformat()
            if analysis_manifest:
                _update_analysis_manifest_execution(analysis_manifest, execution_date, "built" if analysis_manifest.exists() else "unknown")
            store.save_analysis_ref(config.run_id, analysis_date.isoformat(), execution_tag, snapshot_path, len(rows))
            if config.mode == "daily":
                store.clear_execution_activity(config.run_id, execution_tag)
            candles = _execution_candles(market_provider, rows, execution_date, allow_prior_fallback=config.mode != "daily")
            if same_day_snapshot and execution_date == config.end_date:
                _supplement_candles_from_same_day_analysis(candles, same_day_snapshot[1], execution_date, market_status)
            _record_execution_data_status(market_status, rows, candles, execution_date)
            settlement_dates = _settlement_dates(trading_dates, execution_date)
            day_orders: list[dict[str, Any]] = []
            day_trades: list[dict[str, Any]] = []
            price_map = _close_price_map(rows, candles)

            for portfolio_id, spec in POLICIES.items():
                portfolio = portfolios[portfolio_id]
                release_settlements(portfolio, execution_tag)
                orders = generate_policy_orders(portfolio, spec, rows, execution_tag, broker_config)
                if config.mode == "daily":
                    _mark_daily_buy_orders_as_market(orders)
                final_orders, trades = execute_orders(portfolio, orders, candles, settlement_dates, broker_config)
                metrics = portfolio_value(portfolio, price_map)
                peaks[portfolio_id] = max(peaks.get(portfolio_id, config.initial_cash), metrics["equity"])
                store.save_orders(config.run_id, execution_tag, final_orders)
                store.save_trades(config.run_id, execution_tag, trades)
                store.save_state(config.run_id, execution_tag, portfolio, metrics)
                store.save_daily_equity(config.run_id, execution_tag, portfolio_id, metrics, config.initial_cash, peaks[portfolio_id])
                day_orders.extend(final_orders)
                day_trades.extend(trades)
                daily_equity.append(
                    {
                        "trade_date": execution_tag,
                        "portfolio_id": portfolio_id,
                        "equity": metrics["equity"],
                        "cash": metrics["cash"],
                        "unsettled_cash": metrics["unsettled_cash"],
                        "holdings_value": metrics["holdings_value"],
                        "return_pct": round(((metrics["equity"] / config.initial_cash) - 1.0) * 100.0, 4),
                        "drawdown_pct": round(((metrics["equity"] / peaks[portfolio_id]) - 1.0) * 100.0, 4),
                    }
                )
            (orders_dir / f"{execution_date.strftime('%Y%m%d')}.json").write_text(json.dumps(day_orders, ensure_ascii=False, indent=2), encoding="utf-8")
            all_orders.extend(day_orders)
            all_trades.extend(day_trades)
            store.commit()

        planned_orders: list[dict[str, Any]] = []
        if same_day_snapshot:
            report_snapshot_path, report_rows, report_manifest = same_day_snapshot
            latest_analysis = report_rows
            next_execution_date = _next_execution_date(trading_dates, config.end_date)
            _update_analysis_manifest_execution(report_manifest, next_execution_date, "built" if report_manifest.exists() else "unknown")
            store.save_analysis_ref(config.run_id, config.end_date.isoformat(), next_execution_date.isoformat(), report_snapshot_path, len(report_rows))
            planned_orders = _planned_orders_for_next_execution(portfolios, report_rows, next_execution_date, broker_config)
            if planned_orders:
                (orders_dir / f"{next_execution_date.strftime('%Y%m%d')}.planned.json").write_text(
                    json.dumps(planned_orders, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

        if config.mode == "daily":
            daily_equity = store.daily_equity_rows(config.run_id)
        elif not daily_equity:
            daily_equity = store.daily_equity_rows(config.run_id)
        latest_manifest = _latest_analysis_manifest(analysis_dir, config.end_date)
        if latest_manifest and not pairs and config.mode == "daily":
            latest_manifest = _write_carry_forward_manifest(analysis_dir, latest_manifest, config.end_date)
        summary = _summary_payload(config, portfolios, daily_equity, all_orders, all_trades, latest_analysis, market_status)
        if config.mode == "daily":
            summary["execution_analysis_date"] = pairs[-1][0].isoformat() if pairs else None
            summary["execution_date"] = pairs[-1][1].isoformat() if pairs else config.end_date.isoformat()
            summary["report_analysis_date"] = config.end_date.isoformat() if same_day_snapshot else summary["execution_analysis_date"]
            summary["planned_execution_date"] = (
                planned_orders[0].get("date") if planned_orders else _next_execution_date(trading_dates, config.end_date).isoformat()
            )
        if latest_manifest:
            summary["analysis_manifest"] = str(latest_manifest)
            summary["analysis_reports"] = [str(path) for path in _analysis_report_paths_from_manifest(latest_manifest)]
            dashboard_lists = _dashboard_lists_from_manifest(latest_manifest)
            summary["buying_ranking"] = dashboard_lists["buying_ranking"]
            summary["actionable_queue"] = dashboard_lists["actionable_queue"]
        summary["planned_orders"] = planned_orders
        summary["planned_order_count"] = len(planned_orders)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        write_daily_equity_csv(run_dir / "daily-equity.csv", daily_equity)
        dashboard_path = render_dashboard(run_dir / "dashboard.html", summary)
        outputs: dict[str, Any] = {
            "sqlite": run_dir / "simulator.sqlite",
            "dashboard": dashboard_path,
            "summary": summary_path,
            "daily_equity": run_dir / "daily-equity.csv",
        }
        if latest_manifest:
            outputs["analysis_manifest"] = latest_manifest
            outputs["analysis_reports"] = _analysis_report_paths_from_manifest(latest_manifest)
        return outputs
    finally:
        store.close()


def load_or_build_snapshot(
    config: SimulatorConfig,
    analysis_date: date,
    runner: ScreenerRunner,
    analysis_dir: Path,
) -> tuple[Path, list[dict[str, Any]], Path]:
    date_tag = analysis_date.strftime("%Y%m%d")
    day_dir = analysis_dir / date_tag
    path = day_dir / "merged-top30.json"
    manifest_path = day_dir / "daily-analysis-manifest.json"
    if config.analysis_cache == "reuse" and path.exists():
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = list(payload.get("rows") or [])
        if _manifest_reports_exist(manifest_path):
            _set_manifest_cache_status(manifest_path, "reused")
            return path, rows, manifest_path
        _ensure_theme_reports(config, analysis_date, runner, day_dir, cache_status="rebuilt_missing_reports")
        _write_analysis_manifest(config, analysis_date, None, day_dir, cache_status="rebuilt_missing_reports")
        return path, rows, manifest_path
    merged: dict[str, dict[str, Any]] = {}
    for theme in config.themes:
        outputs = runner(
            theme=theme,
            as_of=analysis_date,
            top_n=config.top_n,
            universe_limit=config.universe_limit,
            min_monthly_revenue=0.0,
            lookback=config.lookback,
            timeout=config.timeout,
            output_root=config.output_root,
            theme_mode=config.theme_mode,
            universe_mode=config.universe_mode,
            benchmark="TAIEX",
            output_formats={"json", "md"},
            config_path=None,
            coverage_list_path=None,
            run_backtest=False,
            rebalance="monthly",
            cost_bps=10.0,
            validation_window="1y",
            quality_update_mode="auto",
            quality_update_budget_sec=3.0,
            quality_history_depth=8,
            recommendation_mode=config.recommendation_mode,
            review_top_n=min(config.top_n, 8),
        )
        _copy_theme_report_outputs(theme, analysis_date, outputs, day_dir, cache_status="built")
        payload = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
        for row in payload.get("picks") or []:
            symbol = str(row.get("symbol"))
            candidate = {k: v for k, v in row.items() if k != "_candles"}
            metrics = candidate.get("stock_risk_metrics") or {}
            candidate["risk_adjusted_score"] = metrics.get("risk_adjusted_score", candidate.get("risk_adjusted_score"))
            candidate["sharpe_ratio"] = metrics.get("sharpe_ratio")
            candidate["sortino_ratio"] = metrics.get("sortino_ratio")
            candidate["max_drawdown_pct"] = metrics.get("max_drawdown_pct")
            candidate["annualized_volatility_pct"] = metrics.get("annualized_volatility_pct")
            candidate.setdefault("source_themes", [])
            candidate["source_themes"] = sorted(set(candidate["source_themes"] + [theme]))
            current = merged.get(symbol)
            if current is None or float(candidate.get("idea_score") or 0.0) > float(current.get("idea_score") or 0.0):
                merged[symbol] = candidate
            elif current is not None:
                current["source_themes"] = sorted(set((current.get("source_themes") or []) + [theme]))
    rows = sorted(merged.values(), key=lambda row: (float(row.get("rank_score") or 0.0), float(row.get("idea_score") or 0.0)), reverse=True)[: config.top_n]
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"analysis_date": analysis_date.isoformat(), "themes": config.themes, "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_analysis_manifest(config, analysis_date, None, day_dir, cache_status="built")
    return path, rows, manifest_path


def _ensure_theme_reports(
    config: SimulatorConfig,
    analysis_date: date,
    runner: ScreenerRunner,
    day_dir: Path,
    cache_status: str,
) -> None:
    for theme in config.themes:
        report_path = _local_theme_report_path(day_dir, theme, analysis_date)
        if report_path.exists():
            continue
        outputs = runner(
            theme=theme,
            as_of=analysis_date,
            top_n=config.top_n,
            universe_limit=config.universe_limit,
            min_monthly_revenue=0.0,
            lookback=config.lookback,
            timeout=config.timeout,
            output_root=config.output_root,
            theme_mode=config.theme_mode,
            universe_mode=config.universe_mode,
            benchmark="TAIEX",
            output_formats={"json", "md"},
            config_path=None,
            coverage_list_path=None,
            run_backtest=False,
            rebalance="monthly",
            cost_bps=10.0,
            validation_window="1y",
            quality_update_mode="auto",
            quality_update_budget_sec=3.0,
            quality_history_depth=8,
            recommendation_mode=config.recommendation_mode,
            review_top_n=min(config.top_n, 8),
        )
        _copy_theme_report_outputs(theme, analysis_date, outputs, day_dir, cache_status=cache_status)


def _copy_theme_report_outputs(
    theme: str,
    analysis_date: date,
    outputs: dict[str, Path],
    day_dir: Path,
    cache_status: str,
) -> None:
    theme_dir = day_dir / theme
    theme_dir.mkdir(parents=True, exist_ok=True)
    md_path = outputs.get("md")
    json_path = outputs.get("json")
    if md_path:
        shutil.copyfile(md_path, _local_theme_report_path(day_dir, theme, analysis_date))
    if json_path:
        shutil.copyfile(json_path, theme_dir / f"sector-report-{theme}-{analysis_date.strftime('%Y%m%d')}.json")
    meta_path = theme_dir / "source.json"
    meta_path.write_text(
        json.dumps(
            {
                "theme": theme,
                "analysis_date": analysis_date.isoformat(),
                "cache_status": cache_status,
                "source_md": str(md_path) if md_path else None,
                "source_json": str(json_path) if json_path else None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_analysis_manifest(
    config: SimulatorConfig,
    analysis_date: date,
    execution_date: date | None,
    day_dir: Path,
    cache_status: str,
) -> Path:
    manifest_path = day_dir / "daily-analysis-manifest.json"
    reports = []
    for theme in config.themes:
        theme_dir = day_dir / theme
        source_path = theme_dir / "source.json"
        source_payload = {}
        if source_path.exists():
            source_payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
        reports.append(
            {
                "theme": theme,
                "report_md": str(_local_theme_report_path(day_dir, theme, analysis_date)),
                "report_json": str(theme_dir / f"sector-report-{theme}-{analysis_date.strftime('%Y%m%d')}.json"),
                "source_md": source_payload.get("source_md"),
                "source_json": source_payload.get("source_json"),
            }
        )
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": config.run_id,
                "analysis_date": analysis_date.isoformat(),
                "execution_date": execution_date.isoformat() if execution_date else None,
                "themes": config.themes,
                "cache_status": cache_status,
                "reports": reports,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest_path


def _update_analysis_manifest_execution(manifest_path: Path, execution_date: date, default_cache_status: str) -> None:
    payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    payload["execution_date"] = execution_date.isoformat()
    payload.setdefault("cache_status", default_cache_status)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_carry_forward_manifest(analysis_dir: Path, source_manifest_path: Path, as_of: date) -> Path:
    payload = json.loads(source_manifest_path.read_text(encoding="utf-8-sig"))
    payload["source_manifest"] = str(source_manifest_path)
    payload["carry_forward_as_of"] = as_of.isoformat()
    payload["cache_status"] = "carry_forward"
    day_dir = analysis_dir / as_of.strftime("%Y%m%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = day_dir / "daily-analysis-manifest.json"
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def _set_manifest_cache_status(manifest_path: Path, cache_status: str) -> None:
    payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    payload["cache_status"] = cache_status
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _latest_analysis_manifest(analysis_dir: Path, as_of: date) -> Path | None:
    candidates = []
    for path in analysis_dir.glob("*/daily-analysis-manifest.json"):
        try:
            day = datetime.strptime(path.parent.name, "%Y%m%d").date()
        except ValueError:
            continue
        if day <= as_of:
            candidates.append((day, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _analysis_report_paths_from_manifest(manifest_path: Path) -> list[Path]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    paths = []
    for item in payload.get("reports") or []:
        report = item.get("report_md")
        if report:
            paths.append(Path(report))
    return paths


def _dashboard_lists_from_manifest(manifest_path: Path) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    buying: list[dict[str, Any]] = []
    actionable: list[dict[str, Any]] = []
    for item in payload.get("reports") or []:
        theme = str(item.get("theme") or "")
        report_json = item.get("report_json")
        if not report_json:
            continue
        path = Path(report_json)
        if not path.exists():
            continue
        report = json.loads(path.read_text(encoding="utf-8-sig"))
        buying.extend(_dashboard_list_rows(theme, report.get("buying_ranking") or []))
        actionable.extend(_dashboard_list_rows(theme, report.get("actionable_queue") or []))
    return {
        "buying_ranking": sorted(buying, key=lambda row: (row.get("theme"), int(row.get("list_rank") or row.get("rank") or 999))),
        "actionable_queue": sorted(actionable, key=lambda row: (row.get("theme"), int(row.get("list_rank") or row.get("rank") or 999))),
    }


def _dashboard_list_rows(theme: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**_dashboard_row(row), "theme": theme, "list_rank": row.get("list_rank"), "next_action": row.get("next_action"), "trigger_to_upgrade": row.get("trigger_to_upgrade")} for row in rows]


def _manifest_reports_exist(manifest_path: Path) -> bool:
    if not manifest_path.exists():
        return False
    paths = _analysis_report_paths_from_manifest(manifest_path)
    return bool(paths) and all(path.exists() for path in paths)


def _local_theme_report_path(day_dir: Path, theme: str, analysis_date: date) -> Path:
    return day_dir / theme / f"sector-report-{theme}-{analysis_date.strftime('%Y%m%d')}.md"


def _trading_dates(provider: TwMarketProvider, start_date: date, end_date: date) -> tuple[list[date], dict[str, Any]]:
    lookback = max((end_date - start_date).days + 20, 30)
    status: dict[str, Any] = {
        "is_trading_day": True,
        "as_of": end_date.isoformat(),
        "source": "TWSE FMTQIK",
        "note": "",
        "warnings": [],
    }
    try:
        series = provider.get_taiex_series(as_of=end_date, lookback=lookback)
        dates = sorted({item["date"] for item in series if start_date - timedelta(days=14) <= item["date"] <= end_date})
        dates, fallback_dates = _add_cross_checked_weekdays_after_stale_series(provider, dates, end_date)
        if fallback_dates:
            status["source"] = "TWSE FMTQIK + OHLCV cross-check"
            status["warnings"].append(
                "TAIEX FMTQIK ended before as_of; appended cross-checked weekday trading candidates."
            )
            status["fallback_dates"] = [item.isoformat() for item in fallback_dates]
        status["is_trading_day"] = end_date in dates
        if len(dates) >= 2:
            return dates, status
    except Exception as exc:
        status["source"] = "calendar_weekday_fallback"
        status["warnings"].append(f"TAIEX FMTQIK fetch failed; using weekday fallback: {exc}")
    cursor = start_date - timedelta(days=10)
    dates: list[date] = []
    while cursor <= end_date:
        if _is_calendar_trading_candidate(cursor):
            dates.append(cursor)
        cursor += timedelta(days=1)
    status["is_trading_day"] = end_date in dates
    return dates, status


def _add_cross_checked_weekdays_after_stale_series(
    provider: TwMarketProvider,
    dates: list[date],
    end_date: date,
) -> tuple[list[date], list[date]]:
    if not dates:
        return dates, []
    latest = max(dates)
    cursor = latest + timedelta(days=1)
    augmented = set(dates)
    fallback_dates: list[date] = []
    while cursor <= end_date:
        if _is_calendar_trading_candidate(cursor) and _has_market_ohlcv_on_date(provider, cursor):
            augmented.add(cursor)
            fallback_dates.append(cursor)
        cursor += timedelta(days=1)
    return sorted(augmented), fallback_dates


def _has_market_ohlcv_on_date(provider: TwMarketProvider, day: date) -> bool:
    probes = [("2330", "TWSE"), ("6488", "TPEx")]
    for symbol, market in probes:
        try:
            series = provider.get_ohlcv(symbol, market, as_of=day, lookback=5)
        except Exception:
            continue
        if any(_as_date(item.get("date")) == day for item in series):
            return True
    return False


def _is_calendar_trading_candidate(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    if day in _KNOWN_TW_MARKET_CLOSED_DATES:
        return False
    return True


_KNOWN_TW_MARKET_CLOSED_DATES = {
    date(2026, 5, 1),
}


def _analysis_execution_pairs(trading_dates: list[date], start_date: date) -> list[tuple[date, date]]:
    pairs: list[tuple[date, date]] = []
    for idx in range(1, len(trading_dates)):
        analysis_date = trading_dates[idx - 1]
        execution_date = trading_dates[idx]
        if execution_date >= start_date:
            pairs.append((analysis_date, execution_date))
    return pairs


def _execution_candles(
    provider: TwMarketProvider,
    rows: list[dict[str, Any]],
    execution_date: date,
    allow_prior_fallback: bool = True,
) -> dict[str, dict[str, Any]]:
    candles: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol"))
        try:
            series = provider.get_ohlcv(symbol, str(row.get("market") or "TWSE"), as_of=execution_date, lookback=10)
        except Exception:
            continue
        exact = [item for item in series if _as_date(item.get("date")) == execution_date]
        if exact:
            candles[symbol] = exact[-1]
        elif allow_prior_fallback:
            prior = [item for item in series if _as_date(item.get("date")) and _as_date(item.get("date")) <= execution_date]
            if prior:
                candles[symbol] = prior[-1]
    return candles


def _mark_daily_buy_orders_as_market(orders: list[dict[str, Any]]) -> None:
    for order in orders:
        if order.get("side") == "buy":
            order["order_type"] = "market"
            order["market_order_note"] = "daily automation executes prior after-close buy decisions at next session open"


def _planned_orders_for_next_execution(
    portfolios: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    execution_date: date,
    broker_config: BrokerConfig,
) -> list[dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    execution_tag = execution_date.isoformat()
    for portfolio_id, spec in POLICIES.items():
        portfolio = deepcopy(portfolios[portfolio_id])
        orders = generate_policy_orders(portfolio, spec, rows, execution_tag, broker_config)
        _mark_daily_buy_orders_as_market(orders)
        for order in orders:
            order["status"] = "planned"
            order["planned_from_analysis_date"] = rows[0].get("as_of") if rows else None
        planned.extend(orders)
    return planned


def _supplement_candles_from_same_day_analysis(
    candles: dict[str, dict[str, Any]],
    same_day_rows: list[dict[str, Any]],
    execution_date: date,
    market_status: dict[str, Any],
) -> None:
    added: list[str] = []
    for row in same_day_rows:
        symbol = str(row.get("symbol"))
        if not symbol or symbol in candles:
            continue
        close = float(row.get("close") or row.get("close_price") or 0.0)
        if close <= 0:
            continue
        candles[symbol] = {
            "date": execution_date,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 0.0,
            "source": "same_day_analysis_close_proxy",
        }
        added.append(symbol)
    if added:
        market_status.setdefault("warnings", []).append(
            f"Used same-day analysis close proxy for {len(added)} execution symbols because exact OHLCV was unavailable."
        )
        market_status["execution_price_proxy"] = {
            "execution_date": execution_date.isoformat(),
            "source": "same_day_analysis_close_proxy",
            "count": len(added),
            "symbols": added[:30],
        }


def _next_execution_date(trading_dates: list[date], analysis_date: date) -> date:
    future = [item for item in trading_dates if item > analysis_date]
    if future:
        return future[0]
    cursor = analysis_date + timedelta(days=1)
    while not _is_calendar_trading_candidate(cursor):
        cursor += timedelta(days=1)
    return cursor


def _record_execution_data_status(
    market_status: dict[str, Any],
    rows: list[dict[str, Any]],
    candles: dict[str, dict[str, Any]],
    execution_date: date,
) -> None:
    symbols = [str(row.get("symbol")) for row in rows if row.get("symbol")]
    missing = [symbol for symbol in symbols if symbol not in candles]
    if not missing:
        return
    market_status.setdefault("warnings", []).append(
        f"Missing exact execution OHLCV for {execution_date.isoformat()} on {len(missing)} analysis symbols; affected orders are marked market_data_missing."
    )
    market_status["missing_execution_ohlcv"] = {
        "execution_date": execution_date.isoformat(),
        "count": len(missing),
        "symbols": missing[:30],
    }


def _settlement_dates(trading_dates: list[date], execution_date: date) -> dict[str, str]:
    try:
        idx = trading_dates.index(execution_date)
    except ValueError:
        return {execution_date.isoformat(): (execution_date + timedelta(days=2)).isoformat()}
    settle_idx = min(idx + 2, len(trading_dates) - 1)
    return {execution_date.isoformat(): trading_dates[settle_idx].isoformat()}


def _daily_market_status(config: SimulatorConfig, trading_dates: list[date], base_status: dict[str, Any] | None = None) -> dict[str, Any]:
    last_trading_date = max([item for item in trading_dates if item <= config.end_date], default=None)
    note = f"{config.end_date.isoformat()} 不是交易日"
    if config.end_date == date(2026, 5, 1):
        note = "2026-05-01 勞動節休市"
    elif last_trading_date:
        note = f"{note}，沿用 {last_trading_date.isoformat()} 收盤後狀態"
    status = dict(base_status or {})
    status.update({
        "is_trading_day": False,
        "as_of": config.end_date.isoformat(),
        "last_trading_date": last_trading_date.isoformat() if last_trading_date else None,
        "note": note,
    })
    status.setdefault("source", "unknown")
    status.setdefault("warnings", [])
    return status


def _carry_forward_daily_equity(
    config: SimulatorConfig,
    store: SimulationStore,
    portfolios: dict[str, dict[str, Any]],
    peaks: dict[str, float],
    market_status: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trade_date = config.end_date.isoformat()
    for portfolio_id, portfolio in portfolios.items():
        metrics = portfolio_value(portfolio, {})
        peaks[portfolio_id] = max(peaks.get(portfolio_id, config.initial_cash), metrics["equity"])
        store.save_state(config.run_id, trade_date, portfolio, metrics)
        store.save_daily_equity(config.run_id, trade_date, portfolio_id, metrics, config.initial_cash, peaks[portfolio_id])
        rows.append(
            {
                "trade_date": trade_date,
                "portfolio_id": portfolio_id,
                "equity": metrics["equity"],
                "cash": metrics["cash"],
                "unsettled_cash": metrics["unsettled_cash"],
                "holdings_value": metrics["holdings_value"],
                "return_pct": round(((metrics["equity"] / config.initial_cash) - 1.0) * 100.0, 4),
                "drawdown_pct": round(((metrics["equity"] / peaks[portfolio_id]) - 1.0) * 100.0, 4),
                "market_status": market_status.get("note"),
            }
        )
    return rows


def _close_price_map(rows: list[dict[str, Any]], candles: dict[str, dict[str, Any]]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("symbol"))
        candle = candles.get(symbol)
        prices[symbol] = float((candle or {}).get("close") or row.get("close") or row.get("close_price") or 0.0)
    return prices


def _summary_payload(
    config: SimulatorConfig,
    portfolios: dict[str, dict[str, Any]],
    daily_equity: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    latest_analysis: list[dict[str, Any]],
    market_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    for portfolio_id, portfolio in portfolios.items():
        rows = [row for row in daily_equity if row["portfolio_id"] == portfolio_id]
        last = rows[-1] if rows else {"equity": config.initial_cash, "cash": config.initial_cash, "holdings_value": 0.0, "return_pct": 0.0}
        max_drawdown = min([row.get("drawdown_pct", 0.0) for row in rows] or [0.0])
        portfolio_trades = [trade for trade in trades if trade.get("portfolio_id") == portfolio_id]
        sell_wins = 0
        sell_count = 0
        for trade in portfolio_trades:
            if trade.get("side") == "sell":
                sell_count += 1
                ref_close = float((trade.get("analysis_ref") or {}).get("close") or trade.get("fill_price") or 0.0)
                if float(trade.get("fill_price") or 0.0) >= ref_close:
                    sell_wins += 1
        summaries.append(
            {
                "portfolio_id": portfolio_id,
                "name": portfolio.get("name"),
                "equity": round(float(last.get("equity") or 0.0), 2),
                "cash": round(float(last.get("cash") or 0.0), 2),
                "holdings_value": round(float(last.get("holdings_value") or 0.0), 2),
                "return_pct": round(float(last.get("return_pct") or 0.0), 2),
                "max_drawdown_pct": round(float(max_drawdown), 2),
                "trade_count": len(portfolio_trades),
                "win_rate": round(sell_wins / sell_count, 4) if sell_count else 0.0,
                "turnover_count": len([order for order in orders if order.get("portfolio_id") == portfolio_id and order.get("status") == "filled"]),
                "portfolio_diagnostics": calculate_portfolio_diagnostics(
                    [{"date": row.get("trade_date"), "equity": row.get("equity")} for row in rows]
                ),
            }
        )
        latest_map = {str(row.get("symbol")): row for row in latest_analysis}
        for symbol, position in (portfolio.get("positions") or {}).items():
            row = latest_map.get(symbol, {})
            avg_cost = float(position.get("avg_cost") or 0.0)
            last_price = float(position.get("last_price") or avg_cost)
            positions.append(
                {
                    "portfolio_id": portfolio_id,
                    "symbol": symbol,
                    "name": position.get("name"),
                    "quantity": position.get("quantity"),
                    "avg_cost": round(avg_cost, 2),
                    "last_price": round(last_price, 2),
                    "unrealized_pct": round(((last_price / avg_cost) - 1.0) * 100.0, 2) if avg_cost else 0.0,
                    "recommendation": row.get("recommendation"),
                    "risk_score": row.get("risk_score"),
                }
            )
    return {
        "run_id": config.run_id,
        "themes": config.themes,
        "mode": config.mode,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "initial_cash": config.initial_cash,
        "portfolio_summaries": summaries,
        "daily_equity": daily_equity,
        "positions": positions,
        "orders": orders,
        "trade_count": len(trades),
        "policy_violation_count": len([order for order in orders if order.get("policy_violation")]),
        "latest_analysis": [_dashboard_row(row) for row in latest_analysis],
        "market_status": market_status or {"is_trading_day": True, "note": "", "source": "unknown", "warnings": []},
    }


def _dashboard_row(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row.get("stock_risk_metrics") or {}
    return {
        "rank": row.get("rank"),
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "idea_score": row.get("idea_score"),
        "buyability_score": row.get("buyability_score"),
        "buying_tier": row.get("buying_tier"),
        "decision_tier": row.get("decision_tier"),
        "actionability_score": row.get("actionability_score"),
        "stock_risk_metrics": metrics,
        "risk_adjusted_score": row.get("risk_adjusted_score", metrics.get("risk_adjusted_score")),
        "sharpe_ratio": row.get("sharpe_ratio", metrics.get("sharpe_ratio")),
        "sortino_ratio": row.get("sortino_ratio", metrics.get("sortino_ratio")),
        "max_drawdown_pct": row.get("max_drawdown_pct", metrics.get("max_drawdown_pct")),
        "annualized_volatility_pct": row.get("annualized_volatility_pct", metrics.get("annualized_volatility_pct")),
        "recommendation": row.get("recommendation"),
        "risk_score": row.get("risk_score"),
        "source_themes": ",".join(row.get("source_themes") or []),
    }


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def clone_portfolios(portfolios: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return deepcopy(portfolios)
