from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode

from src.providers.daily_bar_store import (
    StoreImportStats,
    VerifiedDailyBar,
    database_integrity,
    get_sync_state,
    import_verified_bars,
    record_import_run,
)
from src.providers.tw_market_provider import (
    TPEX_TRADING_STOCK_URL,
    TWSE_STOCK_DAY_PRIMARY_URL,
    TWSE_STOCK_DAY_URL,
    TwMarketProvider,
    _try_parse_roc_slash,
)
from src.themes import theme_rule


@dataclass(frozen=True)
class CacheRequestSpec:
    market: str
    symbol: str
    requested_month: date
    source_endpoint: str
    source_url: str
    request_body: str
    source_priority: int

    @property
    def cache_name(self) -> str:
        digest = hashlib.sha256(
            f"{self.source_url}|{self.request_body}".encode("utf-8")
        ).hexdigest()
        return f"{digest}.json"


def _month_cursor(value: str) -> date:
    raw = str(value).strip()
    if len(raw) == 7 and raw[4] == "-":
        return date.fromisoformat(f"{raw}-01")
    if len(raw) == 6 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:]), 1)
    raise ValueError(f"月份格式必須為 YYYY-MM 或 YYYYMM：{value}")


def _month_sequence(start_month: date, end_month: date) -> Iterable[date]:
    current = date(start_month.year, start_month.month, 1)
    end = date(end_month.year, end_month.month, 1)
    while current <= end:
        yield current
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def _cache_request_specs(symbol: str, market: str, month_start: date) -> list[CacheRequestSpec]:
    if market == "TWSE":
        query = urlencode(
            {
                "response": "json",
                "date": month_start.strftime("%Y%m01"),
                "stockNo": symbol,
            }
        )
        return [
            CacheRequestSpec(
                market=market,
                symbol=symbol,
                requested_month=month_start,
                source_endpoint="twse.stock_day.primary",
                source_url=f"{TWSE_STOCK_DAY_PRIMARY_URL}?{query}",
                request_body="",
                source_priority=10,
            ),
            CacheRequestSpec(
                market=market,
                symbol=symbol,
                requested_month=month_start,
                source_endpoint="twse.stock_day.fallback",
                source_url=f"{TWSE_STOCK_DAY_URL}?{query}",
                request_body="",
                source_priority=20,
            ),
        ]
    query = urlencode(
        {
            "code": symbol,
            "date": month_start.strftime("%Y/%m/01"),
            "response": "json",
        }
    )
    return [
        CacheRequestSpec(
            market=market,
            symbol=symbol,
            requested_month=month_start,
            source_endpoint="tpex.trading_stock.get",
            source_url=f"{TPEX_TRADING_STOCK_URL}?{query}",
            request_body="",
            source_priority=10,
        ),
        CacheRequestSpec(
            market=market,
            symbol=symbol,
            requested_month=month_start,
            source_endpoint="tpex.trading_stock.post",
            source_url=TPEX_TRADING_STOCK_URL,
            request_body=query,
            source_priority=20,
        ),
    ]


def _payload_rows(payload: Any, market: str) -> tuple[bool, list[Any], str]:
    if not isinstance(payload, dict):
        return False, [], "payload 不是 JSON object"
    if market == "TWSE":
        if payload.get("stat") != "OK":
            return False, [], f"TWSE stat 不符：{payload.get('stat')}"
        rows = payload.get("data")
        if not isinstance(rows, list):
            return False, [], "TWSE data 不是 list"
        return True, rows, ""
    if str(payload.get("stat") or "").lower() != "ok":
        return False, [], f"TPEx stat 不符：{payload.get('stat')}"
    tables = payload.get("tables")
    if not isinstance(tables, list) or not tables or not isinstance(tables[0], dict):
        return False, [], "TPEx tables 結構不符"
    rows = tables[0].get("data")
    if not isinstance(rows, list):
        return False, [], "TPEx table data 不是 list"
    return True, rows, ""


def _issue(
    *,
    cache_file: Path,
    spec: CacheRequestSpec,
    issue_type: str,
    detail: str,
) -> dict[str, Any]:
    return {
        "cache_file": str(cache_file),
        "market": spec.market,
        "symbol": spec.symbol,
        "requested_month": spec.requested_month.strftime("%Y-%m"),
        "issue_type": issue_type,
        "detail": detail,
    }


def _symbols_for_themes(themes: list[str]) -> list[str]:
    symbols: list[str] = []
    for theme in themes:
        for symbol in theme_rule(theme, universe_mode="coverage").get("coverage_symbols") or []:
            value = str(symbol).strip()
            if value and value not in symbols:
                symbols.append(value)
    return symbols


def collect_verified_cached_bars(
    cache_dir: Path,
    *,
    themes: list[str],
    start_month: date,
    end_month: date,
    max_trade_date: date | None = None,
) -> tuple[list[VerifiedDailyBar], StoreImportStats, list[dict[str, Any]], list[str]]:
    bars: list[VerifiedDailyBar] = []
    stats = StoreImportStats()
    issues: list[dict[str, Any]] = []
    seen_cache_files: set[Path] = set()
    symbols = _symbols_for_themes(themes)
    for symbol in symbols:
        for market in ("TWSE", "TPEx"):
            for month_start in _month_sequence(start_month, end_month):
                for spec in _cache_request_specs(symbol, market, month_start):
                    cache_file = cache_dir / spec.cache_name
                    if not cache_file.exists() or cache_file in seen_cache_files:
                        continue
                    seen_cache_files.add(cache_file)
                    stats.source_payloads_matched += 1
                    try:
                        raw = cache_file.read_bytes()
                        payload = json.loads(raw.decode("utf-8"))
                    except Exception as exc:
                        stats.source_payloads_invalid += 1
                        issues.append(
                            _issue(
                                cache_file=cache_file,
                                spec=spec,
                                issue_type="payload_parse_error",
                                detail=str(exc),
                            )
                        )
                        continue
                    valid_payload, rows, payload_error = _payload_rows(payload, market)
                    if not valid_payload:
                        stats.source_payloads_invalid += 1
                        issues.append(
                            _issue(
                                cache_file=cache_file,
                                spec=spec,
                                issue_type="payload_schema_error",
                                detail=payload_error,
                            )
                        )
                        continue
                    stats.source_payloads_valid += 1
                    fetched_at = datetime.fromtimestamp(cache_file.stat().st_mtime).astimezone().isoformat()
                    payload_hash = hashlib.sha256(raw).hexdigest()
                    for row_index, row in enumerate(rows):
                        if not isinstance(row, list) or len(row) < 7:
                            stats.invalid_rows += 1
                            issues.append(
                                _issue(
                                    cache_file=cache_file,
                                    spec=spec,
                                    issue_type="short_row",
                                    detail=f"row_index={row_index}",
                                )
                            )
                            continue
                        trade_date = _try_parse_roc_slash(str(row[0]))
                        if trade_date is None:
                            stats.invalid_rows += 1
                            issues.append(
                                _issue(
                                    cache_file=cache_file,
                                    spec=spec,
                                    issue_type="invalid_trade_date",
                                    detail=f"row_index={row_index}, value={row[0]}",
                                )
                            )
                            continue
                        if (trade_date.year, trade_date.month) != (
                            month_start.year,
                            month_start.month,
                        ):
                            stats.invalid_rows += 1
                            issues.append(
                                _issue(
                                    cache_file=cache_file,
                                    spec=spec,
                                    issue_type="trade_month_mismatch",
                                    detail=(
                                        f"row_index={row_index}, requested={month_start:%Y-%m}, "
                                        f"actual={trade_date:%Y-%m-%d}"
                                    ),
                                )
                            )
                            continue
                        candle = TwMarketProvider._candle_from_values(
                            trade_date,
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[1],
                        )
                        if candle is None:
                            stats.invalid_rows += 1
                            issues.append(
                                _issue(
                                    cache_file=cache_file,
                                    spec=spec,
                                    issue_type="invalid_ohlcv",
                                    detail=f"row_index={row_index}, values={row[1:7]}",
                                )
                            )
                            continue
                        if max_trade_date is not None and trade_date > max_trade_date:
                            stats.rows_skipped_after_cutoff += 1
                            continue
                        stats.valid_rows += 1
                        bars.append(
                            VerifiedDailyBar(
                                market=market,
                                symbol=symbol,
                                trade_date=trade_date,
                                open=float(candle["open"]),
                                high=float(candle["high"]),
                                low=float(candle["low"]),
                                close=float(candle["close"]),
                                volume=float(candle["volume"]),
                                source_endpoint=spec.source_endpoint,
                                source_url=spec.source_url,
                                source_cache_file=str(cache_file),
                                source_payload_sha256=payload_hash,
                                source_fetched_at=fetched_at,
                                source_priority=spec.source_priority,
                            )
                        )
    stats.issues = issues
    return bars, stats, issues, symbols


def _coverage_summary(db_path: Path, symbols: list[str], required_lookback: int) -> dict[str, Any]:
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for symbol in symbols:
        for market in ("TWSE", "TPEx"):
            state = get_sync_state(db_path, market=market, symbol=symbol)
            if state and int(state.get("verified_bar_count") or 0) > 0:
                by_symbol[symbol].append(
                    {
                        "market": market,
                        "bar_count": int(state["verified_bar_count"]),
                        "first_trade_date": state.get("first_verified_trade_date"),
                        "last_trade_date": state.get("last_verified_trade_date"),
                        "meets_required_lookback": int(state["verified_bar_count"]) >= required_lookback,
                    }
                )
    return {
        "symbol_count": len(symbols),
        "symbols_with_verified_bars": sum(1 for value in by_symbol.values() if value),
        "symbols_meeting_required_lookback": sum(
            1
            for value in by_symbol.values()
            if any(item["meets_required_lookback"] for item in value)
        ),
        "by_symbol": dict(sorted(by_symbol.items())),
    }


def import_market_cache(
    *,
    cache_dir: Path,
    database_path: Path,
    themes: list[str],
    start_month: date,
    end_month: date,
    max_trade_date: date | None = None,
    required_lookback: int = 253,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    started_at = datetime.now().astimezone().isoformat()
    run_id = f"daily-bars-import-{datetime.now().astimezone().strftime('%Y%m%dT%H%M%S%f')}"
    bars, collect_stats, issues, symbols = collect_verified_cached_bars(
        cache_dir,
        themes=themes,
        start_month=start_month,
        end_month=end_month,
        max_trade_date=max_trade_date,
    )
    storage_stats = import_verified_bars(database_path, bars, imported_at=started_at)
    finished_at = datetime.now().astimezone().isoformat()
    coverage = _coverage_summary(database_path, symbols, required_lookback)
    integrity = database_integrity(database_path)
    summary = {
        "run_id": run_id,
        "source_cache_dir": str(cache_dir),
        "database_path": str(database_path),
        "themes": themes,
        "symbol_count": len(symbols),
        "start_month": start_month.strftime("%Y-%m"),
        "end_month": end_month.strftime("%Y-%m"),
        "max_trade_date": max_trade_date.isoformat() if max_trade_date else None,
        "historical_import_only": True,
        "current_day_verified": False,
        "collection": collect_stats.as_dict(),
        "storage": storage_stats.as_dict(),
        "coverage": coverage,
        "database_integrity": integrity,
        "status": "complete" if integrity.get("ok") else "failed",
        "started_at": started_at,
        "finished_at": finished_at,
    }
    record_import_run(
        database_path,
        run_id=run_id,
        source_cache_dir=cache_dir,
        themes=themes,
        symbol_count=len(symbols),
        start_month=start_month.strftime("%Y-%m"),
        end_month=end_month.strftime("%Y-%m"),
        max_trade_date=max_trade_date.isoformat() if max_trade_date else None,
        started_at=started_at,
        finished_at=finished_at,
        status=str(summary["status"]),
        summary=summary,
        issues=issues,
    )
    if manifest_path is None:
        manifest_path = database_path.parent / f"{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["manifest_path"] = str(manifest_path)
    return summary
