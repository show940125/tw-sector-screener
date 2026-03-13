from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.actions import build_action_view
from src.analysis.backtest import run_cross_sectional_backtest
from src.analysis.factors import atr_wilder, momentum_return, percentile_rank, rsi_wilder, sma, trend_score, volatility_annualized
from src.analysis.scoring import score_candidates
from src.config import load_config
from src.providers.tw_market_provider import TwMarketProvider
from src.report.export_structured import write_audit_trail, write_candidate_csv, write_json_report, write_watchlist
from src.report.render_markdown import build_report_filename, render_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台股類股選股 Skill CLI")
    parser.add_argument("--theme", required=True, help="類股主題，例如 半導體 / AI / memory")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="分析截止日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=10, help="輸出前 N 檔")
    parser.add_argument("--universe-limit", type=int, default=60, help="最多分析多少檔候選股")
    parser.add_argument("--min-monthly-revenue", type=float, default=0.0, help="最低月營收門檻（元）")
    parser.add_argument("--lookback", type=int, default=252, help="歷史回看日數")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 逾時秒數")
    parser.add_argument("--theme-mode", choices=["strict", "broad"], default="strict", help="題材池模式")
    parser.add_argument("--benchmark", choices=["TAIEX", "sector", "custom"], default="TAIEX", help="benchmark 模式")
    parser.add_argument("--output-format", default="md,json,csv", help="輸出格式，逗號分隔：md,json,csv")
    parser.add_argument("--config", default=None, help="JSON / YAML config 路徑")
    parser.add_argument("--coverage-list", default=None, help="watchlist symbol 清單路徑（txt/json）")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="官方輸出根目錄")
    parser.add_argument("--output-dir", default=None, help="Deprecated alias，請改用 --output-root")
    parser.add_argument("--run-backtest", action="store_true", help="輸出簡化版截面回測與 validation report")
    parser.add_argument("--rebalance", choices=["weekly", "monthly"], default="monthly", help="回測再平衡頻率")
    parser.add_argument("--cost-bps", type=float, default=10.0, help="回測單次換手成本（bps）")
    parser.add_argument("--validation-window", choices=["1y", "3y", "5y"], default="1y", help="validation 視窗")
    return parser.parse_args()


def _avg(values: list[float | None]) -> float | None:
    valid = [float(v) for v in values if isinstance(v, (int, float))]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _ret_pct(closes: list[float], days: int) -> float | None:
    if len(closes) <= days:
        return None
    base = closes[-days - 1]
    if base == 0:
        return None
    return (closes[-1] / base - 1.0) * 100.0


def _ma_stack(close: float, sma20: float | None, sma60: float | None, sma120: float | None) -> str:
    if sma20 is None or sma60 is None or sma120 is None:
        return "-"
    return "/".join("上" if close > level else "下" for level in (sma20, sma60, sma120))


def _band_from_percentile(value: float | None) -> str:
    if value is None:
        return "N/A"
    if value >= 67:
        return "cheap"
    if value <= 33:
        return "expensive"
    return "neutral"


def _load_coverage_symbols(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if path.suffix.lower() == ".json":
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [str(x).strip() for x in payload if str(x).strip()]
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _load_previous_watchlist(watchlist_dir: Path, theme: str, as_of: date) -> dict[str, Any]:
    pattern = f"watchlist-{theme}-*.json"
    candidates = sorted([x for x in watchlist_dir.glob(pattern) if as_of.strftime("%Y%m%d") not in x.name])
    if not candidates:
        return {}
    latest = candidates[-1]
    try:
        return json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _collect_custom_benchmark(provider: TwMarketProvider, symbols: list[str], as_of: date, lookback: int) -> dict[str, float | None]:
    returns_20d: list[float] = []
    returns_63d: list[float] = []
    for symbol in symbols:
        market = "TPEx" if symbol.startswith("6") or symbol.startswith("8") else "TWSE"
        try:
            closes = [float(x["close"]) for x in provider.get_ohlcv(symbol, market, as_of=as_of, lookback=lookback)]
        except Exception:
            continue
        ret20 = _ret_pct(closes, 20)
        ret63 = momentum_return(closes, 63)
        if ret20 is not None:
            returns_20d.append(ret20)
        if ret63 is not None:
            returns_63d.append(ret63)
    return {"ret_20d": _avg(returns_20d), "ret_63d": _avg(returns_63d)}


def _reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if (row.get("trend_score") or 0.0) >= 70:
        reasons.append("趨勢結構偏多")
    if (row.get("momentum_score") or 0.0) >= 65:
        reasons.append("相對題材仍有動能")
    if (row.get("fundamental_score") or 0.0) >= 60:
        reasons.append("營收與品質訊號沒有掉鍊")
    if (row.get("benchmark_score") or 0.0) >= 60:
        reasons.append("相對 benchmark 仍領先")
    if not reasons:
        reasons.append("訊號混合，暫列觀察名單。")
    return reasons[:3]


def _catalysts(row: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if (row.get("revenue_acceleration") or 0.0) > 0:
        notes.append("月營收加速度轉正")
    if (row.get("gross_margin_trend") or 0.0) > 0:
        notes.append("毛利率趨勢改善")
    if (row.get("eps_trend") or 0.0) > 0:
        notes.append("EPS 近季續增")
    if (row.get("roe_trend") or 0.0) > 0:
        notes.append("ROE 走升")
    if (row.get("rel_to_sector_20d") or 0.0) > 0:
        notes.append("20 日相對題材強勢")
    return notes[:4] or ["等待下一個基本面或事件催化。"]


def _score_label(score: float | None) -> str:
    if score is None:
        return "N/A"
    if score >= 67:
        return "high"
    if score <= 33:
        return "low"
    return "mid"


def _event_risk_state(row: dict[str, Any]) -> str:
    action_view = row.get("action_view") or {}
    flags = row.get("data_quality_flags") or []
    if "quality:fetch_failed" in flags or (row.get("volatility20") or 0.0) >= 45:
        return "elevated"
    if action_view.get("action") == "Overweight":
        return "manageable"
    return "normal"


def _build_watchlist_payload(
    theme: str,
    as_of: date,
    ranked: list[dict[str, Any]],
    coverage_symbols: list[str],
    previous_payload: dict[str, Any],
) -> dict[str, Any]:
    current_map = {row["symbol"]: row for row in ranked}
    previous_ranks = {item["symbol"]: item.get("rank") for item in previous_payload.get("rows", [])}
    target_symbols = coverage_symbols or [row["symbol"] for row in ranked[:10]]
    rows: list[dict[str, Any]] = []
    for symbol in target_symbols:
        row = current_map.get(symbol)
        current_rank = row.get("rank") if row else None
        previous_rank = previous_ranks.get(symbol)
        rating_change_reason = "新納入觀察"
        if previous_rank is not None and current_rank is not None:
            delta = previous_rank - current_rank
            if delta > 0:
                rating_change_reason = "排名上修，研究優先序提高"
            elif delta < 0:
                rating_change_reason = "排名下修，需要重新驗證 thesis"
            else:
                rating_change_reason = "排名不變，維持原判斷"
        elif previous_rank is not None and current_rank is None:
            rating_change_reason = "跌出追蹤名單，先降級觀察"
        rows.append(
            {
                "symbol": symbol,
                "rank": current_rank,
                "previous_rank": previous_rank,
                "rank_delta": (previous_rank - current_rank) if previous_rank is not None and current_rank is not None else None,
                "change_reason": (
                    "upgrade"
                    if previous_rank is not None and current_rank is not None and previous_rank > current_rank
                    else "downgrade"
                    if previous_rank is not None and current_rank is not None and previous_rank < current_rank
                    else "unchanged"
                    if previous_rank is not None and current_rank is not None
                    else "dropped"
                    if previous_rank is not None and current_rank is None
                    else "new"
                ),
                "rating_change_reason": rating_change_reason,
                "event_risk_state": _event_risk_state(row or {}),
                "action": ((row or {}).get("action_view") or {}).get("action"),
                "thesis_summary": (row or {}).get("thesis_summary"),
            }
        )
    return {"theme": theme, "as_of": as_of.isoformat(), "rows": rows}


def _validation_days(window: str) -> int:
    return {"1y": 252, "3y": 252 * 3, "5y": 252 * 5}.get(window, 252)


def _rebalance_step(rebalance: str) -> int:
    return 5 if rebalance == "weekly" else 21


def _resolve_output_root(output_root: Path | None, output_dir: Path | None, warnings: list[str]) -> Path:
    if output_dir and output_root and output_root != DEFAULT_OUTPUT_ROOT and output_root != output_dir:
        warnings.append("--output-dir 已 deprecated，這次以 --output-root 為準。")
        return output_root
    if output_dir and (output_root is None or output_root == DEFAULT_OUTPUT_ROOT):
        warnings.append("--output-dir 已 deprecated，請改用 --output-root。")
        return output_dir
    return output_root or DEFAULT_OUTPUT_ROOT


def _copy_coverage_list(source: Path | None, target_dir: Path) -> Path | None:
    if source is None or not source.exists():
        return None
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / source.name
    shutil.copyfile(source, target_path)
    return target_path


def _build_validation_snapshots(raw_rows: list[dict[str, Any]], validation_window: str, rebalance: str) -> list[dict[str, Any]]:
    if not raw_rows:
        return []
    step = _rebalance_step(rebalance)
    window_days = _validation_days(validation_window)
    common_len = min(len(row.get("_candles") or []) for row in raw_rows)
    if common_len < 25:
        return []
    start_index = max(20, common_len - window_days)
    snapshots: list[dict[str, Any]] = []
    for index in range(start_index, common_len, step):
        rows: list[dict[str, Any]] = []
        rebalance_date: date | None = None
        for row in raw_rows:
            candles = row.get("_candles") or []
            if index >= len(candles):
                continue
            closes = [float(item["close"]) for item in candles[: index + 1]]
            signal = _avg([_ret_pct(closes, 20), _ret_pct(closes, 63), _ret_pct(closes, 126)])
            if signal is None:
                continue
            rebalance_date = candles[index]["date"]
            rows.append({"symbol": row["symbol"], "close": float(candles[index]["close"]), "score": signal})
        if rebalance_date and rows:
            snapshots.append({"rebalance_date": rebalance_date, "rows": rows})
    return snapshots


def _slice_benchmark_series(benchmark_series: list[dict[str, Any]], start_date: date | None) -> list[dict[str, Any]]:
    if start_date is None:
        return benchmark_series
    return [item for item in benchmark_series if item.get("date") >= start_date]


def run(
    theme: str,
    as_of: date,
    top_n: int,
    universe_limit: int,
    min_monthly_revenue: float,
    lookback: int,
    timeout: float,
    output_root: Path | None = None,
    output_dir: Path | None = None,
    theme_mode: str = "strict",
    benchmark: str = "TAIEX",
    output_formats: set[str] | None = None,
    config_path: str | Path | None = None,
    coverage_list_path: str | Path | None = None,
    run_backtest: bool = False,
    rebalance: str = "monthly",
    cost_bps: float = 10.0,
    validation_window: str = "1y",
) -> dict[str, Path]:
    config = load_config(config_path)
    output_formats = output_formats or {"md", "json", "csv"}
    warnings: list[str] = []
    resolved_output_root = _resolve_output_root(
        Path(output_root) if output_root is not None else None,
        Path(output_dir) if output_dir is not None else None,
        warnings,
    )
    provider = TwMarketProvider(timeout=timeout, cache_dir=resolved_output_root / "cache" / "market")
    weights = dict(config.get("weights") or {})
    min_revenue = max(float(config.get("filters", {}).get("min_monthly_revenue", 0.0) or 0.0), min_monthly_revenue)
    universe = provider.load_theme_universe(theme, min_monthly_revenue=min_revenue, theme_mode=theme_mode)
    if not universe:
        raise RuntimeError(f"找不到主題 {theme} 的候選股")

    date_tag = as_of.strftime("%Y%m%d")
    reports_dir = resolved_output_root / "reports" / date_tag / theme
    audit_dir = resolved_output_root / "audit" / date_tag
    watchlists_dir = resolved_output_root / "watchlists" / theme
    backtests_dir = resolved_output_root / "backtests" / theme
    coverage_dir = resolved_output_root / "coverage-lists"
    for path in [reports_dir, audit_dir, watchlists_dir, backtests_dir, coverage_dir]:
        path.mkdir(parents=True, exist_ok=True)

    copied_coverage = _copy_coverage_list(Path(coverage_list_path) if coverage_list_path else None, coverage_dir)
    market_overview: dict[str, Any] = {}
    custom_benchmark = {"ret_20d": None, "ret_63d": None}
    if benchmark == "custom":
        custom_symbols = list(config.get("benchmark", {}).get("symbols") or [])
        if not custom_symbols:
            raise RuntimeError("benchmark=custom 時，config.benchmark.symbols 不可為空。")
        custom_benchmark = _collect_custom_benchmark(provider, custom_symbols, as_of, lookback)

    taiex_series: list[dict[str, Any]] = []
    try:
        taiex_series = provider.get_taiex_series(as_of=as_of, lookback=max(lookback, _validation_days(validation_window) + 40))
        taiex_closes = [float(x["close"]) for x in taiex_series]
        taiex_close = taiex_closes[-1]
        taiex_prev = taiex_closes[-2] if len(taiex_closes) >= 2 else None
        taiex_sma20 = sma(taiex_closes, 20)
        taiex_sma60 = sma(taiex_closes, 60)
        taiex_sma120 = sma(taiex_closes, 120)
        taiex_rsi14 = rsi_wilder(taiex_closes, 14)
        market_overview = {
            "close": taiex_close,
            "change_points": taiex_series[-1].get("change_points"),
            "change_pct": ((taiex_close / taiex_prev - 1.0) * 100.0) if taiex_prev else None,
            "ret_5d": momentum_return(taiex_closes, 5),
            "ret_20d": momentum_return(taiex_closes, 20),
            "ret_63d": momentum_return(taiex_closes, 63),
            "ret_126d": momentum_return(taiex_closes, 126),
            "sma20": taiex_sma20,
            "sma60": taiex_sma60,
            "sma120": taiex_sma120,
            "rsi14": taiex_rsi14,
            "trend_score": trend_score(taiex_close, taiex_sma20, taiex_sma60, taiex_sma120, taiex_rsi14),
            "source": "TWSE exchangeReport/FMTQIK",
        }
    except Exception as exc:
        warnings.append(f"加權指數抓取失敗：{exc}")

    raw_rows: list[dict[str, Any]] = []
    for candidate in universe[:universe_limit]:
        symbol = candidate["symbol"]
        market = candidate["market"]
        try:
            candles = provider.get_ohlcv(symbol, market, as_of=as_of, lookback=max(lookback, _validation_days(validation_window) + 40))
        except Exception as exc:
            warnings.append(f"{symbol} 日線失敗：{exc}")
            continue
        closes = [float(c["close"]) for c in candles]
        volumes = [float(c["volume"]) for c in candles]
        close = closes[-1]
        sma20 = sma(closes, 20)
        sma60 = sma(closes, 60)
        sma120 = sma(closes, 120)
        rsi14 = rsi_wilder(closes, 14)
        atr14 = atr_wilder(candles, 14)
        vol20 = volatility_annualized(closes, 20)
        mom63 = momentum_return(closes, 63)
        mom126 = momentum_return(closes, 126)
        ret5 = _ret_pct(closes, 5)
        ret20 = _ret_pct(closes, 20)
        liquidity20 = 0.0
        if len(closes) >= 20 and len(volumes) >= 20:
            liquidity20 = sum(closes[-20 + i] * volumes[-20 + i] for i in range(20)) / 20.0
        valuation = provider.get_latest_valuation(symbol, market, as_of) or {}
        quarter = provider.get_quarterly_fundamentals(symbol, market, as_of) or {}
        raw_rows.append(
            {
                **candidate,
                "close": close,
                "sma20": sma20,
                "sma60": sma60,
                "sma120": sma120,
                "rsi14": rsi14,
                "atr14": atr14,
                "volatility20": vol20,
                "momentum63": mom63,
                "momentum126": mom126,
                "ret_5d": ret5,
                "ret_20d": ret20,
                "ma_stack": _ma_stack(close, sma20, sma60, sma120),
                "liquidity20": liquidity20,
                "pe": valuation.get("pe"),
                "pb": valuation.get("pb"),
                "dividend_yield": valuation.get("dividend_yield"),
                "trend_score": trend_score(close, sma20, sma60, sma120, rsi14),
                "revenue_yoy_prev": candidate.get("revenue_yoy_prev"),
                "revenue_mom_prev": candidate.get("revenue_mom_prev"),
                "revenue_acceleration": (
                    candidate.get("revenue_yoy") - candidate.get("revenue_yoy_prev")
                    if isinstance(candidate.get("revenue_yoy"), (int, float)) and isinstance(candidate.get("revenue_yoy_prev"), (int, float))
                    else None
                ),
                "revenue_mom_acceleration": (
                    candidate.get("revenue_mom") - candidate.get("revenue_mom_prev")
                    if isinstance(candidate.get("revenue_mom"), (int, float)) and isinstance(candidate.get("revenue_mom_prev"), (int, float))
                    else None
                ),
                "gross_margin_trend": (
                    quarter.get("gross_margin_latest") - quarter.get("gross_margin_prev")
                    if isinstance(quarter.get("gross_margin_latest"), (int, float))
                    and isinstance(quarter.get("gross_margin_prev"), (int, float))
                    else None
                ),
                "eps_trend": (
                    quarter.get("eps_latest") - quarter.get("eps_prev")
                    if isinstance(quarter.get("eps_latest"), (int, float)) and isinstance(quarter.get("eps_prev"), (int, float))
                    else None
                ),
                "roe_trend": (
                    quarter.get("roe_latest") - quarter.get("roe_prev")
                    if isinstance(quarter.get("roe_latest"), (int, float)) and isinstance(quarter.get("roe_prev"), (int, float))
                    else None
                ),
                "quality_data_source": quarter.get("quality_data_source"),
                "quality_periods_used": quarter.get("quality_periods_used") or [],
                "quality_fetch_status": quarter.get("quality_fetch_status"),
                "quality_missing_reason": quarter.get("quality_missing_reason"),
                "data_quality_flags": list(quarter.get("data_quality_flags") or []),
                "_candles": candles,
                **quarter,
            }
        )

    if not raw_rows:
        raise RuntimeError("候選股資料抓取失敗，無法評分")

    theme_avg_ret20 = _avg([row.get("ret_20d") for row in raw_rows if isinstance(row.get("ret_20d"), (int, float))])
    industry_avg_ret20: dict[str, float | None] = {}
    for industry in sorted({str(row.get("industry") or "") for row in raw_rows}):
        values = [row.get("ret_20d") for row in raw_rows if row.get("industry") == industry and isinstance(row.get("ret_20d"), (int, float))]
        industry_avg_ret20[industry] = _avg(values)

    for row in raw_rows:
        row["rel_to_taiex_20d"] = (
            row.get("ret_20d") - market_overview.get("ret_20d")
            if isinstance(row.get("ret_20d"), (int, float)) and isinstance(market_overview.get("ret_20d"), (int, float))
            else None
        )
        row["rel_to_sector_20d"] = (
            row.get("ret_20d") - theme_avg_ret20
            if isinstance(row.get("ret_20d"), (int, float)) and isinstance(theme_avg_ret20, (int, float))
            else None
        )
        row["rel_to_industry_20d"] = (
            row.get("ret_20d") - industry_avg_ret20.get(str(row.get("industry") or ""))
            if isinstance(row.get("ret_20d"), (int, float))
            and isinstance(industry_avg_ret20.get(str(row.get("industry") or "")), (int, float))
            else None
        )
        if benchmark == "custom":
            row["rel_to_custom_20d"] = (
                row.get("ret_20d") - custom_benchmark.get("ret_20d")
                if isinstance(row.get("ret_20d"), (int, float)) and isinstance(custom_benchmark.get("ret_20d"), (int, float))
                else None
            )

    mom63_list = [x["momentum63"] for x in raw_rows if isinstance(x.get("momentum63"), (int, float))]
    mom126_list = [x["momentum126"] for x in raw_rows if isinstance(x.get("momentum126"), (int, float))]
    pe_list = [x["pe"] for x in raw_rows if isinstance(x.get("pe"), (int, float)) and x.get("pe", 0) > 0]
    pb_list = [x["pb"] for x in raw_rows if isinstance(x.get("pb"), (int, float)) and x.get("pb", 0) > 0]
    dy_list = [x["dividend_yield"] for x in raw_rows if isinstance(x.get("dividend_yield"), (int, float))]
    rev_yoy_list = [x["revenue_yoy"] for x in raw_rows if isinstance(x.get("revenue_yoy"), (int, float))]
    rev_mom_list = [x["revenue_mom"] for x in raw_rows if isinstance(x.get("revenue_mom"), (int, float))]
    rev_acc_list = [x["revenue_acceleration"] for x in raw_rows if isinstance(x.get("revenue_acceleration"), (int, float))]
    gm_list = [x["gross_margin_trend"] for x in raw_rows if isinstance(x.get("gross_margin_trend"), (int, float))]
    eps_list = [x["eps_trend"] for x in raw_rows if isinstance(x.get("eps_trend"), (int, float))]
    roe_list = [x["roe_trend"] for x in raw_rows if isinstance(x.get("roe_trend"), (int, float))]
    vol_list = [x["volatility20"] for x in raw_rows if isinstance(x.get("volatility20"), (int, float))]
    liq_list = [x["liquidity20"] for x in raw_rows if isinstance(x.get("liquidity20"), (int, float)) and x.get("liquidity20", 0) > 0]
    rel_taiex_list = [x["rel_to_taiex_20d"] for x in raw_rows if isinstance(x.get("rel_to_taiex_20d"), (int, float))]
    rel_sector_list = [x["rel_to_sector_20d"] for x in raw_rows if isinstance(x.get("rel_to_sector_20d"), (int, float))]
    rel_industry_list = [x["rel_to_industry_20d"] for x in raw_rows if isinstance(x.get("rel_to_industry_20d"), (int, float))]

    scored_input: list[dict[str, Any]] = []
    for row in raw_rows:
        momentum_score = _avg([percentile_rank(row.get("momentum63"), mom63_list), percentile_rank(row.get("momentum126"), mom126_list)])
        value_score = _avg(
            [
                (100.0 - percentile_rank(row.get("pe"), pe_list)) if isinstance(row.get("pe"), (int, float)) and pe_list else None,
                (100.0 - percentile_rank(row.get("pb"), pb_list)) if isinstance(row.get("pb"), (int, float)) and pb_list else None,
                percentile_rank(row.get("dividend_yield"), dy_list),
            ]
        )
        fundamental_score = _avg(
            [
                percentile_rank(row.get("revenue_yoy"), rev_yoy_list),
                percentile_rank(row.get("revenue_mom"), rev_mom_list),
                percentile_rank(row.get("revenue_acceleration"), rev_acc_list),
            ]
        )
        quality_score = _avg(
            [
                percentile_rank(row.get("gross_margin_trend"), gm_list),
                percentile_rank(row.get("eps_trend"), eps_list),
                percentile_rank(row.get("roe_trend"), roe_list),
            ]
        )
        benchmark_score = _avg(
            [
                percentile_rank(row.get("rel_to_taiex_20d"), rel_taiex_list),
                percentile_rank(row.get("rel_to_sector_20d"), rel_sector_list),
                percentile_rank(row.get("rel_to_industry_20d"), rel_industry_list),
            ]
        )
        risk_control_score = _avg(
            [
                (100.0 - percentile_rank(row.get("volatility20"), vol_list))
                if isinstance(row.get("volatility20"), (int, float)) and vol_list
                else None,
                percentile_rank(row.get("liquidity20"), liq_list),
            ]
        )
        scored_input.append(
            {
                **row,
                "momentum_score": momentum_score,
                "value_score": value_score,
                "fundamental_score": fundamental_score,
                "quality_score": quality_score,
                "benchmark_score": benchmark_score,
                "risk_control_score": risk_control_score,
                "valuation_band": _band_from_percentile(value_score),
            }
        )

    ranked = score_candidates(scored_input, weights=weights)
    for idx, row in enumerate(ranked, start=1):
        row["rank"] = idx
        row["action_view"] = build_action_view(
            idea_score=float(row.get("idea_score") or 0.0),
            confidence_score=float(row.get("confidence_score") or 0.0),
            close=float(row.get("close") or 0.0),
            atr14=row.get("atr14"),
            volatility20=row.get("volatility20"),
            rel_to_taiex_20d=row.get("rel_to_taiex_20d"),
            rel_to_sector_20d=row.get("rel_to_sector_20d"),
        )
        row["thesis_summary"] = (
            f"{row['name']} 屬於 {_score_label(row.get('momentum_score'))} 動能 / {_score_label(row.get('fundamental_score'))} 基本面組合，"
            f"估值區間偏 {row.get('valuation_band')}。"
        )
        row["catalyst_notes"] = _catalysts(row)
        row["benchmark_view"] = {
            "rel_to_taiex_20d": row.get("rel_to_taiex_20d"),
            "rel_to_sector_20d": row.get("rel_to_sector_20d"),
            "rel_to_industry_20d": row.get("rel_to_industry_20d"),
        }

    picks: list[dict[str, Any]] = []
    for row in ranked[:top_n]:
        public_row = {key: value for key, value in row.items() if key != "_candles"}
        picks.append(
            {
                **public_row,
                "reasons": _reasons(public_row),
                "trend": {
                    "ret_5d": public_row.get("ret_5d"),
                    "ret_20d": public_row.get("ret_20d"),
                    "mom63": public_row.get("momentum63"),
                    "mom126": public_row.get("momentum126"),
                    "ma_stack": public_row.get("ma_stack"),
                    "rsi14": public_row.get("rsi14"),
                    "volatility20": public_row.get("volatility20"),
                },
            }
        )

    top_rows = ranked[:top_n]
    sector_overview = {
        "universe_count": len(ranked),
        "top_n": len(top_rows),
        "top_avg_idea": _avg([x.get("idea_score") for x in top_rows]),
        "top_avg_confidence": _avg([x.get("confidence_score") for x in top_rows]),
        "avg_ret_20d": theme_avg_ret20,
        "avg_rel_to_taiex_20d": _avg([x.get("rel_to_taiex_20d") for x in raw_rows if isinstance(x.get("rel_to_taiex_20d"), (int, float))]),
        "weights": weights,
    }

    validation_summary: dict[str, Any] = {"mode": "not-run", "window": validation_window, "rebalance": rebalance, "cost_bps": cost_bps}
    outputs: dict[str, Path] = {}
    if run_backtest:
        snapshots = _build_validation_snapshots(raw_rows, validation_window, rebalance)
        metrics = run_cross_sectional_backtest(
            snapshots=snapshots,
            benchmark_series=_slice_benchmark_series(taiex_series, snapshots[0]["rebalance_date"] if snapshots else None),
            top_n=min(top_n, max(len(raw_rows), 1)),
            cost_bps=cost_bps,
        )
        validation_summary = {
            "mode": "price_only_cross_sectional",
            "window": validation_window,
            "rebalance": rebalance,
            "cost_bps": cost_bps,
            "metrics": metrics,
            "limitations": [
                "目前回測主要驗證截面排序與價格延續性，不是完整賣方財務模型。",
                "季度品質因子只在最新季有即時官方資料，前期比較仰賴本地快照累積。",
            ],
        }
        outputs["backtest"] = write_json_report(backtests_dir / f"validation-{theme}-{date_tag}.json", validation_summary)

    top_pick = picks[0] if picks else {}
    summary = (
        f"Thesis：{theme} 類股目前由 `{top_pick.get('symbol', '-')}` {top_pick.get('name', '-') } 領跑，"
        f"top {len(picks)} 平均 idea score `{(sector_overview.get('top_avg_idea') or 0.0):.1f}`。"
        f" Evidence：相對題材 20 日超額 `{((top_pick.get('benchmark_view') or {}).get('rel_to_sector_20d') or 0.0):.2f}%`，"
        f"confidence `{top_pick.get('confidence_score', 0.0):.1f}`。"
        f" Risk：{'；'.join(top_pick.get('data_quality_flags') or ['主要風險在題材回檔'])}。"
        f" Action：`{(top_pick.get('action_view') or {}).get('action', 'Neutral')}`。"
        f" What changes my mind：若相對題材 20 日動能轉負、confidence 下滑或法說/營收驗證失敗，就降級。"
    )

    method = [
        "Rank 看的是 idea score 與資料可信度的合成，不再把缺值直接補成 50 分。",
        "Confidence 拆成 factor coverage 與 data freshness 兩段，避免把資料缺漏跟舊資料混成一團。",
        "Benchmark 同時看相對 TAIEX、相對題材、相對產業，避免只用絕對漲幅自嗨。",
        "Action 與 ranking 拆開：排名是研究優先序，Overweight/Neutral/Underweight 才是動作建議。",
    ]
    if run_backtest:
        method.append("Validation 使用 price-only cross-sectional backtest，先驗證排序有效性，再決定要不要升級更完整模型。")
    risks = [
        "這是研究輔助，不是保證報酬；遇到法說、月營收、AI 出貨節奏變化時，結論需要重新驗證。",
        "若 benchmark-relative 轉負且 confidence 下滑，應優先減碼而不是凹單。",
    ]
    if warnings:
        risks.append(f"資料警示：{len(warnings)} 檔抓取失敗，結果可能有抽樣偏誤。")

    audit_payload = {
        "theme": theme,
        "as_of": as_of.isoformat(),
        "theme_mode": theme_mode,
        "benchmark": benchmark,
        "output_formats": sorted(output_formats),
        "warnings": warnings,
        "weights": weights,
        "config_path": str(config_path) if config_path else None,
        "coverage_list_path": str(coverage_list_path) if coverage_list_path else None,
        "copied_coverage_list_path": str(copied_coverage) if copied_coverage else None,
        "cache_dir": str(getattr(provider, "cache_dir", resolved_output_root / "cache" / "market")),
        "output_root": str(resolved_output_root),
        "provider_versions": {"market_provider": "twse_openapi+tpex_openapi", "validation_engine": "price_only_cross_sectional_v1"},
        "backtest_config": {"enabled": run_backtest, "window": validation_window, "rebalance": rebalance, "cost_bps": cost_bps},
        "universe_count": len(universe),
        "ranked_count": len(ranked),
    }

    stem = f"sector-report-{theme}-{date_tag}"
    if "md" in output_formats:
        md_path = reports_dir / build_report_filename(theme, as_of)
        md_path.write_text(
            render_report(
                {
                    "theme": theme,
                    "as_of": as_of,
                    "summary": summary,
                    "market_overview": market_overview,
                    "sector_overview": sector_overview,
                    "method": method,
                    "picks": picks,
                    "risks": risks,
                    "audit": audit_payload,
                    "sources": ["TWSE OpenAPI", "TWSE exchangeReport", "TPEx OpenAPI", "TPEx afterTrading API"],
                    "validation_summary": validation_summary,
                }
            ),
            encoding="utf-8",
        )
        outputs["md"] = md_path
    if "json" in output_formats:
        outputs["json"] = write_json_report(
            reports_dir / f"{stem}.json",
            {
                "theme": theme,
                "as_of": as_of.isoformat(),
                "summary": summary,
                "picks": picks,
                "sector_overview": sector_overview,
                "market_overview": market_overview,
                "validation_summary": validation_summary,
                "audit": audit_payload,
            },
        )
    if "csv" in output_formats:
        outputs["csv"] = write_candidate_csv(reports_dir / f"{stem}.csv", picks)

    outputs["audit"] = write_audit_trail(audit_dir / f"{stem}.audit.json", audit_payload)
    coverage_symbols = _load_coverage_symbols(Path(coverage_list_path)) if coverage_list_path else []
    previous_watchlist = _load_previous_watchlist(watchlists_dir, theme, as_of)
    outputs["watchlist"] = write_watchlist(
        watchlists_dir / f"watchlist-{theme}-{date_tag}.json",
        _build_watchlist_payload(theme, as_of, ranked, coverage_symbols, previous_watchlist),
    )
    return outputs


def main() -> int:
    args = parse_args()
    try:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
        outputs = run(
            theme=args.theme.strip(),
            as_of=as_of,
            top_n=args.top_n,
            universe_limit=args.universe_limit,
            min_monthly_revenue=args.min_monthly_revenue,
            lookback=args.lookback,
            timeout=args.timeout,
            output_root=Path(args.output_root) if args.output_root else None,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            theme_mode=args.theme_mode,
            benchmark=args.benchmark,
            output_formats={x.strip() for x in str(args.output_format).split(",") if x.strip()},
            config_path=args.config,
            coverage_list_path=args.coverage_list,
            run_backtest=args.run_backtest,
            rebalance=args.rebalance,
            cost_bps=args.cost_bps,
            validation_window=args.validation_window,
        )
        for key, path in outputs.items():
            print(f"[tw-sector-screener] {key}: {path}")
        return 0
    except Exception as exc:
        print(f"[tw-sector-screener] error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
