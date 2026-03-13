from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = Path.home() / "tw-sector-screener-output"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.factors import (
    atr_wilder,
    momentum_return,
    percentile_rank,
    rsi_wilder,
    sma,
    trend_score,
    volatility_annualized,
)
from src.analysis.scoring import score_candidates
from src.providers.tw_market_provider import TwMarketProvider
from src.themes import available_themes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台股全類股 Top100 批次快照")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="分析截止日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=100, help="每個類股輸出前 N 檔")
    parser.add_argument("--lookback", type=int, default=160, help="歷史回看日數（需 >= 127）")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 逾時秒數")
    parser.add_argument("--min-monthly-revenue", type=float, default=0.0, help="最低月營收門檻（元）")
    parser.add_argument("--industry-min-count", type=int, default=1, help="產業最少成分股數")
    parser.add_argument(
        "--max-symbols-per-bucket",
        type=int,
        default=160,
        help="每個類股最多分析幾檔（按月營收排序，<=0 表示不限制）",
    )
    parser.add_argument(
        "--bucket-types",
        default="theme,industry",
        help="要輸出的類股種類，逗號分隔：theme,industry",
    )
    parser.add_argument(
        "--include-buckets",
        default="",
        help="只輸出指定類股，格式: theme:半導體,industry:電子零組件業",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="輸出資料夾")
    return parser.parse_args()


def _avg(values: list[float | None], default: float = 50.0) -> float:
    valid = [float(v) for v in values if isinstance(v, (int, float))]
    if not valid:
        return default
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


def _slug(value: str) -> str:
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()[:10]
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    if not cleaned:
        cleaned = "bucket"
    return f"{cleaned}-{digest}"


def _build_metrics(
    provider: TwMarketProvider,
    candidate: dict[str, Any],
    as_of: date,
    lookback: int,
    warnings: list[str],
) -> dict[str, Any] | None:
    symbol = candidate["symbol"]
    market = candidate["market"]
    try:
        candles = provider.get_ohlcv(symbol, market, as_of=as_of, lookback=lookback)
    except Exception as exc:
        warnings.append(f"{symbol} 日線失敗：{exc}")
        return None

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
    ma_stack = _ma_stack(close, sma20, sma60, sma120)
    liquidity20 = 0.0
    if len(closes) >= 20 and len(volumes) >= 20:
        liquidity20 = sum(closes[-20 + i] * volumes[-20 + i] for i in range(20)) / 20.0
    valuation = provider.get_latest_valuation(symbol, market, as_of) or {}
    return {
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
        "ma_stack": ma_stack,
        "liquidity20": liquidity20,
        "pe": valuation.get("pe"),
        "pb": valuation.get("pb"),
        "dividend_yield": valuation.get("dividend_yield"),
        "trend_score": trend_score(close, sma20, sma60, sma120, rsi14),
    }


def _score_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not raw_rows:
        return []
    mom63_list = [x["momentum63"] for x in raw_rows if isinstance(x.get("momentum63"), (int, float))]
    mom126_list = [x["momentum126"] for x in raw_rows if isinstance(x.get("momentum126"), (int, float))]
    pe_list = [x["pe"] for x in raw_rows if isinstance(x.get("pe"), (int, float)) and x.get("pe", 0) > 0]
    pb_list = [x["pb"] for x in raw_rows if isinstance(x.get("pb"), (int, float)) and x.get("pb", 0) > 0]
    dy_list = [x["dividend_yield"] for x in raw_rows if isinstance(x.get("dividend_yield"), (int, float))]
    rev_yoy_list = [x["revenue_yoy"] for x in raw_rows if isinstance(x.get("revenue_yoy"), (int, float))]
    rev_mom_list = [x["revenue_mom"] for x in raw_rows if isinstance(x.get("revenue_mom"), (int, float))]
    vol_list = [x["volatility20"] for x in raw_rows if isinstance(x.get("volatility20"), (int, float))]
    liq_list = [x["liquidity20"] for x in raw_rows if isinstance(x.get("liquidity20"), (int, float)) and x.get("liquidity20", 0) > 0]

    scored_input: list[dict[str, Any]] = []
    for row in raw_rows:
        momentum_score = _avg(
            [
                percentile_rank(row.get("momentum63"), mom63_list),
                percentile_rank(row.get("momentum126"), mom126_list),
            ]
        )
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
                "momentum_score": round(momentum_score, 2),
                "value_score": round(value_score, 2),
                "fundamental_score": round(fundamental_score, 2),
                "risk_control_score": round(risk_control_score, 2),
            }
        )
    return score_candidates(scored_input)


def _to_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _write_bucket_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = [
        "rank",
        "symbol",
        "name",
        "market",
        "industry",
        "total_score",
        "trend_score",
        "momentum_score",
        "value_score",
        "fundamental_score",
        "risk_control_score",
        "close",
        "ret_5d",
        "ret_20d",
        "momentum63",
        "momentum126",
        "sma20",
        "sma60",
        "sma120",
        "ma_stack",
        "rsi14",
        "volatility20",
        "atr14",
        "liquidity20",
        "monthly_revenue",
        "revenue_yoy",
        "revenue_mom",
        "pe",
        "pb",
        "dividend_yield",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _to_csv_value(row.get(k)) for k in headers})


def _build_index_markdown(
    as_of: date,
    top_n: int,
    lookback: int,
    summaries: list[dict[str, Any]],
    warning_count: int,
    output_dir: Path,
) -> str:
    lines = [
        "# 台股全類股 Top100 快照索引",
        "",
        f"- 截止日：`{as_of.isoformat()}`",
        f"- 每類股輸出上限：`{top_n}`",
        f"- 回看日數：`{lookback}`",
        f"- 類股數：`{len(summaries)}`",
        f"- 資料警示數：`{warning_count}`",
        "",
        "## 類股清單",
        "| 類型 | 類股名稱 | 母體檔數 | 分析檔數 | 輸出檔數 | 第一名 | 檔案 |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for item in summaries:
        lines.append(
            "| {bucket_type} | {bucket_name} | {universe_count} | {analyzed_count} | {output_count} | {top_pick} | `{file_name}` |".format(
                bucket_type=item["bucket_type"],
                bucket_name=item["bucket_name"],
                universe_count=item["universe_count"],
                analyzed_count=item["analyzed_count"],
                output_count=item["output_count"],
                top_pick=item["top_pick"] or "-",
                file_name=item["file_path"].relative_to(output_dir).as_posix(),
            )
        )
    return "\n".join(lines) + "\n"


def run(
    as_of: date,
    top_n: int,
    lookback: int,
    timeout: float,
    min_monthly_revenue: float,
    industry_min_count: int,
    max_symbols_per_bucket: int,
    bucket_types: set[str],
    include_buckets: set[str],
    output_dir: Path,
) -> tuple[Path, Path]:
    provider = TwMarketProvider(timeout=timeout)
    warnings: list[str] = []

    buckets: list[dict[str, Any]] = []
    if "theme" in bucket_types:
        for theme in available_themes():
            theme_universe = provider.load_theme_universe(theme, min_monthly_revenue=min_monthly_revenue)
            if theme_universe:
                buckets.append({"bucket_type": "theme", "bucket_name": theme, "universe": theme_universe})
    if "industry" in bucket_types:
        industry_universes = provider.load_industry_universes(
            min_monthly_revenue=min_monthly_revenue,
            min_count=industry_min_count,
        )
        for industry, sector_universe in industry_universes.items():
            buckets.append({"bucket_type": "industry", "bucket_name": industry, "universe": sector_universe})

    if not buckets:
        raise RuntimeError("沒有可分析的類股池，請調整參數。")

    if include_buckets:
        buckets = [
            bucket
            for bucket in buckets
            if f"{bucket['bucket_type']}:{bucket['bucket_name']}" in include_buckets
        ]
        if not buckets:
            raise RuntimeError("include-buckets 未命中任何類股，請檢查名稱是否正確。")

    date_tag = as_of.strftime("%Y%m%d")
    batch_dir = output_dir / f"sector-top100-{date_tag}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    master_csv_path = output_dir / f"sector-top100-master-{date_tag}.csv"
    index_md_path = output_dir / f"sector-top100-index-{date_tag}.md"

    master_headers = [
        "bucket_type",
        "bucket_name",
        "rank",
        "symbol",
        "name",
        "market",
        "industry",
        "total_score",
        "trend_score",
        "momentum_score",
        "value_score",
        "fundamental_score",
        "risk_control_score",
        "close",
        "ret_5d",
        "ret_20d",
        "momentum63",
        "momentum126",
        "ma_stack",
        "rsi14",
        "volatility20",
        "monthly_revenue",
        "revenue_yoy",
        "revenue_mom",
    ]
    summaries: list[dict[str, Any]] = []
    metrics_cache: dict[tuple[str, str], dict[str, Any] | None] = {}

    with master_csv_path.open("w", encoding="utf-8-sig", newline="") as master_handle:
        master_writer = csv.DictWriter(master_handle, fieldnames=master_headers)
        master_writer.writeheader()

        sorted_buckets = sorted(
            buckets,
            key=lambda item: (item["bucket_type"], -len(item["universe"]), str(item["bucket_name"])),
        )
        for idx, bucket in enumerate(sorted_buckets, start=1):
            bucket_type = str(bucket["bucket_type"])
            bucket_name = str(bucket["bucket_name"])
            original_universe = list(bucket["universe"])
            if max_symbols_per_bucket > 0:
                candidates = original_universe[:max_symbols_per_bucket]
            else:
                candidates = original_universe

            raw_rows: list[dict[str, Any]] = []
            for candidate in candidates:
                cache_key = (candidate["symbol"], candidate["market"])
                if cache_key not in metrics_cache:
                    metrics_cache[cache_key] = _build_metrics(
                        provider=provider,
                        candidate=candidate,
                        as_of=as_of,
                        lookback=lookback,
                        warnings=warnings,
                    )
                metrics = metrics_cache[cache_key]
                if metrics is None:
                    continue
                raw_rows.append({**candidate, **metrics})

            ranked = _score_rows(raw_rows)
            top_rows = [{**row, "rank": rank} for rank, row in enumerate(ranked[:top_n], start=1)]

            slug = _slug(f"{bucket_type}-{bucket_name}")
            bucket_file_path = batch_dir / f"{slug}.csv"
            _write_bucket_csv(bucket_file_path, top_rows)

            top_pick = ""
            if top_rows:
                top_pick = f"{top_rows[0]['symbol']} {top_rows[0]['name']}"

            summaries.append(
                {
                    "bucket_type": bucket_type,
                    "bucket_name": bucket_name,
                    "universe_count": len(original_universe),
                    "analyzed_count": len(raw_rows),
                    "output_count": len(top_rows),
                    "top_pick": top_pick,
                    "file_path": bucket_file_path,
                }
            )

            for row in top_rows:
                payload = {key: _to_csv_value(row.get(key)) for key in master_headers}
                payload["bucket_type"] = bucket_type
                payload["bucket_name"] = bucket_name
                master_writer.writerow(payload)

            print(
                "[{}/{}] {}:{} universe={} analyzed={} out={}".format(
                    idx,
                    len(sorted_buckets),
                    bucket_type,
                    bucket_name,
                    len(original_universe),
                    len(raw_rows),
                    len(top_rows),
                )
                ,
                flush=True,
            )

    index_content = _build_index_markdown(
        as_of=as_of,
        top_n=top_n,
        lookback=lookback,
        summaries=summaries,
        warning_count=len(warnings),
        output_dir=output_dir,
    )
    index_md_path.write_text(index_content, encoding="utf-8")
    if warnings:
        warning_path = output_dir / f"sector-top100-warnings-{date_tag}.txt"
        warning_path.write_text("\n".join(warnings), encoding="utf-8")

    return index_md_path, master_csv_path


def main() -> int:
    args = parse_args()
    if args.lookback < 127:
        print("[sector-top100] error: --lookback 需 >= 127")
        return 1
    try:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    except ValueError:
        print("[sector-top100] error: --as-of 格式需為 YYYY-MM-DD")
        return 1

    bucket_types = {x.strip() for x in str(args.bucket_types).split(",") if x.strip()}
    invalid_types = bucket_types - {"theme", "industry"}
    if invalid_types:
        print(f"[sector-top100] error: bucket type 不支援 {sorted(invalid_types)}")
        return 1

    try:
        index_path, master_path = run(
            as_of=as_of,
            top_n=args.top_n,
            lookback=args.lookback,
            timeout=args.timeout,
            min_monthly_revenue=args.min_monthly_revenue,
            industry_min_count=args.industry_min_count,
            max_symbols_per_bucket=args.max_symbols_per_bucket,
            bucket_types=bucket_types,
            include_buckets={x.strip() for x in str(args.include_buckets).split(",") if x.strip()},
            output_dir=Path(args.output_dir),
        )
        print(f"[sector-top100] index: {index_path}")
        print(f"[sector-top100] master: {master_path}")
        return 0
    except Exception as exc:
        print(f"[sector-top100] error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
