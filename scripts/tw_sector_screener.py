from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.factors import (
    atr_wilder,
    momentum_return,
    percentile_rank,
    position_plan,
    rsi_wilder,
    sma,
    trend_score,
    volatility_annualized,
)
from src.analysis.scoring import score_candidates
from src.providers.tw_market_provider import TwMarketProvider
from src.report.render_markdown import build_report_filename, render_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台股類股選股 Skill CLI")
    parser.add_argument("--theme", required=True, help="類股主題，例如 半導體 / AI / 記憶體")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="分析截止日 (YYYY-MM-DD)")
    parser.add_argument("--top-n", type=int, default=10, help="輸出前 N 檔")
    parser.add_argument("--universe-limit", type=int, default=60, help="最多分析多少檔候選股")
    parser.add_argument("--min-monthly-revenue", type=float, default=0.0, help="最低月營收門檻（元）")
    parser.add_argument("--lookback", type=int, default=252, help="歷史回看日數")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP 逾時秒數")
    parser.add_argument("--output-dir", default=str(ROOT_DIR / "output"), help="報告輸出目錄")
    return parser.parse_args()


def _avg(values: list[float | None], default: float = 50.0) -> float:
    valid = [float(v) for v in values if isinstance(v, (int, float))]
    if not valid:
        return default
    return sum(valid) / len(valid)


def _reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if row.get("trend_score", 0.0) >= 70:
        reasons.append("均線結構偏多")
    if row.get("momentum_score", 0.0) >= 70:
        reasons.append("63/126 日動能在同類股前段")
    if row.get("value_score", 0.0) >= 65:
        reasons.append("估值在同類股相對合理")
    if row.get("fundamental_score", 0.0) >= 60:
        reasons.append("營收成長分位偏高")
    if row.get("risk_control_score", 0.0) >= 60:
        reasons.append("波動與流動性平衡較佳")
    return reasons[:3] or ["分數來自多因子綜合排序，非單一指標。"]


def run(
    theme: str,
    as_of: date,
    top_n: int,
    universe_limit: int,
    min_monthly_revenue: float,
    lookback: int,
    timeout: float,
    output_dir: Path,
) -> Path:
    provider = TwMarketProvider(timeout=timeout)
    warnings: list[str] = []
    universe = provider.load_theme_universe(theme, min_monthly_revenue=min_monthly_revenue)
    if not universe:
        raise RuntimeError(f"找不到主題 {theme} 的候選股")

    raw_rows: list[dict[str, Any]] = []
    for candidate in universe[:universe_limit]:
        symbol = candidate["symbol"]
        market = candidate["market"]
        try:
            candles = provider.get_ohlcv(symbol, market, as_of=as_of, lookback=lookback)
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
        liquidity20 = 0.0
        if len(closes) >= 20 and len(volumes) >= 20:
            liquidity20 = sum(closes[-20 + i] * volumes[-20 + i] for i in range(20)) / 20.0
        valuation = provider.get_latest_valuation(symbol, market, as_of) or {}
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
                "liquidity20": liquidity20,
                "pe": valuation.get("pe"),
                "pb": valuation.get("pb"),
                "dividend_yield": valuation.get("dividend_yield"),
                "trend_score": trend_score(close, sma20, sma60, sma120, rsi14),
            }
        )

    if not raw_rows:
        raise RuntimeError("候選股資料抓取失敗，無法評分")

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
                (100.0 - percentile_rank(row.get("volatility20"), vol_list)) if vol_list else None,
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

    ranked = score_candidates(scored_input)
    picks: list[dict[str, Any]] = []
    for idx, row in enumerate(ranked[:top_n], start=1):
        position = position_plan(
            score=float(row["total_score"]),
            close=float(row.get("close") or 0.0),
            atr14=row.get("atr14"),
            volatility20=row.get("volatility20"),
        )
        picks.append(
            {
                "rank": idx,
                "symbol": row["symbol"],
                "name": row["name"],
                "market": row["market"],
                "close": row["close"],
                "total_score": row["total_score"],
                "reasons": _reasons(row),
                "position": position,
            }
        )

    avg_score = sum(x["total_score"] for x in picks) / len(picks) if picks else 0.0
    summary = (
        f"{theme} 類股共評分 {len(ranked)} 檔，前 {len(picks)} 檔平均分數 {avg_score:.1f}。"
        f"第一名 {picks[0]['symbol']} {picks[0]['name']}，但仍需分批與風險控管。"
        if picks
        else f"{theme} 類股沒有可輸出的結果。"
    )

    method = [
        "共識 1：Momentum（63/126 日）在中期選股有實務效度，採同類股分位而非固定門檻。",
        "共識 2：Value（PE/PB/殖利率）需做產業內相對比較，避免跨產業失真。",
        "共識 3：Fundamental 以月營收年增/月增作為短中期基本面濾網。",
        "共識 4：Risk overlay（波動與流動性）用來限制倉位，不和趨勢訊號互相覆蓋。",
    ]
    risks = [
        "這是研究排序，不是保證報酬；請搭配你自己的交易系統與停損規則。",
        "題材股輪動快，若量縮或跌破關鍵均線，需主動減碼。",
    ]
    if warnings:
        risks.append(f"資料警示：{len(warnings)} 檔抓取失敗，結果可能有抽樣偏誤。")

    report = render_report(
        {
            "theme": theme,
            "as_of": as_of,
            "summary": summary,
            "method": method,
            "picks": picks,
            "risks": risks,
            "sources": ["TWSE OpenAPI", "TWSE exchangeReport", "TPEx OpenAPI", "TPEx afterTrading API"],
        }
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / build_report_filename(theme, as_of)
    output_path.write_text(report, encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    try:
        as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
        output = run(
            theme=args.theme.strip(),
            as_of=as_of,
            top_n=args.top_n,
            universe_limit=args.universe_limit,
            min_monthly_revenue=args.min_monthly_revenue,
            lookback=args.lookback,
            timeout=args.timeout,
            output_dir=Path(args.output_dir),
        )
        print(f"[tw-sector-screener] report: {output}")
        return 0
    except Exception as exc:
        print(f"[tw-sector-screener] error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
