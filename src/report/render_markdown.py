from __future__ import annotations

from datetime import date
from typing import Any


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        return f"{value:.{digits}f}"
    return str(value)


def build_report_filename(theme: str, as_of: date) -> str:
    return f"sector-report-{theme}-{as_of.strftime('%Y%m%d')}.md"


def render_report(context: dict[str, Any]) -> str:
    theme = context["theme"]
    as_of = context["as_of"]
    summary = context["summary"]
    method = context.get("method") or []
    picks = context.get("picks") or []
    market = context.get("market_overview") or {}
    sector = context.get("sector_overview") or {}
    risks = context.get("risks") or []
    sources = context.get("sources") or []
    audit = context.get("audit") or {}
    validation = context.get("validation_summary") or {}

    method_lines = "\n".join(f"- {x}" for x in method) or "- N/A"
    risk_lines = "\n".join(f"- {x}" for x in risks) or "- N/A"
    source_lines = "\n".join(f"- {x}" for x in sources) or "- N/A"
    audit_lines = "\n".join(
        [
            f"- theme mode：`{audit.get('theme_mode', 'N/A')}`",
            f"- benchmark：`{audit.get('benchmark', 'N/A')}`",
            f"- output formats：`{','.join(audit.get('output_formats') or []) or 'N/A'}`",
            f"- warnings：`{len(audit.get('warnings') or [])}`",
            f"- output root：`{audit.get('output_root', 'N/A')}`",
        ]
    )
    validation_lines = "- N/A"
    if validation:
        metrics = validation.get("metrics") or {}
        windows = validation.get("windows") or {}
        window_lines: list[str] = []
        for window_name in ["1y", "3y", "5y"]:
            payload = windows.get(window_name) or {}
            if payload.get("status") != "ok":
                window_lines.append(f"- {window_name}：`insufficient_data`")
                continue
            win_metrics = payload.get("metrics") or {}
            window_lines.append(
                f"- {window_name}：excess `{_fmt(win_metrics.get('excess_return_pct'))}`% / drawdown `{_fmt(win_metrics.get('max_drawdown_pct'))}`% / hit `{_fmt(win_metrics.get('hit_rate'), 4)}`"
            )
        validation_lines = "\n".join(
            [
                f"- mode：`{validation.get('mode', 'N/A')}`；window：`{validation.get('window', 'N/A')}`；rebalance：`{validation.get('rebalance', 'N/A')}`；cost `{_fmt(validation.get('cost_bps'))}` bps",
                f"- excess return `{_fmt(metrics.get('excess_return_pct'))}`%；max drawdown `{_fmt(metrics.get('max_drawdown_pct'))}`%；hit rate `{_fmt(metrics.get('hit_rate'), 4)}`",
                f"- factor sleeves：price `{_fmt(((metrics.get('factor_sleeves') or {}).get('price') or {}).get('excess_return_pct'))}`%、fundamental `{_fmt(((metrics.get('factor_sleeves') or {}).get('fundamental') or {}).get('excess_return_pct'))}`%、quality `{_fmt(((metrics.get('factor_sleeves') or {}).get('quality') or {}).get('excess_return_pct'))}`%",
                *window_lines,
            ]
        )

    market_lines = "- N/A"
    if market:
        market_lines = "\n".join(
            [
                f"- 收盤 `{_fmt(market.get('close'))}`，1D {_fmt(market.get('change_points'))} 點 / {_fmt(market.get('change_pct'))}%",
                f"- 報酬：5D {_fmt(market.get('ret_5d'))}%、20D {_fmt(market.get('ret_20d'))}%、63D {_fmt(market.get('ret_63d'))}%、126D {_fmt(market.get('ret_126d'))}%",
                f"- 均線：SMA20 `{_fmt(market.get('sma20'))}`、SMA60 `{_fmt(market.get('sma60'))}`、SMA120 `{_fmt(market.get('sma120'))}`；RSI14 `{_fmt(market.get('rsi14'))}`；趨勢分數 `{_fmt(market.get('trend_score'))}`",
                f"- 來源：{market.get('source') or 'N/A'}",
            ]
        )

    sector_lines = "- N/A"
    if sector:
        weights = sector.get("weights") or {}
        quality = sector.get("quality_coverage_summary") or {}
        weight_text = "、".join(f"{k} {int(float(v) * 100)}%" for k, v in weights.items()) if weights else "N/A"
        sector_lines = "\n".join(
            [
                f"- 評分母體 `{sector.get('universe_count', 'N/A')}` 檔，Top{sector.get('top_n', 'N/A')} 平均 idea score `{_fmt(sector.get('top_avg_idea'))}` / 平均 confidence `{_fmt(sector.get('top_avg_confidence'))}`",
                f"- 因子權重：{weight_text}",
                f"- Benchmark 視角：20D 題材平均 `{_fmt(sector.get('avg_ret_20d'))}`%，相對大盤 `{_fmt(sector.get('avg_rel_to_taiex_20d'))}`%",
                f"- Quality coverage：當期完整 `{_fmt(quality.get('current_complete_pct'))}`%，前期完整 `{_fmt(quality.get('previous_complete_pct'))}`%",
            ]
        )

    rows = []
    action_lines = []
    trend_rows = []
    for item in picks:
        action_view = item.get("action_view") or {}
        benchmark_view = item.get("benchmark_view") or {}
        rows.append(
            "| {rank} | {symbol} | {name} | {idea} | {conf} | {action} | {thesis} | {why_now} | {why_not} |".format(
                rank=item.get("rank", "-"),
                symbol=item.get("symbol", "-"),
                name=item.get("name", "-"),
                idea=_fmt(item.get("idea_score")),
                conf=_fmt(item.get("confidence_score")),
                action=action_view.get("action", "-"),
                thesis=item.get("thesis_summary") or "-",
                why_now=" / ".join(action_view.get("why_now", [])) or "-",
                why_not=" / ".join(action_view.get("why_not", [])) or "-",
            )
        )
        action_lines.append(
            "\n".join(
                [
                    f"- `{item.get('symbol')}` {item.get('name')}：`{action_view.get('action', '-')}`，進場區間 `{_fmt((action_view.get('entry_range') or ['N/A'])[0])}` ~ `{_fmt((action_view.get('entry_range') or ['N/A', 'N/A'])[1])}`",
                    f"  add trigger：{action_view.get('add_trigger') or 'N/A'}",
                    f"  trim trigger：{action_view.get('trim_trigger') or 'N/A'}",
                    f"  data flags：{' / '.join(item.get('data_quality_flags') or []) or 'clean'}",
                ]
            )
        )
        t = item.get("trend") or {}
        if t:
            trend_rows.append(
                "| {rank} | {symbol} | {close} | {ret_20d} | {rel_taiex} | {rel_sector} | {rel_industry} | {rsi14} | {vol20} |".format(
                    rank=item.get("rank", "-"),
                    symbol=item.get("symbol", "-"),
                    close=_fmt(item.get("close")),
                    ret_20d=_fmt(t.get("ret_20d")),
                    rel_taiex=_fmt(benchmark_view.get("rel_to_taiex_20d")),
                    rel_sector=_fmt(benchmark_view.get("rel_to_sector_20d")),
                    rel_industry=_fmt(benchmark_view.get("rel_to_industry_20d")),
                    rsi14=_fmt(t.get("rsi14")),
                    vol20=_fmt(t.get("volatility20")),
                )
            )

    table_body = "\n".join(rows) or "| - | - | - | - | - | - | - | - | - |"
    action_body = "\n".join(action_lines) or "- N/A"
    trend_body = "\n".join(trend_rows) or "| - | - | - | - | - | - | - | - | - |"

    return f"""# 台股類股選股報告

- 主題：`{theme}`
- 截止日：`{as_of.isoformat()}`

## 摘要
{summary}

## 加權總攬（TAIEX）
{market_lines}

## 類股總攬
{sector_lines}

## 方法與共識
{method_lines}

## 候選清單
| 排名 | 代碼 | 名稱 | Idea Score | Confidence | Action | Thesis Summary | Why Now | Why Not |
|---:|---|---|---:|---:|---|---|---|---|
{table_body}

## 前 {len(picks)} 名個股趨勢（Top {len(picks)}）
| 排名 | 代碼 | 收盤 | 20D% | 相對大盤20D | 相對題材20D | 相對產業20D | RSI14 | 波動20% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
{trend_body}

## 倉位建議
{action_body}

## 風險提示
{risk_lines}

## Validation
{validation_lines}

## 資料與流程稽核
{audit_lines}

## 資料來源
{source_lines}
"""
