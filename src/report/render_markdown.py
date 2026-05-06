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
    buying_ranking = context.get("buying_ranking") or []
    actionable_queue = context.get("actionable_queue") or []
    watchlist_candidates = context.get("watchlist_candidates") or []
    research_list = context.get("research_list") or picks
    market = context.get("market_overview") or {}
    sector = context.get("sector_overview") or {}
    universe = context.get("universe_overview") or sector
    risks = context.get("risks") or []
    sources = context.get("sources") or []
    audit = context.get("audit") or {}
    validation = context.get("validation_summary") or {}
    macro = context.get("macro_regime_overlay") or {}

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
            f"- quarterly store：`{audit.get('quarterly_store_path', 'N/A')}`；period requirement：`{audit.get('quality_period_requirement', 'N/A')}`；refresh run：`{audit.get('refresh_run_id', 'N/A')}`",
            f"- quality update：mode `{audit.get('quality_update_mode', 'N/A')}` / decision `{audit.get('quality_update_decision', 'N/A')}` / budget `{_fmt(audit.get('quality_update_budget_sec'))}` sec / backfill `{audit.get('backfill_run_id', 'N/A')}`",
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
                f"- portfolio diagnostics：VaR95 `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('var95_pct'))}`%、CVaR95 `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('cvar95_pct'))}`%、Ulcer `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('ulcer_index'))}`、Omega `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('omega_ratio'))}`",
                f"- benchmark attribution：alpha `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('alpha_pct'))}`%、beta `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('beta'))}`、IR `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('information_ratio'))}`、tracking error `{_fmt((metrics.get('portfolio_diagnostics') or {}).get('tracking_error_pct'))}`%",
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
        rec_dist = sector.get("recommendation_distribution") or {}
        tier_dist = sector.get("decision_tier_distribution") or {}
        buying_tier_dist = sector.get("buying_tier_distribution") or {}
        rec_text = "、".join(f"{key} {value}" for key, value in rec_dist.items()) if rec_dist else "N/A"
        tier_text = "、".join(f"{key} {value}" for key, value in tier_dist.items()) if tier_dist else "N/A"
        buying_tier_text = "、".join(f"{key} {value}" for key, value in buying_tier_dist.items()) if buying_tier_dist else "N/A"
        weight_text = "、".join(f"{k} {int(float(v) * 100)}%" for k, v in weights.items()) if weights else "N/A"
        sector_lines = "\n".join(
            [
                f"- 評分母體 `{sector.get('universe_count', 'N/A')}` 檔，Top{sector.get('top_n', 'N/A')} 平均 idea score `{_fmt(sector.get('top_avg_idea'))}` / 平均 confidence `{_fmt(sector.get('top_avg_confidence'))}`",
                f"- 建議評估分布：{rec_text}",
                f"- 買進分層分布：{buying_tier_text}",
                f"- 決策梯度分布：{tier_text}",
                f"- 因子權重：{weight_text}",
                f"- Benchmark 視角：20D 題材平均 `{_fmt(sector.get('avg_ret_20d'))}`%，相對大盤 `{_fmt(sector.get('avg_rel_to_taiex_20d'))}`%",
                f"- Quality coverage：當期完整 `{_fmt(quality.get('current_complete_pct'))}`%，前期完整 `{_fmt(quality.get('previous_complete_pct'))}`%",
                f"- History coverage：近 `{sector.get('history_depth_target', 'N/A')}` 季完整覆蓋 `{_fmt(quality.get('history_complete_pct'))}`%",
            ]
        )

    macro_lines = "- N/A"
    if macro:
        macro_lines = "\n".join(
            [
                f"- regime：`{macro.get('regime', 'N/A')}`；risk level：`{macro.get('risk_level', 'N/A')}`；risk adjustment `{_fmt(macro.get('risk_adjustment'))}`",
                f"- source：`{macro.get('source', 'N/A')}`；tier：`{macro.get('tier', 'supplementary')}`；rank signal：`{macro.get('rank_signal', False)}`",
                f"- evidence refs：{', '.join(macro.get('evidence_refs') or []) or 'N/A'}",
            ]
        )

    universe_lines = "- N/A"
    if universe:
        buckets = universe.get("bucket_counts") or universe.get("universe_bucket_counts") or {}
        bucket_text = "、".join(f"{key} {value}" for key, value in buckets.items()) if buckets else "N/A"
        universe_lines = "\n".join(
            [
                f"- mode：`{universe.get('universe_mode', 'N/A')}`；source：`{universe.get('universe_source', 'curated_theme_library')}`",
                f"- universe size：`{universe.get('universe_size_before_limit', universe.get('universe_count', 'N/A'))}`；ranked `{universe.get('ranked_count', universe.get('universe_count', 'N/A'))}`；limit applied `{universe.get('universe_limit_applied', False)}`",
                f"- buckets：{bucket_text}",
            ]
        )

    def _list_table(items: list[dict[str, Any]], empty_text: str) -> str:
        rows: list[str] = []
        for item in items:
            action_view = item.get("action_view") or {}
            metrics = item.get("stock_risk_metrics") or {}
            rows.append(
                "| {rank} | {symbol} | {name} | {bucket} | {tier} | {buyability} | {risk_adj} | {sharpe} | {sortino} | {maxdd} | {vol} | {idea} | {conf} | {risk} | {rec} | {action} | {reason} | {excluded} |".format(
                    rank=item.get("list_rank", item.get("rank", "-")),
                    symbol=item.get("symbol", "-"),
                    name=item.get("name", "-"),
                    bucket=item.get("primary_bucket") or ",".join(item.get("theme_buckets") or []) or "-",
                    tier=item.get("buying_tier") or "-",
                    buyability=_fmt(item.get("buyability_score")),
                    risk_adj=_fmt(item.get("risk_adjusted_score") or metrics.get("risk_adjusted_score")),
                    sharpe=_fmt(metrics.get("sharpe_ratio")),
                    sortino=_fmt(metrics.get("sortino_ratio")),
                    maxdd=_fmt(metrics.get("max_drawdown_pct")),
                    vol=_fmt(metrics.get("annualized_volatility_pct")),
                    idea=_fmt(item.get("idea_score")),
                    conf=_fmt(item.get("confidence_score")),
                    risk=_fmt(item.get("risk_score")),
                    rec=item.get("recommendation") or "-",
                    action=action_view.get("action", "-"),
                    reason=item.get("monitoring_reason") or item.get("research_reason") or item.get("thesis_summary") or "-",
                    excluded=item.get("exclusion_from_buying_reason") or "-",
                )
            )
        if rows:
            return "\n".join(rows)
        return f"| - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | {empty_text} | - |"

    buying_body = _list_table(buying_ranking, "今日無買進候選")
    def _actionable_table(items: list[dict[str, Any]], empty_text: str) -> str:
        rows: list[str] = []
        for item in items:
            metrics = item.get("stock_risk_metrics") or {}
            rows.append(
                "| {rank} | {symbol} | {name} | {bucket} | {tier} | {score} | {risk_adj} | {sharpe} | {sortino} | {maxdd} | {ready} | {starter} | {next_action} | {why_not} | {trigger} |".format(
                    rank=item.get("list_rank", item.get("rank", "-")),
                    symbol=item.get("symbol", "-"),
                    name=item.get("name", "-"),
                    bucket=item.get("primary_bucket") or ",".join(item.get("theme_buckets") or []) or "-",
                    tier=item.get("decision_tier") or "-",
                    score=_fmt(item.get("actionability_score")),
                    risk_adj=_fmt(item.get("risk_adjusted_score") or metrics.get("risk_adjusted_score")),
                    sharpe=_fmt(metrics.get("sharpe_ratio")),
                    sortino=_fmt(metrics.get("sortino_ratio")),
                    maxdd=_fmt(metrics.get("max_drawdown_pct")),
                    ready=item.get("entry_readiness") or "-",
                    starter=_fmt(item.get("starter_position_pct")),
                    next_action=item.get("next_action") or "-",
                    why_not=item.get("why_not_buy_now") or "-",
                    trigger=item.get("trigger_to_upgrade") or "-",
                )
            )
        if rows:
            return "\n".join(rows)
        return f"| - | - | - | - | - | - | - | - | - | - | - | - | {empty_text} | - | - |"

    actionable_body = _actionable_table(actionable_queue, "今日無可行動候選，空手等待或換主題")
    watchlist_body = _list_table(watchlist_candidates, "今日無追蹤/處理候選")
    research_body = _list_table(research_list, "今日無研究清單")

    action_lines = []
    trend_rows = []
    for item in picks:
        action_view = item.get("action_view") or {}
        benchmark_view = item.get("benchmark_view") or {}
        action_lines.append(
            "\n".join(
                [
                    f"- `{item.get('symbol')}` {item.get('name')}：`{item.get('recommendation', '-')}` / `{action_view.get('action', '-')}`，研究動作 `{item.get('research_action_view', '-')}`，進場區間 `{_fmt((action_view.get('entry_range') or ['N/A'])[0])}` ~ `{_fmt((action_view.get('entry_range') or ['N/A', 'N/A'])[1])}`",
                    f"  target：{_fmt((item.get('target_range') or {}).get('low'))} / {_fmt((item.get('target_range') or {}).get('base'))} / {_fmt((item.get('target_range') or {}).get('high'))}",
                    f"  add trigger：{action_view.get('add_trigger') or 'N/A'}",
                    f"  trim trigger：{action_view.get('trim_trigger') or 'N/A'}",
                    f"  invalidation：{' / '.join(item.get('invalidation_conditions') or []) or 'N/A'}",
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

## Macro Regime Overlay
{macro_lines}

## Coverage Universe
{universe_lines}

## Buying Ranking / 買進優先序
| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
{buying_body}

## Actionable Queue / 可行動候選隊列
這份隊列回答「不能正式買，那現在最接近能做什麼」。它不會把 `賣出` 或 hard blocker 標的包裝成買進。

| 排名 | 代碼 | 名稱 | Bucket | Tier | Actionability | Risk Adj | Sharpe | Sortino | Max DD% | Readiness | 試單x | 下一步 | 為何尚未正式買 | 升級條件 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---|---:|---|---|---|
{actionable_body}

## Watchlist / 追蹤與處理清單
| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
{watchlist_body}

## Research List / 題材研究清單
這份清單是題材內完整研究排序，不等於買進排名。

| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
{research_body}

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
