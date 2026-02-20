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
    risks = context.get("risks") or []
    sources = context.get("sources") or []

    method_lines = "\n".join(f"- {x}" for x in method) or "- N/A"
    risk_lines = "\n".join(f"- {x}" for x in risks) or "- N/A"
    source_lines = "\n".join(f"- {x}" for x in sources) or "- N/A"

    rows = []
    position_lines = []
    for item in picks:
        rows.append(
            "| {rank} | {symbol} | {name} | {market} | {score} | {close} | {reasons} |".format(
                rank=item.get("rank", "-"),
                symbol=item.get("symbol", "-"),
                name=item.get("name", "-"),
                market=item.get("market", "-"),
                score=_fmt(item.get("total_score")),
                close=_fmt(item.get("close")),
                reasons=" / ".join(item.get("reasons", [])) or "-",
            )
        )
        position = item.get("position", {})
        position_lines.append(
            f"- `{item.get('symbol')}` {item.get('name')}：上限 {_fmt(position.get('max_position_pct'))}%、首筆 {_fmt(position.get('initial_position_pct'))}%、單筆風險 {_fmt(position.get('risk_budget_pct'))}%"
        )
        if position.get("stop_price") is not None:
            position_lines.append(
                f"  停損價 {_fmt(position.get('stop_price'))}（距離 {_fmt(position.get('stop_distance_pct'))}%），{position.get('share_formula')}"
            )

    table_body = "\n".join(rows) or "| - | - | - | - | - | - | - |"
    position_body = "\n".join(position_lines) or "- N/A"

    return f"""# 台股類股選股報告

- 主題：`{theme}`
- 截止日：`{as_of.isoformat()}`

## 摘要
{summary}

## 方法與共識
{method_lines}

## 候選清單
| 排名 | 代碼 | 名稱 | 市場 | 總分 | 收盤價 | 人話理由 |
|---:|---|---|---|---:|---:|---|
{table_body}

## 倉位建議
{position_body}

## 風險提示
{risk_lines}

## 資料來源
{source_lines}
"""
