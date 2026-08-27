from __future__ import annotations

from typing import Any


def _percent(value: Any) -> str:
    return f"{value:.2f}%" if isinstance(value, (int, float)) else "N/A"


def _number(value: Any) -> str:
    return f"{value:.4f}" if isinstance(value, (int, float)) else "N/A"


def render_validation_markdown(theme: str, as_of: str, validation: dict[str, Any]) -> str:
    """Render a research-only human reading layer for a validation JSON payload."""
    metrics = validation.get("metrics") or {}
    diagnostics = metrics.get("portfolio_diagnostics") or {}
    limitations = validation.get("limitations") or []
    tracking = validation.get("candidate_tracking") or {}
    tracking_rows = tracking.get("rows") or []
    excess = metrics.get("excess_return_pct")
    interpretation = (
        "歷史驗證相對基準呈現正超額，但結果應視為研究訊號，需配合當前資料品質與風險條件覆核。"
        if isinstance(excess, (int, float)) and excess > 0
        else "歷史驗證未顯示正超額；不應據此提高研究優先序或風險承擔。"
    )
    limitation_lines = "\n".join(f"- {item}" for item in limitations) or "- N/A"
    tracking_lines = "\n".join(
        f"| {row.get('rank', 'N/A')} | {row.get('symbol', 'N/A')} {row.get('name', '')} | {_percent(row.get('return_20d_pct'))} | {_percent(row.get('return_60d_pct'))} | {_percent(row.get('return_120d_pct'))} | {_percent(row.get('return_252d_pct'))} | {_percent(row.get('relative_to_taiex_20d_pct'))} | {_percent(row.get('annualized_volatility_pct'))} | {_percent(row.get('max_drawdown_pct'))} | {row.get('data_start', 'N/A')} to {row.get('data_end', 'N/A')} | {', '.join(row.get('data_gaps') or []) or 'none'} |"
        for row in tracking_rows
    ) or "| N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |"
    return f"""# {theme} 驗證回測解讀

> 分析日：`{as_of}`。本文件由同名 validation JSON 產生，為研究解讀，不構成交易指令、下單建議或模擬器輸入。

## 方法

- mode：`{validation.get('mode', 'N/A')}`；base mode：`{validation.get('base_mode', 'N/A')}`
- window：`{validation.get('window', 'N/A')}`；rebalance：`{validation.get('rebalance', 'N/A')}`；cost：`{validation.get('cost_bps', 'N/A')}` bps

## 主要結果

| 指標 | 數值 |
| --- | ---: |
| 策略總報酬 | {_percent(metrics.get('strategy_total_return_pct'))} |
| 基準總報酬 | {_percent(metrics.get('benchmark_total_return_pct'))} |
| 超額報酬 | {_percent(excess)} |
| 最大回撤 | {_percent(metrics.get('max_drawdown_pct'))} |
| 年化波動 | {_percent(metrics.get('annualized_volatility_pct'))} |
| 命中率 | {_number(metrics.get('hit_rate'))} |
| 換手率 | {_percent(metrics.get('turnover_pct'))} |
| 再平衡次數 | {metrics.get('rebalance_count', 'N/A')} |

## 風險診斷

- status：`{diagnostics.get('status', 'N/A')}`；VaR 95：{_percent(diagnostics.get('var95_pct'))}；CVaR 95：{_percent(diagnostics.get('cvar95_pct'))}
- Ulcer Index：{_number(diagnostics.get('ulcer_index'))}；Omega：{_number(diagnostics.get('omega_ratio'))}；Beta：{_number(diagnostics.get('beta'))}

## 本日候選 Top {tracking.get('candidate_count', 0)} 個股歷史追蹤

> `{tracking.get('mode', 'N/A')}`：此表只回顧今天入選候選的既有價格歷史，並非把今天排名套回過去的無前視偏誤驗證。

| Rank | 個股 | 20日 | 60日 | 120日 | 252日 | 相對TAIEX 20日 | 年化波動 | 最大回撤 | 資料期間 | 資料缺口 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
{tracking_lines}

- 追蹤限制：{tracking.get('limitation', 'N/A')}

## 解讀

{interpretation}

## 限制與使用邊界

{limitation_lines}

回測使用歷史資料，不能保證未來表現；閱讀時應優先檢查資料新鮮度、樣本期與缺失資料警示。
"""
