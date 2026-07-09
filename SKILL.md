---
name: tw-sector-screener
description: Use when screening Taiwan sector/theme stocks and producing research-grade ranked ideas with confidence, action views, structured outputs, watchlists, and audit trails.
---

# TW Sector Screener

用免費資料源做台股題材/類股研究初篩，輸出可追溯的 `buying ranking + actionable queue + watchlist + research list + action view`，而不是把一個分數硬扮成投資決策。
現在每檔候選會另外輸出 `買入 / 持有 / 賣出` 研究建議評估；買進榜與研究榜拆開，避免把研究優先序誤讀成買進優先序。

## Use This Skill

適用：
- 想看整個題材或子題材哪些股票值得先研究
- 需要 `Markdown + JSON + CSV + audit trail + watchlist`
- 需要把買進排序、可行動候選、追蹤清單、研究排序遷入日常 coverage / rerank 流程

不適用：
- 即時下單或自動交易
- Tick 級或盤中訊號
- 完整 sell-side 財報模型替代品

## Data Strategy

資料來源優先：
1. TWSE OpenAPI + exchangeReport
2. TPEx OpenAPI + afterTrading API

目前進度：
- `A / Data Quality Hardening`：已建立 SQLite 季度資料層，並補季度刷新、歷史回補與 quality coverage summary
- `B / Validation V3`：已升級 factor-aware validation，並加入 portfolio diagnostics
- `C / Three-List Output`：已拆出 buying ranking、watchlist 與 research list
- `D / Theme Coverage Expansion`：`AI` / `半導體` 預設使用 `coverage` universe；`core` 僅作高純度追蹤池
- `E / Actionable Queue`：已補決策梯度，讓 `buying_ranking = 0` 時仍能回答下一步動作
- `F / Stock Risk Metrics`：已加入單股 Sharpe / Sortino / drawdown / volatility 與 risk-adjusted score，輔助買進排序與 simulator analysis
- `G / Buying Gate V2`：已把買進榜拆成 `formal_buy`、`risk_adjusted_buy`、`tactical_buy`，讓低風險高 RiskAdj 標的不再被單一 idea 門檻排除

## Command

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --universe-mode coverage `
  --benchmark TAIEX `
  --as-of 2026-04-29 `
  --top-n 20 `
  --run-backtest `
  --quality-update-mode auto `
  --quality-update-budget-sec 3 `
  --quality-history-depth 8 `
  --recommendation-mode deterministic `
  --output-format md,json,csv `
  --coverage-list "%USERPROFILE%\tw-reports\coverage-list.txt" `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

季度快照刷新：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\refresh_quarterly_snapshots.py" `
  --as-of 2026-04-29 `
  --theme-mode strict `
  --themes AI,半導體 `
  --universe-mode coverage `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

歷史回補：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\backfill_quarterly_history.py" `
  --as-of 2026-04-29 `
  --themes AI,半導體 `
  --periods 8 `
  --batch-size 20 `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

全類股 Top100 快照：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_universe_top100.py" `
  --as-of 2026-04-29 `
  --top-n 100 `
  --lookback 160 `
  --bucket-types theme,industry `
  --max-symbols-per-bucket 160 `
  --output-dir "%USERPROFILE%\tw-sector-screener-output"
```

投資模擬器：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_investment_simulator.py" `
  --themes AI,半導體 `
  --universe-mode coverage `
  --start-date 2026-04-01 `
  --end-date 2026-04-29 `
  --initial-cash 1000000 `
  --top-n 20 `
  --recommendation-mode deterministic `
  --analysis-cache reuse `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

每日自動化模式：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_investment_simulator.py" `
  --mode daily `
  --daily-analysis-mode same-day `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of today `
  --initial-cash 1000000 `
  --top-n 20 `
  --recommendation-mode deterministic `
  --analysis-cache reuse `
  --config "%USERPROFILE%\.codex\skills\tw-sector-screener\simulator.config.example.json" `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

## Parameters

- `--theme`：類股/主題
- `--universe-mode`：`core` / `coverage` / `broad`，預設 `coverage`
- `--theme-mode`：deprecated legacy option；`strict` 會映射到 `core`，同時指定時以 `--universe-mode` 為準
- `--benchmark`：`TAIEX` / `sector` / `custom`
- `--output-format`：`md,json,csv`
- `--config`：JSON / YAML config
- `--coverage-list`：watchlist symbol 清單
- `--run-backtest`
- `--rebalance`
- `--cost-bps`
- `--validation-window`
- `--quality-update-mode`
- `--quality-update-budget-sec`
- `--quality-history-depth`
- `--recommendation-mode`：`deterministic | llm-review | off`
- `--review-top-n`：`llm-review` metadata 標記前 N 檔
- `--llm-provider` / `--llm-model`：預留給 deep review metadata；目前失敗會回落 deterministic review
- `--decision-ledger`：SQLite 決策紀錄路徑
- `--no-target-price`：關閉目標區間推估
- `--top-n`
- `--universe-limit`
- `--min-monthly-revenue`
- `--lookback`
- `--output-root`
- `--output-dir`（deprecated）

投資模擬器參數：
- `--mode`：`historical | daily | historical-plus-daily`
- `--themes`：預設 `AI,半導體`
- `--universe-mode`：`core | coverage | broad`，預設 `coverage`
- `--as-of`：可用 `YYYY-MM-DD` 或 `today`
- `--initial-cash`：每個 portfolio 初始資金
- `--analysis-cache`：`reuse | refresh`
- `--daily-analysis-mode`：`prior-close | same-day`；16:30 盤後自動化固定用 `same-day` 兩段式流程：先執行前一交易日決策在今日的成交，再用今日收盤資料產生今日報告與下一交易日 planned orders
- `--config`：交易成本與 `lot_size` 設定；預設 `lot_size=1` 表示零股模式

## Output Contract

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`
- `decisions/<theme>/decision-review-<theme>-<yyyymmdd>.json`
- `decisions/decision-ledger.sqlite`
- `simulations/<run_id>/simulator.sqlite`
- `simulations/<run_id>/dashboard.html`
- `simulations/<run_id>/summary.json`
- `simulations/<run_id>/daily-equity.csv`

`daily-equity.csv` 是從 `simulator.sqlite` 的 `daily_equity` ledger 重新輸出的完整歷史快照；daily rerun 以 `run_id + trade_date + portfolio_id` 取代同日同投組資料，避免 append-only CSV 產生重複列。Dashboard Equity Curve 讀這份完整序列。

Validation v3 contract：
- `validation_summary.mode = validation_report_v3`
- `metrics.portfolio_diagnostics` 包含 VaR / CVaR / Ulcer / Omega / Tail Ratio / rolling metrics / alpha-beta attribution
- `audit.connector_contract_version` 與 `audit.supplementary_connectors` 會揭露補充資料來源
- `reports` JSON 固定包含 `buying_ranking`、`actionable_queue`、`watchlist_candidates`、`research_list`、`picks`
- `reports` JSON 固定包含 `universe_overview`；候選標的會揭露 `theme_buckets`、`primary_bucket`、`coverage_reason` 與 `core_watchlist_member`
- 候選標的固定揭露 `decision_tier`、`actionability_score`、`blocked_by`、`next_action`、`trigger_to_upgrade` 與 `why_not_buy_now`
- 候選標的固定揭露 `stock_risk_metrics` 與 `risk_adjusted_score`
- `picks` 保留為 research top N alias，讓 simulator 不漏掉賣出/降風險訊號
- `audit.ranking_policy_version = tw-three-list-v1`
- `audit.action_queue_policy_version = tw-actionable-queue-v1`
- `audit.stock_risk_metrics_version = stock-risk-v1`

報告至少要能回答：
- 哪些標的應先研究
- 哪些標的現在可買
- 若現在沒有正式買進，下一步最接近能做的是什麼
- 哪些標的需要追蹤、持有管理或降風險
- 結論可信度有多高
- 為什麼現在能看
- 為什麼不能太衝
- 何時加碼
- 何時減碼
- 現在每檔是 `買入 / 持有 / 賣出` 哪一種研究建議評估

## Notes

- 預設用 `coverage` universe 做正式選股；`core` 是高純度追蹤池，`broad` 才做探索式關鍵字擴張。
- 缺值會直接反映在 `confidence_score` 與 `data_quality_flags`，不再默默補中性分。
- `confidence_score` 現在拆成 `factor_coverage_confidence` 與 `data_freshness_confidence`。
- `quality_score` 目前採官方最新季抓取 + SQLite append-only 歷史累積。
- `idea score` 是研究優先序；`action view` 才是部位動作。
- `buyability_score` 是買進優先序；`buying_tier` 是買進資格分層；`idea_score` 是研究優先序。
- `actionability_score` 是「非正式買進但可行動程度」；它只進 actionable queue，不會把標的升級為正式買進。
- `risk_adjusted_score` 來自單股 Sharpe / Sortino / drawdown / volatility，只輔助買進排序，不覆蓋 `idea_score`。
- `recommendation` 是研究建議評估；LLM review 不改 deterministic ranking。
- `macro_regime_overlay` 是 supplementary risk overlay，只能影響 risk/action，不得直接升級 ranking。
- 每日 16:30 盤後自動化是兩段式收盤後工作流：前一交易日的 planned orders 在今日執行成交；今日收盤資料再產生當日 `analysis_date` 的 AI / 半導體報告與下一交易日 planned orders。
- daily 模式成交模擬優先使用 execution date 的精確 OHLCV；若個股日線 endpoint 尚未到齊，但同日報告已產生收盤價，可用 `same_day_analysis_close_proxy` 作為 market buy 成交代理價並在 `market_status.execution_price_proxy` 顯示診斷；不得靜默退回舊 K 線。
- repo 以 `Feature Branch + PR` 維護，分支名稱固定使用 `codex/` 前綴。
- 官方執行輸出固定放在 `%USERPROFILE%\tw-sector-screener-output`，不進 git；repo 內只保留 `examples/sample-reports/` 樣本。
