---
name: tw-sector-screener
description: Use when screening Taiwan sector/theme stocks and producing research-grade ranked ideas with confidence, action views, structured outputs, watchlists, and audit trails.
---

# TW Sector Screener

用免費資料源做台股題材/類股研究初篩，輸出可追溯的 `idea ranking + action view`，而不是把一個分數硬扮成投資決策。

## Use This Skill

適用：
- 想看整個題材或子題材哪些股票值得先研究
- 需要 `Markdown + JSON + CSV + audit trail + watchlist`
- 需要把研究排序遷入日常 coverage / rerank 流程

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
- `B / Validation V2`：已升級 factor-aware validation
- `C / D / E`：仍待後續優化

## Command

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --theme-mode strict `
  --benchmark TAIEX `
  --as-of 2026-03-12 `
  --top-n 8 `
  --run-backtest `
  --quality-update-mode auto `
  --quality-update-budget-sec 3 `
  --quality-history-depth 8 `
  --output-format md,json,csv `
  --coverage-list "%USERPROFILE%\tw-reports\coverage-list.txt" `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

季度快照刷新：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\refresh_quarterly_snapshots.py" `
  --as-of 2026-03-12 `
  --theme-mode strict `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

歷史回補：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\backfill_quarterly_history.py" `
  --as-of 2026-03-12 `
  --themes AI,半導體 `
  --periods 8 `
  --batch-size 20 `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

全類股 Top100 快照：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_universe_top100.py" `
  --as-of 2026-03-12 `
  --top-n 100 `
  --lookback 160 `
  --bucket-types theme,industry `
  --max-symbols-per-bucket 160 `
  --output-dir "%USERPROFILE%\tw-sector-screener-output"
```

## Parameters

- `--theme`：類股/主題
- `--theme-mode`：`strict` / `broad`
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
- `--top-n`
- `--universe-limit`
- `--min-monthly-revenue`
- `--lookback`
- `--output-root`
- `--output-dir`（deprecated）

## Output Contract

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`

報告至少要能回答：
- 哪些標的應先研究
- 結論可信度有多高
- 為什麼現在能看
- 為什麼不能太衝
- 何時加碼
- 何時減碼

## Notes

- 預設用 `strict` 題材池，避免 AI 題材被 telecom / panel 類 proxy 污染。
- 缺值會直接反映在 `confidence_score` 與 `data_quality_flags`，不再默默補中性分。
- `confidence_score` 現在拆成 `factor_coverage_confidence` 與 `data_freshness_confidence`。
- `quality_score` 目前採官方最新季抓取 + SQLite append-only 歷史累積。
- `idea score` 是研究優先序；`action view` 才是部位動作。
- repo 以 `Feature Branch + PR` 維護，分支名稱固定使用 `codex/` 前綴。
- 官方執行輸出固定放在 `%USERPROFILE%\tw-sector-screener-output`，不進 git；repo 內只保留 `examples/sample-reports/` 樣本。
