# TW Sector Screener

[![test](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml/badge.svg)](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml)

`tw-sector-screener` 用公開資料整理台股題材研究的第一輪工作。  
它先處理母體，再整理排序、理由、風險、驗證與追蹤，讓研究工作有一個清楚的起點。

這個 repo 的定位很明白：`research screener + explainable note generator + workflow adapter`。  
若你的工作在研究初篩、主題 rerank、watchlist 追蹤、audit trail 與可讀報告，這個工具有其位置。若你的目標在盤中訊號、自動交易或完整 sell-side 財務模型，應另用別的工具。

## Why This Exists

市場上會吐分數的腳本很多，肯把分數來由交代清楚的工具不多。  
能進研究流程的工具，至少要回答幾件事：

- 題材母體是什麼
- 哪些標的應先研究
- 為什麼現在值得看
- 風險在哪裡
- 何時加碼、何時減碼
- 這套排序有沒有經過基本驗證

`tw-sector-screener` 就是照這個次序來安排。

## What It Covers

- 題材池管理：支援 `strict` / `broad`，並提供 curated theme library
- 研究排序：輸出 `idea_score`
- 可解釋動作：輸出 `Overweight / Neutral / Underweight`、`why_now`、`why_not`、`add_trigger`、`trim_trigger`
- 結構化輸出：同時產生 `Markdown / JSON / CSV`
- 工作流支援：提供 `watchlist`、`audit trail`、`validation report`
- 資料品質揭露：拆分 `factor_coverage_confidence` 與 `data_freshness_confidence`
- 本地快取：降低 TWSE / TPEx 重複抓取成本

## Boundaries

這個 repo 目前不處理以下工作：

- 盤中訊號
- 自動下單
- tick 級交易
- 完整財務模型
- 保證報酬的推論

工具各有分工。把分工說清楚，後面的判斷才會穩。

## Current Capability

目前主題與子題材包含：

- `AI`
- `AI infra`
- `AI server/ODM`
- `半導體`
- `foundry`
- `IC design`
- `memory`

目前報告至少會交付：

- 題材摘要與市場總覽
- 候選清單與排名
- benchmark-relative 視角
- 倉位建議與加減碼條件
- validation 摘要
- audit trail

## Current Build Status

本版先把最關鍵的兩件事往前推了一步。

### A / Data Quality Hardening

- 已加入季度快照刷新工具
- 已加入 `quality_coverage_summary`
- 報告與 audit 會直接揭露當期與前期品質資料覆蓋率

### B / Validation V2

- validation 已升級為 `factor_aware_cross_sectional_v2`
- 固定輸出 `1Y / 3Y / 5Y` 視窗
- 已提供 `price / fundamental / quality` factor sleeves

### Pending

以下部分仍待後續優化：

- `C / Theme Coverage Expansion`
- `D / Workflow Deepening`
- `E / Action Engine Upgrade`

## Quick Start

核心 screener 無需額外 API key，主要依賴官方公開資料源。

```powershell
python "C:\Users\a0953041880\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --theme-mode strict `
  --benchmark TAIEX `
  --as-of 2026-03-12 `
  --top-n 8 `
  --run-backtest `
  --validation-window 1y `
  --output-format md,json,csv `
  --coverage-list "C:\Users\a0953041880\tw-reports\coverage-list.txt"
```

預設官方輸出根目錄：

- `C:\Users\a0953041880\tw-sector-screener-output`

主要輸出結構：

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`

全類股 Top100 批次快照：

```powershell
python "C:\Users\a0953041880\.codex\skills\tw-sector-screener\scripts\tw_sector_universe_top100.py" `
  --as-of 2026-03-12 `
  --top-n 100 `
  --lookback 160 `
  --bucket-types theme,industry `
  --max-symbols-per-bucket 160
```

季度快照刷新與覆蓋率摘要：

```powershell
python "C:\Users\a0953041880\.codex\skills\tw-sector-screener\scripts\refresh_quarterly_snapshots.py" `
  --as-of 2026-03-12 `
  --theme-mode strict
```

## CLI Surface

核心參數如下：

- `--theme`: 主題名稱
- `--theme-mode`: `strict` / `broad`
- `--benchmark`: `TAIEX` / `sector` / `custom`
- `--output-format`: `md,json,csv`
- `--config`: JSON / YAML config 路徑
- `--coverage-list`: watchlist symbol 清單，支援 `txt` / `json`
- `--run-backtest`: 產出 validation report
- `--rebalance`: `weekly` / `monthly`
- `--cost-bps`: validation 交易成本
- `--validation-window`: `1y` / `3y` / `5y`
- `--output-root`: 官方輸出根目錄
- `--output-dir`: deprecated alias，保留相容

`config.example.json` 可作為自訂權重與 benchmark 的起點，見 [config.example.json](./config.example.json)。

## Data Sources

目前資料來源以官方公開資料為主：

- TWSE OpenAPI
- TWSE `exchangeReport`
- TPEx OpenAPI
- TPEx `afterTrading` API

季度品質資料目前採「官方最新季 + 本地快照回補前一期」模式。  
最新季通常拿得到；前一期覆蓋會隨日常執行與快照累積漸漸變厚。這是現階段的真實限制，文件應當照實說明。

## How To Read The Report

- `Idea Score`: 研究優先序
- `Confidence`: 結論可靠度
- `Factor Coverage / Data Freshness`: 一個看缺值，一個看資料新鮮度
- `Action View`: `Overweight / Neutral / Underweight`
- `Why Now / Why Not`: 現在能看與需要保守的理由
- `Add Trigger / Trim Trigger`: 加碼與減碼條件
- `Validation`: 目前排序框架的驗證結果
- `Audit`: 本次參數、資料來源、警示與快取路徑

## Repo Layout

```text
tw-sector-screener/
├─ scripts/                  # CLI entrypoints and batch utilities
├─ src/                      # scoring, provider, themes, reporting
├─ tests/                    # unittest suite
├─ docs/                     # roadmap and design/decision docs
├─ examples/sample-reports/  # tracked sample outputs only
├─ .github/                  # CI and PR template
├─ README.md
├─ SKILL.md
└─ CONTRIBUTING.md
```

## Development Workflow

這個 repo 採 `Feature Branch + PR` 流程。

- `main` 只放可用版本
- 新功能或方法論調整一律從 `codex/` 前綴分支開始
- PR 必須附測試結果
- 若變更 CLI、config 或報告契約，需同步更新文件與樣本

具體規則見：

- [CONTRIBUTING.md](./CONTRIBUTING.md)
- [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)
- [examples/sample-reports/README.md](./examples/sample-reports/README.md)

## Quality Bar

目前 repo 的最低交付標準：

- `python -m unittest discover -s tests` 必須通過
- `tw-sector-screener-output/` 不進 git
- repo 內只保留人工挑選的 sample reports
- 影響報告契約的變更，需同步更新 sample reports

## Current Limits

目前仍有三個明顯限制：

- 季度品質資料前期覆蓋仍薄
- validation 雖已升級，基本面與品質因子仍偏快照型
- `AI strict` 題材池純度高，coverage 仍偏窄

因此，這個工具適合做研究前端漏斗，離完整機構研究平台還有一段路。

## Roadmap

下一階段優先順序如下：

1. `C / Theme Coverage Expansion`
2. `D / Workflow Deepening`
3. `E / Action Engine Upgrade`

詳見 [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)。

## Sample Outputs

repo 內只追蹤少量樣本，不追蹤完整執行輸出。  
目前保留的樣本見：

- [examples/sample-reports/ai-20260312/report.md](./examples/sample-reports/ai-20260312/report.md)
- [examples/sample-reports/ai-20260312/audit.json](./examples/sample-reports/ai-20260312/audit.json)
- [examples/sample-reports/ai-20260312/validation.json](./examples/sample-reports/ai-20260312/validation.json)

## License

見 [LICENSE](./LICENSE)。
