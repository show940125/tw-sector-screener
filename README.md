# TW Sector Screener

[![test](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml/badge.svg)](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml)

`tw-sector-screener` 不是替人下單的黑盒子。  
它做的事比較老實，也比較有用：把台股題材研究的第一輪工作做對，先把母體收乾淨，再把排序、理由、風險、驗證與追蹤一併交代清楚。

這個 repo 的定位，不是「找神股」；是 `research screener + explainable note generator + workflow adapter`。  
若你需要的是研究初篩、主題 rerank、watchlist 追蹤、audit trail 與可讀報告，它有用。若你要盤中訊號、自動交易、完整 sell-side 財務模型，它不做，也不該硬做。

## Why This Exists

市場上不缺會吐分數的腳本，缺的是肯把分數來龍去脈說明白的工具。  
真正能進研究流程的工具，至少要回答五件事：

- 這個題材的母體是什麼
- 哪些標的值得先研究
- 為什麼現在看、又為什麼不能太衝
- 若要加碼或減碼，觸發條件是什麼
- 這套排序是否經得起基本驗證，而不是只看起來像研究

`tw-sector-screener` 就是照這五件事來設計。

## What It Does

- 題材池管理：支援 `strict` / `broad`，並提供 curated theme library
- 研究排序：輸出 `idea_score`，而不是把一個分數假裝成投資結論
- 可解釋動作：輸出 `Overweight / Neutral / Underweight`、`why_now`、`why_not`、`add_trigger`、`trim_trigger`
- 結構化輸出：同時產生 `Markdown / JSON / CSV`
- 工作流支援：提供 `watchlist`、`audit trail`、`validation report`
- 資料品質揭露：拆分 `factor_coverage_confidence` 與 `data_freshness_confidence`
- 本地快取：降低 TWSE / TPEx 重複抓取成本

## What It Does Not Do

- 不做盤中訊號
- 不做自動下單
- 不做 tick 級交易
- 不取代完整財務模型
- 不保證報酬

這些邊界不是保守，是分工。工具的本事，貴在知道自己該做什麼，不該裝什麼。

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

季度品質資料目前採「官方最新季 + 本地快照回補前一期」模式。這種做法誠實，但也有邊界：  
最新季通常拿得到，前一期覆蓋會隨你日常執行與快照累積而變厚。這是現階段已知限制，不是 README 詞藻可以掩蓋的事。

## How To Read The Report

- `Idea Score`: 研究優先序，不是下單按鈕
- `Confidence`: 結論可靠度
- `Factor Coverage / Data Freshness`: 分開看缺值問題與資料新鮮度問題
- `Action View`: `Overweight / Neutral / Underweight`
- `Why Now / Why Not`: 現在能看與不能太衝的理由
- `Add Trigger / Trim Trigger`: 加碼與減碼的執行條件
- `Validation`: 目前排序框架的基本驗證結果
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

這個 repo 現在走正式的 `Feature Branch + PR` 流程。

- `main` 只放可用版本
- 新功能或方法論調整一律從 `codex/` 前綴分支開始
- PR 必須附測試結果
- 若變更 CLI、config 或報告契約，必須同步更新文件與樣本

具體規則見：

- [CONTRIBUTING.md](./CONTRIBUTING.md)
- [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)
- [examples/sample-reports/README.md](./examples/sample-reports/README.md)

## Quality Bar

目前 repo 的最低交付標準：

- `python -m unittest discover -s tests` 必須通過
- `tw-sector-screener-output/` 不進 git
- repo 內只保留人工挑選的 sample reports
- 影響報告契約的變更，必須同步更新 sample reports

## Current Limits

這個 repo 目前最該誠實承認的三個限制是：

- 季度品質資料前期覆蓋仍不夠厚
- validation 仍是 v1，屬於 `price_only_cross_sectional`
- `AI strict` 題材池純度高，但 coverage 仍偏窄

也正因如此，這個工具現在適合做前端漏斗，不適合裝成完整機構研究平台。

## Roadmap

下一階段優先順序已經寫定，不再靠臨時起意：

1. `P0 / Data Quality Hardening`
2. `P1 / Validation V2`
3. `P1 / Theme Coverage Expansion`
4. `P2 / Workflow Deepening`
5. `P2 / Action Engine Upgrade`

詳見 [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)。

## Sample Outputs

repo 內只追蹤少量樣本，不追蹤完整執行輸出。  
目前保留的樣本見：

- [examples/sample-reports/ai-20260312/report.md](./examples/sample-reports/ai-20260312/report.md)
- [examples/sample-reports/ai-20260312/audit.json](./examples/sample-reports/ai-20260312/audit.json)
- [examples/sample-reports/ai-20260312/validation.json](./examples/sample-reports/ai-20260312/validation.json)

## License

見 [LICENSE](./LICENSE)。
