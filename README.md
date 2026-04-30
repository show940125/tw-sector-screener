# TW Sector Screener

[![test](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml/badge.svg)](https://github.com/show940125/tw-sector-screener/actions/workflows/test.yml)

`tw-sector-screener` 是一個台股主題研究的初篩工具。  
它處理的核心問題，是把一整個題材裡原本混在一起的股票，整理成一份可以拿來研究、比較、追蹤的候選名單。

如果用一句話說，它做的是這件事：  
先替你決定「這個主題該看哪些股票」，再替你整理「先看誰、理由是什麼、風險在哪裡、後續怎麼追」。

這個工具面對的典型情境，是研究一個題材時，手上先有一個大方向，例如 `AI`、`半導體`、`記憶體`，但還不知道該從哪幾檔開始。  
它會先建立題材母體，再用公開資料整理價格動能、基本面、品質指標、相對大盤與相對同題材表現，最後輸出一份有排序、有理由、有風險提示的報告。

輸出的結果，不是一串裸分數。  
它會給你：

- 候選清單
- 研究優先順序
- `買入 / 持有 / 賣出` 研究建議評估
- 每檔股票入選的主要理由
- 加碼與減碼的參考條件
- confidence / risk / evidence refs
- 資料完整度與可信度
- audit trail 與 validation 摘要

因此，這個 repo 最適合拿來做研究工作的第一步。  
你可以把它當成每天先跑一次的題材雷達，用來縮小範圍、排出先後、追蹤變化，然後再決定哪些標的值得進一步做深入研究。

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
- 可解釋動作：輸出 `買入 / 持有 / 賣出` 研究建議評估，以及 `Overweight / Neutral / Underweight`、`why_now`、`why_not`、`add_trigger`、`trim_trigger`
- 決策風險層：每檔候選輸出 `confidence_score`、`risk_score`、`target_range`、失效條件與 evidence refs
- 可選 LLM review：支援 `llm-review` 模式，失敗或格式不合時回落 deterministic recommendation
- 結構化輸出：同時產生 `Markdown / JSON / CSV`
- 工作流支援：提供 `watchlist`、`audit trail`、`validation report`
- 決策紀錄：輸出 `decision-review` JSON 與 SQLite decision ledger
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
- `買入 / 持有 / 賣出` 建議分布
- benchmark-relative 視角
- 倉位建議、加減碼條件與失效條件
- validation 摘要
- audit trail

## Current Build Status

本版已把排序、資料品質、validation 與研究建議評估接起來。排序仍由 deterministic factor engine 主導；recommendation decision layer 只負責把候選標的翻成後續研究動作，不反向改寫排名。

### A / Data Quality Hardening

- 已建立 SQLite 季度資料層，路徑固定在官方 output root 下的 `cache/market/quarterly_fundamentals.sqlite`
- 已加入季度刷新工具與 `quality_coverage_summary`
- 已加入歷史季度回補 CLI，並支援近 8 季 history coverage 統計
- 報告與 audit 會直接揭露當期與前期品質資料覆蓋率，以及所用的季度 store 路徑

### B / Validation V2

- validation 已升級為 `factor_aware_cross_sectional_v2`
- 固定輸出 `1Y / 3Y / 5Y` 視窗
- 已提供 `price / fundamental / quality` factor sleeves

### C / Recommendation Decision Layer

- 每檔 candidate 會輸出 `買入 / 持有 / 賣出`
- 新增 `risk_score`、`action_view`、`target_range`、`position_note`、`invalidation_conditions`、`evidence_refs`
- `reports`、`audit`、`watchlist`、`decision-review` 與 SQLite decision ledger 都會保留 recommendation 欄位
- `--recommendation-mode deterministic` 可獨立運作；`--recommendation-mode llm-review` 可做可選反方檢查與風險補強
- LLM review 不覆蓋原始分數與資料；若輸出格式錯誤、證據不足或 risk gate 失敗，會回落或降級為 deterministic / 持有

### Pending

以下部分仍待後續優化：

- `D / Theme Coverage Expansion`
- `E / Workflow Deepening`
- `F / Action Engine Upgrade` 的事件狀態機仍可再深化

## Quick Start

核心 screener 無需額外 API key，主要依賴官方公開資料源。

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --theme-mode strict `
  --benchmark TAIEX `
  --as-of 2026-03-12 `
  --top-n 8 `
  --run-backtest `
  --validation-window 1y `
  --quality-update-mode auto `
  --quality-update-budget-sec 3 `
  --quality-history-depth 8 `
  --recommendation-mode deterministic `
  --output-format md,json,csv `
  --coverage-list "%USERPROFILE%\tw-reports\coverage-list.txt"
```

預設官方輸出根目錄：

- `%USERPROFILE%\tw-sector-screener-output`

主要輸出結構：

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`
- `decisions/<theme>/decision-review-<theme>-<yyyymmdd>.json`
- `decision-ledger.sqlite`

LLM review 範例：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --theme-mode strict `
  --as-of 2026-03-12 `
  --top-n 8 `
  --recommendation-mode llm-review `
  --review-top-n 8 `
  --llm-provider openai `
  --llm-model gpt-4o-mini
```

全類股 Top100 批次快照：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_universe_top100.py" `
  --as-of 2026-03-12 `
  --top-n 100 `
  --lookback 160 `
  --bucket-types theme,industry `
  --max-symbols-per-bucket 160
```

季度快照刷新與覆蓋率摘要：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\refresh_quarterly_snapshots.py" `
  --as-of 2026-03-12 `
  --theme-mode strict
```

歷史季度回補：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\backfill_quarterly_history.py" `
  --as-of 2026-03-12 `
  --themes AI,半導體 `
  --periods 8 `
  --batch-size 20
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
- `--quality-update-mode`: `auto` / `skip` / `force`
- `--quality-update-budget-sec`: 前台更新檢查延遲預算
- `--quality-history-depth`: history coverage 目標季數
- `--recommendation-mode`: `deterministic` / `llm-review` / `off`
- `--review-top-n`: `llm-review` 模式下標記審查的前 N 檔
- `--llm-provider`: `openai` / `openrouter` / `local` / `custom`
- `--llm-model`: LLM review 使用的模型名稱
- `--decision-ledger`: SQLite 決策紀錄路徑
- `--no-target-price`: 關閉目標區間推估
- `--output-root`: 官方輸出根目錄
- `--output-dir`: deprecated alias，保留相容

`config.example.json` 可作為自訂權重與 benchmark 的起點，見 [config.example.json](./config.example.json)。

## Data Sources

目前資料來源以官方公開資料為主：

- TWSE OpenAPI
- TWSE `exchangeReport`
- TPEx OpenAPI
- TPEx `afterTrading` API

季度品質資料目前採「官方最新季抓取 + SQLite append-only 歷史累積」模式。  
最新季通常拿得到；前一期與更早期的覆蓋會隨日常刷新逐步變厚。這是現階段的真實限制，文件就該老實寫。

## How To Read The Report

- `Idea Score`: 研究優先序
- `Confidence`: 結論可靠度
- `Factor Coverage / Data Freshness`: 一個看缺值，一個看資料新鮮度
- `Action View`: `Overweight / Neutral / Underweight`
- `Recommendation`: `買入 / 持有 / 賣出` 研究建議評估
- `Risk Score`: 波動、趨勢破壞、估值過熱、資料缺口與 risk gate 的綜合風險
- `Evidence Refs`: recommendation 使用到的結構化證據路徑
- `Why Now / Why Not`: 現在能看與需要保守的理由
- `Add Trigger / Trim Trigger`: 加碼與減碼條件
- `Decision Ledger`: 記錄每次建議、信心、風險、失效條件與 evidence refs
- `Validation`: 目前排序框架的驗證結果
- `Audit`: 本次參數、資料來源、警示與快取路徑
- `History Coverage`: 近 8 季完整覆蓋程度

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
- recommendation 相關變更需保留 deterministic mode 可獨立運作
- LLM review 失敗時 CLI 不應失敗，報告需回落 deterministic recommendation
- `tw-sector-screener-output/` 不進 git
- repo 內只保留人工挑選的 sample reports
- 影響報告契約的變更，需同步更新 sample reports

## Current Limits

目前仍有幾個明顯限制：

- 季度品質資料前期覆蓋仍薄
- validation 雖已升級，基本面與品質因子仍偏快照型
- `AI strict` 題材池純度高，coverage 仍偏窄
- LLM review 是可選研究層，不是資料源；沒有 evidence refs 的主張不應升級 recommendation
- target range 會在資料不足時輸出 `null`，不硬編目標價

因此，這個工具適合做研究前端漏斗，離完整機構研究平台還有一段路。

## Roadmap

下一階段優先順序如下：

1. `D / Theme Coverage Expansion`
2. `E / Workflow Deepening`
3. `F / Action Engine Upgrade`

詳見 [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)。

## Sample Outputs

repo 內只追蹤少量樣本，不追蹤完整執行輸出。  
目前保留的樣本見：

- [examples/sample-reports/ai-20260312/report.md](./examples/sample-reports/ai-20260312/report.md)
- [examples/sample-reports/ai-20260312/audit.json](./examples/sample-reports/ai-20260312/audit.json)
- [examples/sample-reports/ai-20260312/validation.json](./examples/sample-reports/ai-20260312/validation.json)

## License

見 [LICENSE](./LICENSE)。
