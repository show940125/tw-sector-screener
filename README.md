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

- Buying Ranking / 買進優先序
- Actionable Queue / 可行動候選隊列
- Watchlist / 追蹤與處理清單
- Research List / 題材研究清單
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

- 題材池管理：支援 `core` / `coverage` / `broad` universe；`coverage` 是正式選股預設，`core` 是高純度追蹤池
- 決策梯度輸出：`buying_ranking` 回答現在可買誰，`actionable_queue` 回答買進榜為 0 時下一步看誰，`watchlist_candidates` 回答哪些要追蹤/處理，`research_list` 保留完整題材研究排序
- 研究排序：輸出 `idea_score`；買進排序另輸出 `buyability_score`
- 可解釋動作：輸出 `買入 / 持有 / 賣出` 研究建議評估，以及 `Overweight / Neutral / Underweight`、`why_now`、`why_not`、`add_trigger`、`trim_trigger`
- 決策風險層：每檔候選輸出 `confidence_score`、`risk_score`、`target_range`、失效條件與 evidence refs
- 可選 LLM review：支援 `llm-review` 模式，失敗或格式不合時回落 deterministic recommendation
- 結構化輸出：同時產生 `Markdown / JSON / CSV`
- 工作流支援：提供 `watchlist`、`audit trail`、`validation report`
- 決策紀錄：輸出 `decision-review` JSON 與 SQLite decision ledger
- 單股風險調整：每檔輸出 Sharpe、Sortino、max drawdown、volatility 與 `risk_adjusted_score`
- 補充資料 contract：外部 connector 只能以 supplementary JSON contract 進入風險 overlay，不直接改寫 ranking
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
- Buying Ranking、Actionable Queue、Watchlist、Research List 四種清單
- `買入 / 持有 / 賣出` 建議分布
- benchmark-relative 視角
- macro regime overlay
- 倉位建議、加減碼條件與失效條件
- validation v3 摘要與 portfolio risk diagnostics
- audit trail

## Current Build Status

本版已把排序、資料品質、validation 與研究建議評估接起來。排序仍由 deterministic factor engine 主導；recommendation decision layer 只負責把候選標的翻成後續研究動作，不反向改寫排名。

### A / Data Quality Hardening

- 已建立統一 canonical SQLite 資料層，路徑固定在官方 output root 下的 `cache/market/market_data.sqlite`；原 daily/quarterly SQLite 僅作保留的遷移來源
- 已加入季度刷新工具與 `quality_coverage_summary`
- 已加入歷史季度回補 CLI，並支援近 8 季 history coverage 統計
- 報告與 audit 會直接揭露當期與前期品質資料覆蓋率，以及所用的季度 store 路徑

### H / Unified Market Data

- canonical DB schema v2 已加入 dataset catalog、source registry、fetch attempts、sync items、incremental state 與 quality issue occurrence
- `daily_bars` 採 DB-first；253 根歷史已齊且 current-day verified 時不逐月重抓，只補 missing range
- 大型 raw payload 使用 hash/URI 隔離儲存；`scripts/verify_market_data.py` 提供只讀 integrity、foreign-key、coverage 與 benchmark gate
- 年度財務、完整 PIT 季度修訂、估值歷史與公司事件 adapter 仍依開發文件分階段補齊

### B / Validation V3

- validation 已升級為 `validation_report_v3`，保留 `factor_aware_cross_sectional_v2` 作為 base mode
- 固定輸出 `1Y / 3Y / 5Y` 視窗
- 已提供 `price / fundamental / quality` factor sleeves
- 已補 `portfolio_diagnostics`：VaR、CVaR、Ulcer Index、Omega、Tail Ratio、rolling Sharpe/volatility/drawdown、alpha/beta、information ratio、tracking error

### C / Recommendation Decision Layer

- 每檔 candidate 會輸出 `買入 / 持有 / 賣出`
- 報告會同步輸出 `buying_ranking`、`actionable_queue`、`watchlist_candidates`、`research_list`；舊 `picks` 欄位保留為 research top N alias，供既有研究流程使用
- `buying_ranking` 採 Buying Gate V2：可收 `formal_buy`、`risk_adjusted_buy`、`tactical_buy`，但 `research_list` 仍不是買進榜
- `actionable_queue` 不放寬正式買進條件；它只列出 near buy、starter position 與 wait-for-trigger 的下一步動作
- 新增 `risk_score`、`action_view`、`target_range`、`position_note`、`invalidation_conditions`、`evidence_refs`
- `reports`、`audit`、`watchlist`、`decision-review` 與 SQLite decision ledger 都會保留 recommendation 欄位
- `--recommendation-mode deterministic` 可獨立運作；`--recommendation-mode llm-review` 可做可選反方檢查與風險補強
- LLM review 不覆蓋原始分數與資料；若輸出格式錯誤、證據不足或 risk gate 失敗，會回落或降級為 deterministic / 持有
- `macro_regime_overlay` 是 supplementary risk overlay，只影響 `risk_score`、`position_note` 與 `action_view`，不升級 `idea_score` 或排名

### F / Stock Risk Metrics

- 每檔候選已加入 `stock_risk_metrics`：annualized return、volatility、Sharpe、Sortino、max drawdown、downside volatility、Calmar、win rate 與 return-to-drawdown
- 新增 `risk_adjusted_score`，用來輔助 `buyability_score` 與 `actionability_score`
- 研究輸出會保留 RiskAdj / Sharpe / drawdown 欄位，方便檢查是否偏向高波動低效率標的

### D / Theme Coverage Expansion

- `AI` 與 `半導體` 已拆成 `core`、`coverage`、`broad` 三種 universe
- `coverage` 預設涵蓋 AI server / ODM、foundry、IC design、memory / HBM、advanced packaging / PCB / substrate、cooling / thermal、networking / optical、power / connector / chassis、testing / equipment / materials
- 報告會揭露 `universe_mode`、`universe_size_before_limit`、`universe_limit_applied`、子題材 bucket 與 `core_watchlist_member`
- 舊 `--theme-mode strict` 保留為 deprecated alias，實際映射到 `core`

### Pending

以下部分仍待後續優化：

- `E / Workflow Deepening`
- `F / Action Engine Upgrade` 的事件狀態機仍可再深化

## Quick Start

核心 screener 無需額外 API key，主要依賴官方公開資料源。

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --universe-mode coverage `
  --benchmark TAIEX `
  --as-of 2026-04-29 `
  --top-n 30 `
  --validation-window 1y `
  --quality-update-mode auto `
  --quality-update-budget-sec 3 `
  --quality-history-depth 8 `
  --recommendation-mode deterministic `
  --output-format md,json,csv `
  --coverage-list "%USERPROFILE%\tw-reports\coverage-list.txt"
```

`--run-backtest` 僅在明確需要 validation interpretation 時使用；每日自動化只在星期一加上此旗標。

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
  --universe-mode coverage `
  --as-of 2026-04-29 `
  --top-n 30 `
  --recommendation-mode llm-review `
  --review-top-n 8 `
  --llm-provider openai `
  --llm-model gpt-4o-mini
```

全類股 Top100 批次快照：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_universe_top100.py" `
  --as-of 2026-04-29 `
  --top-n 100 `
  --lookback 160 `
  --bucket-types theme,industry `
  --max-symbols-per-bucket 160
```

季度快照刷新與覆蓋率摘要：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\refresh_quarterly_snapshots.py" `
  --as-of 2026-04-29 `
  --themes AI,半導體 `
  --theme-mode strict `
  --universe-mode coverage `
  --output-root "%USERPROFILE%\tw-sector-screener-output"
```

歷史季度回補：

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\backfill_quarterly_history.py" `
  --as-of 2026-04-29 `
  --themes AI,半導體 `
  --periods 8 `
  --batch-size 20
```

## CLI Surface

核心參數如下：

- `--theme`: 主題名稱
- `--universe-mode`: `core` / `coverage` / `broad`，預設 `coverage`
- `--theme-mode`: deprecated legacy option；`strict` 會映射到 `core`，同時指定時以 `--universe-mode` 為準
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
- `--market-database`: canonical `market_data.sqlite` 路徑
- `scripts/sync_market_data.py --datasets ... --mode incremental|full --dry-run`: 市場資料同步契約

`config.example.json` 可作為自訂權重與 benchmark 的起點，見 [config.example.json](./config.example.json)。

## Data Sources

目前資料來源以官方公開資料為主：

- TWSE OpenAPI
- TWSE `exchangeReport`
- TPEx OpenAPI
- TPEx `afterTrading` API
- MOPS 官方公開資料（財務與公司事件補充）

季度品質資料目前採「官方最新季抓取 + SQLite append-only 歷史累積」模式。  
最新季通常拿得到；前一期與更早期的覆蓋會隨日常刷新逐步變厚。這是現階段的真實限制，文件就該老實寫。

市場資料統一寫入 `%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite`。`daily_bars` 保存官方 verified 日線；`period_bars` 保存由日線派生的週/月/季/年線；季度/年度財務、月營收、估值、TAIEX、security master、題材 membership、raw payload 與 sync quality metadata 分表保存。provider 先讀 canonical DB，歷史 253 根已齊時只補缺少的當月/當日；正常交易日最新日線不是 `as_of` 就 fail-closed。

歷史 cache import 不會自行把當日列標為已驗證；只有 provider 通過當日來源回應後，DB-first 才會重用該 `as_of` 的當日列，避免用舊 cache 靜默補成今日資料。

既有 canonical rows 會建立 `market_data_sync_state` 的 `migrated` checkpoint，但這不等同於當日來源驗證；只有增量同步成功後才標記為 `verified`。

同步與遷移驗證：

```powershell
Set-Location -LiteralPath "$env:USERPROFILE\.codex\skills\tw-sector-screener"
python scripts\sync_market_data.py `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-04-29 `
  --lookback 253 `
  --mode incremental `
  --datasets daily_bars,index_bars,security_master,monthly_revenue,period_bars `
  --output-root "$env:USERPROFILE\tw-sector-screener-output" `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite"

python scripts\verify_market_data.py `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite" `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-04-29 `
  --lookback 253 `
  --benchmark TAIEX
```

市場資料 schema、PIT 限制、raw payload 政策與分階段回補路線見 [docs/market-data-database-development.md](docs/market-data-database-development.md)；資料缺口的實作順序見 [docs/market-data-completion-roadmap.md](docs/market-data-completion-roadmap.md)，PowerShell 執行與 watchdog 規則見 [docs/market-data-operations.md](docs/market-data-operations.md)。

canonical DB 會同時保留 `effective_date`、`published_at`、`fetched_at`、來源 URL、payload hash、validation status 與 fallback/redirect 診斷；回測不得把發布日晚於觀察日的資料前置使用。

## How To Read The Report

- `Idea Score`: 研究優先序
- `Coverage Universe`: 本次正式候選池規模、限制與子題材分布
- `Primary Bucket / Theme Buckets`: 標的在 AI/半導體產業鏈中的主要位置與附屬分類
- `Confidence`: 結論可靠度
- `Factor Coverage / Data Freshness`: 一個看缺值，一個看資料新鮮度
- `Action View`: `Overweight / Neutral / Underweight`
- `Recommendation`: `買入 / 持有 / 賣出` 研究建議評估
- `Decision Tier`: `buy_now / near_buy / starter_position / wait_for_trigger / avoid`
- `Actionable Queue`: 當正式買進為 0 時，指出最接近能做的標的、卡關原因與升級條件
- `Risk Score`: 波動、趨勢破壞、估值過熱、資料缺口與 risk gate 的綜合風險
- `Evidence Refs`: recommendation 使用到的結構化證據路徑
- `Why Now / Why Not`: 現在能看與需要保守的理由
- `Add Trigger / Trim Trigger`: 加碼與減碼條件
- `Decision Ledger`: 記錄每次建議、信心、風險、失效條件與 evidence refs
- `Validation`: 目前排序框架的驗證結果
- `Audit`: 本次參數、資料來源、警示與快取路徑
- `History Coverage`: 近 8 季完整覆蓋程度

Validation JSON 目前採 `validation_report_v3`，其中 `metrics.portfolio_diagnostics` 固定揭露風險診斷欄位；audit 會同步保留 `connector_contract_version`、`supplementary_connectors` 與 `macro_regime_overlay`。
報告 JSON 另固定輸出 `buying_ranking`、`actionable_queue`、`watchlist_candidates`、`research_list`、backward-compatible `picks` 與 `universe_overview`；候選標的會揭露 `theme_buckets`、`primary_bucket`、`coverage_reason`、`core_watchlist_member`、`buying_tier`、`decision_tier`、`actionability_score`、`stock_risk_metrics` 與 `risk_adjusted_score`。audit 會保留 `ranking_policy_version = tw-three-list-v1`、`buying_gate_policy_version = tw-buying-gate-v2`、`action_queue_policy_version = tw-actionable-queue-v1`、`stock_risk_metrics_version = stock-risk-v1`、`list_counts` 與 universe 統計。

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
- 年度財務、歷史估值與公司事件尚未完成全 coverage 的 point-in-time adapter/backfill
- validation 雖已升級，基本面與品質因子仍偏快照型
- macro regime overlay 目前是 supplementary/local proxy contract，外部 macro connector 尚未直接作為 ranking signal
- coverage universe 是 curated 靜態清單，仍需定期人工校準新上市、新轉型與錯配標的
- LLM review 是可選研究層，不是資料源；沒有 evidence refs 的主張不應升級 recommendation
- target range 會在資料不足時輸出 `null`，不硬編目標價

因此，這個工具適合做研究前端漏斗，離完整機構研究平台還有一段路。

## Roadmap

下一階段優先順序如下：

1. `E / Workflow Deepening`
2. `F / Action Engine Upgrade`

詳見 [docs/optimization-roadmap-v2.md](./docs/optimization-roadmap-v2.md)。

## Sample Outputs

repo 內只追蹤少量樣本，不追蹤完整執行輸出。  
目前保留的樣本見：

- [examples/sample-reports/ai-20260429/report.md](./examples/sample-reports/ai-20260429/report.md)
- [examples/sample-reports/ai-20260429/audit.json](./examples/sample-reports/ai-20260429/audit.json)
- [examples/sample-reports/ai-20260429/validation.json](./examples/sample-reports/ai-20260429/validation.json)
- [examples/sample-reports/ai-20260429/decision-review.json](./examples/sample-reports/ai-20260429/decision-review.json)
- [examples/sample-reports/ai-20260430/report.md](./examples/sample-reports/ai-20260430/report.md)
- [examples/sample-reports/ai-20260430/report.json](./examples/sample-reports/ai-20260430/report.json)
- [examples/sample-reports/ai-20260430/audit.json](./examples/sample-reports/ai-20260430/audit.json)

## License

見 [LICENSE](./LICENSE)。
