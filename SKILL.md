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
3. MOPS 官方公開資料作財務與公司事件補充

目前進度：
- `A / Data Quality Hardening`：已建立 SQLite 季度資料層，並補季度刷新、歷史回補與 quality coverage summary
- `B / Validation V3`：已升級 factor-aware validation，並加入 portfolio diagnostics
- `C / Three-List Output`：已拆出 buying ranking、watchlist 與 research list
- `D / Theme Coverage Expansion`：`AI` / `半導體` 預設使用 `coverage` universe；`core` 僅作高純度追蹤池
- `E / Actionable Queue`：已補決策梯度，讓 `buying_ranking = 0` 時仍能回答下一步動作
- `F / Stock Risk Metrics`：已加入單股 Sharpe / Sortino / drawdown / volatility 與 risk-adjusted score，輔助買進排序與研究分析
- `G / Buying Gate V2`：已把買進榜拆成 `formal_buy`、`risk_adjusted_buy`、`tactical_buy`，讓低風險高 RiskAdj 標的不再被單一 idea 門檻排除
- `H / Unified Market Data`：canonical `market_data.sqlite` 已升級 schema v4，加入 DB-first 增量 checkpoint、partition gap/completeness controls、來源/抓取 provenance、PIT facts、研究資料表、品質 issue occurrence 與只讀驗證命令

## Canonical Market Data SQLite

官方 output root 的 canonical database 是：

`%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite`

它以同一個 SQLite 分開保存 `daily_bars`、由日線派生的 `period_bars`（W/M/Q/Y）、季度與年度財務表、月營收、估值快照、TAIEX/index bars、security master、universe membership，以及 v4 的 `financial_fact_observations`、交易狀態、公司行動/adjusted bars、lifecycle、benchmark membership、market stats、法人/融資融券、market events、source links、raw payload、sync runs、partition checkpoints、gap ledger、completeness runs 與 quality issues。原有 `daily_bars.sqlite` 與 `quarterly_fundamentals.sqlite` 是保留的遷移來源，不是新的寫入目標；研究表已建立 schema/upsert 邊界，但空表或短歷史仍是未完成資料集。

日線 provider 採 DB-first：已有至少 253 根 verified bars、最新交易日符合 `as_of` 且 `daily_bar_sync_state.last_current_day_verified_date` 已由來源驗證時只讀 SQLite；只在缺少歷史區間或當月/當日尾端時增量抓取。正常交易日若最新 verified bar 不等於 `as_of`，會 fail-closed，不以舊 cache 冒充當日收盤。所有來源保留 effective/published/fetched timestamps、來源 URL、payload hash、validation status 與 fallback/redirect 診斷，回測仍須遵守 point-in-time 限制。

cache import 是歷史資料整理，不會自動取得當日驗證資格；`daily_bar_sync_state.last_current_day_verified_date` 只有在 provider 通過當日來源回應驗證後才會設定。之後同一 `as_of` 的 DB hit 才可免重查，並在 audit 中記為 current-day verified。缺少發布日的財務/事件資料可以留在 DB 或 quarantine，但不能進正式 PIT query。

統一 DB 的 `market_data_sync_state` 會從既有 canonical rows 建立 `migrated` checkpoint，僅代表 SQLite 已有的資料範圍；歷史 enrichment 另使用 `market_data_partition_state` 保存月份／交易日 partition 的 request range、payload hash、row count 與狀態，只有增量同步重新驗證來源後才會更新為 `verified`。`--mode incremental` 可重用 exact verified checkpoint；`--mode full` 是受控 reconcile，會略過 checkpoint 並重新驗證來源。

先同步 curated coverage 日線與 benchmark：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\sync_market_data.py `
  --profile daily `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-04-29 `
  --lookback 253 `
  --mode incremental `
  --datasets daily_bars,index_bars,security_master,monthly_revenue,period_bars `
  --output-root "$env:USERPROFILE\tw-sector-screener-output" `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite"
```

`daily` 省略 `--from-date` 時會取得 `lookback` window；只有明確傳入 `--from-date` 才會以日期範圍讀取/補抓。歷史回補使用 `--profile enrichment --from-date YYYY-MM-DD --to-date YYYY-MM-DD`，不要塞進日報 watchdog。已交付並有 production adapter 的 enrichment dataset 包含 `monthly_revenue`、`valuation_snapshots`、`financial_facts`、`corporate_actions` 與 `market_sessions`；目前月營收最低批次是 12 個月，後三者為 bounded official snapshot，來源缺列會明確記錄為 `partial`／gap，不用零或舊值補上。尚未有 adapter 的資料集被選取時，會輸出 `not_implemented` 並以非零 exit code 結束，不能被當成成功。

受控 enrichment 範例：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\sync_market_data.py `
  --profile enrichment `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --from-date 2021-01-01 `
  --to-date 2026-08-27 `
  --datasets 'monthly_revenue,valuation_snapshots' `
  --mode incremental `
  --output-root "$env:USERPROFILE\tw-sector-screener-output" `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite"
```

當期研究快照（不重新抓歷史月營收）可用：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\sync_market_data.py `
  --profile enrichment `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --datasets 'financial_facts,corporate_actions,market_sessions,adjusted_bars' `
  --mode incremental `
  --output-root "$env:USERPROFILE\tw-sector-screener-output" `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite"
```

`financial_facts` 的 `partial` 代表官方當期快照缺列，應查看 manifest 的 `coverage_gaps` 與 gap ledger；正式 PIT query 只選有可用／發布日期且 `validation_status=verified` 的 facts。`adjusted_bars` 在 adjustment factors 尚未完整以前只作可重建研究序列。

cache import、遷移與 integrity 可重跑驗證；import 是來源整理，不等同於當日驗證，當日 gate 由 sync/provider 最後確認：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python -c "from pathlib import Path; from src.providers.market_data_store import ensure_market_data_db, database_integrity; p=Path.home()/'tw-sector-screener-output'/'cache'/'market'; ensure_market_data_db(p/'market_data.sqlite', daily_source=p/'daily_bars.sqlite', quarterly_source=p/'quarterly_fundamentals.sqlite'); print(database_integrity(p/'market_data.sqlite'))"
```

只讀驗證（不初始化、不遷移、不改 DB）：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\verify_market_data.py `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite" `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-04-29 `
  --lookback 253 `
  --benchmark TAIEX
```

## Command

```powershell
python "%USERPROFILE%\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --universe-mode coverage `
  --benchmark TAIEX `
  --as-of 2026-04-29 `
  --top-n 30 `
  --lookback 253 `
  --quality-update-mode auto `
  --quality-update-budget-sec 3 `
  --quality-history-depth 8 `
  --recommendation-mode deterministic `
  --output-format md,json,csv `
  --coverage-list "%USERPROFILE%\tw-reports\coverage-list.txt" `
  --market-database "%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite" `
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

`--run-backtest` 僅在明確需要 validation interpretation 時使用；每日自動化只在星期一加上此旗標。

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
- `--market-database`：canonical `market_data.sqlite` 路徑
- `--profile`：`daily | enrichment`
- `--from-date` / `--to-date`：真正的歷史同步範圍；daily 的 to-date 必須等於 as-of
- `scripts/sync_market_data.py --profile ... --datasets ... --mode incremental|full --dry-run`：市場資料同步契約

## Output Contract

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.md`（Monday validation interpretation）
- `decisions/<theme>/decision-review-<theme>-<yyyymmdd>.json`
- `decisions/decision-ledger.sqlite`

coverage gate 未通過時，報告可以輸出新鮮的診斷 artifacts，但不得建立可誤用的 decision ledger 或 validation backtest。

市場資料同步 manifest 位於 `audit/<yyyymmdd>/market-sync-<yyyymmdd>.json/.md`；必須揭露 profile、requested range、DB hits、network requests、missing partitions、fallback、source warnings 與 integrity。`verify_market_data.py` 是 read-only gate，正常交易日還檢查 current-day marker；schema/PIT/adapter 契約見 `docs/market-data-database-development.md`，PowerShell 與 210 秒 watchdog 規則見 `docs/market-data-operations.md`。

Validation v3 contract：
- `validation_summary.mode = validation_report_v3`
- `metrics.portfolio_diagnostics` 包含 VaR / CVaR / Ulcer / Omega / Tail Ratio / rolling metrics / alpha-beta attribution
- `audit.connector_contract_version` 與 `audit.supplementary_connectors` 會揭露補充資料來源
- `reports` JSON 固定包含 `buying_ranking`、`actionable_queue`、`watchlist_candidates`、`research_list`、`picks`
- `reports` JSON 固定包含 `universe_overview`；候選標的會揭露 `theme_buckets`、`primary_bucket`、`coverage_reason` 與 `core_watchlist_member`
- 候選標的固定揭露 `decision_tier`、`actionability_score`、`blocked_by`、`next_action`、`trigger_to_upgrade` 與 `why_not_buy_now`
- 候選標的固定揭露 `stock_risk_metrics` 與 `risk_adjusted_score`
- `picks` 保留為 research top N alias，讓既有研究流程不漏掉賣出/降風險訊號
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
- 每日 16:30 盤後自動化執行 market-data sync 與 AI / 半導體研究報告；當日為正常交易日時，日線最新 verified bar 必須等於 `as_of`，否則整個主題輸出標記 failed。
- canonical market data 先查 SQLite；完整 253 根且 current-day marker 已驗證時為 DB hit，不逐月重抓。缺少歷史區間只補 missing range；legacy migration 以 size/mtime fingerprint 去重；大型 raw payload 以 hash/URI 外置並由 integrity check 驗證。
- schema 與資料表契約見 [docs/market-data-database-development.md](docs/market-data-database-development.md)；資料補齊順序見 [docs/market-data-completion-roadmap.md](docs/market-data-completion-roadmap.md)，PowerShell／sync／verify／watchdog 操作見 [docs/market-data-operations.md](docs/market-data-operations.md)。
- repo 以 `Feature Branch + PR` 維護，分支名稱固定使用 `codex/` 前綴。
- 官方執行輸出固定放在 `%USERPROFILE%\tw-sector-screener-output`，不進 git；repo 內只保留 `examples/sample-reports/` 樣本。
