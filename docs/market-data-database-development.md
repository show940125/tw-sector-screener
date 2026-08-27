# 統一市場資料 SQLite 開發文件

狀態：`schema v4 + bounded enrichment snapshots delivered`。本輪已交付統一 schema、來源與 PIT 欄位、DB-first 日線增量路徑、分割 checkpoint／缺口 ledger／completeness run、safe redirect/fallback、月營收 12 個月歷史回補、目前財報 facts／公司行動／交易日曆快照 adapter、調整後價格的可重建骨架、sync profile 與 read-only verification；60 個月月營收／估值、20 季／8 年財務 revision、完整交易狀態與法人資料仍須依下方 roadmap 逐項執行，不能把「有表」宣稱成「有完整資料」。

本文件是資料層的產品契約與開發路線圖。它只描述研究資料與可稽核同步，不包含交易、下單、持倉或發布系統。

## 1. 目標與邊界

canonical database 固定位於官方輸出根目錄，不進 Git：

```text
%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite
```

目標是讓 AI、半導體及日後其他 curated `coverage` universe 共用一個可恢復、可稽核、可增量同步的資料層：

- `daily_bars` 是官方驗證的 raw OHLCV；以 `(market, symbol, trade_date)` 冪等 upsert，永不被 adjusted series 覆寫。
- `period_bars` 的 W/M/Q/Y 價格線由 verified daily bars 派生，保存期間、交易日數、來源最後交易日與 derivation version。
- 基本面、月營收、估值、公司行動、security master、universe membership、benchmark 與研究增強資料分表保存。
- canonical row 要能追溯到來源 URL、payload hash、取得時間、可用/發布時間與 validation status；source payload 與 fetch attempt 另存，不把 raw JSON 塞進固定欄位後就丟失來源。
- 正常交易日的 current-day gate 必須 fail-closed；昨天或舊 cache 不得冒充今天收盤。

資料層不直接涵蓋：即時 tick、未定義第三方資料、交易執行、planned order、模擬器、dashboard 或網站發布。

## 2. 資料來源與 provenance 政策

第一優先是官方或可稽核公開來源：

1. [TWSE OpenAPI](https://openapi.twse.com.tw/) 與 TWSE historical/exchangeReport
2. [TPEx OpenAPI](https://www.tpex.org.tw/openapi/) 與 TPEx afterTrading
3. [MOPS](https://mops.twse.com.tw/) 的財務報告、月營收與事件公告

每個來源 adapter 要保存 primary、fallback、bulk fallback 的實際結果。HTTP redirect 的規則是：

- 只接受同源 HTTPS、allowlist host、最多三層；拒絕外部 host、明文 HTTP、循環與超過層數的導向。
- 308/307 保留 method/body；301/302/303 只在明確允許時轉 GET。
- 原始 request URL、Location chain、final URL、fallback level、HTTP status 與 payload hash 必須寫入 fetch attempt。
- 安全導向成功後，308 不列為 unresolved warning；被拒絕或所有 fallback 失敗才列為品質錯誤。

小於 10 MiB 的 JSON payload 可 inline 存在 `source_payloads.raw_payload_json`；較大或非 JSON payload 存在 database 同層 `raw_payloads/<dataset>/<sha256>.*`，SQLite 保留 URI、byte size 與 hash。external payload 不得以同名覆寫，integrity 檢查必須能確認 URI 存在。

## 3. Point-in-time（PIT）資料契約

資料所描述的日期與研究者當時能知道的日期必須分開：

| 欄位 | 意義 | 正式研究規則 |
|---|---|---|
| `effective_date` | 資料描述的交易日、財務期間或事件生效日 | 必須 `<= observation_date` |
| `published_at` | 官方公布時間 | 必須 `<= information_cutoff` |
| `available_date` | 只有日期可用時的研究可取得日 | 必須 `<= information_cutoff` |
| `fetched_at` | 本機抓取時間 | 只代表 provenance/freshness，不可代替發布日 |
| `source_payload_id`/hash | 原始來源指紋與關聯 | 用於重播、parity 與 revision 追溯 |
| `revision_id`/`revision_sequence` | 同一 fact 的修訂鏈 | 不得用後來版本覆蓋歷史版本 |
| `validation_status` | `verified`、`partial`、`quarantined`、`failed` | 只有符合資料集 gate 的 row 可進正式 factor |
| `data_gap_reason` | 缺少、不可用或未定義的明確原因 | 不可用 null/0 靜默掩蓋 |

統一的只讀查詢語意是：

```text
query(dataset, symbol, observation_date, information_cutoff)
```

只允許 `effective_date <= observation_date` 且 `available_date` 或 `published_at <= information_cutoff` 的 row。發布日缺失的資料可保存作 raw/描述性研究，但必須標示 PIT 不完整，不得進入正式 PIT 回測或宣稱 look-ahead-free。

## 4. canonical schema 與目前狀態

### 4.1 現行分析資料

| 表 | 用途 | 目前交付狀態 |
|---|---|---|
| `daily_bars` | verified 日線 OHLCV | production DB-first；歷史長度仍依 sync coverage 驗收 |
| `period_bars` | W/M/Q/Y 日線派生 | production；增量只重建受影響 symbol |
| `index_bars` | TAIEX 與其他 benchmark bars | production；current-day 仍需逐次驗證 |
| `security_master_snapshots` | 代號、名稱、產業的 effective snapshot | production path；歷史 lifecycle 尚未完整 |
| `universe_membership` | theme/market/mode 的有效期間 | production path；歷史 membership 尚待補齊 |
| `monthly_revenue` | 月營收、MoM、YoY 與可用日 | 已完成 54 檔×12 月（648 rows，2025-08～2026-07）；TWSE IIH／TPEx MOPS SPA、DB-first 與分割 checkpoint 已驗證；60 月仍未完成 |
| `valuation_snapshots` | PE/PB/殖利率快照 | 已有 TWSE/TPEx parser、實際交易日保存、驗證、upsert 與分割 checkpoint；實際 60 個月回補尚未執行 |
| `quarterly_company_fundamentals` | 相容季度快照 | 已有歷史列，但 PIT/revision completeness 仍為 partial |
| `financial_fact_observations` | PIT 財務 facts | 已寫入目前官方季度快照 212 rows；53/54 coverage 有效值，6415 缺少官方當期列並記錄 gap；20 季／8 年 revision 尚未完成 |
| `annual_company_fundamentals` | 年度財務相容表 | schema/upsert 已有；年度歷史 adapter 尚未完成，仍為空 |
| `corporate_actions` | 股利、分割、除權息、減資等事件 | 已接入 TWSE／TPEx 官方當期快照；目前僅寫入本次來源涵蓋事件，完整歷史仍待回補 |
| `market_sessions` | 官方交易日／假日資訊 | 已接入 TWSE 年度假日快照並映射 TPEx 共用市場日曆；不是完整歷史交易日表 |

本輪 live enrichment（2026-08-27）已驗證：日線 23,882 rows、54 檔達至少 253 根、TAIEX 有資料；月營收 648 rows／54 檔／12 月；季度相容表約 4,708 rows、PIT facts 目前為官方當期快照；公司行動與交易日曆已非空但仍是當期／年度來源邊界。實際狀態必須以當次 manifest、`verify_market_data.py` 與資料庫盤點為準，不能由歷史數字推論已完成深歷史回補。

### 4.2 v4 研究資料表

本輪已建立 schema、索引與 typed upsert/query 邊界；已完成的當期快照也不代表資料完成：

- `financial_fact_observations`：季度/年度統一 facts，支援 unit、consolidation、dimension、available/published date 與 revision lineage；目前只寫入官方當期 bulk snapshot，缺列明確進 gap ledger。
- `market_sessions`、`security_trading_status`：交易日、停牌、處置、漲跌停與不可交易狀態。
- `adjustment_factors`、`adjusted_bars`：公司行動因子與可重建 backward-adjusted/total-return 價格；raw bars 不變。
- `security_lifecycle`：上市、下市、改名、產業/市場有效期間。
- `benchmark_membership`：TAIEX/產業指數的 PIT 成分與權重。
- `daily_market_stats`：成交值、成交筆數、週轉率、市值、股數與流通股數；無來源保持 null，不以 0 代替。
- `institutional_flows`、`margin_short_snapshots`：三大法人、融資融券、借券等研究欄位。
- `market_events`：重大訊息、法說、股東會、股利/除權息事件；原文仍以 raw/quarantine 為準。
- `market_data_source_links`：canonical identity 到 `source_payloads` 的可追溯關聯。

`quarterly_company_fundamentals` 與 `annual_company_fundamentals` 暫時保留作相容 materialized store；完整新 adapter 應優先寫 `financial_fact_observations`，再由明確版本的 projection 產生固定欄位表，避免固定欄位遺失新 fact。

### 4.3 控制與品質表

`market_data_dataset_catalog`、`market_data_source_registry`、`source_payloads`、`market_data_fetch_attempts`、`market_data_sync_runs`、`market_data_sync_items`、`market_data_sync_state`、`market_data_sync_issues`、`market_data_quality_issues`、`market_data_quality_issue_occurrences` 與 `market_data_quarantine` 保留作同步狀態、來源、缺口、去重 issue 與未知 payload 隔離。

v4 另加入 `market_data_partition_state`、`market_data_gap_ledger` 與 `market_data_completeness_runs`。前者以 dataset/market/symbol/partition 保存請求範圍、payload hash、row count 與驗證狀態；中者以固定 partition 聚合重複缺口；後者保存一次 completeness gate 的 expected/actual rows、partitions 與缺口清單。這三表是增量同步的控制面，不取代 canonical row 的 provenance。

Migration 前先執行 `scripts\backup_market_data.py`。它會保存來源與備份 SHA-256、schema/integrity/FK、payload hash/link 與 table-count parity manifest；只有 `status=complete` 且 `logical_parity=true` 才可進入下一階段。SQLite backup 的實體檔案 hash 可能因頁面重組而不同，不能用 `sha256_equal` 單獨判定失敗。

`market_data_sync_state.last_status` 的語義分離如下：

- `migrated`：由既有 SQLite 匯入，只有本機存在範圍的證據。
- `verified`：本次來源驗證、解析與完整性 gate 通過。
- `partial`：部分 partition 或欄位可用，仍有明確缺口。
- `quarantined`：payload 存在但未通過 schema/來源政策。
- `failed`：本次嘗試未完成，不能重用作 current-day 成功。
- `not_implemented`：資料集被明確選取，但尚無通過 contract/PIT 測試的可執行來源 adapter；必須以非零狀態結束。

## 5. DB-first、增量同步與派生資料

provider 啟動時保留 legacy `daily_bars.sqlite` 與 `quarterly_fundamentals.sqlite` 作一次性遷移來源，但 canonical DB 是唯一日常寫入目標。legacy migration 以檔案 size/mtime fingerprint checkpoint 去重；未變更的來源不會每次重新 attach/copy。

日線查詢規則：

1. 先查 canonical `(market, symbol, trade_date)`，完整 253 根且 tail 已符合 `as_of` 時直接 DB hit。
2. 缺歷史時只往缺少的月份/日期抓取；新增列以來源 priority、日期與 payload identity 冪等 upsert。
3. 正常交易日必須由當次來源回應確認 `latest_verified_trade_date == as_of`，並寫入 `daily_bar_sync_state.last_current_day_verified_date`；cache import 的列不能取得這個資格。
4. 當日資料失敗、HTTP 308 被拒絕、資料 schema/日期/OHLCV 驗證失敗或 benchmark 不足時，sync/report fail-closed。
5. 寫入日線後只重建受影響 symbol 的 `period_bars`；完整 reconcile 仍可明確呼叫 `rebuild_period_bars()`。
6. `adjusted_bars` 由 raw daily bars 與已驗證 adjustment factor 重建，保留 `derivation_version`、`source_latest_trade_date` 與 hash/rebuild evidence；不回寫 raw close。

歷史 enrichment 以月份或交易日為 partition。只有 checkpoint 的 requested range、payload hash、row count 與 `verified` 狀態同時吻合時才可 DB hit；`--mode full` 會明確略過 checkpoint 以供受控 reconcile。已驗證的 partition 不會因新程序啟動而逐月重查，缺口則只重抓該 partition。

## 6. Adapter 與 sync contract

`src/providers/market_data_adapters.py` 定義未來 dataset adapter 必須實作的 contract：

```text
dataset_key
identity_key
partition_key
fetch_range()
parse()
validate()
upsert()
completeness_report()
```

`DatasetSpec.implemented` 代表目前是否有經測試的可執行 provider path，不代表達成歷史 coverage。現行 daily provider 是既有 façade；validated registry 已正式接入 `monthly_revenue`、`valuation_snapshots`、`financial_facts`、`corporate_actions` 與 `market_sessions`，均由 transport-independent parser、validation、upsert、completeness checkpoint 與 fixture tests 保護。這些 enrichment 目前是 bounded snapshot／12 月月營收，不等於 60 月、20 季或 8 年完整歷史；`annual_fundamentals`、法人、融資融券、market stats 等尚未實作時，被選中會標示 `not_implemented` 並以非零 exit code 結束，不得 silent no-op。

sync manifest 每次要輸出：requested range、expected rows/partitions、actual rows、missing partitions、network requests、DB hits、fallback、redirect/source warnings、failures、integrity 與 profile。`--from-date`/`--to-date` 是真正的 fetch/validation 範圍，不只是 provenance 欄位。

## 7. 操作命令

所有相對路徑命令先執行：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
$outputRoot = Join-Path $env:USERPROFILE 'tw-sector-screener-output'
$database = Join-Path $outputRoot 'cache\market\market_data.sqlite'
```

### 7.1 日常 profile

省略 `--from-date` 時，daily profile 會要求 `to-date/as-of` 當日加上 `lookback` bars；不要把 `as_of` 偽裝成 from-date，否則 DB hit 可能只回傳一列而誤觸 253 gate：

```powershell
python scripts\sync_market_data.py `
  --profile daily `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --lookback 253 `
  --mode incremental `
  --datasets 'daily_bars,index_bars,security_master,monthly_revenue,period_bars' `
  --output-root "$outputRoot" `
  --database "$database"
```

### 7.2 受控 enrichment profile

歷史回補不塞進 16:30 日報 watchdog；使用明確 range，並只選已有 validated adapter 的資料集。月營收至少 12 個月的已交付範例與當期研究快照可分開執行；若選到尚未實作 dataset，sync 會輸出 failed manifest 而不是跳過：

```powershell
python scripts\sync_market_data.py `
  --profile enrichment `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --from-date 2021-01-01 `
  --to-date 2026-08-27 `
  --datasets 'monthly_revenue' `
  --mode incremental `
  --output-root "$outputRoot" `
  --database "$database"
```

### 7.3 Read-only verification 與 PIT query

```powershell
python scripts\verify_market_data.py `
  --database "$database" `
  --themes 'AI,半導體' `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --lookback 253 `
  --benchmark TAIEX `
  --output (Join-Path $outputRoot 'audit\20260827\market-data-verify-20260827.json')
```

`verify_market_data.py` 使用 SQLite read-only URI，不初始化、不遷移、不修補 DB；正常交易日還會檢查每檔 `last_current_day_verified_date`。程式內 `query_market_data_as_of()` 與 `query_financial_facts_as_of()` 同樣排除缺少 available/published date 的正式 PIT row；retrieval-date row 可保留作描述性資料，但不會被正式 PIT 查詢選出。

## 8. 分階段補齊路線

### Phase 0：正確性與效率（schema v4 foundation 已交付）

- schema v4 tables/indexes、source payload linkage、issue fingerprint dedupe、legacy migration fingerprint、partition checkpoint、gap ledger 與 completeness run。
- DB-first daily/index/security/revenue path、explicit 308 safe redirect、TWSE fallback、TPEx POST/bulk fallback、current-day fail-closed。
 - daily/enrichment profile、真正的 range arguments、dry-run no network/no DB mutation、monthly revenue/valuation/financial facts/corporate actions/market sessions validated adapters、unimplemented dataset non-zero failure。
- read-only integrity/coverage/PIT contract verifier 與受影響 symbol 的 period/adjusted rebuild。

本階段不宣稱已完成深歷史回補；只確保後續回補不再以 silent no-op、舊 cache 或部分 coverage 失敗冒充完成。深歷史回補必須另行執行 enrichment profile 並以 manifest／completeness run 驗收。

### Phase 1：價格、交易狀態與公司行動

將 54 檔與 TAIEX 擴至至少 1,260 根交易日或上市以來；補 `market_sessions`、停牌/處置/漲跌停、上市下市、股利、分割、減資、除權息事件；再以 deterministic adjustment factor 重建 adjusted/total-return series。raw series、事件來源與 derivation version 必須保持可重播。

### Phase 2：PIT 基本面與估值

月營收至少 60 個月、估值至少 60 個月、季度至少 20 季、年度至少 8 年或上市以來；每個 fact 保存 unit、period、published/available date、revision、payload link。未具發布時間或單位不明者只進 quarantine/partial，不進 factor validation。

### Phase 3：市場脈絡與可交易性

補產業/類股 benchmark bars 與歷史成分、成交值、成交筆數、週轉率、市值、流通股數及低流動性標籤；先以 shadow features/警告供研究使用，不直接改 deterministic 權重。

### Phase 4：法人流向與事件研究

補三大法人、融資融券、借券、重大訊息、法說會、股東會與股利事件；先做輔助欄位與 interpretation。至少 95% active coverage、PIT 可重建且通過 20 交易日 shadow test 後，才可申請納入 ranking。

### Phase 5：選配資料

董監事/大股東、ESG、宏觀、新聞全文與分析師預估只有在來源穩定、發布時間清楚、revision 可重播且有明確研究假說後才進 canonical；否則留在 raw/quarantine，不作排名輸入。

## 9. 完成門檻、風險與不可宣稱事項

- 完整 coverage 必須通過 DB hit/no-network、missing partition、冪等重跑、斷點恢復、PIT revision replay、invalid payload/quarantine、308 loop/fallback 與 adjusted rebuild/hash tests。
- 54 檔與 benchmark 必須 current-day gate 通過；日線、月營收、季度、年度的歷史長度分別以 1,260/60/20/8 或上市以來為準，所有例外要有 `data_gap_reason`。
- SQLite `integrity_check`、foreign key、schema version、source payload URI、issue fingerprint、migration parity 與 manifest 均通過，才可標記 dataset `verified`。
- 有 253 根只代表 raw price lookback 足夠，不代表基本面、公司行動、adjusted series 或 PIT 回測完整。
- 週末/休市日不要求當日交易 bar；正常市場日不能用前一天或 cache import 假裝今日成功。
- 研究資料的資料庫完成，不會自動改變既有 deterministic ranking；新增 features 先 shadow test，經明確 acceptance 後才可申請接入。
