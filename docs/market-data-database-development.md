# 統一市場資料 SQLite 開發文件

狀態：`schema v2 foundation delivered`；年度財報、完整季度 point-in-time 回補、估值歷史與公司事件 adapter 仍依本文分階段實作。

本文件是資料層的產品契約與開發路線圖。它不把「已建立資料表」誤寫成「資料已完整回補」，也不把研究資料層延伸成任何執行或交易系統。

## 1. 目標與邊界

canonical database 固定位於官方輸出根目錄，不進 Git：

```text
%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite
```

目標是讓 AI、半導體及日後其他 curated `coverage` universe 共用一個可恢復、可稽核、可增量同步的資料層：

- 日線 OHLCV 是價格研究的 canonical raw series；每支股票以 `(market, symbol, trade_date)` 冪等 upsert。
- 週、月、季、年價格線由 verified daily bars 派生，保留 derivation version、期間起訖與實際交易日數。
- 季度/年度財務、月營收、估值、股利/分割/除權息、security master、題材 membership 與 benchmark 各自分表。
- 每筆可進入分析的資料都要能追溯到來源端點、來源 URL、payload hash、取得時間、可用/發布時間與 validation status。
- 正常交易日的 current-day gate 必須 fail-closed；昨天或舊 cache 不得冒充今天收盤。

本層不包含：下單、持倉、模擬器、dashboard 發布、GitHub Pages、即時 tick 或未經定義的第三方資料直接進入排名。

## 2. 資料來源政策

第一優先是官方或可稽核公開來源：

1. [TWSE OpenAPI](https://openapi.twse.com.tw/) 與官方 historical/exchangeReport 端點
2. [TPEx OpenAPI](https://www.tpex.org.tw/openapi/) 與 afterTrading 端點
3. [MOPS](https://mops.twse.com.tw/) 作為財務報告、月營收與公司事件的官方公開補充來源

endpoint adapter 必須保留 primary、fallback、bulk fallback 的實際結果。HTTP 308 只可接受：同源 HTTPS、allowlist 內 host、最多三層、無循環、保留原 method/body；所有導向與 fallback 都要寫入 fetch attempt。已安全恢復的導向不列為 unresolved warning，拒絕的導向則必須進 quality issue。

## 3. Point-in-time 契約

資料日期與資料可被研究者知道的日期分開：

| 欄位 | 意義 | 用法 |
|---|---|---|
| `effective_date` | 資料描述的市場/財務期間或事件生效日 | 決定資料屬於哪一天/期間 |
| `published_at` | 官方公布或發布時間 | point-in-time 回測的可用界線 |
| `available_date` | 若來源只提供日期，代表研究可取得日 | 不可晚於觀察日才前置使用 |
| `fetched_at` | 本機取得時間 | 追溯與 freshness，不等同發布日 |
| `source_payload_sha256` | 原始來源 payload 指紋 | 連接 raw payload 與 canonical row |
| `validation_status` | `verified`、`partial`、`quarantined`、`failed` | 只有合格狀態可進入對應分析 |

若官方來源未提供發布日，資料可保存但必須標記缺失；不得以 `fetched_at` 代替 `published_at` 而宣稱無 look-ahead。

價格調整政策：保存未調整的官方 raw OHLCV；除權息、分割等公司事件另存 `corporate_actions`，adjusted series 在查詢/分析時由事件與 derivation version 計算。不得覆寫 raw close。

## 4. canonical schema

### 4.1 分析資料表

| 表 | 內容 | 狀態 |
|---|---|---|
| `daily_bars` | verified 日線 OHLCV | 現行 production path |
| `period_bars` | W/M/Q/Y 日線派生 | 現行 production path |
| `index_bars` | TAIEX 與其他 benchmark | 現行 production path |
| `security_master_snapshots` | 代號、名稱、產業與 effective snapshot | 現行 production path |
| `universe_membership` | theme/market/membership 的有效期間 | 現行 production path |
| `quarterly_company_fundamentals` | 季度快照與原有季度 store 匯入 | 已有，PIT 欄位仍需增強 |
| `annual_company_fundamentals` | 年度財務 facts | schema 已有，回補 adapter 待做 |
| `monthly_revenue` | 月營收、MoM、YoY 與 available date | 現行 production path |
| `valuation_snapshots` | PE/PB/殖利率快照與 available date | schema 已有，歷史 adapter 待做 |
| `corporate_actions` | 股利、分割、除權息等事件 | schema 已有，官方回補 adapter 待做 |

目前 live DB 的資料集級盤點是：`daily_bars`、`period_bars`、`index_bars`、`security_master_snapshots`、季度財務、月營收與估值快照已有資料；年度財務與公司行動表仍是空表，不能宣稱已完成全歷史回補。`market_data_sync_state` 會由既有 canonical rows 建立 `migrated` checkpoint；這只表示可從 SQLite 讀到的範圍，不等同於當日來源驗證，下一次增量同步成功後才會更新為 `verified`。

### 4.2 控制、來源與品質表

| 表 | 用途 |
|---|---|
| `market_data_dataset_catalog` | dataset 頻率、canonical table、來源政策與 PIT 要求 |
| `market_data_source_registry` | endpoint allowlist、market、priority 與 redirect 能力 |
| `source_payloads` | payload metadata；小 payload inline，大 payload hash/URI 外置 |
| `market_data_fetch_attempts` | 每次 network/cache/fallback 嘗試與 redirect chain |
| `market_data_sync_runs` | 一次同步的範圍、狀態、摘要 |
| `market_data_sync_items` | 每個 dataset/symbol 的同步結果 |
| `market_data_sync_state` | dataset 級增量 checkpoint |
| `market_data_sync_issues` | 舊版相容的首見 issue 索引 |
| `market_data_quality_issues` | 以 fingerprint 去重的品質 issue |
| `market_data_quality_issue_occurrences` | 同一 issue 在不同 run 的每次觀察 |
| `market_data_quarantine` | 結構錯誤、未知格式或來源政策不允許的資料隔離區 |

### 4.3 raw payload 儲存

小於 10 MiB 的 JSON payload 保存在 `source_payloads.raw_payload_json`。超過限制或不適合直接放 SQLite 的 payload 保存於 canonical DB 同層的 `raw_payloads/<dataset>/<sha256>.json`，SQLite 只保存 `storage_mode=external`、URI、byte size 與 hash descriptor。外置檔案必須以 hash 命名、不可覆寫既有 hash；`database_integrity` 會檢查 URI 是否仍存在。

## 5. 同步狀態機

```text
scope -> DB checkpoint check -> fetch primary
     -> safe redirect/fallback -> parse/validate
     -> raw payload persist -> canonical upsert
     -> period derivation -> sync state -> audit manifest
```

每個 item 的狀態至少包括 `planned`、`fetching`、`verified`、`partial`、`failed`、`quarantined`。失敗要保留 request range、最後來源、HTTP 狀態、錯誤與 data gap；不得把舊值更新成新的 `current_day_verified`。

增量規則：

1. 先查 canonical SQLite，而不是先按月份掃 URL cache。
2. 若 verified bar 數量達 lookback、最新日符合 `as_of` 且 current-day marker 已由來源驗證，該標的 `DB hit`，不發 network request。
3. 缺少歷史區間時只抓缺少月份/日期；已存在日期以來源 priority 與 fetched time 冪等選擇。
4. 當日為正常交易日時，必須有 `latest_verified_trade_date == as_of`；當日失敗即整個 theme/report fail-closed。
5. 對外輸出的 report/sync manifest 必須揭露 cache status、source warnings、data gaps 與 unresolved redirects。

## 6. 現有命令與驗證

初始化/遷移（來源庫保留，不刪除）：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\import_market_history_cache.py
```

同步 curated coverage：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\sync_market_data.py `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --lookback 253 `
  --mode incremental `
  --datasets daily_bars,index_bars,security_master,monthly_revenue,period_bars `
  --output-root "$env:USERPROFILE\tw-sector-screener-output" `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite"
```

只讀完整性與 coverage 驗證（不初始化、不遷移、不改 SQLite）：

```powershell
Set-Location -LiteralPath 'C:\Users\a0953041880\.codex\skills\tw-sector-screener'
python scripts\verify_market_data.py `
  --database "$env:USERPROFILE\tw-sector-screener-output\cache\market\market_data.sqlite" `
  --themes AI,半導體 `
  --universe-mode coverage `
  --as-of 2026-08-27 `
  --lookback 253 `
  --benchmark TAIEX
```

驗收不只看 SQLite `integrity_check=ok`，還要檢查：AI/半導體 coverage 每檔 bar 數、最新交易日、benchmark、payload external URI、foreign key、品質 issue fingerprints，以及 screener 的 fresh report artifacts。

## 7. 分階段擴充路線

### Phase 0 — 已交付的基礎層

- daily/quarterly legacy migration 與 period derivation。
- DB-first daily provider、safe redirect、fallback、current-day fail-closed。
- schema v2 control/provenance/quality tables。
- sync CLI 的 incremental/full/dry-run contract。
- read-only `verify_market_data.py`。

### Phase 1 — PIT 財務資料

- 將季度 store 的 `as_of_date`、published/available date 與 source payload 連到 canonical payload id。
- 依 TWSE OpenAPI/MOPS 的官方欄位建立季度/年度 field mapping、單位與 ROC/Gregorian period parser。
- 同一家公司同一報告期間允許多次修訂，但以 `(symbol, period, published_at, payload_hash)` 保留版本，不覆寫歷史觀測。
- 對缺少發布日或欄位單位不明者進 quarantine，不進 ranking。

### Phase 2 — 月營收、估值與公司事件

- 月營收以 `revenue_month` 為 effective period、`available_date` 為 PIT gate；避免把後來補抓的月份資料當成當時已知。
- 估值快照保存觀察日、發布日與計算/來源方法；PE/PB 為 null 時保留 null，不補中性值。
- 股利、分割、除權息與減資事件建立事件型 adapter，先驗證日期/倍率/金額，再供 adjusted query layer 使用。

### Phase 3 — 研究查詢與回測整合

- 加入只讀 query API：`as_of`、dataset status、source priority、PIT cutoff。
- 將 backtest 的 adjusted/unadjusted 選擇明確化，輸出資料版本與 payload hashes。
- 加入 gap report、symbol-level completeness、source disagreement 與修訂版資料敏感度分析。

### Phase 4 — 擴大 coverage

- 只擴充 curated coverage；先建立 security master 與 membership effective periods，再同步 bars。
- 全市場資料另立成本與容量評估，不因 coverage 需求直接下載全市場所有歷史 payload。

## 8. 風險與不可宣稱事項

- 「DB 有 253 根」只代表價格歷史長度合格，不代表基本面、公司事件或調整後價格已完整。
- 官方資料可能修訂；append-only payload 與 published_at 才能追溯當時版本。
- cache import 可以補歷史 bars，但不會授予 current-day verified 資格。
- 週末或休市日不要求 `as_of` 有交易 bar；正常市場日則要求。
- 未完成 adapter 的 dataset 只可標記 `planned/not_available`，不可在報告中當成已驗證因子。

## 9. 完成門檻

每一階段都要同時通過：

1. migration/import 可重跑且 row parity、hash、SQLite integrity 與 foreign key 均通過。
2. DB hit 測試證明完整資料不發 network；缺資料只補 missing range。
3. redirect/fallback/invalid payload/expired external raw file 有隔離測試。
4. current-day 缺口使 sync/report fail-closed，不產生可誤用的部分排名。
5. 文件、skill、automation、CLI help、測試與 sample contract 同步更新。
