# 市場資料補齊 Roadmap

本文件是資料庫擴充的設計與交付順序，不代表本輪已完成所有資料回補。任何未通過來源、PIT、重複列、日期或 coverage gate 的資料集，都只能標記為 `partial`／`planned`，不得悄悄進入排名。

## 1. 現況基線（2026-08-27）

目前 scope 是 AI 與半導體 `coverage` universe 的聯集 54 檔；canonical database 位於 `%USERPROFILE%\tw-sector-screener-output\cache\market\market_data.sqlite`。

| Dataset | 目前觀測 | 狀態 | 補齊判定 |
|---|---:|---|---|
| `daily_bars` | 23,882 rows | production | 54 檔至少 253 根，且 2026-08-26 current-day verified |
| `period_bars` | 6,858 rows | production | 由 verified daily bars 派生 W/M/Q/Y；不是獨立來源 |
| `index_bars` | 283 rows | production | TAIEX current-day verified |
| `security_master_snapshots` | 54 rows | snapshot | 目前 coverage identity／industry 可用，歷史異動仍待補 |
| `quarterly_company_fundamentals` | 4,708 rows | partial | 有既有季度資料，但 PIT 發布日與 revision lineage 待增強 |
| `monthly_revenue` | 54 rows | snapshot | 目前各候選一筆；60 月歷史 adapter 待做 |
| `valuation_snapshots` | 54 rows | snapshot | 目前各候選一筆；歷史 PE/PB/殖利率 adapter 待做 |
| `annual_company_fundamentals` | 0 rows | planned | 尚未建立年度 facts 回補流程 |
| `corporate_actions` | 0 rows | planned | 尚未建立股利、分割、除權息事件回補流程 |
| `market_data_sync_state` | 601 rows | control | 已由 canonical rows 建立 `migrated` checkpoint；不等同於 current-day verified |

schema v3 另已建立但尚待歷史回補的研究表：`financial_fact_observations`、`market_sessions`、`security_trading_status`、`adjustment_factors`、`adjusted_bars`、`security_lifecycle`、`benchmark_membership`、`daily_market_stats`、`institutional_flows`、`margin_short_snapshots`、`market_events` 與 `market_data_source_links`。表存在、typed upsert 可用，不等於各資料集已達 coverage gate。

## 2. 共同資料契約

所有正式資料集都必須保存：

- `effective_date`：資料描述的期間或事件日期。
- `published_at`／`available_date`：研究者可取得資料的時間；缺失要明確標記，不得以 `fetched_at` 代替。
- `fetched_at`、source endpoint／URL、payload SHA-256、validation status。
- `market_data_fetch_attempts`、`market_data_sync_items` 與 `market_data_sync_state`。
- 原始 payload；小 payload 可 inline，較大或非 JSON payload 使用 hash/URI 外置。

官方來源優先使用 [TWSE OpenAPI](https://openapi.twse.com.tw/)、[TPEx OpenAPI](https://www.tpex.org.tw/openapi/) 與 [MOPS](https://mops.twse.com.tw/)。來源 registry 必須保留 endpoint priority 與實際 fallback 結果；來源格式改變時先進 quarantine，不直接改寫 canonical facts。

## 3. 分階段補齊

### Phase 0：資料層正確性、契約與可觀測性（schema v3 foundation 已完成）

- schema v3、dataset catalog、source registry、fetch attempts、source payload links、PIT facts 與 quality issue dedupe。
- legacy daily／quarterly migration 與 W/M/Q/Y period derivation。
- DB-first provider、同源 HTTPS safe 308 redirect、TWSE/TPEx fallback、current-day fail-closed、read-only verifier。
- daily/enrichment sync profile、真正的 from/to range、dry-run no network/no DB mutation、未實作 dataset non-zero failure。
- 受影響 symbol 的 period/adjusted rebuild、legacy migration fingerprint 與 current-day verification marker。

本階段交付的是「不再靜默略過或重抓已知資料」的資料層能力，不代表下列所有資料已完成歷史回補。任何空表、短歷史或缺少發布日的 dataset 仍標為 partial/planned。

### Phase 1：月營收與估值歷史

目標是把目前每檔一筆 snapshot 擴成可重建的歷史序列，至少補 60 個月；若上市未滿 60 個月則保存上市以來並列明缺口。

- 月營收：以 `revenue_month` 為 effective period，以官方發布／可取得日為 `available_date`；驗證 MoM／YoY 的月份連續性與數值型別。
- 估值：以交易日為 effective date，保存 PE、PB、殖利率與當日來源；缺值不可用鄰日靜默填補。
- 每次同步依 symbol／月份或交易日 partition 增量抓取，已存在且 hash 相同的 payload 不重抓。
- 完成門檻：54/54 coverage、目標期間缺口清單可解釋、重跑冪等、source payload hash 可追溯。

### Phase 2：年度財務 facts

目標是建立至少 8 個可取得年度的 revenue、gross profit、net income、equity、EPS、ROE facts，並與季度資料分表。

- 來源順序：TWSE／TPEx OpenAPI 可驗證 facts，MOPS 公開財報作補充與原始依據。
- 每一列必須有 fiscal year、`available_date` 或 `published_at`；財報發布後才可被 point-in-time query 看見。
- 同一 fiscal year 的修訂不得覆蓋舊值；以 payload hash、source version 或 revision identity 保存 lineage。
- 完成門檻：54 檔各年度 coverage 報表、0 duplicate fact identity、PIT replay 測試通過、未知格式進 quarantine。

### Phase 3：公司行動與 adjusted series

目標是補股利、現金股利、股票股利、分割、合併、除權息與其他會影響價格解讀的事件。

- `daily_bars` 永遠保留官方 raw OHLCV，不直接覆寫 close。
- 事件存入 `corporate_actions`，包含 action date、ex/record/payment date、ratio、cash amount、來源與發布日。
- adjusted series 必須由事件與固定 derivation version 計算，且可由 raw bars + events 重建。
- 完成門檻：事件 identity 冪等、跨來源衝突可見、調整前後重建 hash 穩定、事件缺口不被當成「無事件」。

### Phase 4：季度 PIT 與 revision lineage

目前季度表有資料，但不能把 row count 當成 PIT 完整。這一階段補：

- 原始季度 payload 與發布日／可取得日的分離。
- 同一季度多次修訂的 revision chain、source hash 與 effective/available interval。
- `as_of` query 只能讀到當時已發布的版本；不能使用後來修訂回填過去研究日。
- 財務數值與 period price bars 分開驗收；缺季度不可用最近季靜默代替。

### Phase 5：價格狀態、歷史 identity、membership 與品質深化

- security master 的上市狀態、名稱／產業變動與 effective interval。
- theme membership 的版本與來源，區分當日 universe 與事後重建 universe。
- `market_sessions`、停牌/處置、漲跌停與不可交易狀態；成交值、市值、流通股數與週轉率等 market stats。
- 產業/類股 benchmark bars 與 PIT 成分，避免相對產業表現只用候選股平均值。
- source SLA、缺口重試、payload retention、raw storage GC 與 schema migration manifest。
- 將資料集狀態分為 `verified`、`partial`、`quarantined`、`failed`、`planned`，讓報告能直接揭露限制。

### Phase 6：法人流向與事件研究

- 三大法人日/週/月、融資融券、借券、重大訊息、法說會、股東會與股利公告。
- 先以 research-only 輔助欄位與 warning 使用；達 95% active coverage、PIT 可重建並完成 20 個交易日 shadow test 後，才可申請接入 ranking。

### Phase 7：選配資料

董監事/大股東、ESG、宏觀、新聞全文與分析師預估只有在來源穩定、發布時間清楚、revision 可重播且有明確研究假說後才進 canonical；否則只進 raw/quarantine。

## 4. 每一個 adapter 的交付順序

1. 先寫 schema／identity／PIT contract 與 fixture。
2. 寫 parser、數值／日期／重複／來源結構驗證。
3. 接 `market_data_fetch_attempts`、raw payload、sync state、quality issue。
4. 先以 dry-run 列出缺口與預計寫入量，再做小批次 backfill。
5. 做 row parity、hash、冪等重跑、斷點恢復與 read-only replay。
6. 只有 dataset gate 通過後，才允許它影響研究 ranking；否則維持 raw/quarantine 或 diagnostic-only。

## 5. 共通驗收 gate

- 來源是 allowlist 內的官方／可稽核公開來源，URL、payload hash、抓取時間可追溯。
- effective date、published／available date 與 fetched date 沒有混用。
- unique identity 與 duplicate count 通過；重新執行不增加重複 canonical rows。
- 缺口、來源錯誤、格式錯誤與 stale data 都有 manifest／quality issue，不被靜默補值。
- 253 日線與 current-day gate 仍由 `daily_bars`／benchmark verifier 獨立負責。
- 任何資料集回補失敗只使該資料集標為 partial；不得把部分資料宣稱完整，也不得影響 daily report 的 fail-closed 規則。
- SQLite integrity、foreign key、raw external payload existence、schema version 與 migration manifest 全部通過。

## 6. 本輪未宣稱完成的資料回補

本輪已實作 schema、PIT/query、DB-first/incremental、同步狀態與測試，但不在沒有 watchdog、source fixture 與完整 manifest 的情況下宣稱已完成年度／公司行動／法人等網路大量回補。後續回補需使用 enrichment profile、小批次、可恢復 checkpoint 與 read-only verification；未知格式不寫入 canonical，不把未完成資料集接入 ranking，也不把 SQLite/raw payload 上傳 repository。
