# TW Market Data Contract Sync Implementation Plan

## Objective

完成 canonical `market_data.sqlite` 的 DB-first、可增量、可稽核資料契約，並讓 AI/半導體 daily report workflow 使用同一個同步與驗證邊界。此計畫不包含任何交易執行或發佈動作。

## Delivered in this implementation

- canonical market-data schema version 2。
- dataset catalog、source registry、fetch attempts、sync items、dataset checkpoints。
- quality issue fingerprint deduplication plus occurrence history。
- source payload hybrid storage：小 JSON inline，大 payload 以 hash/URI 外置。
- `scripts/sync_market_data.py`：datasets、incremental/full mode、date range metadata、dry-run。
- `scripts/verify_market_data.py`：不初始化、不遷移、不寫入 DB 的 integrity/coverage gate。
- adapter contracts in `src/providers/market_data_adapters.py`。
- development contract in `docs/market-data-database-development.md`。

## Follow-up work, in order

1. 將 provider 的每一個 HTTP request 實際連接 `market_data_fetch_attempts`，補上 endpoint、redirect chain、HTTP status、fallback level 與 payload hash，而不是只記錄高階 provider attempt。
2. 完成 quarterly/annual adapter：欄位 mapping、單位、期間解析、published/available date、revision history 與 quarantine。
3. 完成 monthly revenue、valuation、corporate-actions 歷史 adapter，並建立 adjusted-price query layer；raw OHLCV 不可被覆寫。
4. 將 screener 的 market DB 參數、lookback、coverage gate 與 sync manifest 驗證接入 CI/automation smoke tests。
5. 對既有 canonical DB 做一次 read-only parity/hash/integrity manifest；驗證外置 raw payload 仍可讀。

## Acceptance gates

- schema version = 2 and `PRAGMA integrity_check = ok`。
- foreign-key check has zero rows。
- duplicate quality issue fingerprints do not create duplicate master issues, but every observation is counted in occurrences。
- DB hit with verified current-day data performs no network call。
- a missing current-day bar cannot pass by using an older cache row。
- `verify_market_data.py` returns non-zero for missing lookback/current-day/benchmark and does not mutate the DB。
- all changes have focused tests and `python -m unittest discover -s tests` passes。

## Evidence and limitations

The current implementation establishes the schema and contracts; it does not claim that every annual, quarterly, valuation, or corporate-action row has already been backfilled. Any dataset without a validated adapter remains outside the ranking gate and must be reported as unavailable or planned.
