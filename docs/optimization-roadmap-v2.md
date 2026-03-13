# TW Sector Screener Optimization Roadmap v2

## Summary

這份 roadmap 不是願望清單，而是照正式評估報告 v2 排過優先級的實作順序。  
原則只有一條：先補最會拖累研究可信度的缺口，再處理 workflow 深化與體驗優化。

## Current Status

- `Milestone A / P0 Data Quality Hardening`：已建立 SQLite 季度資料層，季度刷新、coverage summary、audit metadata 都已接上；歷史厚度仍需持續累積。
- `Milestone B / P1 Validation V2`：已升級為 `factor_aware_cross_sectional_v2` 與多視窗 validation。
- `Milestone C / D / E`：尚未完成，仍屬下一輪優化主體。

## Milestones

### Milestone A: P0 / Data Quality Hardening

- 目標：
  - 建立季度快照累積機制與回補排程
  - 讓核心 theme 池可比較最近兩期品質資料
- 驗收：
  - AI / 半導體核心池前兩期 `quality_score` 覆蓋率達 `80%+`
  - 前 3 名候選不再常態出現 `quality:unavailable`
- 建議分支：
  - `codex/p0-quarterly-snapshot-coverage`

### Milestone B: P1 / Validation V2

- 目標：
  - 把 validation 從 `price_only_cross_sectional` 升級成 `factor-aware`
  - 固定輸出 `1Y / 3Y / 5Y`
- 驗收：
  - 可拆出價格、基本面、品質訊號的邊際效果
  - validation report 固定輸出超額報酬、回撤、換手、成本後報酬
- 建議分支：
  - `codex/p1-factor-aware-validation`

### Milestone C: P1 / Theme Coverage Expansion

- 目標：
  - 擴充 `AI strict` 到 `12-18` 檔
  - 不混入明顯 telecom/operator proxy
- 驗收：
  - 純度維持
  - coverage 提升
  - watchlist 與 rerank 研究價值提高
- 建議分支：
  - `codex/p1-ai-theme-expansion`

### Milestone D: P2 / Workflow Deepening

- 目標：
  - watchlist 新增歷次 rerank 對比、異動摘要、主因拆解
  - audit 新增可直接用於研究會議的版本比較資訊
- 驗收：
  - 同主題跨日期比較可直接進會議紀錄
- 建議分支：
  - `codex/p2-watchlist-delta-summary`

### Milestone E: P2 / Action Engine Upgrade

- 目標：
  - action engine 加入事件狀態機
  - 處理法說、月營收、產業節奏切換
- 驗收：
  - 報告能區分交易型加碼、研究型加碼、事件前降風險
- 建議分支：
  - `codex/p2-action-engine-upgrade`

## Re-rating Targets

- 專業度上修到 `7.6+`
  - 條件：核心主題池 `70%+` 有最近兩期品質資料，validation 升級為 factor-aware
- 投資/加減碼可執行性上修到 `7.5+`
  - 條件：action view 納入事件框架
- 工作流可近用性上修到 `8.8+`
  - 條件：watchlist 與 audit 可直接支援研究會議比較
- 機構級研究流程可遷入性上修到 `7.0+`
  - 條件：形成 `1Y / 3Y / 5Y` 一致驗證框架與穩定 theme coverage

## Repo Operating Rules

- 官方執行輸出不進 git，固定放在 `C:\Users\a0953041880\tw-sector-screener-output`
- repo 只保留 `examples/sample-reports/` 內的人工挑選樣本
- 每個 milestone 都需要：
  - 測試
  - 文件同步
  - sample reports 更新
  - tag / release note
