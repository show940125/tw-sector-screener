# Archived simulator

這個目錄保存舊版投資模擬器、dashboard／Pages 發布腳本、測試、設定範例與歷史 change log。它們是 repository 的歷史記錄，不是現行 sector report workflow 的入口，也不屬於目前的資料庫同步與排名契約。

## Archive boundary

- `program/scripts/`：舊 simulator 與 dashboard 發布 entrypoints。
- `program/src/simulator/`：舊 simulator engine、broker、policy、store 與 dashboard implementation。
- `program/tests/`：與舊 simulator 對應的測試。
- `program/simulator.config.example.json`：舊設定範例。
- `program/.github/workflows/`：舊 dashboard Pages workflow 的歷史副本。
- `CHANGELOG.md`：搬移前完整歷史 change log；root `CHANGELOG.md` 僅保留現行變更索引。

封存採保留原始檔案內容與相對結構的方式，沒有刪除歷史程式。現行文件與每日 automation 不再把這些檔案當作可執行流程；若未來要研究舊行為，應以此目錄與 [archive-manifest.json](archive-manifest.json) 為準。
