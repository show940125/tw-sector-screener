# Contributing

## Branching

- `main` 只放可用版本。
- 所有功能、修補、方法論調整都從 `codex/` 前綴的 feature branch 開始。
- 分支命名建議：
  - `codex/p0-quarterly-snapshot-coverage`
  - `codex/p1-factor-aware-validation`
  - `codex/p1-ai-theme-expansion`
  - `codex/p2-watchlist-delta-summary`

## Commit Rules

- commit type 固定使用：`feat:`、`fix:`、`docs:`、`test:`、`chore:`
- 一個 commit 只處理一種邏輯變更，不把功能、文件、樣本更新混成一包。
- 若是輸出契約變更，文件與樣本要一起更新，但請拆成相鄰的小 commits，不要做成巨型雜燴。

## Pull Request Rules

- PR 必須附測試結果，至少包含：
  - `python -m unittest discover -s tests`
- 若影響 CLI、輸出契約或 config：
  - 必須同步更新 `README.md`
  - 必須同步更新 `SKILL.md`
  - 必須同步更新 `config.example.json`
- 若影響報告欄位、watchlist、audit、validation：
  - 必須同步更新 `examples/sample-reports/`
- 若影響評分方法、研究框架或 roadmap：
  - 必須同步更新正式評估報告或 roadmap 文件

## Samples And Generated Outputs

- 官方執行輸出一律放在：
  - `C:\Users\a0953041880\tw-sector-screener-output`
- 這個目錄不進 git。
- repo 內只保留人工挑選的樣本輸出，放在：
  - `examples/sample-reports/`
- 樣本最低要求：
  - `report.md`
  - `audit.json`
  - `validation.json`

## Verification Before Merge

- merge 前至少確認：
  - 測試全過
  - `git status` 沒有非預期產物
  - 受影響的樣本輸出已更新
  - 已知限制寫進 PR 說明

## Release And Tags

- 每完成一個大里程碑就建立 tag。
- tag 建議格式：
  - `v0.2.0-p0-data-quality`
  - `v0.3.0-validation-v2`
- GitHub release note 只寫四件事：
  - 新增能力
  - 契約變更
  - 驗證結果
  - 已知限制與下一個缺口
