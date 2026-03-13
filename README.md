# TW Sector Screener

台股題材/產業初篩工具。  
現在的定位不是「幫你直接下單」，而是把研究排序、信心分數、benchmark-relative 視角、action 建議、watchlist 與 audit trail 一次吐出來，讓它能進日常研究流程。

## 現在能做什麼

- 嚴格題材池與廣義題材池切換：`strict` / `broad`
- 主題拆分：`AI`、`AI infra`、`AI server/ODM`、`memory`、`foundry`、`IC design`、`半導體`
- 排名與動作分離：`idea score` 不再直接假裝等於下單建議
- 缺值顯式揭露：`missing_factor_count`、`confidence_score`、`data_quality_flags`
- Structured outputs：`md`、`json`、`csv`
- Workflow 輸出：`audit trail`、`watchlist`
- 本地 cache：降低重複抓 TWSE / TPEx 的等待時間

## 快速開始

```powershell
python "C:\Users\...\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme AI `
  --theme-mode strict `
  --benchmark TAIEX `
  --as-of 2026-03-12 `
  --top-n 8 `
  --run-backtest `
  --validation-window 1y `
  --output-format md,json,csv `
  --coverage-list "C:\Users\a0953041880\tw-reports\coverage-list.txt" `
  --output-root "C:\Users\a0953041880\tw-sector-screener-output"
```

輸出根目錄預設固定為：

- `C:\Users\...\tw-sector-screener-output`

輸出結構：

- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.md`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.json`
- `reports/<yyyymmdd>/<theme>/sector-report-<theme>-<yyyymmdd>.csv`
- `audit/<yyyymmdd>/sector-report-<theme>-<yyyymmdd>.audit.json`
- `watchlists/<theme>/watchlist-<theme>-<yyyymmdd>.json`
- `backtests/<theme>/validation-<theme>-<yyyymmdd>.json`
- `cache/market/`
- `coverage-lists/`

## 參數

- `--theme`：主題名稱
- `--theme-mode`：`strict` / `broad`
- `--benchmark`：`TAIEX` / `sector` / `custom`
- `--output-format`：逗號分隔，支援 `md`、`json`、`csv`
- `--config`：JSON/YAML config 路徑
- `--coverage-list`：watchlist symbol 清單，支援 txt/json
- `--run-backtest`：輸出 price-only validation report
- `--rebalance`：`weekly` / `monthly`
- `--cost-bps`：回測交易成本
- `--validation-window`：`1y` / `3y` / `5y`
- `--top-n`：輸出前 N 檔
- `--universe-limit`：最多分析幾檔
- `--min-monthly-revenue`：最低月營收門檻
- `--lookback`：回看日數
- `--output-root`：官方輸出根目錄
- `--output-dir`：deprecated alias，仍可用但不建議

## Config 範例

```json
{
  "weights": {
    "trend_score": 0.28,
    "momentum_score": 0.22,
    "value_score": 0.16,
    "fundamental_score": 0.16,
    "quality_score": 0.10,
    "benchmark_score": 0.05,
    "risk_control_score": 0.03
  },
  "benchmark": {
    "type": "custom",
    "symbols": ["2330", "2454", "2382"]
  },
  "filters": {
    "min_monthly_revenue": 1000000000
  }
}
```

## 報告怎麼讀

- `Idea Score`：研究優先序，不是下單按鈕
- `Confidence`：資料覆蓋度與可用性
- `Factor Coverage / Data Freshness`：把缺值與資料新鮮度拆開看
- `Action`：`Overweight` / `Neutral` / `Underweight`
- `Why Now / Why Not`：現在可看與不能太衝的理由
- `Add Trigger / Trim Trigger`：加減碼條件
- `Validation`：看這套排序在近 1Y / 3Y / 5Y 的簡化驗證表現
- `Audit`：本次參數、警示、cache 與 coverage 設定

## 注意事項

- 這是研究輔助工具，不是投顧牌照替代品。
- 缺值不再補成中性分；看到低 confidence，就該保守，不要硬凹。
- `quality_score` 目前用官方最新季 + 本地快照回補前期，前期快照會隨你日常執行逐步變完整。
- 若要用 `benchmark=custom`，請在 config 內提供 `benchmark.symbols`。

## 開發與版本控制

- repo 採 `Feature Branch + PR` 流程，不直接在 `main` 堆功能。
- 分支名稱固定用 `codex/` 前綴，例如：
  - `codex/p0-quarterly-snapshot-coverage`
  - `codex/p1-factor-aware-validation`
- 官方執行輸出固定在 `C:\Users\...\tw-sector-screener-output`，不進 git。
