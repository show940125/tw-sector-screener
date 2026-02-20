---
name: tw-sector-screener
description: Use when screening Taiwan sector/theme stocks (semiconductor, AI, memory) and outputting ranked buy candidates with explainable position plans.
---

# TW Sector Screener

用免費資料源做台股「類股/題材」選股，輸出 Top N 候選名單、分數、理由與可執行倉位建議。

## Use This Skill

適用：
- 想看整個類股（半導體、AI、記憶體）哪些股票相對值得研究/布局
- 要有可追蹤的 Markdown 報告（可回測口徑）
- 要把「分數」轉成可執行倉位規則

不適用：
- 即時下單或自動交易
- Tick 級資料需求

## Data Strategy

資料來源優先：
1. TWSE OpenAPI + exchangeReport
2. TPEx OpenAPI + afterTrading API

## Command

```powershell
python "C:\Users\a0953041880\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme 半導體 `
  --as-of 2026-02-20 `
  --top-n 10 `
  --universe-limit 60
```

## Parameters

- `--theme`：類股/主題（必填）
- `--as-of`：分析截止日 `YYYY-MM-DD`
- `--top-n`：輸出前 N 檔
- `--universe-limit`：候選股最大分析數
- `--min-monthly-revenue`：月營收最低門檻（元）
- `--lookback`：回看日數（預設 252）
- `--output-dir`：輸出目錄

## Output Contract

- 檔名：`sector-report-<theme>-<yyyymmdd>.md`
- 必含章節：
  - `摘要`
  - `方法與共識`
  - `候選清單`
  - `倉位建議`
  - `風險提示`
  - `資料來源`

## Notes

- 研究用途，非投資建議。
- 因子權重預設：Trend 35%、Momentum 25%、Value 20%、Fundamental 15%、Risk 5%。
