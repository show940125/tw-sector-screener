# TW Sector Screener

台股類股選股工具skill。  
目標是從一整個主題（例如半導體、AI、記憶體）裡，找出相對值得優先研究的個股，並給出可執行的倉位建議。

## 功能

- 主題式選股（`半導體`、`AI`、`記憶體`）
- 多因子評分與排序（Trend / Momentum / Value / Fundamental / Risk）
- 輸出 Markdown 報告（含前 N 名名單與人話解讀）
- 倉位建議（上限、分批比例、單筆風險、停損價、股數公式）

## 資料來源

- TWSE OpenAPI + exchangeReport
- TPEx OpenAPI + afterTrading API

## 快速開始

### 1. 直接執行

```powershell
python "C:\Users\...\.codex\skills\tw-sector-screener\scripts\tw_sector_screener.py" `
  --theme 半導體 `
  --as-of 2026-02-20 `
  --top-n 10 `
  --universe-limit 60 `
  --output-dir C:\Users\...\tw-reports
```

### 2. 查看結果

報告檔名格式：

```text
sector-report-<theme>-<yyyymmdd>.md
```

例如：

```text
C:\Users\...\tw-reports\sector-report-半導體-20260220.md
```

## 參數說明

- `--theme`：主題名稱（必填）
- `--as-of`：分析截止日，格式 `YYYY-MM-DD`
- `--top-n`：輸出前 N 檔
- `--universe-limit`：最多分析多少候選股
- `--min-monthly-revenue`：最低月營收門檻（元）
- `--lookback`：回看交易日數（預設 252）
- `--timeout`：HTTP timeout 秒數（預設 10）
- `--output-dir`：報告輸出資料夾

## 評分邏輯（預設權重）

- Trend：35%
- Momentum：25%
- Value：20%
- Fundamental：15%
- Risk control：5%

### 因子定義（簡版）

- Trend：均線結構 + RSI（Wilder）
- Momentum：63/126 日報酬在同主題內的分位
- Value：PE / PB / 殖利率在同主題內的相對分位
- Fundamental：月營收 YoY / MoM 分位
- Risk control：波動率與流動性的相對分位

## 報告內容

每份報告至少包含：

- `摘要`
- `方法與共識`
- `候選清單`
- `倉位建議`
- `風險提示`
- `資料來源`

## 倉位建議怎麼用（實務）

報告中會有像這種資訊：

```text
上限 12.00%、首筆 4.80%、單筆風險 0.40%
停損價 234.63（距離 15.60%）
可買股數 = (資金 x 單筆風險%) / (進場價 - 停損價)
```

操作概念：

1. 先決定單檔最多放多少資金（上限%）
2. 不一次買滿，先下首筆，走勢確認後再分批加碼
3. 先鎖住單筆最大可承受虧損（單筆風險%）
4. 用停損距離反推可買股數，避免部位過大

## 注意事項

- 這是研究工具，不是投資建議
- 分數是排序工具，不保證報酬
- 建議搭配你自己的進出場規則與風險控管

