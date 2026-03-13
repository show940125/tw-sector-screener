# 台股類股選股報告

- 主題：`AI`
- 截止日：`2026-03-12`

## 摘要
Thesis：AI 類股目前由 `2345` 智邦 領跑，top 8 平均 idea score `61.5`。 Evidence：相對題材 20 日超額 `22.02%`，confidence `82.2`。 Risk：missing:quality_score；quality:unavailable；quality:previous_period_unavailable。 Action：`Neutral`。 What changes my mind：若相對題材 20 日動能轉負、confidence 下滑或法說/營收驗證失敗，就降級。

## 加權總攬（TAIEX）
- 收盤 `33581.86`，1D -532.33 點 / -1.56%
- 報酬：5D -0.27%、20D 6.19%、63D 22.82%、126D 38.56%
- 均線：SMA20 `33427.64`、SMA60 `31048.70`、SMA120 `28996.02`；RSI14 `54.05`；趨勢分數 `90.00`
- 來源：TWSE exchangeReport/FMTQIK

## 類股總攬
- 評分母體 `9` 檔，Top8 平均 idea score `61.51` / 平均 confidence `85.88`
- 因子權重：trend_score 28%、momentum_score 22%、value_score 16%、fundamental_score 16%、quality_score 10%、benchmark_score 5%、risk_control_score 3%
- Benchmark 視角：20D 題材平均 `10.00`%，相對大盤 `3.81`%
- Quality coverage：當期完整 `88.89`%，前期完整 `0.00`%

## 方法與共識
- Rank 看的是 idea score 與資料可信度的合成，不再把缺值直接補成 50 分。
- Confidence 拆成 factor coverage 與 data freshness 兩段，避免把資料缺漏跟舊資料混成一團。
- Benchmark 同時看相對 TAIEX、相對題材、相對產業，避免只用絕對漲幅自嗨。
- Action 與 ranking 拆開：排名是研究優先序，Overweight/Neutral/Underweight 才是動作建議。
- Validation 已升級成 factor-aware cross-sectional v2，固定輸出 1Y / 3Y / 5Y 視窗與 factor sleeves。

## 候選清單
| 排名 | 代碼 | 名稱 | Idea Score | Confidence | Action | Thesis Summary | Why Now | Why Not |
|---:|---|---|---:|---:|---|---|---|---|
| 1 | 2345 | 智邦 | 71.58 | 82.20 | Neutral | 智邦 屬於 high 動能 / high 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 2 | 3017 | 奇鋐 | 67.92 | 86.40 | Neutral | 奇鋐 屬於 high 動能 / mid 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 3 | 6669 | 緯穎 | 64.76 | 86.40 | Neutral | 緯穎 屬於 mid 動能 / high 基本面組合，估值區間偏 neutral。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 4 | 2330 | 台積電 | 63.43 | 86.40 | Neutral | 台積電 屬於 high 動能 / low 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 5 | 3231 | 緯創 | 63.30 | 86.40 | Neutral | 緯創 屬於 mid 動能 / high 基本面組合，估值區間偏 neutral。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 6 | 2382 | 廣達 | 59.80 | 86.40 | Neutral | 廣達 屬於 mid 動能 / mid 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 7 | 2376 | 技嘉 | 53.92 | 86.40 | Underweight | 技嘉 屬於 low 動能 / mid 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 8 | 2454 | 聯發科 | 47.39 | 86.40 | Underweight | 聯發科 屬於 mid 動能 / low 基本面組合，估值區間偏 neutral。 | 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 / 對同題材沒有明顯領先 |

## 前 8 名個股趨勢（Top 8）
| 排名 | 代碼 | 收盤 | 20D% | 相對大盤20D | 相對題材20D | 相對產業20D | RSI14 | 波動20% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2345 | 1505.00 | 32.02 | 25.83 | 22.02 | 0.00 | 62.90 | 74.11 |
| 2 | 3017 | 1875.00 | 25.84 | 19.65 | 15.84 | 18.09 | 64.87 | 73.60 |
| 3 | 6669 | 4085.00 | 13.31 | 7.12 | 3.31 | 5.56 | 53.89 | 56.02 |
| 4 | 2330 | 1885.00 | 6.80 | 0.61 | -3.20 | 1.05 | 53.64 | 38.96 |
| 5 | 3231 | 132.50 | 3.52 | -2.68 | -6.49 | -4.24 | 49.61 | 35.68 |
| 6 | 2382 | 288.50 | 5.68 | -0.51 | -4.32 | -2.07 | 52.52 | 34.79 |
| 7 | 2376 | 228.00 | 0.22 | -5.97 | -9.78 | -7.53 | 49.06 | 38.77 |
| 8 | 2454 | 1785.00 | 4.69 | -1.50 | -5.31 | -1.05 | 53.84 | 55.11 |

## 倉位建議
- `2345` 智邦：`Neutral`，進場區間 `1463.56` ~ `1525.72`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3017` 奇鋐：`Neutral`，進場區間 `1823.38` ~ `1900.81`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `6669` 緯穎：`Neutral`，進場區間 `3988.89` ~ `4133.06`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2330` 台積電：`Neutral`，進場區間 `1856.79` ~ `1899.10`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3231` 緯創：`Neutral`，進場區間 `130.21` ~ `133.64`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2382` 廣達：`Neutral`，進場區間 `284.08` ~ `290.71`
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2376` 技嘉：`Underweight`，進場區間 `224.11` ~ `229.95`
  add trigger：先等資料或趨勢修復，不急著撿便宜。
  trim trigger：若已有部位，事件前先把風險降到你睡得著的水位。
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2454` 聯發科：`Underweight`，進場區間 `1746.44` ~ `1804.28`
  add trigger：先等資料或趨勢修復，不急著撿便宜。
  trim trigger：若已有部位，事件前先把風險降到你睡得著的水位。
  data flags：missing:quality_score / quality:previous_period_unavailable

## 風險提示
- 這是研究輔助，不是保證報酬；遇到法說、月營收、AI 出貨節奏變化時，結論需要重新驗證。
- 若 benchmark-relative 轉負且 confidence 下滑，應優先減碼而不是凹單。
- 季度品質前期覆蓋仍未達高水位，quality score 的歷史比較仍需靠 SQLite 歷史累積補厚。

## Validation
- mode：`factor_aware_cross_sectional_v2`；window：`1y`；rebalance：`monthly`；cost `10.00` bps
- excess return `10.72`%；max drawdown `-12.49`%；hit rate `0.5909`
- factor sleeves：price `4.95`%、fundamental `7.18`%、quality `-9.11`%
- 1y：excess `10.72`% / drawdown `-12.49`% / hit `0.5909`
- 3y：excess `0.30`% / drawdown `-15.17`% / hit `0.5625`
- 5y：excess `0.30`% / drawdown `-15.17`% / hit `0.5625`

## 資料與流程稽核
- theme mode：`strict`
- benchmark：`TAIEX`
- output formats：`csv,json,md`
- warnings：`0`
- output root：`C:\Users\a0953041880\tw-sector-screener-output`
- quarterly store：`C:\Users\a0953041880\tw-sector-screener-output\cache\market\quarterly_fundamentals.sqlite`；period requirement：`2`；refresh run：`None`

## 資料來源
- TWSE OpenAPI
- TWSE exchangeReport
- TPEx OpenAPI
- TPEx afterTrading API
