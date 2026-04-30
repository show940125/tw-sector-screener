# 台股類股選股報告

- 主題：`AI`
- 截止日：`2026-04-29`

## 摘要
Thesis：AI 類股目前由 `2382` 廣達 領跑，top 8 平均 idea score `61.7`。 Evidence：相對題材 20 日超額 `-14.59%`，confidence `86.4`。 Risk：missing:quality_score；quality:previous_period_unavailable。 Action：`Neutral`；建議評估 `賣出`。 What changes my mind：若相對題材 20 日動能轉負、confidence 下滑或法說/營收驗證失敗，就降級。

## 加權總攬（TAIEX）
- 收盤 `39303.50`，1D -218.23 點 / -0.55%
- 報酬：5D 3.76%、20D 20.87%、63D 25.14%、126D 42.16%
- 均線：SMA20 `36284.16`、SMA60 `34187.63`、SMA120 `31342.02`；RSI14 `73.64`；趨勢分數 `80.00`
- 來源：TWSE exchangeReport/FMTQIK

## 類股總攬
- 評分母體 `9` 檔，Top8 平均 idea score `61.70` / 平均 confidence `86.40`
- 建議評估分布：賣出 2、持有 5、買入 1
- 因子權重：trend_score 28%、momentum_score 22%、value_score 16%、fundamental_score 16%、quality_score 10%、benchmark_score 5%、risk_control_score 3%
- Benchmark 視角：20D 題材平均 `29.18`%，相對大盤 `8.31`%
- Quality coverage：當期完整 `100.00`%，前期完整 `0.00`%
- History coverage：近 `8` 季完整覆蓋 `0.00`%

## 方法與共識
- Rank 看的是 idea score 與資料可信度的合成，不再把缺值直接補成 50 分。
- Confidence 拆成 factor coverage 與 data freshness 兩段，避免把資料缺漏跟舊資料混成一團。
- Benchmark 同時看相對 TAIEX、相對題材、相對產業，避免只用絕對漲幅自嗨。
- Action 與 ranking 拆開：排名是研究優先序，Overweight/Neutral/Underweight 才是動作建議。
- Validation 已升級成 factor-aware cross-sectional v2，固定輸出 1Y / 3Y / 5Y 視窗與 factor sleeves。

## 候選清單
| 排名 | 代碼 | 名稱 | Idea Score | Confidence | Risk | 建議 | Action | Thesis Summary | Why Now | Why Not |
|---:|---|---|---:|---:|---:|---|---|---|---|---|
| 1 | 2382 | 廣達 | 70.57 | 86.40 | 70.00 | 賣出 | Neutral | 廣達 屬於 mid 動能 / high 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 2 | 3017 | 奇鋐 | 68.88 | 86.40 | 83.56 | 持有 | Neutral | 奇鋐 屬於 high 動能 / high 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 3 | 2356 | 英業達 | 63.62 | 86.40 | 60.00 | 買入 | Neutral | 英業達 屬於 low 動能 / high 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 4 | 2454 | 聯發科 | 62.65 | 86.40 | 72.44 | 持有 | Neutral | 聯發科 屬於 high 動能 / mid 基本面組合，估值區間偏 neutral。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 5 | 2330 | 台積電 | 60.01 | 86.40 | 36.67 | 持有 | Neutral | 台積電 屬於 mid 動能 / mid 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |
| 6 | 2345 | 智邦 | 58.57 | 86.40 | 95.00 | 持有 | Neutral | 智邦 屬於 high 動能 / low 基本面組合，估值區間偏 expensive。 | 20 日相對大盤仍有超額動能 / 同題材內相對強勢 / 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 |
| 7 | 2376 | 技嘉 | 55.17 | 86.40 | 95.00 | 賣出 | Neutral | 技嘉 屬於 low 動能 / mid 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 波動偏高，容易把正確方向洗掉 / 對同題材沒有明顯領先 |
| 8 | 3231 | 緯創 | 54.16 | 86.40 | 64.44 | 持有 | Underweight | 緯創 屬於 low 動能 / mid 基本面組合，估值區間偏 cheap。 | 資料覆蓋度足夠，結論可用度較高 | 對同題材沒有明顯領先 |

## 前 8 名個股趨勢（Top 8）
| 排名 | 代碼 | 收盤 | 20D% | 相對大盤20D | 相對題材20D | 相對產業20D | RSI14 | 波動20% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2382 | 322.00 | 14.59 | -6.28 | -14.59 | -7.82 | 56.99 | 41.51 |
| 2 | 3017 | 2835.00 | 32.79 | 11.92 | 3.61 | 10.38 | 70.34 | 68.46 |
| 3 | 2356 | 46.30 | 11.84 | -9.03 | -17.34 | -10.57 | 56.79 | 32.53 |
| 4 | 2454 | 2575.00 | 70.53 | 49.66 | 41.35 | 24.03 | 80.38 | 68.29 |
| 5 | 2330 | 2180.00 | 22.47 | 1.61 | -6.71 | -24.03 | 65.45 | 38.90 |
| 6 | 2345 | 2210.00 | 35.17 | 14.30 | 5.99 | 0.00 | 66.48 | 76.22 |
| 7 | 2376 | 272.50 | 19.52 | -1.35 | -9.66 | -2.89 | 56.59 | 52.84 |
| 8 | 3231 | 140.50 | 11.95 | -8.91 | -17.23 | -10.46 | 57.90 | 35.84 |

## 倉位建議
- `2382` 廣達：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `316.71` ~ `324.65`
  target：300.83 / 311.42 / 322.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3017` 奇鋐：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2754.09` ~ `2875.45`
  target：2673.19 / 2835.00 / 2996.81
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2356` 英業達：`買入` / `Neutral`，研究動作 `研究型加碼`，進場區間 `45.48` ~ `46.71`
  target：45.48 / 47.93 / 49.56
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA20 且 benchmark-relative 同步轉弱
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2454` 聯發科：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2514.60` ~ `2605.20`
  target：2454.19 / 2575.00 / 2695.81
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2330` 台積電：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2149.98` ~ `2195.01`
  target：2119.95 / 2180.00 / 2240.05
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2345` 智邦：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2149.75` ~ `2240.12`
  target：2089.50 / 2210.00 / 2330.50
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2376` 技嘉：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `267.46` ~ `275.02`
  target：252.34 / 262.42 / 272.50
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3231` 緯創：`持有` / `Underweight`，研究動作 `降風險`，進場區間 `138.24` ~ `141.63`
  target：135.97 / 140.50 / 145.03
  add trigger：先等資料或趨勢修復，不急著撿便宜。
  trim trigger：若已有部位，事件前先把風險降到你睡得著的水位。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable

## 風險提示
- 這是研究輔助，不是保證報酬；遇到法說、月營收、AI 出貨節奏變化時，結論需要重新驗證。
- 若 benchmark-relative 轉負且 confidence 下滑，應優先減碼而不是凹單。
- 季度品質前期覆蓋仍未達高水位，quality score 的歷史比較仍需靠 SQLite 歷史累積補厚。
- 近 8 季完整覆蓋仍偏薄，長期品質比較要再靠回補批次補齊。

## Validation
- mode：`factor_aware_cross_sectional_v2`；window：`1y`；rebalance：`monthly`；cost `10.00` bps
- excess return `27.79`%；max drawdown `-2.85`%；hit rate `0.6364`
- factor sleeves：price `33.71`%、fundamental `31.75`%、quality `8.98`%
- 1y：excess `27.79`% / drawdown `-2.85`% / hit `0.6364`
- 3y：excess `21.12`% / drawdown `-14.41`% / hit `0.5625`
- 5y：excess `21.12`% / drawdown `-14.41`% / hit `0.5625`

## 資料與流程稽核
- theme mode：`strict`
- benchmark：`TAIEX`
- output formats：`csv,json,md`
- warnings：`0`
- output root：`%USERPROFILE%\tw-sector-screener-output`
- quarterly store：`%USERPROFILE%\tw-sector-screener-output\cache\market\quarterly_fundamentals.sqlite`；period requirement：`2`；refresh run：`None`
- quality update：mode `auto` / decision `sync-repair` / budget `3.00` sec / backfill `backfill-auto-check-449800f7`

## 資料來源
- TWSE OpenAPI
- TWSE exchangeReport
- TPEx OpenAPI
- TPEx afterTrading API
