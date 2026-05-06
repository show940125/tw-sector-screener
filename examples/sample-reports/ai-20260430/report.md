# 台股類股選股報告

- 主題：`AI`
- 截止日：`2026-04-30`

## 摘要
Thesis：AI 類股目前 買進榜首 `2330` 台積電，buyability `62.35`；正式可買 1 檔；接近可買 4 檔；小部位試單 3 檔；等待觸發 0 檔；避開/降風險 13 檔。下一步看 `3037` 欣興：3037 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。；最高研究優先標的 `8271` 宇瞻 目前建議 `持有`，recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility。 Research List top 20 平均 idea score `67.6`。 Evidence：相對題材 20 日超額 `-11.02%`，confidence `82.2`。 Risk：missing:quality_score；quality:unavailable；quality:previous_period_unavailable。 Action：`Neutral`；建議評估 `買入`。 What changes my mind：若相對題材 20 日動能轉負、confidence 下滑或法說/營收驗證失敗，就降級。

## 加權總攬（TAIEX）
- 收盤 `38926.63`，1D -376.87 點 / -0.96%
- 報酬：5D 3.21%、20D 22.71%、63D 23.03%、126D 42.58%
- 均線：SMA20 `36644.34`、SMA60 `34307.30`、SMA120 `31433.50`；RSI14 `69.56`；趨勢分數 `80.00`
- 來源：TWSE exchangeReport/FMTQIK

## 類股總攬
- 評分母體 `54` 檔，Top20 平均 idea score `67.62` / 平均 confidence `83.19`
- 建議評估分布：持有 17、賣出 3
- 決策梯度分布：avoid 13、starter_position 3、near_buy 4
- 因子權重：trend_score 28%、momentum_score 22%、value_score 16%、fundamental_score 16%、quality_score 10%、benchmark_score 5%、risk_control_score 3%
- Benchmark 視角：20D 題材平均 `32.33`%，相對大盤 `9.62`%
- Quality coverage：當期完整 `12.96`%，前期完整 `0.00`%
- History coverage：近 `8` 季完整覆蓋 `0.00`%

## 方法與共識
- Rank 看的是 idea score 與資料可信度的合成，不再把缺值直接補成 50 分。
- Confidence 拆成 factor coverage 與 data freshness 兩段，避免把資料缺漏跟舊資料混成一團。
- Benchmark 同時看相對 TAIEX、相對題材、相對產業，避免只用絕對漲幅自嗨。
- Action 與 ranking 拆開：排名是研究優先序，Overweight/Neutral/Underweight 才是動作建議。
- Validation 已升級成 v3：保留 factor sleeves，並加入 portfolio risk diagnostics 與 benchmark-relative attribution。

## Macro Regime Overlay
- regime：`neutral`；risk level：`normal`；risk adjustment `0.00`
- source：`macro-regime-local-proxy`；tier：`supplementary`；rank signal：`False`
- evidence refs：market_overview.trend_score, market_overview.ret_20d, market_overview.rsi14, macro_regime_overlay, macro-regime-local-proxy

## Coverage Universe
- mode：`coverage`；source：`curated_theme_library`
- universe size：`54`；ranked `54`；limit applied `False`
- buckets：ai_server_odm 9、semiconductor 28、foundry 5、ic_design 8、power_connector_chassis 3、display_proxy 2、networking_optical 1、telecom_proxy 3、memory_hbm 9、cooling_thermal 4、advanced_packaging_substrate 3、pcb_ccl 3、testing_equipment 3、materials 3

## Buying Ranking / 買進優先序
| 排名 | 代碼 | 名稱 | Bucket | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 2330 | 台積電 | semiconductor | 62.35 | 75.70 | 1.88 | 3.27 | -30.22 | 34.41 | 62.47 | 82.20 | 43.19 | 買入 | Neutral | 研究排名靠前，等待買進條件確認。 | - |

## Actionable Queue / 可行動候選隊列
這份隊列回答「不能正式買，那現在最接近能做什麼」。它不會把 `賣出` 或 hard blocker 標的包裝成買進。

| 排名 | 代碼 | 名稱 | Bucket | Tier | Actionability | Risk Adj | Sharpe | Sortino | Max DD% | Readiness | 試單x | 下一步 | 為何尚未正式買 | 升級條件 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---|---:|---|---|---|
| 1 | 3037 | 欣興 | advanced_packaging_substrate | near_buy | 60.81 | 60.00 | 2.19 | 4.95 | -54.29 | near | 0.00 | 3037 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。 | recommendation=持有；risk_score 66.0 > 65 | risk_score 降到 65 以下，且 recommendation 升級為買入。 |
| 2 | 3017 | 奇鋐 | cooling_thermal | starter_position | 60.58 | 66.41 | 2.67 | 4.48 | -41.19 | small-size-only | 0.25 | 3017 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 77.1 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 3 | 2356 | 英業達 | ai_server_odm | near_buy | 59.91 | 35.38 | 0.05 | 0.08 | -30.75 | near | 0.00 | 2356 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。 | recommendation=持有；risk_score 66.5 > 65 | risk_score 降到 65 以下，且 recommendation 升級為買入。 |
| 4 | 3711 | 日月光投控 | semiconductor | near_buy | 59.06 | 70.23 | 1.99 | 3.38 | -37.33 | near | 0.00 | 3711 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。 | recommendation=持有；risk_score 72.5 > 65 | risk_score 降到 65 以下，且 recommendation 升級為買入。 |
| 5 | 2303 | 聯電 | semiconductor | wait_for_trigger | 59.03 | 65.77 | 1.52 | 2.23 | -29.66 | waiting | 0.00 | 2303 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 67.8 > 65；idea_score 58.3 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 6 | 2454 | 聯發科 | semiconductor | near_buy | 59.02 | 64.05 | 1.25 | 2.38 | -26.60 | near | 0.00 | 2454 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。 | recommendation=持有；risk_score 70.6 > 65 | risk_score 降到 65 以下，且 recommendation 升級為買入。 |
| 7 | 2317 | 鴻海 | ai_server_odm | wait_for_trigger | 58.33 | 42.06 | 0.60 | 0.86 | -38.86 | waiting | 0.00 | 2317 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 55.4 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 8 | 3189 | 景碩 | advanced_packaging_substrate | starter_position | 57.83 | 63.60 | 2.44 | 4.06 | -42.80 | small-size-only | 0.25 | 3189 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 81.7 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 9 | 8046 | 南電 | advanced_packaging_substrate | starter_position | 57.81 | 61.51 | 2.76 | 5.03 | -46.99 | small-size-only | 0.25 | 8046 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 81.7 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 10 | 3443 | 創意 | semiconductor | wait_for_trigger | 57.26 | 64.48 | 1.87 | 3.30 | -41.17 | waiting | 0.00 | 3443 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 73.4 > 65；idea_score 57.1 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 11 | 2412 | 中華電 | telecom_proxy | wait_for_trigger | 56.91 | 63.86 | 0.59 | 0.79 | -6.57 | waiting | 0.00 | 2412 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 52.3 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 12 | 2382 | 廣達 | ai_server_odm | wait_for_trigger | 56.34 | 44.73 | 0.58 | 0.85 | -33.89 | waiting | 0.00 | 2382 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 56.2 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 13 | 3231 | 緯創 | ai_server_odm | wait_for_trigger | 55.99 | 41.87 | 0.55 | 0.84 | -36.43 | waiting | 0.00 | 3231 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 55.4 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 14 | 2345 | 智邦 | networking_optical | wait_for_trigger | 55.81 | 64.59 | 1.92 | 3.07 | -42.61 | waiting | 0.00 | 2345 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 83.6 > 65；idea_score 61.7 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 15 | 2357 | 華碩 | ai_server_odm | wait_for_trigger | 55.16 | 26.12 | -0.21 | -0.27 | -37.80 | waiting | 0.00 | 2357 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 54.7 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 16 | 3034 | 聯詠 | semiconductor | wait_for_trigger | 54.56 | 18.99 | -0.73 | -1.01 | -35.65 | waiting | 0.00 | 3034 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 57.3 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 17 | 3045 | 台灣大 | telecom_proxy | wait_for_trigger | 54.41 | 48.89 | 0.03 | 0.04 | -12.18 | waiting | 0.00 | 3045 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 51.1 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 18 | 6669 | 緯穎 | ai_server_odm | wait_for_trigger | 54.09 | 65.70 | 1.53 | 2.67 | -30.08 | waiting | 0.00 | 6669 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 67.8 > 65；idea_score 50.2 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 19 | 2379 | 瑞昱 | semiconductor | wait_for_trigger | 52.69 | 40.49 | 0.10 | 0.13 | -23.93 | waiting | 0.00 | 2379 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 48.4 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 20 | 2353 | 宏碁 | ai_server_odm | wait_for_trigger | 50.41 | 16.87 | -0.71 | -0.94 | -38.02 | waiting | 0.00 | 2353 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 66.5 > 65；idea_score 50.1 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |

## Watchlist / 追蹤與處理清單
| 排名 | 代碼 | 名稱 | Bucket | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 8271 | 宇瞻 | semiconductor | 61.55 | 66.26 | 2.23 | 3.38 | -37.65 | 66.30 | 77.45 | 86.40 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 2 | 2337 | 旺宏 | semiconductor | 63.83 | 71.09 | 2.62 | 4.45 | -27.81 | 74.75 | 77.30 | 86.40 | 85.78 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 85.8 > 65；hard blocker: extreme-volatility |
| 3 | 4967 | 十銓 | semiconductor | 59.86 | 68.80 | 2.08 | 3.58 | -33.26 | 64.76 | 76.13 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 4 | 3260 | 威剛 | semiconductor | 59.08 | 66.95 | 2.49 | 3.71 | -36.19 | 66.48 | 75.29 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 5 | 2451 | 創見 | semiconductor | 56.61 | 66.23 | 1.79 | 2.76 | -32.70 | 60.17 | 69.85 | 82.20 | 92.85 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 92.8 > 65 |
| 6 | 3017 | 奇鋐 | cooling_thermal | 58.61 | 66.41 | 2.67 | 4.48 | -41.19 | 57.74 | 69.05 | 82.20 | 77.11 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 77.1 > 65 |
| 7 | 6274 | 台燿 | pcb_ccl | 56.28 | 69.07 | 2.85 | 4.59 | -34.64 | 60.49 | 68.67 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 8 | 2356 | 英業達 | ai_server_odm | 53.54 | 35.38 | 0.05 | 0.08 | -30.75 | 36.94 | 68.13 | 82.20 | 66.52 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.5 > 65 |
| 9 | 8996 | 高力 | cooling_thermal | 54.25 | 61.07 | 2.02 | 3.56 | -47.87 | 71.46 | 67.80 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 10 | 6147 | 頎邦 | semiconductor | 56.31 | 75.53 | 1.86 | 3.04 | -23.70 | 48.90 | 64.10 | 85.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 11 | 4919 | 新唐 | semiconductor | 49.78 | 42.89 | 1.06 | 1.74 | -49.65 | 59.36 | 66.09 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 12 | 8046 | 南電 | advanced_packaging_substrate | 55.39 | 61.51 | 2.76 | 5.03 | -46.99 | 71.05 | 65.89 | 82.20 | 81.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.7 > 65 |
| 13 | 3037 | 欣興 | advanced_packaging_substrate | 57.08 | 60.00 | 2.19 | 4.95 | -54.29 | 91.03 | 62.37 | 86.40 | 65.96 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.0 > 65 |
| 14 | 3189 | 景碩 | advanced_packaging_substrate | 55.65 | 63.60 | 2.44 | 4.06 | -42.80 | 66.97 | 65.55 | 82.20 | 81.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.7 > 65 |
| 15 | 3526 | 凡甲 | power_connector_chassis | 50.21 | 46.84 | 0.54 | 0.76 | -27.41 | 37.30 | 65.35 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65 |
| 16 | 3006 | 晶豪科 | semiconductor | 52.98 | 58.44 | 1.55 | 2.51 | -36.59 | 70.49 | 61.18 | 86.40 | 87.26 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 87.3 > 65；idea_score 61.2 < 62 |
| 17 | 3653 | 健策 | cooling_thermal | 52.47 | 61.75 | 2.10 | 4.06 | -46.76 | 66.07 | 63.87 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 18 | 2454 | 聯發科 | semiconductor | 56.12 | 64.05 | 1.25 | 2.38 | -26.60 | 44.55 | 62.92 | 82.20 | 70.63 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 70.6 > 65 |
| 19 | 3711 | 日月光投控 | semiconductor | 57.00 | 70.23 | 1.99 | 3.38 | -37.33 | 48.74 | 62.76 | 82.20 | 72.48 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 72.5 > 65 |
| 20 | 3324 | 雙鴻 | cooling_thermal | 49.07 | 47.59 | 1.14 | 1.74 | -42.88 | 58.10 | 62.70 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |

## Research List / 題材研究清單
這份清單是題材內完整研究排序，不等於買進排名。

| 排名 | 代碼 | 名稱 | Bucket | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 8271 | 宇瞻 | semiconductor | 61.55 | 66.26 | 2.23 | 3.38 | -37.65 | 66.30 | 77.45 | 86.40 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 2 | 2337 | 旺宏 | semiconductor | 63.83 | 71.09 | 2.62 | 4.45 | -27.81 | 74.75 | 77.30 | 86.40 | 85.78 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 85.8 > 65；hard blocker: extreme-volatility |
| 3 | 4967 | 十銓 | semiconductor | 59.86 | 68.80 | 2.08 | 3.58 | -33.26 | 64.76 | 76.13 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 4 | 3260 | 威剛 | semiconductor | 59.08 | 66.95 | 2.49 | 3.71 | -36.19 | 66.48 | 75.29 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 5 | 2451 | 創見 | semiconductor | 56.61 | 66.23 | 1.79 | 2.76 | -32.70 | 60.17 | 69.85 | 82.20 | 92.85 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 92.8 > 65 |
| 6 | 3017 | 奇鋐 | cooling_thermal | 58.61 | 66.41 | 2.67 | 4.48 | -41.19 | 57.74 | 69.05 | 82.20 | 77.11 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 77.1 > 65 |
| 7 | 6274 | 台燿 | pcb_ccl | 56.28 | 69.07 | 2.85 | 4.59 | -34.64 | 60.49 | 68.67 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 8 | 2356 | 英業達 | ai_server_odm | 53.54 | 35.38 | 0.05 | 0.08 | -30.75 | 36.94 | 68.13 | 82.20 | 66.52 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.5 > 65 |
| 9 | 8996 | 高力 | cooling_thermal | 54.25 | 61.07 | 2.02 | 3.56 | -47.87 | 71.46 | 67.80 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 10 | 6147 | 頎邦 | semiconductor | 56.31 | 75.53 | 1.86 | 3.04 | -23.70 | 48.90 | 64.10 | 85.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 11 | 4919 | 新唐 | semiconductor | 49.78 | 42.89 | 1.06 | 1.74 | -49.65 | 59.36 | 66.09 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 12 | 8046 | 南電 | advanced_packaging_substrate | 55.39 | 61.51 | 2.76 | 5.03 | -46.99 | 71.05 | 65.89 | 82.20 | 81.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.7 > 65 |
| 13 | 3037 | 欣興 | advanced_packaging_substrate | 57.08 | 60.00 | 2.19 | 4.95 | -54.29 | 91.03 | 62.37 | 86.40 | 65.96 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.0 > 65 |
| 14 | 3189 | 景碩 | advanced_packaging_substrate | 55.65 | 63.60 | 2.44 | 4.06 | -42.80 | 66.97 | 65.55 | 82.20 | 81.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.7 > 65 |
| 15 | 3526 | 凡甲 | power_connector_chassis | 50.21 | 46.84 | 0.54 | 0.76 | -27.41 | 37.30 | 65.35 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65 |
| 16 | 3006 | 晶豪科 | semiconductor | 52.98 | 58.44 | 1.55 | 2.51 | -36.59 | 70.49 | 61.18 | 86.40 | 87.26 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 87.3 > 65；idea_score 61.2 < 62 |
| 17 | 3653 | 健策 | cooling_thermal | 52.47 | 61.75 | 2.10 | 4.06 | -46.76 | 66.07 | 63.87 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 18 | 2454 | 聯發科 | semiconductor | 56.12 | 64.05 | 1.25 | 2.38 | -26.60 | 44.55 | 62.92 | 82.20 | 70.63 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 70.6 > 65 |
| 19 | 3711 | 日月光投控 | semiconductor | 57.00 | 70.23 | 1.99 | 3.38 | -37.33 | 48.74 | 62.76 | 82.20 | 72.48 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 72.5 > 65 |
| 20 | 3324 | 雙鴻 | cooling_thermal | 49.07 | 47.59 | 1.14 | 1.74 | -42.88 | 58.10 | 62.70 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |

## 前 20 名個股趨勢（Top 20）
| 排名 | 代碼 | 收盤 | 20D% | 相對大盤20D | 相對題材20D | 相對產業20D | RSI14 | 波動20% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8271 | 220.00 | 69.23 | 46.52 | 36.90 | 34.05 | 62.87 | 96.70 |
| 2 | 2337 | 154.00 | 33.33 | 10.63 | 1.01 | -1.85 | 57.57 | 104.44 |
| 3 | 4967 | 281.00 | 41.21 | 18.50 | 8.88 | 6.03 | 63.46 | 75.07 |
| 4 | 3260 | 436.00 | 30.15 | 7.44 | -2.18 | -5.03 | 57.08 | 88.19 |
| 5 | 2451 | 252.50 | 19.67 | -3.04 | -12.66 | -15.51 | 50.83 | 60.79 |
| 6 | 3017 | 2835.00 | 42.46 | 19.75 | 10.13 | 24.66 | 70.34 | 61.77 |
| 7 | 6274 | 1010.00 | 77.50 | 54.80 | 45.18 | 21.31 | 71.55 | 76.21 |
| 8 | 2356 | 45.90 | 14.61 | -8.10 | -17.72 | -3.19 | 54.58 | 29.91 |
| 9 | 8996 | 1250.00 | 47.75 | 25.05 | 15.43 | 0.00 | 64.69 | 80.62 |
| 10 | 6147 | 163.00 | 134.53 | 111.82 | 102.20 | 99.35 | 75.40 | 100.15 |
| 11 | 4919 | 147.50 | 65.73 | 43.02 | 33.40 | 30.55 | 67.00 | 90.20 |
| 12 | 8046 | 1005.00 | 92.53 | 69.82 | 60.20 | 36.33 | 83.16 | 72.91 |
| 13 | 3037 | 883.00 | 98.65 | 75.94 | 66.32 | 42.45 | 78.24 | 70.16 |
| 14 | 3189 | 528.00 | 68.69 | 45.98 | 36.36 | 33.51 | 74.53 | 71.71 |
| 15 | 3526 | 330.00 | 22.22 | -0.49 | -10.11 | -33.98 | 72.98 | 57.83 |
| 16 | 3006 | 169.50 | 13.00 | -9.71 | -19.33 | -22.18 | 52.79 | 71.69 |
| 17 | 3653 | 5380.00 | 41.77 | 19.06 | 9.44 | -14.43 | 68.36 | 75.30 |
| 18 | 2454 | 2610.00 | 75.17 | 52.46 | 42.84 | 39.99 | 81.06 | 66.87 |
| 19 | 3711 | 478.00 | 45.51 | 22.80 | 13.18 | 10.33 | 67.88 | 57.67 |
| 20 | 3324 | 1140.00 | 29.69 | 6.98 | -2.64 | 6.31 | 58.15 | 71.67 |

## 倉位建議
- `8271` 宇瞻：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `212.14` ~ `223.93`
  target：204.28 / 220.00 / 235.72
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / extreme-volatility / quality:previous_period_unavailable
- `2337` 旺宏：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `147.30` ~ `157.35`
  target：140.60 / 154.00 / 167.40
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / extreme-volatility / quality:previous_period_unavailable
- `4967` 十銓：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `271.43` ~ `285.78`
  target：261.87 / 281.00 / 300.13
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3260` 威剛：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `419.35` ~ `444.32`
  target：402.70 / 436.00 / 469.30
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `2451` 創見：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `243.44` ~ `257.03`
  target：216.24 / 234.37 / 252.50
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3017` 奇鋐：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2757.55` ~ `2873.72`
  target：2680.10 / 2835.00 / 2989.90
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `6274` 台燿：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `982.32` ~ `1023.84`
  target：954.65 / 1010.00 / 1065.35
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `2356` 英業達：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `45.11` ~ `46.29`
  target：44.32 / 45.90 / 47.48
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `8996` 高力：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `1204.24` ~ `1272.88`
  target：1158.48 / 1250.00 / 1341.52
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `6147` 頎邦：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `157.08` ~ `165.96`
  target：151.16 / 163.00 / 174.84
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / extreme-volatility / quality:partial_current_metrics / quality:previous_period_unavailable
- `4919` 新唐：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `142.28` ~ `150.11`
  target：137.07 / 147.50 / 157.93
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `8046` 南電：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `975.48` ~ `1019.76`
  target：945.96 / 1005.00 / 1064.04
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3037` 欣興：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `859.04` ~ `894.98`
  target：835.07 / 883.00 / 930.93
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3189` 景碩：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `510.69` ~ `536.66`
  target：493.38 / 528.00 / 562.62
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3526` 凡甲：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `323.30` ~ `333.35`
  target：303.19 / 316.60 / 330.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3006` 晶豪科：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `162.84` ~ `172.83`
  target：142.85 / 156.17 / 169.50
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3653` 健策：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `5228.90` ~ `5455.55`
  target：5077.80 / 5380.00 / 5682.20
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `2454` 聯發科：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2549.62` ~ `2640.19`
  target：2489.25 / 2610.00 / 2730.75
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3711` 日月光投控：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `465.76` ~ `484.12`
  target：453.52 / 478.00 / 502.48
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3324` 雙鴻：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `1104.36` ~ `1157.82`
  target：1068.73 / 1140.00 / 1211.27
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable

## 風險提示
- 這是研究輔助，不是保證報酬；遇到法說、月營收、AI 出貨節奏變化時，結論需要重新驗證。
- 若 benchmark-relative 轉負且 confidence 下滑，應優先減碼而不是凹單。
- 季度品質前期覆蓋仍未達高水位，quality score 的歷史比較仍需靠 SQLite 歷史累積補厚。
- 近 8 季完整覆蓋仍偏薄，長期品質比較要再靠回補批次補齊。

## Validation
- mode：`validation_report_v3`；window：`1y`；rebalance：`monthly`；cost `10.00` bps
- excess return `123.84`%；max drawdown `-10.14`%；hit rate `0.6591`
- factor sleeves：price `106.04`%、fundamental `74.35`%、quality `7.31`%
- portfolio diagnostics：VaR95 `-9.10`%、CVaR95 `-10.14`%、Ulcer `3.86`、Omega `7.37`
- benchmark attribution：alpha `534.43`%、beta `2.24`、IR `9.25`、tracking error `184.99`%
- 1y：excess `123.84`% / drawdown `-10.14`% / hit `0.6591`
- 3y：excess `138.06`% / drawdown `-12.87`% / hit `0.6000`
- 5y：excess `138.06`% / drawdown `-12.87`% / hit `0.6000`

## 資料與流程稽核
- theme mode：`strict`
- benchmark：`TAIEX`
- output formats：`csv,json,md`
- warnings：`0`
- output root：`C:\webtemp\tw-sector-ai-20260430-stockrisk-sample2-20260505180433`
- quarterly store：`C:\webtemp\tw-sector-ai-20260430-stockrisk-sample2-20260505180433\cache\market\quarterly_fundamentals.sqlite`；period requirement：`2`；refresh run：`None`
- quality update：mode `skip` / decision `skipped` / budget `3.00` sec / backfill `backfill-auto-check-349cb701`

## 資料來源
- TWSE OpenAPI
- TWSE exchangeReport
- TPEx OpenAPI
- TPEx afterTrading API
