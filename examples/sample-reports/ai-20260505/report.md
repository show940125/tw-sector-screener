# 台股類股選股報告

- 主題：`AI`
- 截止日：`2026-05-05`

## 摘要
Thesis：AI 類股目前 買進榜首 `2308` 台達電，buyability `63.38`，tier `formal_buy`；formal buy 1 檔；risk-adjusted buy 1 檔；tactical buy 1 檔；actionable 17 檔；接近可買 1 檔；小部位試單 3 檔；等待觸發 2 檔；避開/降風險 12 檔。下一步看 `2308` 台達電：2308 已在正式買進榜，按 buyability 排序與風控分批。；最高研究優先標的 `8271` 宇瞻 目前建議 `持有`，recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility。 Research List top 20 平均 idea score `69.8`。 Evidence：相對題材 20 日超額 `16.29%`，confidence `86.4`。 Risk：missing:quality_score；quality:previous_period_unavailable。 Action：`Neutral`；建議評估 `買入`。 What changes my mind：若相對題材 20 日動能轉負、confidence 下滑或法說/營收驗證失敗，就降級。

## 加權總攬（TAIEX）
- 收盤 `40769.29`，1D 64.15 點 / 0.16%
- 報酬：5D 2.91%、20D 25.17%、63D 30.48%、126D 46.90%
- 均線：SMA20 `37430.70`、SMA60 `34598.11`、SMA120 `31640.94`；RSI14 `76.45`；趨勢分數 `72.00`
- 來源：TWSE exchangeReport/FMTQIK

## 類股總攬
- 評分母體 `54` 檔，Top20 平均 idea score `69.80` / 平均 confidence `83.25`
- 建議評估分布：持有 14、賣出 5、買入 1
- 買進分層分布：formal_buy 1、risk_adjusted_buy 1、tactical_buy 1
- 決策梯度分布：avoid 12、starter_position 3、wait_for_trigger 2、buy_now 2、near_buy 1
- 因子權重：trend_score 28%、momentum_score 22%、value_score 16%、fundamental_score 16%、quality_score 10%、benchmark_score 5%、risk_control_score 3%
- Benchmark 視角：20D 題材平均 `35.11`%，相對大盤 `9.95`%
- Quality coverage：當期完整 `14.81`%，前期完整 `0.00`%
- History coverage：近 `8` 季完整覆蓋 `0.00`%

## 方法與共識
- Rank 看的是 idea score 與資料可信度的合成，不再把缺值直接補成 50 分。
- Confidence 拆成 factor coverage 與 data freshness 兩段，避免把資料缺漏跟舊資料混成一團。
- Benchmark 同時看相對 TAIEX、相對題材、相對產業，避免只用絕對漲幅自嗨。
- Action 與 ranking 拆開：排名是研究優先序，Overweight/Neutral/Underweight 才是動作建議。
- Validation 已升級成 v3：保留 factor sleeves，並加入 portfolio risk diagnostics 與 benchmark-relative attribution。

## Macro Regime Overlay
- regime：`overheated`；risk level：`watch`；risk adjustment `5.00`
- source：`macro-regime-local-proxy`；tier：`supplementary`；rank signal：`False`
- evidence refs：market_overview.trend_score, market_overview.ret_20d, market_overview.rsi14, macro_regime_overlay, macro-regime-local-proxy

## Coverage Universe
- mode：`coverage`；source：`curated_theme_library`
- universe size：`54`；ranked `54`；limit applied `False`
- buckets：ai_server_odm 9、semiconductor 28、foundry 5、ic_design 8、power_connector_chassis 3、display_proxy 2、networking_optical 1、telecom_proxy 3、memory_hbm 9、cooling_thermal 4、advanced_packaging_substrate 3、pcb_ccl 3、testing_equipment 3、materials 3

## Buying Ranking / 買進優先序
| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 2308 | 台達電 | power_connector_chassis | formal_buy | 63.38 | 73.62 | 3.22 | 5.39 | -30.22 | 50.10 | 65.45 | 86.40 | 52.44 | 買入 | Neutral | 研究排名靠前，等待買進條件確認。 | - |
| 2 | 2330 | 台積電 | semiconductor | risk_adjusted_buy | 60.26 | 78.25 | 2.05 | 3.49 | -28.31 | 33.76 | 59.54 | 82.20 | 50.96 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | - |
| 3 | 3037 | 欣興 | advanced_packaging_substrate | tactical_buy | 58.64 | 61.77 | 2.86 | 4.92 | -46.46 | 67.24 | 65.98 | 86.40 | 70.04 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | - |

## Actionable Queue / 可行動候選隊列
這份隊列回答「不能正式買，那現在最接近能做什麼」。它不會把 `賣出` 或 hard blocker 標的包裝成買進。

| 排名 | 代碼 | 名稱 | Bucket | Tier | Actionability | Risk Adj | Sharpe | Sortino | Max DD% | Readiness | 試單x | 下一步 | 為何尚未正式買 | 升級條件 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---|---:|---|---|---|
| 1 | 3017 | 奇鋐 | cooling_thermal | starter_position | 63.24 | 66.50 | 2.64 | 4.39 | -40.89 | small-size-only | 0.25 | 3017 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 83.0 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 2 | 8046 | 南電 | advanced_packaging_substrate | starter_position | 62.78 | 61.51 | 2.71 | 4.95 | -46.99 | small-size-only | 0.25 | 8046 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 83.0 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 3 | 2382 | 廣達 | ai_server_odm | near_buy | 59.98 | 40.46 | 0.47 | 0.65 | -37.04 | near | 0.00 | 2382 接近可買，先掛在候選隊列，等風險分數降到 65 以下再進正式買進榜。 | recommendation=持有；risk_score 66.7 > 65 | risk_score 降到 65 以下，且 recommendation 升級為買入。 |
| 4 | 3189 | 景碩 | advanced_packaging_substrate | wait_for_trigger | 59.88 | 63.60 | 2.40 | 3.99 | -42.80 | waiting | 0.00 | 3189 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 89.5 > 65 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 5 | 2303 | 聯電 | semiconductor | wait_for_trigger | 58.94 | 67.29 | 1.58 | 2.50 | -29.66 | waiting | 0.00 | 2303 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 65.4 > 65；idea_score 56.7 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 6 | 2454 | 聯發科 | semiconductor | starter_position | 58.79 | 72.54 | 1.71 | 3.49 | -26.60 | small-size-only | 0.25 | 2454 僅適合 0.25x 小部位試單，不能當正式買進。 | recommendation=持有；risk_score 81.2 > 65 | 波動降溫或短線過熱解除後，risk_score 降到 65 以下再升級正式買進。 |
| 7 | 3231 | 緯創 | ai_server_odm | wait_for_trigger | 58.48 | 57.38 | 0.89 | 1.45 | -22.71 | waiting | 0.00 | 3231 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 55.9 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 8 | 2317 | 鴻海 | ai_server_odm | wait_for_trigger | 57.34 | 45.78 | 0.78 | 1.13 | -38.86 | waiting | 0.00 | 2317 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 54.5 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 9 | 3443 | 創意 | semiconductor | wait_for_trigger | 57.30 | 66.62 | 2.07 | 3.69 | -39.72 | waiting | 0.00 | 3443 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 74.7 > 65；idea_score 57.5 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 10 | 3711 | 日月光投控 | semiconductor | wait_for_trigger | 56.79 | 70.64 | 2.11 | 3.63 | -36.46 | waiting | 0.00 | 3711 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 75.6 > 65；idea_score 59.1 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 11 | 6257 | 矽格 | semiconductor | wait_for_trigger | 56.45 | 71.09 | 1.94 | 2.63 | -30.49 | waiting | 0.00 | 6257 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 84.0 > 65 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 12 | 2345 | 智邦 | networking_optical | wait_for_trigger | 56.02 | 64.88 | 2.17 | 3.48 | -43.80 | waiting | 0.00 | 2345 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 86.7 > 65 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 13 | 2357 | 華碩 | ai_server_odm | wait_for_trigger | 55.67 | 27.10 | -0.16 | -0.20 | -37.80 | waiting | 0.00 | 2357 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 57.6 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 14 | 2449 | 京元電子 | semiconductor | wait_for_trigger | 54.38 | 68.62 | 2.08 | 3.69 | -37.84 | waiting | 0.00 | 2449 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 82.1 > 65；idea_score 57.1 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 15 | 2412 | 中華電 | telecom_proxy | wait_for_trigger | 53.45 | 63.87 | 0.59 | 0.80 | -6.57 | waiting | 0.00 | 2412 先等待觸發條件，不急著進場。 | recommendation=持有；idea_score 46.9 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 16 | 3034 | 聯詠 | semiconductor | wait_for_trigger | 51.84 | 20.95 | -0.63 | -0.87 | -35.65 | waiting | 0.00 | 3034 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 66.9 > 65；idea_score 52.8 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |
| 17 | 6669 | 緯穎 | ai_server_odm | wait_for_trigger | 50.42 | 55.22 | 1.28 | 2.17 | -38.07 | waiting | 0.00 | 6669 先等待觸發條件，不急著進場。 | recommendation=持有；risk_score 73.8 > 65；idea_score 46.7 < 62 | 相對大盤與相對題材 20 日動能轉正；最新季度品質資料補齊且 confidence 回升 |

## Watchlist / 追蹤與處理清單
| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 8271 | 宇瞻 | semiconductor | not_buyable | 61.56 | 66.24 | 2.29 | 3.47 | -37.65 | 66.37 | 77.48 | 86.40 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 2 | 3017 | 奇鋐 | cooling_thermal | not_buyable | 62.01 | 66.50 | 2.64 | 4.39 | -40.89 | 58.02 | 77.80 | 82.20 | 83.04 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 83.0 > 65 |
| 3 | 8046 | 南電 | advanced_packaging_substrate | not_buyable | 60.94 | 61.51 | 2.71 | 4.95 | -46.99 | 71.05 | 77.67 | 82.20 | 83.04 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 83.0 > 65 |
| 4 | 2337 | 旺宏 | semiconductor | not_buyable | 60.87 | 71.09 | 2.88 | 4.81 | -27.81 | 73.49 | 73.18 | 86.40 | 91.70 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 91.7 > 65；hard blocker: extreme-volatility |
| 5 | 2451 | 創見 | semiconductor | not_buyable | 60.15 | 69.52 | 1.96 | 3.03 | -32.70 | 60.58 | 76.43 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65 |
| 6 | 8996 | 高力 | cooling_thermal | not_buyable | 57.16 | 60.71 | 1.97 | 3.49 | -47.87 | 71.61 | 73.91 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 7 | 4967 | 十銓 | semiconductor | not_buyable | 58.98 | 69.97 | 2.26 | 3.85 | -33.26 | 59.55 | 73.86 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 8 | 3189 | 景碩 | advanced_packaging_substrate | not_buyable | 58.52 | 63.60 | 2.40 | 3.99 | -42.80 | 66.96 | 73.83 | 82.20 | 89.52 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 89.5 > 65 |
| 9 | 3260 | 威剛 | semiconductor | not_buyable | 56.33 | 66.96 | 2.48 | 3.68 | -36.19 | 66.42 | 69.65 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 10 | 3006 | 晶豪科 | semiconductor | not_buyable | 54.86 | 58.94 | 1.56 | 2.57 | -36.59 | 70.06 | 65.39 | 86.40 | 89.48 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 89.5 > 65 |
| 11 | 3653 | 健策 | cooling_thermal | not_buyable | 54.64 | 62.29 | 1.96 | 3.70 | -44.62 | 66.63 | 68.09 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 12 | 2382 | 廣達 | ai_server_odm | not_buyable | 54.20 | 40.46 | 0.47 | 0.65 | -37.04 | 37.82 | 67.44 | 82.20 | 66.70 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.7 > 65 |
| 13 | 4919 | 新唐 | semiconductor | not_buyable | 49.94 | 43.54 | 1.06 | 1.76 | -49.03 | 58.95 | 66.16 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 14 | 8150 | 南茂 | semiconductor | not_buyable | 53.68 | 62.71 | 1.67 | 2.71 | -37.72 | 56.86 | 65.96 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 15 | 2454 | 聯發科 | semiconductor | not_buyable | 57.66 | 72.54 | 1.71 | 3.49 | -26.60 | 48.03 | 65.84 | 82.20 | 81.19 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.2 > 65 |
| 16 | 2356 | 英業達 | ai_server_odm | not_buyable | 51.59 | 38.46 | 0.20 | 0.30 | -30.75 | 36.84 | 64.40 | 82.20 | 71.52 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 71.5 > 65 |
| 17 | 6274 | 台燿 | pcb_ccl | not_buyable | 53.87 | 68.82 | 3.13 | 5.09 | -34.64 | 61.61 | 63.84 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 18 | 2345 | 智邦 | networking_optical | not_buyable | 54.21 | 64.88 | 2.17 | 3.48 | -43.80 | 58.74 | 63.62 | 82.20 | 86.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 86.7 > 65 |
| 19 | 6257 | 矽格 | semiconductor | not_buyable | 55.16 | 71.09 | 1.94 | 2.63 | -30.49 | 47.26 | 62.17 | 82.20 | 83.96 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 84.0 > 65 |
| 20 | 6147 | 頎邦 | semiconductor | not_buyable | 54.29 | 76.67 | 1.97 | 3.28 | -23.70 | 49.70 | 59.58 | 85.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；idea_score 59.6 < 62；hard blocker: extreme-volatility |

## Research List / 題材研究清單
這份清單是題材內完整研究排序，不等於買進排名。

| 排名 | 代碼 | 名稱 | Bucket | Buying Tier | Buyability | Risk Adj | Sharpe | Sortino | Max DD% | Vol% | Idea | Confidence | Risk | 建議 | Action | 入榜/監控理由 | 未列買進原因 |
|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|
| 1 | 8271 | 宇瞻 | semiconductor | not_buyable | 61.56 | 66.24 | 2.29 | 3.47 | -37.65 | 66.37 | 77.48 | 86.40 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 2 | 3017 | 奇鋐 | cooling_thermal | not_buyable | 62.01 | 66.50 | 2.64 | 4.39 | -40.89 | 58.02 | 77.80 | 82.20 | 83.04 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 83.0 > 65 |
| 3 | 8046 | 南電 | advanced_packaging_substrate | not_buyable | 60.94 | 61.51 | 2.71 | 4.95 | -46.99 | 71.05 | 77.67 | 82.20 | 83.04 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 83.0 > 65 |
| 4 | 2337 | 旺宏 | semiconductor | not_buyable | 60.87 | 71.09 | 2.88 | 4.81 | -27.81 | 73.49 | 73.18 | 86.40 | 91.70 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 91.7 > 65；hard blocker: extreme-volatility |
| 5 | 2451 | 創見 | semiconductor | not_buyable | 60.15 | 69.52 | 1.96 | 3.03 | -32.70 | 60.58 | 76.43 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65 |
| 6 | 8996 | 高力 | cooling_thermal | not_buyable | 57.16 | 60.71 | 1.97 | 3.49 | -47.87 | 71.61 | 73.91 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 7 | 4967 | 十銓 | semiconductor | not_buyable | 58.98 | 69.97 | 2.26 | 3.85 | -33.26 | 59.55 | 73.86 | 82.20 | 95.00 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 8 | 3189 | 景碩 | advanced_packaging_substrate | not_buyable | 58.52 | 63.60 | 2.40 | 3.99 | -42.80 | 66.96 | 73.83 | 82.20 | 89.52 | 持有 | Overweight | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 89.5 > 65 |
| 9 | 3260 | 威剛 | semiconductor | not_buyable | 56.33 | 66.96 | 2.48 | 3.68 | -36.19 | 66.42 | 69.65 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 10 | 3037 | 欣興 | advanced_packaging_substrate | tactical_buy | 58.64 | 61.77 | 2.86 | 4.92 | -46.46 | 67.24 | 65.98 | 86.40 | 70.04 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | - |
| 11 | 2308 | 台達電 | power_connector_chassis | formal_buy | 63.38 | 73.62 | 3.22 | 5.39 | -30.22 | 50.10 | 65.45 | 86.40 | 52.44 | 買入 | Neutral | 研究排名靠前，等待買進條件確認。 | - |
| 12 | 3006 | 晶豪科 | semiconductor | not_buyable | 54.86 | 58.94 | 1.56 | 2.57 | -36.59 | 70.06 | 65.39 | 86.40 | 89.48 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 89.5 > 65 |
| 13 | 3653 | 健策 | cooling_thermal | not_buyable | 54.64 | 62.29 | 1.96 | 3.70 | -44.62 | 66.63 | 68.09 | 82.20 | 95.00 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 14 | 2382 | 廣達 | ai_server_odm | not_buyable | 54.20 | 40.46 | 0.47 | 0.65 | -37.04 | 37.82 | 67.44 | 82.20 | 66.70 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 66.7 > 65 |
| 15 | 4919 | 新唐 | semiconductor | not_buyable | 49.94 | 43.54 | 1.06 | 1.76 | -49.03 | 58.95 | 66.16 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 16 | 8150 | 南茂 | semiconductor | not_buyable | 53.68 | 62.71 | 1.67 | 2.71 | -37.72 | 56.86 | 65.96 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65；hard blocker: extreme-volatility |
| 17 | 2454 | 聯發科 | semiconductor | not_buyable | 57.66 | 72.54 | 1.71 | 3.49 | -26.60 | 48.03 | 65.84 | 82.20 | 81.19 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 81.2 > 65 |
| 18 | 2356 | 英業達 | ai_server_odm | not_buyable | 51.59 | 38.46 | 0.20 | 0.30 | -30.75 | 36.84 | 64.40 | 82.20 | 71.52 | 賣出 | Neutral | 已觸發賣出/降風險，保留於追蹤清單以處理既有部位。 | recommendation=賣出；risk_score 71.5 > 65 |
| 19 | 6274 | 台燿 | pcb_ccl | not_buyable | 53.87 | 68.82 | 3.13 | 5.09 | -34.64 | 61.61 | 63.84 | 82.20 | 95.00 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 95.0 > 65 |
| 20 | 2345 | 智邦 | networking_optical | not_buyable | 54.21 | 64.88 | 2.17 | 3.48 | -43.80 | 58.74 | 63.62 | 82.20 | 86.74 | 持有 | Neutral | 仍屬題材重要標的，但目前未通過買進條件，保留追蹤。 | recommendation=持有；risk_score 86.7 > 65 |

## 前 20 名個股趨勢（Top 20）
| 排名 | 代碼 | 收盤 | 20D% | 相對大盤20D | 相對題材20D | 相對產業20D | RSI14 | 波動20% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8271 | 232.00 | 59.45 | 34.29 | 24.34 | 17.69 | 66.00 | 93.33 |
| 2 | 3017 | 2705.00 | 30.68 | 5.51 | -4.44 | 14.02 | 60.71 | 64.80 |
| 3 | 8046 | 997.00 | 75.53 | 50.36 | 40.42 | 23.29 | 80.04 | 69.93 |
| 4 | 2337 | 154.00 | 33.33 | 8.17 | -1.78 | -8.43 | 57.57 | 104.44 |
| 5 | 2451 | 283.00 | 21.46 | -3.71 | -13.65 | -20.30 | 62.10 | 59.14 |
| 6 | 8996 | 1190.00 | 37.41 | 12.25 | 2.30 | 0.00 | 58.74 | 78.51 |
| 7 | 4967 | 289.00 | 38.28 | 13.11 | 3.17 | -3.48 | 67.19 | 71.58 |
| 8 | 3189 | 527.00 | 56.85 | 31.68 | 21.73 | 15.09 | 73.43 | 71.78 |
| 9 | 3260 | 448.00 | 23.93 | -1.24 | -11.18 | -17.83 | 58.89 | 81.87 |
| 10 | 3037 | 903.00 | 73.99 | 48.82 | 38.88 | 21.75 | 77.95 | 66.81 |
| 11 | 2308 | 2165.00 | 51.40 | 26.23 | 16.29 | -0.84 | 72.76 | 51.66 |
| 12 | 3006 | 182.00 | 20.93 | -4.23 | -14.18 | -20.83 | 58.24 | 70.44 |
| 13 | 3653 | 4780.00 | 28.84 | 3.68 | -6.27 | -23.40 | 53.35 | 81.36 |
| 14 | 2382 | 321.00 | 12.83 | -12.34 | -22.28 | -3.83 | 55.87 | 40.16 |
| 15 | 4919 | 165.00 | 78.57 | 53.41 | 43.46 | 36.81 | 72.84 | 91.04 |
| 16 | 8150 | 84.10 | 48.32 | 23.16 | 13.21 | 6.56 | 68.53 | 91.50 |
| 17 | 2454 | 3155.00 | 115.36 | 90.19 | 80.25 | 73.60 | 88.24 | 71.02 |
| 18 | 2356 | 46.95 | 14.93 | -10.23 | -20.18 | -1.72 | 59.22 | 28.85 |
| 19 | 6274 | 1220.00 | 96.46 | 71.29 | 61.34 | 44.22 | 81.58 | 79.14 |
| 20 | 2345 | 2495.00 | 56.92 | 31.75 | 21.81 | 41.20 | 74.21 | 65.02 |

## 倉位建議
- `8271` 宇瞻：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `224.60` ~ `235.70`
  target：217.21 / 232.00 / 246.79
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / extreme-volatility / quality:previous_period_unavailable
- `3017` 奇鋐：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2624.70` ~ `2745.15`
  target：2544.40 / 2705.00 / 2865.60
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `8046` 南電：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `967.25` ~ `1011.88`
  target：937.49 / 997.00 / 1056.51
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `2337` 旺宏：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `147.30` ~ `157.35`
  target：140.60 / 154.00 / 167.40
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / extreme-volatility / quality:previous_period_unavailable
- `2451` 創見：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `273.92` ~ `287.54`
  target：246.66 / 264.83 / 283.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `8996` 高力：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `1144.17` ~ `1212.92`
  target：1098.34 / 1190.00 / 1281.66
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `4967` 十銓：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `280.49` ~ `293.25`
  target：271.98 / 289.00 / 306.02
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3189` 景碩：`持有` / `Overweight`，研究動作 `降風險`，進場區間 `510.30` ~ `535.35`
  target：493.60 / 527.00 / 560.40
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `3260` 威剛：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `432.37` ~ `455.81`
  target：385.49 / 416.74 / 448.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `3037` 欣興：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `878.45` ~ `915.28`
  target：853.89 / 903.00 / 952.11
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:previous_period_unavailable
- `2308` 台達電：`買入` / `Neutral`，研究動作 `交易型加碼`，進場區間 `2115.22` ~ `2189.89`
  target：2115.22 / 2264.56 / 2364.11
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA20 且 benchmark-relative 同步轉弱
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3006` 晶豪科：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `175.44` ~ `185.28`
  target：155.77 / 168.89 / 182.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:previous_period_unavailable
- `3653` 健策：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `4625.51` ~ `4857.25`
  target：4162.03 / 4471.01 / 4780.00
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `2382` 廣達：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `315.88` ~ `323.56`
  target：310.75 / 321.00 / 331.25
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `4919` 新唐：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `159.74` ~ `167.63`
  target：154.49 / 165.00 / 175.51
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `8150` 南茂：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `81.56` ~ `85.37`
  target：79.03 / 84.10 / 89.17
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / extreme-volatility / quality:previous_period_unavailable
- `2454` 聯發科：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `3084.14` ~ `3190.43`
  target：3013.28 / 3155.00 / 3296.72
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `2356` 英業達：`賣出` / `Neutral`，研究動作 `賣出/移出觀察`，進場區間 `46.22` ~ `47.32`
  target：44.02 / 45.48 / 46.95
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 重新站回 SMA20/SMA60 且風險分數下降後再評估
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `6274` 台燿：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `1188.89` ~ `1235.55`
  target：1157.78 / 1220.00 / 1282.22
  add trigger：站回 20 日高點附近且量能未明顯萎縮時，再加第二筆。
  trim trigger：跌破 20 日均線且相對題材動能轉負，先減碼；若跌破風險區，再降到觀察倉。
  invalidation：相對題材 20 日動能轉負且無法修復 / confidence 低於 55 或新增重大資料警示 / 跌破 SMA60 或 quality coverage 惡化時降級
  data flags：missing:quality_score / quality:unavailable / quality:previous_period_unavailable
- `2345` 智邦：`持有` / `Neutral`，研究動作 `降風險`，進場區間 `2432.14` ~ `2526.43`
  target：2369.27 / 2495.00 / 2620.73
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
- excess return `205.37`%；max drawdown `-0.85`%；hit rate `0.6864`
- factor sleeves：price `124.92`%、fundamental `164.16`%、quality `26.16`%
- portfolio diagnostics：VaR95 `-0.44`%、CVaR95 `-0.85`%、Ulcer `0.25`、Omega `157.85`
- benchmark attribution：alpha `2758.91`%、beta `0.43`、IR `14.02`、tracking error `157.77`%
- 1y：excess `205.37`% / drawdown `-0.85`% / hit `0.6864`
- 3y：excess `167.01`% / drawdown `-13.97`% / hit `0.6500`
- 5y：excess `167.01`% / drawdown `-13.97`% / hit `0.6500`

## 資料與流程稽核
- theme mode：`strict`
- benchmark：`TAIEX`
- output formats：`csv,json,md`
- warnings：`0`
- output root：`C:\webtemp\tw-sector-ai-20260505-buygatev2-final-20260505183131`
- quarterly store：`C:\webtemp\tw-sector-ai-20260505-buygatev2-final-20260505183131\cache\market\quarterly_fundamentals.sqlite`；period requirement：`2`；refresh run：`None`
- quality update：mode `skip` / decision `skipped` / budget `3.00` sec / backfill `backfill-auto-check-9cd42e5c`

## 資料來源
- TWSE OpenAPI
- TWSE exchangeReport
- TPEx OpenAPI
- TPEx afterTrading API
