# Changelog

本檔記錄 `tw-sector-screener` 的主要功能、輸出契約與樣本更新。格式參考 Keep a Changelog，日期採台北時間。

## [Unreleased]

### Added

- 新增 canonical market-data schema v2 的 dataset/source registry、fetch attempts、sync items、incremental checkpoints 與品質 issue occurrences。
- 新增 `scripts/verify_market_data.py`，以 read-only 模式驗證 SQLite integrity、foreign keys、253 根日線、current-day 與 benchmark gate。
- 新增 `src/providers/market_data_adapters.py` 的 dataset adapter contract 與 `docs/market-data-database-development.md` 開發文件。
- `scripts/sync_market_data.py` 新增 dataset scope、incremental/full mode、date provenance 與 dry-run 介面。

### Changed

- README、SKILL、CONTRIBUTING 與 backtest research skill 同步 canonical DB-first、PIT 與增量同步契約。
- 明確規範 canonical SQLite、raw payload 與完整每日輸出不進 Git；`examples/sample-reports/` 只保存少量人工挑選樣本。
- 未完成 adapter 的年度財務、歷史估值、公司事件與季度 PIT revision 不列入已完成資料能力。

## [0.3.0] - 2026-05-05

### Added

- 新增投資模擬器：三種 portfolio 人格、委託/成交紀錄、SQLite ledger、daily equity、HTML dashboard。
- 新增 portfolio diagnostics：VaR、CVaR、Ulcer Index、Omega、Tail Ratio、rolling metrics、alpha/beta、information ratio、tracking error。
- 新增 stock risk metrics：Sharpe、Sortino、max drawdown、volatility、Calmar、win rate 與 `risk_adjusted_score`。
- 新增 connector adapter contract 與 macro regime overlay；外部 connector 預設 supplementary，不直接升級 ranking。
- 新增 `examples/sample-reports/ai-20260505/`，展示 Buying Gate V2 後的 AI coverage report。

### Changed

- Buying Ranking 升級為 Buying Gate V2，新增 `formal_buy`、`risk_adjusted_buy`、`tactical_buy`。
- `idea_score` 保留研究優先序，買進判斷改由 `buying_tier`、`buyability_score`、`risk_adjusted_score` 與風險門檻共同處理。
- simulator policy 銜接 `buying_tier`：穩健型可買 `risk_adjusted_buy`，激進型可小部位買 `tactical_buy`，保守型維持低風險限制。

## [0.2.0] - 2026-04-30

### Added

- 新增 `core`、`coverage`、`broad` universe modes；AI / 半導體預設改用 coverage universe。
- 新增 AI/半導體 coverage buckets：AI server / ODM、foundry、IC design、memory / HBM、advanced packaging、cooling、networking、power、equipment / materials。
- 新增 `actionable_queue` 與決策梯度：`buy_now`、`near_buy`、`starter_position`、`wait_for_trigger`、`avoid`。
- 新增 `examples/sample-reports/ai-20260430/`，展示 coverage universe 與三清單報告。

### Changed

- 報告從單一候選清單拆成 `buying_ranking`、`actionable_queue`、`watchlist_candidates`、`research_list`。
- 預設 `--top-n` 改為 20；各清單先從完整 ranked universe 建構，再各自截取前 20。
- `picks` 保留為 backward-compatible research top N alias。

## [0.1.0] - 2026-04-29

### Added

- 初始 deterministic factor ranking、recommendation layer、risk gate、audit trail、watchlist 與 validation report。
- 新增 `examples/sample-reports/ai-20260429/` 作為早期報告契約樣本。
