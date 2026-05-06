# Changelog

本檔記錄 `tw-sector-screener` 的主要功能、輸出契約與樣本更新。格式參考 Keep a Changelog，日期採台北時間。

## [Unreleased]

### Added

- 新增 `daily-dashboard` GitHub Actions workflow，於台北時間週一至週五 18:30 產生每日 simulator dashboard 並發布到 `gh-pages`。
- 新增 GitHub Pages 靜態輸出入口：`latest/dashboard.html`、`latest/summary.json`、`latest/daily-equity.csv`、`manifest.json` 與 `archive/YYYYMMDD/`。
- 新增 `scripts/publish_pages_dashboard.py`，負責從 simulator run directory staging dashboard、summary、daily equity 與 Pages index。

### Changed

- README 加入 Latest Dashboard 連結與每日 Pages archive 規則。
- 明確規範完整每日輸出不進 `main`；`examples/sample-reports/` 只保存少量人工挑選樣本。

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
