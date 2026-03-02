## 1.0.0 (2026-03-02)

> **Milestone**: quant101 graduates from a backtesting experiment into a structured
> quant research laboratory.  64 source files (12.8K lines), 201 tests (2.6K lines),
> 7 research notebooks, 59-page LaTeX encyclopedia.  All infrastructure needed to
> research, validate, and backtest equity alpha factors is in place.

### Feat (Phase 7 — AlphaConfig & Multi-Factor)

- **portfolio**: `src/portfolio/alpha_config.py` — `AlphaConfig` dataclass: single config object holding factor list (with per-factor `FactorConfig`: winsorize/normalize/neutralize params, `direction: Literal[1, -1]`), sizing method, combination method, portfolio construction params (`n_long`, `n_short`, `target_vol`, `rebal_every_n`). Replaces 10+ keyword args with one object.
- **portfolio**: `src/portfolio/factors.py` — factor function registry: `register_factor()`, `get_factor_fn()`, `list_factors()`. 3 built-in factors: `bbiboll`, `vol_ratio`, `momentum`. Extracted from inline lambdas in `pipeline.py`.
- **portfolio**: Wired IC-weighted combination through `build_factor_pipeline()` — `ic_weight`, `mean_variance`, `risk_parity` combination methods now fully functional (were dead code).
- **portfolio**: `FactorConfig.direction` field — factors can declare their expected IC sign; pipeline auto-flips signal for short-alpha factors (e.g., `direction=-1` for BBIBOLL).
- **notebook**: `notebooks/factor_diagnostics.ipynb` — 8-section factor diagnostic notebook: per-factor IC/IR, cumulative L/S returns, direction check, IC correlation matrix, AlphaConfig comparison.
- **notebook**: `notebooks/pipeline_demo.ipynb` — rewritten as 2×2 ablation study isolating sizing (EW vs Signal-Weighted) × rebalancing (daily vs weekly). Key finding: sizing alone accounts for 0.68 Sharpe swing.
- **tests**: `tests/test_alpha_config.py` — 24 tests for AlphaConfig/FactorConfig. `tests/test_position_sizing.py` — 13 tests (7 rewritten for signal-weighted sizing). **201 tests total, all passing.**

### Fix (Phase 7 — Position Sizing)

- **risk**: `position_sizing.py` — three successive bugs fixed in the "Half-Kelly" sizing method:
  1. **Normalization destroyed Kelly property**: normalizing `sum(|w|) = 1` discards the leverage signal. Fix: direction from μ sign, leverage not normalized internally.
  2. **All configs identical (Sharpe = 0.343)**: Kelly used `|μ_i|/σ_i²` where μ came from returns history — completely ignored the factor signal. Fix: two-stage design (factor selects stocks, Kelly sizes by `|μ|/σ²`).
  3. **Historical μ is stealth momentum**: 60-day rolling mean return has SNR ≈ 0.02 — pure noise that injects momentum bias conflicting with mean-reversion factors. Fix: replaced `|μ_i|` with `|z_i|` (cross-sectional z-score of factor signal). Final formula: `w_i ∝ direction_i × |z_i| / σ_i²`.

### Refactor (Phase 7)

- **risk**: Renamed `size_half_kelly()` → `size_signal_weighted()` across entire codebase (15 files). Docstring explicitly states "Not true Kelly" — Kelly determines portfolio leverage, not relative allocation. Honest naming prevents misleading implications.

### Docs (Phase 7)

- **docs**: `quant_lab.tex` Entry 4 — "Kelly Criterion Is a Leverage Tool, Not an Allocation Tool": three-bug journey, 2×2 ablation results, root cause (`z/σ²` under-weights high-vol opportunities where mean-reversion alpha is strongest), conceptual error (scalar Kelly ignores correlations), multivariate Kelly reference (`f* = Σ⁻¹μ`), resolution and interview one-liner.

### Feat (Phase 6 — Backtest Refactor) ⚠️ BREAKING

- **backtest**: `src/backtest/portfolio_tracker.py` — `PortfolioTracker` class: pure-computation portfolio simulator from weight + return DataFrames. Outputs `TrackingResult` (frozen dataclass) with `portfolio_daily`, `turnover`, `position_count`, compatible with `PerformanceAnalyzer`. Supports optional transaction cost via `cost_bps` parameter.
- **backtest**: `src/backtest/weight_backtester.py` — `WeightBacktester` class: **alpha→backtest bridge** that accepts portfolio weight DataFrames (output of `portfolio.pipeline.run_alpha_pipeline()`) and produces full backtest analytics. Methods: `run()`, `run_from_pipeline()`, `compare()`, `export()`, `print_summary()`. Returns `BacktestResult` container with convenience properties (`sharpe`, `total_return`, `max_drawdown`). Auto-preprocesses benchmark (close→benchmark_return).
- **backtest**: `src/backtest/result_exporter.py` — `export_legacy_results()` function extracted from `BacktestEngine.export_results()`. Standalone — no engine instance required. Handles quantstats HTML, trades CSV, open positions CSV, portfolio daily CSV, metrics TXT, config TXT. Null benchmark no longer crashes.
- **backtest**: Updated `__init__.py` — exports `PortfolioTracker`, `TrackingResult`, `WeightBacktester`, `BacktestResult`, `export_legacy_results`. Version bumped to 2.0.0.
- **tests**: `tests/test_backtest_refactor.py` — 32 tests: `TestPortfolioTracker` (9), `TestWeightBacktester` (7), `TestResultExporter` (4), `TestBugFixes` (3), `TestBacktestExports` (9). **164 tests total, all passing.**

### Breaking Changes (Phase 6)

- **backtest**: `engine.py` — removed 90-line `export_results()` body, now delegates to `result_exporter.export_legacy_results()`. Removed `pandas` and `quantstats` imports from engine (moved to result_exporter). Engine is now a thin orchestrator (~150 lines, down from ~310).
- **backtest**: `backtester.py` — rewritten with CLI (`argparse`). Default mode changed from BBIBOLL strategy to pipeline. Two modes: `--mode strategy` (legacy) and `--mode pipeline` (new). New `run_pipeline_backtest()` function wires `portfolio.pipeline` → `WeightBacktester` end-to-end.

### Fix (Phase 6)

- **backtest**: `engine.py` `export_results()` — null benchmark crash fixed (guard added before `benchmark.with_columns(...)`)
- **backtest**: `strategy_base.py` — `trade_rules` type hint fixed from 2-tuple to 3-tuple `tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]` to match actual usage in `run_backtest()`
- **backtest**: `portfolio_tracker.py` — datetime resolution mismatch (`datetime[ns]` vs `datetime[μs]`) fixed with auto-cast before join in `run()`
- **backtest**: `engine.py` — dead `datetime` import removed
- **backtest**: `performance_analyzer.py` — dead `timedelta` import removed

### Feat (Phase 5 — Portfolio Pipeline)

- **portfolio**: `src/portfolio/pipeline.py` — 7-stage signal→weights→returns pipeline: `compute_daily_returns()`, `compute_next_day_returns()`, `build_factor_pipeline()` (with extensible factor registry: bbiboll, vol_ratio, momentum), `build_sizing_methods()` (all 4 sizing methods), `resample_weights()`, `compute_portfolio_return()`, `run_alpha_pipeline()` (all-in-one). Replaces ~80 lines of boilerplate duplicated across 4 notebooks.
- **portfolio**: `src/portfolio/walk_forward_runner.py` — `run_walk_forward()` executes pipeline per walk-forward fold, collects IS/OOS Sharpe/return/vol per fold, computes mean OOS Sharpe, Sharpe decay (overfitting signal), and OOS Sharpe std (stability). `fold_results_to_dataframe()` for analysis.
- **data**: `src/data/universe.py` — named stock universe registry: `US_LARGE_CAP_50` (50 tickers, sector-organized), `US_LARGE_CAP_52` (52 tickers), sector mapping dict, `get_universe()`, `list_universes()`, `register_universe()`. Replaces hardcoded ticker lists in every notebook.
- **tests**: `tests/test_portfolio.py` — 25 tests: `TestComputeDailyReturns` (4), `TestComputeNextDayReturns` (3), `TestResampleWeights` (4), `TestComputePortfolioReturn` (4), `TestFactorRegistry` (3), `TestUniverse` (7). **132 tests total, all passing.**
- **notebook**: `notebooks/pipeline_demo.ipynb` — demonstrates all 7 pipeline stages end-to-end

### Fix (Phase 5)

- **data**: SQL injection risk in `data_loader.py` — credential values now escaped with single-quote doubling before interpolation into DuckDB `SET` statements

### Feat (Phase 4.5 — Cleanup Sprint)

- **tests**: Full pytest suite — `tests/conftest.py` (shared fixtures: synthetic returns, factor DataFrames, weight DataFrames, turnover arrays), `test_validation.py` (34 tests), `test_execution.py` (25 tests), `test_risk.py` (26 tests), `test_alpha.py` (22 tests). **107 tests, all passing.**
- **constants**: `src/constants.py` — single source of truth for `TRADING_DAYS_PER_YEAR=252`, column name conventions (`DATE_COL`, `TICKER_COL`, `VALUE_COL`, `WEIGHT_COL`, `RETURN_COL`, `OHLCV_DATE_COL`)

### Fix

- **backtest**: Fee calculation bug in `performance_analyzer.py` — old formula `len(trades) * end_value * fee_rate` (nonsensical: multiplied total portfolio by trade count) → fixed to per-position approximation `n_trades * avg_position_value * fee_rate`
- **backtest**: Removed dead import `from config import all_tickers_dir` in `backtester.py`
- **backtest**: Replaced 4 hardcoded `252` magic numbers with `TRADING_DAYS_PER_YEAR` constant in `performance_analyzer.py`

### Refactor

- **deps**: Pruned `pyproject.toml` — removed 13 unused dependencies from main (tensorflow, pytorch-tabnet, redis, flask, flask-socketio, igraph, influxdb-client, pydantic, lxml, anywidget, massive, prometheus-client, optuna). Added missing used deps (numpy, scipy, plotly). Created `[ml]` and `[infra]` optional dependency groups. Main deps: 35 → 25.
- **config**: Updated pytest `testpaths` from `["src"]` to `["tests"]`

### Feat (prior — Phase 4)

- **validation**: `walk_forward.py` — `WalkForwardFold` dataclass, `walk_forward_split()` (rolling/anchored modes), `apply_folds_to_dates()`, `summarize_folds()`. Rolling 126d train / 63d test / 5d purged embargo.
- **validation**: `statistical_tests.py` — `bootstrap_sharpe_ci()` (circular block bootstrap, 10K resamples), `sharpe_pvalue()` (Lo 2002 adjusted SE), `probabilistic_sharpe_ratio()` (Bailey & de Prado 2012), `deflated_sharpe_ratio()` (Bailey & de Prado 2014, adjusts for multiple trials)
- **validation**: `multiple_testing.py` — `bonferroni()`, `holm_bonferroni()`, `benjamini_hochberg()`, `apply_all_corrections()`. FWER and FDR control for strategy sweeps.
- **notebook**: `validation.ipynb` — full validation gauntlet for 16-config sweep (4 sizing × 4 freq). Walk-forward IS/OOS: HK_W avg OOS SR=1.15, 60% hit rate. Bootstrap: 95% CI=[−0.58, 2.65] includes 0. PSR=91.8%, DSR=34.2% (16 trials). Multiple-testing: 0/16 survive BH correction. **Verdict: no config is statistically significant.**
- **execution**: `cost_model.py` — ABC `CostModel` with `estimate()` + `estimate_array()`, 4 implementations: `FixedCostModel` (flat bps), `SpreadCostModel` (half bid-ask spread), `SqrtImpactCostModel` (Almgren-style η·σ·√(participation)), `CompositeCostModel` (sum of models)
- **execution**: `cost_analysis.py` — `compute_turnover()` (weight-diff-based, full outer join), `compute_net_returns()`, `sharpe_vs_cost_curve()` (sweep), `breakeven_cost()` (binary search for Sharpe=0)
- **notebook**: `cost_analysis.ipynb` — end-to-end cost analysis + rebalancing frequency experiment + sub-period stability check. Key findings: (1) all methods net-negative at 5 bps with daily rebalancing; Half-Kelly breakeven 1.8 bps. (2) Weekly Half-Kelly SR=1.04 net at 5 bps full-sample, but sub-period check reveals **only 2/4 half-years positive** (2025-H1 SR=−2.59). Full-sample result is an aggregation artifact — alpha is period-dependent. Includes `resample_weights()`, 4×4 sweep, efficiency frontier, stability heatmap
- **risk**: `risk_metrics.py` — VaR (historical + parametric), CVaR (historical + parametric), drawdown series, max drawdown, skewness, excess kurtosis, tail ratio, `risk_summary()` all-in-one
- **risk**: `return_analysis.py` — normality tests (Jarque-Bera + Shapiro-Wilk), QQ-plot data, Gaussian comparison histogram, multi-level tail analysis, `distribution_summary()` all-in-one
- **risk**: `position_sizing.py` — equal-weight, inverse-volatility, volatility-target, half-Kelly long-short sizing; `compute_realized_volatility()` utility
- **notebook**: `risk_analysis.ipynb` — end-to-end risk analysis (14 code cells): distribution tests, VaR/CVaR comparison, drawdown, position sizing comparison across 4 methods
- **config**: Renamed machine_config.yaml → basic_config.yaml, added standalone single-machine mode
- **config**: Unified config structure — `update.mode` replaces `machine.role`, `data.data_dir` replaces split server/client dirs
- **scripts**: Renamed weekly_update.sh → data_update.sh, added standalone mode (9-task pipeline)
- **docs**: Rewrote `quant_lab.tex` — lab-module structure (5 Parts + Appendices, 17 chapters, 55 pages). Methodology-oriented, not hardcoded architecture. Each chapter: Motivation → Concepts → Implementation → Experiment → Reflection

### Refactor

- **data**: Merged `data_fetcher/` + `data_supply/` into unified `src/data/` package (`data/fetcher/` + `data/loader/`)
- **strategy**: Renamed `strategies/` → `strategy/`, `bbibollStrategy.py` → `bbiboll_strategy.py` (PEP 8)
- **i18n**: Translated all Chinese comments, docstrings, and print messages to English (~35 items across 3 files)
- **config**: Removed hardcoded `sppc_dir`, deferred `splits_data` to lazy getter `get_splits_data()`, removed dead `llmContext` loader
- **cleanup**: Deleted 5 dead files (`main.py`, 2 backtest examples, `plotter.py`, `cuda_env_test.py`)

### Fix

- **risk**: `cvar_parametric()` sign error — conditional left-tail mean is μ − σ·φ(z)/(1−α), not μ + σ·φ(z)/(1−α); was producing negative CVaR
- **data/fetcher**: `csvgz_to_parquet.py` — argparse rejected its own default (`zstd` missing from choices)
- **data/fetcher**: `splits_fetch.py` — removed wrong unused `matplotlib.pylab` import
- **data/fetcher**: `fmp_fundamental_fetch.py` — print message said "csv" but file writes parquet
- **data/fetcher**: `indices_fetch.py` — removed debug print left in production code
- **backtest**: `performance_analyzer.py` — trading fee now configurable (was hardcoded 0.7%), calmar ratio properly implemented (was stub returning 0.0)
- **backtest**: `backtester.py` — fixed boolean precedence bug (`or` vs `and not`), fixed typo "isexported"
- **scripts**: `write_overview_csv.py` — removed wrong unused `duckdb` import

---

## 0.2.0 (2026-03-01)

### Feat

- **backtest**: trades_analyzer — post-hoc analysis of open positions across rolling backtest dates with Plotly animated scatter charts
- **backtest**: BBIBOLL rolling weekly backtest runner across 60+ date windows
- **strategy**: BBIBOLL strategy — BBI + Bollinger Band deviation with stop-loss/take-profit rules
- **strategy/indicators**: Decorator-based indicator registry with per-ticker group application
- **strategy/indicators**: BBI + Bollinger deviation percentile rank indicator (TA-Lib)
- **strategy/indicators**: OBV indicator
- **data/loader**: Risk-free rate (IRX) and SPX benchmark loader
- **data/loader**: Trading calendar-aware date utilities — weekly/monthly backtest date sequences
- **data/loader**: FIGI-based ticker mapping via connected components (tracks name changes/delistings)
- **data/fetcher**: Float shares fetcher (FinancialModelingPrep, async paginated)
- **data/fetcher**: Index daily aggregates fetcher (SPX, IRX) with incremental updates
- **data/fetcher**: Rsync-based multi-machine data sync (server → client)
- **scripts**: file_examiner — inspects Polygon data directory tree, reports coverage dates
- **scripts**: data_update.sh — mode-aware incremental data refresh orchestrator (standalone/server/client)
- **scripts**: low_volume_ticker_update — event-stream state machine for zero-volume ticker tracking
- **config**: Machine-role-aware data directory resolution (server/client via YAML)
- **config**: Asset overview loader with CSV-based error corrections (add/remove)
- **utils**: Configurable logger with console + rotating file handlers
- **visualizer**: Interactive candlestick chart (seolpyo-mplchart)
- **longport**: Watchlist import from strategy outputs to Longport/Longbridge broker
- **live_monitor**: Market Mover Monitor with Redis Stream backend (separated to jerryib_trader repo)
- **live_monitor**: News fetcher — momo web, FMP, benzinga
- **live_monitor**: Factor engine prototype
- **live_monitor**: Trades timespan replayer (v2/v3)
- **quantstats**: Integrated QuantStats HTML report generation

### Fix

- **backtest**: trades_analyzer delisted status bug fixed
- **scripts**: versatile → indices_update, changed dir, fixed proxy problem
- **data/supply**: Low-volume ticker incremental update fixed
- **live_monitor**: Replay mode migrated to Redis Stream

### Refactor

- **src**: Pruned & cleaned up — removed unused modules
- **src**: Restructured from `src/quant101/*` to `src/*`
- **src**: Removed Chinese — English as default project language
- **config**: data_dir refactored for multi-machine support
- **live_monitor**: Decoupled to standalone repository (jerryib_trader)
- **live_monitor**: GridTrader backend — Redis ZSET → Redis Stream, InfluxDB integration

---

## 0.1.0 (2025-09-19)

### Feat

- **data/fetcher**: Polygon.io S3 flat file downloader (stocks, options, indices, crypto, forex)
- **data/fetcher**: CSV.gz → Parquet converter with schema mapping per data type
- **data/fetcher**: All-tickers fetcher (stocks, OTC, indices) via Polygon REST API
- **data/fetcher**: Stock splits fetcher with incremental update support
- **data/loader**: Core data loader — OHLCV from Parquet/S3, split adjustment, timeframe resampling, caching
- **data/loader**: DataPathLoader — file path calculation for Polygon flat files (local + S3)
- **backtest**: BacktestEngine with abstract StrategyBase interface
- **backtest**: PerformanceAnalyzer — Sharpe, Sortino, CAGR, max drawdown, win rate, payoff ratio
- **backtest**: BacktestVisualizer — equity curves, monthly heatmaps, candlestick with signals
- **config**: Central configuration with data directory management
- **config**: Splits data with CSV-based error correction (add/remove error types)
- **visualizer**: Matplotlib plotter via seolpyo-mplchart

### Notes

- `low_volume_tickers.csv`: Tickers with extended zero-volume periods (>50 days) are skipped.
  Most cases are relisted tickers or ticker name reuse, which are hard to distinguish programmatically.
  This is a known data quality tradeoff — saves time but loses some coverage.
