## Unreleased

### Project Direction

> **v1.0.0 Goal**: Upgrade quant101 from a backtesting experiment into a structured
> quant trader & researcher learning project. Incremental restructuring — add new
> modules without breaking existing ones.

### Feat

- **risk**: `risk_metrics.py` — VaR (historical + parametric), CVaR (historical + parametric), drawdown series, max drawdown, skewness, excess kurtosis, tail ratio, `risk_summary()` all-in-one
- **risk**: `return_analysis.py` — normality tests (Jarque-Bera + Shapiro-Wilk), QQ-plot data, Gaussian comparison histogram, multi-level tail analysis, `distribution_summary()` all-in-one
- **risk**: `position_sizing.py` — equal-weight, inverse-volatility, volatility-target, half-Kelly long-short sizing; `compute_realized_volatility()` utility
- **notebook**: `risk_analysis.ipynb` — end-to-end risk analysis (14 code cells): distribution tests, VaR/CVaR comparison, drawdown, position sizing comparison across 4 methods

### Fix

- **risk**: `cvar_parametric()` sign error — conditional left-tail mean is μ − σ·φ(z)/(1−α), not μ + σ·φ(z)/(1−α); was producing negative CVaR
- **config**: Renamed machine_config.yaml → basic_config.yaml, added standalone single-machine mode
- **scripts**: Renamed weekly_update.sh → data_update.sh, added standalone mode (9-task pipeline)
- **config**: Unified config structure — `update.mode` replaces `machine.role`, `data.data_dir` replaces split server/client dirs
- **docs**: Rewrote `quant_lab.tex` — lab-module structure (5 Parts + Appendices, 17 chapters, 55 pages). Methodology-oriented, not hardcoded architecture. Each chapter: Motivation → Concepts → Implementation → Experiment → Reflection

### Refactor

- **data**: Merged `data_fetcher/` + `data_supply/` into unified `src/data/` package (`data/fetcher/` + `data/loader/`)
- **strategy**: Renamed `strategies/` → `strategy/`, `bbibollStrategy.py` → `bbiboll_strategy.py` (PEP 8)
- **i18n**: Translated all Chinese comments, docstrings, and print messages to English (~35 items across 3 files)
- **config**: Removed hardcoded `sppc_dir`, deferred `splits_data` to lazy getter `get_splits_data()`, removed dead `llmContext` loader
- **cleanup**: Deleted 5 dead files (`main.py`, 2 backtest examples, `plotter.py`, `cuda_env_test.py`)

### Fix

- **data/fetcher**: `csvgz_to_parquet.py` — argparse rejected its own default (`zstd` missing from choices)
- **data/fetcher**: `splits_fetch.py` — removed wrong unused `matplotlib.pylab` import
- **data/fetcher**: `fmp_fundamental_fetch.py` — print message said "csv" but file writes parquet
- **data/fetcher**: `indices_fetch.py` — removed debug print left in production code
- **backtest**: `performance_analyzer.py` — trading fee now configurable (was hardcoded 0.7%), calmar ratio properly implemented (was stub returning 0.0)
- **backtest**: `backtester.py` — fixed boolean precedence bug (`or` vs `and not`), fixed typo "isexported"
- **scripts**: `write_overview_csv.py` — removed wrong unused `duckdb` import

### Planned

- **alpha**: Factor research framework — IC / Rank IC / IR / decay / turnover analysis
- **alpha**: Cross-sectional factor construction, neutralization, orthogonalization
- **alpha**: Factor combination (linear, mean-variance, risk parity)
- **risk**: ~~Factor risk model, covariance estimation~~ (partially done — risk_metrics.py)
- **risk**: ~~Portfolio optimization, position sizing (Kelly, risk parity)~~ ✅
- **execution**: Slippage and market impact modeling for backtests
- **backtest**: Engine rewrite — Polars ETL + numba core loop
- **backtest**: Walk-forward / out-of-sample validation framework
- **data**: ~~Unify data_fetcher + data_supply into a single `data/` module~~ ✅
- **data**: Universe construction module (liquid universe, sector mapping)
- **research**: Structured research notebook templates (factor exploration → signal → backtest → report)
- **cli**: Unified entry point via typer/click
- **docs**: LaTeX learning journal — probability, statistics, time series, alpha research, risk, microstructure, ML, stochastic calculus

### Known Issues (Carried Forward)

- Backtest open position tracking needs fix
- Stock dividends not handled
- Low-volume tickers (max_duration_days > 50) are skipped — see v0.1.0 notes

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
