## Unreleased

### Project Direction

> **v1.0.0 Goal**: Upgrade quant101 from a backtesting experiment into a structured
> quant trader & researcher learning project. Incremental restructuring — add new
> modules without breaking existing ones.

### Feat

- **config**: Renamed machine_config.yaml → basic_config.yaml, added standalone single-machine mode
- **scripts**: Renamed weekly_update.sh → data_update.sh, added standalone mode (9-task pipeline)
- **config**: Unified config structure — `update.mode` replaces `machine.role`, `data.data_dir` replaces split server/client dirs

### Planned

- **alpha**: Factor research framework — IC / Rank IC / IR / decay / turnover analysis
- **alpha**: Cross-sectional factor construction, neutralization, orthogonalization
- **alpha**: Factor combination (linear, mean-variance, risk parity)
- **risk**: Factor risk model, covariance estimation
- **risk**: Portfolio optimization, position sizing (Kelly, risk parity)
- **execution**: Slippage and market impact modeling for backtests
- **backtest**: Engine rewrite — Polars ETL + numba core loop
- **backtest**: Walk-forward / out-of-sample validation framework
- **data**: Unify data_fetcher + data_supply into a single `data/` module
- **data**: Universe construction module (liquid universe, sector mapping)
- **research**: Structured research notebook templates (factor exploration → signal → backtest → report)
- **cli**: Unified entry point via typer/click (replace empty main.py)
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
- **strategies**: BBIBOLL strategy — BBI + Bollinger Band deviation with stop-loss/take-profit rules
- **strategies/indicators**: Decorator-based indicator registry with per-ticker group application
- **strategies/indicators**: BBI + Bollinger deviation percentile rank indicator (TA-Lib)
- **strategies/indicators**: OBV indicator
- **data_supply**: Risk-free rate (IRX) and SPX benchmark loader
- **data_supply**: Trading calendar-aware date utilities — weekly/monthly backtest date sequences
- **data_supply**: FIGI-based ticker mapping via connected components (tracks name changes/delistings)
- **data_fetcher**: Float shares fetcher (FinancialModelingPrep, async paginated)
- **data_fetcher**: Index daily aggregates fetcher (SPX, IRX) with incremental updates
- **data_fetcher**: Rsync-based multi-machine data sync (server → client)
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
- **data_supply**: Low-volume ticker incremental update fixed
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

- **data_fetcher**: Polygon.io S3 flat file downloader (stocks, options, indices, crypto, forex)
- **data_fetcher**: CSV.gz → Parquet converter with schema mapping per data type
- **data_fetcher**: All-tickers fetcher (stocks, OTC, indices) via Polygon REST API
- **data_fetcher**: Stock splits fetcher with incremental update support
- **data_supply**: Core data loader — OHLCV from Parquet/S3, split adjustment, timeframe resampling, caching
- **data_supply**: DataPathLoader — file path calculation for Polygon flat files (local + S3)
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
