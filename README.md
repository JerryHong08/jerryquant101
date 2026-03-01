# Quant101

A personal quantitative trader & researcher learning project.

Built around US equities data from [Polygon.io](https://polygon.io/) flat files,
processed with [Polars](https://pola.rs/), backtested with a custom engine, and
documented as a learning journal in LaTeX.

> **Current version**: 0.2.0 — See [CHANGELOG.md](CHANGELOG.md) for details.
> **Detailed documentation**: See [docs/quant_lab.tex](docs/quant_lab.tex) for the full learning guide.

---

## Architecture

```
quant101/
├── src/
│   ├── config.py              # Central config — data paths, machine role, asset loaders
│   ├── data_fetcher/          # Data acquisition — Polygon.io, FMP, yfinance
│   │   ├── polygon_downloader.py      # S3 flat file download (stocks, options, indices, crypto)
│   │   ├── csvgz_to_parquet.py        # CSV.gz → Parquet conversion with schema mapping
│   │   ├── all_tickers_fetch.py       # Full ticker list (stocks, OTC, indices) via REST API
│   │   ├── splits_fetch.py            # Stock splits with incremental updates
│   │   ├── indices_fetch.py           # Index daily aggs (SPX, IRX)
│   │   ├── fmp_fundamental.py         # Float shares (async, paginated)
│   │   └── fetch_from_server.py       # Rsync multi-machine data sync
│   ├── data_supply/           # Data loading & transformation
│   │   ├── data_loader.py             # Core OHLCV loader — split-adjusted, resampled, cached
│   │   ├── benchmark_loader.py        # IRX risk-free rate, SPX benchmark
│   │   ├── date_utils.py              # Trading calendar date math (XNYS)
│   │   ├── path_loader.py             # File path resolver (local + S3)
│   │   └── ticker_utils.py            # FIGI-based ticker mapping & universe filtering
│   ├── backtest/              # Backtesting framework
│   │   ├── backtest_engine.py         # Orchestrator — runs strategies, exports reports
│   │   ├── strategy_base.py           # Abstract base: prepare_data → generate_signals → simulate
│   │   ├── performance_analyzer.py    # Sharpe, Sortino, CAGR, drawdown, win rate
│   │   ├── backtest_visualizer.py     # Equity curves, heatmaps, candlestick with signals
│   │   ├── run_backtest.py            # End-to-end runner (main entry point)
│   │   └── trades_analyzer.py         # Post-hoc position analysis with Plotly animation
│   ├── strategies/            # Trading strategies & indicators
│   │   ├── bbiboll_strategy.py        # BBI + Bollinger Band deviation strategy
│   │   └── indicators/               # Registry-based indicator system (TA-Lib)
│   ├── visualizer/            # Standalone charting
│   ├── longport/              # Longport/Longbridge broker integration
│   └── utils/                 # Logger, shared utilities
├── scripts/                   # Operational scripts
│   ├── incremental_update/            # data_update.sh, low_volume_ticker_update.py
│   └── file_examiner.py              # Data directory inspector
├── docs/                      # LaTeX learning journal & architecture guide
├── notebooks/                 # Research & exploration notebooks
├── data/                      # Error correction CSVs, fundamentals
└── backtest_output/           # Generated reports, charts, position analysis
```

---

## Quick Start

### 1. Data Setup

Configure your data paths and update mode in `basic_config.yaml` (copy from `basic_config.yaml.example`), then:

```bash
# Download recent Polygon.io flat files
python src/data_fetcher/polygon_downloader.py \
    --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

# Convert to Parquet
python src/data_fetcher/csvgz_to_parquet.py \
    --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

# Or run the full incremental update (standalone mode by default)
bash scripts/incremental_update/data_update.sh
```

### 2. Run a Backtest

```bash
python src/backtest/run_backtest.py
```

### 3. Data Directory Structure

```
polygon_data/
├── lake/           # Parquet files (converted from csv.gz)
├── processed/      # Cached/resampled data
└── raw/            # Original csv.gz + metadata (splits, tickers, indices)
```

---

## Roadmap

### v1.0.0 — Quant Research Framework (Next)

- [ ] **Alpha Research**: Factor IC/IR analysis, cross-sectional factor construction, decay & turnover
- [ ] **Risk & Portfolio**: Factor risk model, covariance estimation, portfolio optimization
- [ ] **Backtest Rewrite**: Polars ETL + numba engine, walk-forward validation
- [ ] **Execution Model**: Slippage & market impact modeling
- [ ] **Data Unification**: Merge data_fetcher + data_supply into single `data/` module
- [ ] **Universe Construction**: Liquid universe, sector mapping
- [ ] **CLI**: Unified entry point via typer (replace empty main.py)
- [ ] **Research Notebooks**: Templated workflow — factor exploration → signal → backtest → report
- [ ] **Documentation**: LaTeX learning journal covering probability, time series, alpha, risk, ML

### Open Bugs

- [ ] Backtest open position tracking
- [ ] Stock dividends not handled
- [ ] Low-volume tickers skipped (>50 days zero volume)

---

## Related

- **Live Trading**: [jerryib_trader](https://github.com/JerryHong08/jerryib_trader) — Market Mover Monitor + GridTrader (separated from this repo)
- **Data Source**: [Polygon.io Flat Files](https://polygon.io/flat-files)
- **Documentation**: [docs/quant_lab.tex](docs/quant_lab.tex) — detailed learning journal
