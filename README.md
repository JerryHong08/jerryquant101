# Quant101

A personal quantitative trader & researcher learning project.

Built around US equities data from [Massive(Polygon.io)](https://massive.com/) flat files,
processed with [Polars](https://pola.rs/), backtested with a custom engine, and
documented as a learning journal in LaTeX.

> **Current version**: 1.0.0 — See [CHANGELOG.md](CHANGELOG.md) for details.
> **Detailed documentation**: See [docs/latex/quant_lab.pdf](docs/latex/quant_lab.pdf) for the full learning guide (59 pages).
> **Online docs**: [https://jerryhong08.github.io/jerryquant101/](https://jerryhong08.github.io/jerryquant101/)

---

## Quick Start

### 1. Data Setup

Configure your data paths and update mode in `basic_config.yaml` (copy from `basic_config.yaml.example`), then:

```bash
# Data acquisition scripts under data/fetcher
# Example: 
# Download recent Polygon.io whole market daily flat files
python src/data/fetcher/polygon_downloader.py \
    --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

# Convert to Parquet
python src/data/fetcher/csvgz_to_parquet.py \
    --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

# incremental update (standalone mode by default)
bash scripts/incremental_update/data_update.sh
```

### 2. Run a Backtest

```bash
python src/backtest/run_backtest.py
```

### 3. Data Directory Structure

```bash
polygon_data/
├── lake/           # Parquet files (converted from csv.gz)
├── processed/      # Cached/resampled data
└── raw/            # Original csv.gz + metadata (splits, tickers, indices)
```

---

## Roadmap

> Completed work is recorded in [CHANGELOG.md](CHANGELOG.md).  
> Phases 0–7 are done (v1.0.0). Items below are what remains.

### Factor Research (ongoing)

- [ ] **More factors**: Cross-sectional momentum (12-1 month), short-term reversal, low-volatility
- [ ] **Regime tagging**: `src/data/regime.py` — bull/bear/sideways from rolling SPX returns
- [ ] **Portfolio construction**: Market-neutral long-short, factor exposure targeting

### Phase 8 — ML Integration (`src/ml/`)

- [ ] **Feature engineering**: Factor values as features, lagged returns, volatility features
- [ ] **Time-series validation**: Purged k-fold, embargo gap
- [ ] **Tree models**: LightGBM/XGBoost factor combination, feature importance analysis

### Phase 9 — Infrastructure

- [ ] **CLI**: Unified `typer` entry point (backtest, alpha, data-update)
- [ ] **Research notebooks**: Templated workflow — factor exploration → signal → backtest → report

### Known Issues

- [ ] Position Sizing method about long&short allocation is wrong, to be fixed.!!!
- [ ] Stock dividends not handled (needed for dividend yield factor)
- [ ] `run_pipeline_backtest()` does not pass full `AlphaConfig` yet (uses keyword args)
- [ ] `data_loader.py` 1,192-line monolith with mixed concerns → split gradually
- [ ] AWS creds loaded at module-level import (should be lazy)
- [ ] Date column naming: `"timestamps"` (OHLCV) vs `"date"` (alpha/risk/execution)
- [ ] `src/constants.py` only wired into `performance_analyzer.py`
- [ ] Low-volume tickers skipped (>50 days zero volume) — conscious data quality tradeoff

---

## Related

- **Live Trading**: [jerryib_trader](https://github.com/JerryHong08/jerryib_trader) — Market Mover Monitor + GridTrader (separated from this repo)
- **Data Source**: [Polygon.io Flat Files](https://polygon.io/flat-files)
- **Documentation**: [docs/latex/quant_lab.pdf](docs/latex/quant_lab.pdf) — detailed learning journal
