# Quant101

A personal quantitative trader & researcher learning project.

Built around US equities data from [Massive(Polygon.io)](https://massive.com/) flat files,
processed with [Polars](https://pola.rs/), backtested with a custom engine, and
documented as a learning journal in LaTeX.

> **Current version**: 1.0.0 — See [CHANGELOG.md](CHANGELOG.md) for details.
> **Detailed documentation**: See [guidance/quant_lab.pdf](guidance/quant_lab.pdf) for the full learning guide (59 pages).
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
python src/backtest/backtest.py
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

### Phase 8 — Alpha 101 Factor Zoo

Build a composable operator library inspired by [WorldQuant 101 Formulaic Alphas](https://arxiv.org/abs/1601.00991),
then implement, evaluate, and combine a diverse set of formulaic factors.

**Sprint 1 — Operator library** (`src/alpha/operators.py`)
- [ ] Time-series operators: `delta`, `delay`, `ts_rank`, `ts_min`, `ts_max`, `ts_argmin`, `ts_argmax`, `decay_linear`, `ts_sum`, `ts_product`, `ts_stddev`, `ts_corr`, `ts_cov`
- [ ] Cross-sectional operators: `cs_rank`, `cs_scale`, `cs_zscore`, `signed_power`
- [ ] Unit tests for each operator

**Sprint 2 — Factor implementation** (`src/alpha/alpha101/`)
- [ ] Implement 15–20 formulas spanning different families (momentum, mean-reversion, volume, volatility, correlation-based)
- [ ] Each factor registered in the factor registry (`src/portfolio/factors.py`)
- [ ] Run each through `FactorAnalyzer` — IC, quantile spread, decay, turnover

**Sprint 3 — Orthogonalization & combination** (`src/alpha/orthogonalization.py`)
- [ ] Factor correlation matrix across all surviving factors
- [ ] Incremental IC: marginal contribution of each factor after controlling for others
- [ ] Residualization: `new_factor = new_factor - beta * old_factors`
- [ ] IC-weighted combination (upgrade from equal-weight)
- [ ] Re-run Phase 2/3 notebooks with improved composite; check if breakeven cost rises above 5 bps

### Phase 9 — Portfolio Construction Upgrade

_Graduate from rank-and-select to optimization-based construction once alpha is strong enough._

- [ ] **Constrained MVO**: Ledoit-Wolf shrinkage for Σ, weight bounds, leverage cap
- [ ] **Turnover-aware optimization**: Add `γ‖wₜ − wₜ₋₁‖₁` penalty to objective
- [ ] **Regime tagging**: `src/data/regime.py` — bull/bear/sideways from rolling SPX returns

### Phase 10 — ML Integration (`src/ml/`)

- [ ] **Feature engineering**: Factor values as features, lagged returns, volatility features
- [ ] **Time-series validation**: Purged k-fold, embargo gap
- [ ] **Tree models**: LightGBM/XGBoost factor combination, feature importance analysis

### Phase 11 — Infrastructure

- [ ] **CLI**: Unified `typer` entry point (backtest, alpha, data-update)
- [ ] **Research notebooks**: Templated workflow — factor exploration → signal → backtest → report

### Known Issues

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
- **Documentation**: [guidance/quant_lab.pdf](guidance/quant_lab.pdf) — detailed learning journal
