# Quant101

A personal quantitative trader & researcher learning project.

Built around US equities data from [Massive(Polygon.io)](https://massive.com/) flat files,
processed with [Polars](https://pola.rs/), backtested with a custom engine, and
documented as a learning journal in LaTeX.

> **Current version**: 0.4.5 — See [CHANGELOG.md](CHANGELOG.md) for details.
> **Detailed documentation**: See [docs/quant_lab.tex](docs/quant_lab.tex) for the full learning guide.

---

## Architecture

```bash
src/
├── constants.py                   # Shared constants — TRADING_DAYS_PER_YEAR, column name conventions
├── config.py                      # Central config — data paths, asset loaders, lazy getters
├── alpha/                         # Factor research — signals, evaluation, preprocessing, combination
├── risk/                          # Risk & portfolio — VaR/CVaR, distribution analysis, position sizing
├── execution/                     # Transaction costs — fixed, spread, sqrt-impact, composite models
├── validation/                    # Walk-forward, bootstrap CI, PSR/DSR, multiple-testing corrections
├── data/
│   ├── fetcher/               # Data acquisition — Polygon.io S3, FMP, yfinance, rsync
│   └── loader/                # Data loading — OHLCV, split adjustment, resampling, caching
├── backtest/                  # Backtesting — engine, strategy base, performance analyzer, visualizer
├── strategy/                  # Trading strategies & indicator registry
│   └── indicators/            # Technical indicators (BBIBOLL, OBV, etc.)
├── visualizer/                # Standalone charting
├── longport/                  # Longport broker integration
├── examples/                  # Usage examples
└── utils/                     # Logger, shared utilities

tests/
├── conftest.py                # Shared fixtures — synthetic returns, factors, weights, turnover
├── test_validation.py         # 34 tests — walk-forward, bootstrap, p-values, multiple testing
├── test_execution.py          # 25 tests — cost models, turnover, net returns, breakeven
├── test_risk.py               # 26 tests — VaR, CVaR, drawdown, distribution stats
└── test_alpha.py              # 22 tests — winsorize, z-score, rank, neutralize, combination
```

**Target modules** (not yet built):
- `src/portfolio/` — Signal→weights pipeline, universe registry
- `src/ml/` — Feature engineering, time-series validation, tree models

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

### Phase 0 — Foundation (✅ Complete)

- [x] Data pipeline: Polygon.io S3 flat files → Parquet, split adjustment, FIGI ticker mapping
- [x] Backtest engine: Abstract `StrategyBase` → `BacktestEngine` → `PerformanceAnalyzer`
- [x] BBIBOLL strategy: BBI + Bollinger Band deviation, indicator registry
- [x] Data unification: Merged `data_fetcher/` + `data_supply/` into `src/data/` (fetcher + loader)
- [x] Code quality: English-only codebase, dead code removed, 7 critical bugs fixed
- [x] Documentation: LaTeX learning journal (55 pages, 5 Parts, 17 chapters)
- [x] Config: `basic_config.yaml` with standalone/server/client modes

### Phase 1 — Alpha Research Framework (`src/alpha/`) (✅ Complete)

> LaTeX reference: Part III, Chapters 9–12

- [x] **Factor base**: Factor abstraction — `(date, ticker, value)` signal DataFrame convention
- [x] **Factor evaluation**: `FactorAnalyzer` — IC series, IR, IC decay curve, turnover, quantile returns
- [x] **Factor preprocessing**: `preprocess_factor()` — winsorize, z-score, rank-normalize, sector neutralize
- [x] **Factor construction**: Convert BBIBOLL deviation to cross-sectional factor, 20-day momentum factor
- [x] **Factor combination**: Equal-weight, IC-weight, mean-variance, risk-parity on IC covariance
- [x] **Forward returns**: Utility to compute 1/5/10/20-day forward returns for the universe
- [x] **Validation notebook**: `notebooks/alpha_research.ipynb` — end-to-end BBIBOLL factor analysis (30 cells, all passing)
- [x] **Alpha iteration**: `notebooks/alpha_iteration.ipynb` — STR, Volume-Price Divergence, Vol Ratio factors; IC correlation analysis; diversification confirmed (BBIBOLL + Vol Ratio composite |IR|=0.136 > best individual 0.122)

### Phase 2 — Risk & Portfolio (`src/risk/`) (✅ Complete)

> LaTeX reference: Part IV, Chapters 13–14

- [x] **Risk measures**: VaR (historical + parametric), CVaR, drawdown, skewness, kurtosis, tail ratio
- [x] **Return distribution analysis**: Normality tests (Jarque-Bera, Shapiro-Wilk), QQ-plot data, Gaussian comparison, tail analysis
- [x] **Position sizing**: Equal-weight, inverse-volatility, volatility-target, half-Kelly
- [ ] **Portfolio construction**: Market-neutral long-short, factor exposure targeting
- [x] **Validation notebook**: `notebooks/risk_analysis.ipynb` — end-to-end risk analysis with BBIBOLL + Vol Ratio composite (14 code cells, all passing; 3 bugs found and fixed)

### Phase 3 — Execution & Cost Modeling (`src/execution/`) (✅ Complete)

> LaTeX reference: Part IV, Chapter 15

- [x] **Cost model**: ABC `CostModel` + 4 implementations (Fixed, Spread, SqrtImpact, Composite)
- [x] **Cost analysis**: Turnover computation, net returns, Sharpe-vs-cost curves, breakeven cost
- [x] **Validation notebook**: `notebooks/cost_analysis.ipynb` — 4 sizing methods compared gross vs net, all methods net-negative at 5 bps, Half-Kelly breakeven = 1.8 bps

### Phase 4 — Walk-Forward Validation (`src/validation/`) (✅ Complete)

> LaTeX reference: Part IV, Chapter 16

- [x] **Walk-forward splitter**: Rolling/anchored modes, purged embargo gap, date mapping
- [x] **Statistical tests**: Bootstrap Sharpe CI (circular block), Lo (2002) p-values, PSR (Bailey & de Prado 2012), DSR (Bailey & de Prado 2014)
- [x] **Multiple testing**: Bonferroni, Holm-Bonferroni, Benjamini-Hochberg corrections
- [x] **Validation notebook**: `notebooks/validation.ipynb` — 16-config gauntlet: walk-forward IS/OOS, bootstrap, PSR/DSR, p-value corrections. Verdict: 0/16 survive correction (DSR=34.2% for best config)

### Phase 4.5 — Test Suite + Cleanup (✅ Complete)

- [x] **Test suite**: `tests/` — 107 pytest tests across 4 modules (validation, execution, risk, alpha), shared fixtures in `conftest.py`
- [x] **Constants**: `src/constants.py` — `TRADING_DAYS_PER_YEAR`, column name conventions
- [x] **Bug fix**: Fee calculation in `performance_analyzer.py` (was mathematically wrong)
- [x] **Dependency pruning**: `pyproject.toml` main deps 35 → 25, unused moved to `[ml]`/`[infra]` optional groups
- [x] **Cleanup**: Dead imports removed, hardcoded `252` → constant, `testpaths` fixed

### Phase 5 — Portfolio Pipeline (`src/portfolio/`)

- [ ] **Signal→weights bridge**: `run_alpha_pipeline()` — replace 80-line duplicated pipeline across notebooks
- [ ] **Universe registry**: `src/data/universe.py` — `SP500_TOP50`, `LIQUID_US`, etc.
- [ ] **Walk-forward harness**: `run_walk_forward(pipeline_fn, folds)` — execute per fold, collect IS/OOS metrics

### Phase 6 — Backtest Refactor

> Targeted surgery on legacy code, not a full rewrite.

- [ ] **Extract God class**: Split `engine.py` into composable pieces (data loading, position tracking, reporting)
- [ ] **Accept portfolio weights**: Make `BacktestEngine` work with weight DataFrames directly, not just `StrategyBase` subclasses
- [ ] **Fix open position tracking bug**

### Phase 7 — Multi-Factor Alpha

- [ ] **More factors**: Momentum (12-1 month), quality/earnings, mean reversion, value
- [ ] **Factor registry**: `src/alpha/registry.py` — register factors by name, `get_factor("bbiboll_dev")`
- [ ] **Regime tagging**: `src/data/regime.py` — bull/bear/sideways from rolling SPX returns

### Phase 8 — ML Integration (`src/ml/`)

> LaTeX reference: Part V, Chapters 17–19

- [ ] **Feature engineering**: Factor values as features, lagged returns, volatility features
- [ ] **Time-series validation**: Purged k-fold, embargo gap
- [ ] **Tree models**: LightGBM/XGBoost factor combination, feature importance analysis

### Phase 9 — Infrastructure

- [ ] **Universe construction**: Liquid universe module, sector/industry mapping (GICS)
- [ ] **CLI**: Unified `typer` entry point (backtest, alpha, data-update)
- [ ] **Research notebooks**: Templated workflow — factor exploration → signal → backtest → report

### Known Issues

> Issues are fixed as natural byproducts of each phase — no separate fix sprint needed.

**Legacy backtest (High)** — fix in Phase 6
- [ ] `engine.py` God class — mixes data loading, signal routing, position tracking, reporting → extract in Phase 6
- [ ] No alpha→backtest bridge — notebooks use inline workaround → **resolved by Phase 5** (`pipeline.py`)
- [ ] Open position tracking bug → fix during Phase 6 refactor
- [ ] Stock dividends not handled → fix in Phase 7 (needed for dividend yield factor)

**Data layer (Medium)** — fix in Phase 5–6
- [ ] `data_loader.py` 1,192-line monolith with mixed concerns → split gradually during Phase 6
- [ ] AWS creds loaded at module-level import (should be lazy) → fix in Phase 6 when refactoring data loading
- [ ] SQL injection risk in DuckDB credential queries → quick fix, do in Phase 5
- [ ] Date column naming: `"timestamps"` (OHLCV) vs `"date"` (alpha/risk/execution) → enforce via `constants.py` in Phase 5

**Other (Low)** — fix in Phase 5
- [ ] `src/constants.py` only wired into `performance_analyzer.py` — wire into pipeline in Phase 5
- [ ] No universe module — stock universe hardcoded in every notebook → **resolved by Phase 5** (`universe.py`)
- [ ] Low-volume tickers skipped (>50 days zero volume) → keep as-is (conscious data quality tradeoff)

---

## Related

- **Live Trading**: [jerryib_trader](https://github.com/JerryHong08/jerryib_trader) — Market Mover Monitor + GridTrader (separated from this repo)
- **Data Source**: [Polygon.io Flat Files](https://polygon.io/flat-files)
- **Documentation**: [docs/quant_lab.tex](docs/quant_lab.tex) — detailed learning journal
