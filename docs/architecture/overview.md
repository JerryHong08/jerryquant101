# Architecture Overview

## Two Backtest Paths

Quant101 offers two mental models for systematic investing, each with its
own code path:

```
Strategy Path ("I Trade")                Pipeline Path ("I Allocate")
─────────────────────────                ────────────────────────────
Indicators per ticker                    Factors for all stocks simultaneously
Row-by-row signal logic                  Cross-sectional ranking
Buy/sell events                          Portfolio weights
Trade list + P&L                         Weight matrix + return series

BBIBOLLStrategy                          AlphaConfig
    → BacktestEngine                         → run_alpha_pipeline()
        → PerformanceAnalyzer                    → WeightBacktester
```

The **pipeline path** is the primary research workflow. The strategy path
is kept for execution-level analysis and comparison.

## Module Map

```
src/
├── data/              ← Data layer (OHLCV loading, universe)
│   ├── fetcher/       ← Polygon.io S3, FMP, yfinance
│   ├── loader/        ← OHLCV loading, split adjustment, caching
│   └── universe.py    ← Named stock universe registry
│
├── alpha/             ← Factor research
│   ├── factor_analyzer.py    ← IC, IR, decay, quantile returns
│   ├── preprocessing.py      ← Winsorize, z-score, rank, neutralize
│   ├── combination.py        ← EW, IC-weight, mean-var, risk-parity
│   └── forward_returns.py    ← 1/5/10/20-day forward returns
│
├── portfolio/         ← Alpha pipeline (the core)
│   ├── alpha_config.py       ← AlphaConfig + FactorConfig dataclasses
│   ├── factors.py            ← Factor function registry
│   ├── pipeline.py           ← 7-stage signal→weights→returns
│   └── walk_forward_runner.py ← Per-fold IS/OOS evaluation
│
├── risk/              ← Risk measurement & position sizing
│   ├── risk_metrics.py       ← VaR, CVaR, drawdown, tail ratio
│   ├── return_analysis.py    ← Normality tests, QQ-plot, tails
│   └── position_sizing.py    ← EW, inverse-vol, vol-target, signal-weighted
│
├── execution/         ← Transaction cost modeling
│   ├── cost_model.py         ← Fixed, Spread, SqrtImpact, Composite
│   └── cost_analysis.py      ← Turnover, net returns, breakeven
│
├── validation/        ← Out-of-sample rigor
│   ├── walk_forward.py       ← Rolling/anchored walk-forward splitter
│   ├── statistical_tests.py  ← Bootstrap CI, PSR, DSR
│   └── multiple_testing.py   ← Bonferroni, Holm, BH corrections
│
├── backtest/          ← Backtesting engine
│   ├── engine.py             ← Legacy strategy executor
│   ├── weight_backtester.py  ← Pipeline→backtest bridge
│   ├── portfolio_tracker.py  ← Weight×return→equity curve
│   └── result_exporter.py    ← Reports, HTML, CSV export
│
└── strategy/          ← Trading strategies (legacy path)
    ├── bbiboll_strategy.py   ← BBI + Bollinger strategy
    └── indicators/           ← Technical indicator registry
```

## Data Flow

The pipeline path follows a strict linear flow:

```
OHLCV Data
    │
    ▼
compute_daily_returns()        ← close → daily returns
    │
    ▼
build_factor_pipeline()        ← compute factors, preprocess, combine
    │  uses: FactorConfig.direction, preprocessing params
    │  uses: combination method (EW, IC-weight, etc.)
    │
    ▼
build_sizing_methods()         ← factor signal → portfolio weights
    │  methods: equal-weight, inverse-vol, vol-target, signal-weighted
    │
    ▼
resample_weights()             ← daily weights → rebalance frequency
    │
    ▼
compute_portfolio_return()     ← weights × returns → portfolio return
    │
    ▼
WeightBacktester               ← analytics, Sharpe, drawdown, export
```

All stages are wrapped by `run_alpha_pipeline(ohlcv, config=AlphaConfig(...))`,
which returns a dict with `portfolio_returns`, `sharpe`, `weights`, and diagnostic
DataFrames.

## Configuration: AlphaConfig

The entire pipeline is driven by a single config object:

```python
from portfolio.alpha_config import AlphaConfig, FactorConfig

config = AlphaConfig(
    factor_configs={
        "bbiboll": FactorConfig(direction=-1),
        "vol_ratio": FactorConfig(direction=-1),
    },
    combination_method="ic_weight",
    sizing_method="Equal-Weight",
    n_long=10,
    n_short=10,
    rebal_every_n=5,
)

results = run_alpha_pipeline(ohlcv, config=config)
print(f"Sharpe: {results['sharpe']:.3f}")
```
