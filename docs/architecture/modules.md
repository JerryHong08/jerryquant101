# Module Reference

Detailed reference for each `src/` module.

## `src/alpha/` — Factor Research

### `factor_analyzer.py`

`FactorAnalyzer` — the core factor evaluation class.

```python
from alpha.factor_analyzer import FactorAnalyzer

fa = FactorAnalyzer(factor_df, forward_returns_df)
ic_series = fa.ic_series(horizon=1)         # Daily rank IC
ir = fa.information_ratio(horizon=1)        # IC mean / IC std
decay = fa.ic_decay(horizons=[1, 5, 10, 20])  # IC at multiple horizons
quantile_ret = fa.quantile_returns(n_quantiles=5)
turnover = fa.turnover()
```

**Inputs:**

- `factor_df`: `pl.DataFrame` with columns `(date, ticker, value)` — the factor signal
- `forward_returns_df`: `pl.DataFrame` with columns `(date, ticker, forward_return_1d, ...)`

### `preprocessing.py`

```python
from alpha.preprocessing import preprocess_factor

clean = preprocess_factor(
    factor_df,
    winsorize_pct=0.025,        # Clip at 2.5th/97.5th percentile
    normalize_method="zscore",   # "zscore" | "rank" | None
    neutralize="sector",         # "sector" | None
)
```

### `combination.py`

Combine multiple factor signals into a composite:

```python
from alpha.combination import combine_factors

composite = combine_factors(
    [factor_a, factor_b],
    method="ic_weight",         # "equal" | "ic_weight" | "mean_variance" | "risk_parity"
    ic_series_list=[ic_a, ic_b],  # Required for ic_weight
)
```

---

## `src/risk/` — Risk Measurement & Position Sizing

### `risk_metrics.py`

```python
from risk.risk_metrics import risk_summary

summary = risk_summary(returns_series)
# Returns dict: VaR_95, CVaR_95, max_drawdown, skewness, kurtosis, tail_ratio
```

Individual functions: `var_historical()`, `var_parametric()`, `cvar_historical()`,
`cvar_parametric()`, `drawdown_series()`, `max_drawdown()`, `tail_ratio()`.

### `position_sizing.py`

Four sizing methods, all producing `pl.DataFrame` with `(date, ticker, weight)`:

| Method | Function | Formula |
|--------|----------|---------|
| Equal Weight | `size_equal_weight()` | $w_i = \pm 1/N$ |
| Inverse Volatility | `size_inverse_volatility()` | $w_i \propto 1/\sigma_i$ |
| Volatility Target | `size_volatility_target()` | $w_i = \sigma_{\text{target}} / \sigma_i$ |
| Signal-Weighted | `size_signal_weighted()` | $w_i \propto z_i / \sigma_i^2$ |

!!! note "Signal-Weighted ≠ Kelly"
    The signal-weighted method was originally called "Half-Kelly" but renamed
    for honesty. Real Kelly determines **portfolio leverage** (how much capital
    to deploy), not relative allocation. See [Position Sizing](../research/position-sizing.md)
    for the full story.

---

## `src/execution/` — Transaction Costs

### `cost_model.py`

Abstract `CostModel` with 4 implementations:

```python
from execution.cost_model import FixedCostModel, SqrtImpactCostModel, CompositeCostModel

fixed = FixedCostModel(bps=5)                    # 5 bps flat
impact = SqrtImpactCostModel(eta=0.1, sigma=vol)  # Almgren-style √impact
composite = CompositeCostModel([fixed, impact])    # Sum of models
```

### `cost_analysis.py`

```python
from execution.cost_analysis import compute_turnover, breakeven_cost

turnover = compute_turnover(weights_today, weights_yesterday)
be = breakeven_cost(portfolio_returns, turnover)  # Binary search for Sharpe=0
```

---

## `src/validation/` — Walk-Forward & Statistical Tests

### `walk_forward.py`

```python
from validation.walk_forward import walk_forward_split

folds = walk_forward_split(
    dates, train_days=126, test_days=63,
    embargo_days=5, mode="rolling"
)
```

### `statistical_tests.py`

```python
from validation.statistical_tests import (
    bootstrap_sharpe_ci,
    probabilistic_sharpe_ratio,
    deflated_sharpe_ratio,
)

ci_low, ci_high = bootstrap_sharpe_ci(returns, n_bootstrap=10000)
psr = probabilistic_sharpe_ratio(returns, sr_benchmark=0)
dsr = deflated_sharpe_ratio(returns, n_trials=16)
```

### `multiple_testing.py`

```python
from validation.multiple_testing import apply_all_corrections

results = apply_all_corrections(p_values)
# Returns: bonferroni, holm, benjamini_hochberg adjusted p-values
```

---

## `src/backtest/` — Backtesting

### `weight_backtester.py`

The bridge from pipeline weights to full analytics:

```python
from backtest.weight_backtester import WeightBacktester

bt = WeightBacktester(weights_df, returns_df, benchmark_df)
result = bt.run(cost_bps=5)

print(result.sharpe, result.total_return, result.max_drawdown)
bt.print_summary()
bt.export("output/my_backtest/")
```

### `portfolio_tracker.py`

Pure-computation portfolio simulator:

```python
from backtest.portfolio_tracker import PortfolioTracker

tracker = PortfolioTracker(cost_bps=5)
result = tracker.run(weights_df, returns_df)
# result.portfolio_daily: daily portfolio returns
# result.turnover: daily turnover
```
