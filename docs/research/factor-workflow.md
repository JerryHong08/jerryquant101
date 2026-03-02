# Factor Research Workflow

The end-to-end process for researching a new alpha factor in Quant101.

## The Research Loop

```
1. Hypothesis    → Why should this alpha exist?
2. Construct     → Compute the factor signal
3. Evaluate      → IC, IR, decay, turnover
4. Preprocess    → Winsorize, normalize, neutralize
5. Backtest      → Portfolio returns with sizing & costs
6. Validate      → Walk-forward, bootstrap, multiple testing
7. Reflect       → What did we learn? Next iteration?
```

!!! tip "Research Discipline"
    Never accept "it works" as a conclusion. Always ask:

    - What economic mechanism supports this alpha?
    - Under what regime will it fail?
    - Is it statistically significant after multiple-testing correction?

## Step 1: Hypothesis

Before writing any code, articulate:

- **The signal**: What information does the factor capture?
- **The mechanism**: Why should this predict future returns?
- **The expected sign**: Should high values predict high or low returns?
- **The decay profile**: How quickly should the signal lose power?

Example for BBIBOLL:

> *Stocks with BBI deviation far below the lower Bollinger Band are
> temporarily oversold and likely to mean-revert. Expected IC sign: negative
> (low deviation → high future return). Should decay within 5–10 days.*

## Step 2: Construct the Factor

All factors follow the `(date, ticker, value)` convention:

```python
import polars as pl
from portfolio.factors import register_factor

@register_factor("my_factor")
def compute_my_factor(ohlcv: pl.LazyFrame, **params) -> pl.DataFrame:
    """Compute my custom factor signal."""
    return (
        ohlcv.group_by("ticker")
        .agg(...)  # Your signal logic
        .select(["date", "ticker", pl.col("signal").alias("value")])
        .collect()
    )
```

Once registered, the factor is available in `AlphaConfig`:

```python
config = AlphaConfig(
    factor_configs={"my_factor": FactorConfig(direction=1)},
    ...
)
```

## Step 3: Evaluate with IC/IR

```python
from alpha.factor_analyzer import FactorAnalyzer
from alpha.forward_returns import compute_forward_returns

fwd = compute_forward_returns(returns_df, horizons=[1, 5, 10, 20])
fa = FactorAnalyzer(factor_df, fwd)

print(f"IC:  {fa.ic_series(horizon=1).mean():.4f}")
print(f"IR:  {fa.information_ratio(horizon=1):.4f}")
```

**Interpretation guidelines:**

| Metric | Weak | Decent | Strong | Suspicious |
|--------|------|--------|--------|------------|
| \|IC\| | < 0.02 | 0.02–0.05 | 0.05–0.10 | > 0.15 |
| \|IR\| | < 0.05 | 0.05–0.15 | 0.15–0.30 | > 0.50 |

!!! warning "Weak signals are normal"
    Single-factor IRs rarely exceed 0.3. The power comes from combining
    many weak-but-orthogonal signals (Fundamental Law of Active Management):

    $$\text{IR}_{\text{portfolio}} \approx \text{IR}_{\text{single}} \times \sqrt{N}$$

## Step 4: Preprocess

```python
from alpha.preprocessing import preprocess_factor

clean = preprocess_factor(
    factor_df,
    winsorize_pct=0.025,
    normalize_method="zscore",
    neutralize=None,  # or "sector"
)
```

**Why each step matters:**

- **Winsorize**: Prevents outliers from dominating cross-sectional rank
- **Z-score**: Makes signals comparable across factors for combination
- **Sector neutralize**: Removes sector beta — pure stock selection alpha

## Step 5: Backtest

```python
from portfolio.pipeline import run_alpha_pipeline
from portfolio.alpha_config import AlphaConfig, FactorConfig

config = AlphaConfig(
    factor_configs={"my_factor": FactorConfig(direction=1)},
    sizing_method="Equal-Weight",
    rebal_every_n=5,
    n_long=10,
    n_short=10,
)
results = run_alpha_pipeline(ohlcv, config=config)
print(f"Sharpe: {results['sharpe']:.3f}")
```

**Compare multiple configs:**

```python
from backtest.weight_backtester import WeightBacktester

configs = {
    "EW_daily": AlphaConfig(..., sizing_method="Equal-Weight", rebal_every_n=1),
    "EW_weekly": AlphaConfig(..., sizing_method="Equal-Weight", rebal_every_n=5),
    "SW_daily": AlphaConfig(..., sizing_method="Signal-Weighted", rebal_every_n=1),
}
for name, cfg in configs.items():
    r = run_alpha_pipeline(ohlcv, config=cfg)
    print(f"{name}: Sharpe={r['sharpe']:.3f}")
```

## Step 6: Validate

```python
from portfolio.walk_forward_runner import run_walk_forward

wf_results = run_walk_forward(ohlcv, config=config)
print(f"Mean OOS Sharpe: {wf_results['mean_oos_sharpe']:.3f}")
print(f"Sharpe decay:    {wf_results['sharpe_decay']:.3f}")
```

Then run the statistical gauntlet:

```python
from validation.statistical_tests import (
    bootstrap_sharpe_ci,
    deflated_sharpe_ratio,
)

ci = bootstrap_sharpe_ci(portfolio_returns)
dsr = deflated_sharpe_ratio(portfolio_returns, n_trials=16)
```

!!! danger "Multiple Testing"
    If you test 16 configs, use `apply_all_corrections()` on the p-values.
    In our experience, **0 out of 16 configs survived** Benjamini-Hochberg
    correction for the BBIBOLL factor alone. This is expected for weak signals —
    the goal is multi-factor combination.

## Step 7: Reflect & Document

Write up findings in the LaTeX journal (`docs/latex/quant_lab.tex`):

1. What was the hypothesis?
2. What did IC/IR look like?
3. Did it survive walk-forward?
4. What regime does it work in?
5. What's the next iteration?

Factor research is an iterative process. Most factors will fail.
The discipline is in the process, not the outcome.
