# Validation & Statistical Tests

How to determine whether your backtest results are real or noise.

## The Problem

A Sharpe ratio of 0.8 looks great — but is it statistically significant?
With enough parameter configurations, you will find one that looks good
purely by chance. Quant101 provides a full validation toolkit to answer this.

## Walk-Forward Analysis

Split data into rolling train/test windows with a purged embargo gap
to prevent look-ahead bias:

```python
from validation.walk_forward import walk_forward_split

folds = walk_forward_split(
    dates,
    train_days=126,     # ~6 months training
    test_days=63,       # ~3 months testing
    embargo_days=5,     # 5-day gap to prevent leakage
    mode="rolling",     # or "anchored"
)
```

Run the full pipeline per fold:

```python
from portfolio.walk_forward_runner import run_walk_forward

wf = run_walk_forward(ohlcv, config=config)
print(f"Mean OOS Sharpe: {wf['mean_oos_sharpe']:.3f}")
print(f"Sharpe decay:    {wf['sharpe_decay']:.3f}")
# Decay > 0 → IS Sharpe > OOS Sharpe (overfitting signal)
```

## Bootstrap Confidence Intervals

Circular block bootstrap preserves autocorrelation structure in
return series:

```python
from validation.statistical_tests import bootstrap_sharpe_ci

ci_low, ci_high = bootstrap_sharpe_ci(
    returns,
    n_bootstrap=10000,
    confidence=0.95,
)
print(f"95% CI: [{ci_low:.2f}, {ci_high:.2f}]")
```

!!! warning
    If the 95% CI includes zero, you **cannot reject** the hypothesis
    that your Sharpe is indistinguishable from noise.

## Probabilistic Sharpe Ratio (PSR)

From Bailey & de Prado (2012) — the probability that the true Sharpe
exceeds a benchmark, accounting for skewness and kurtosis:

```python
from validation.statistical_tests import probabilistic_sharpe_ratio

psr = probabilistic_sharpe_ratio(returns, sr_benchmark=0)
print(f"PSR: {psr:.1%}")  # e.g., 91.8%
```

PSR > 95% required for confidence. Adjusts for non-normal returns
(unlike a simple t-test on Sharpe).

## Deflated Sharpe Ratio (DSR)

From Bailey & de Prado (2014) — adjusts PSR for **multiple trials**.
If you tested 16 configurations, the expected maximum Sharpe by chance
increases. DSR corrects for this:

```python
from validation.statistical_tests import deflated_sharpe_ratio

dsr = deflated_sharpe_ratio(returns, n_trials=16)
print(f"DSR: {dsr:.1%}")
```

!!! danger "Our Result"
    For the BBIBOLL factor across 16 configurations:

    - Best config PSR = 91.8%
    - **DSR = 34.2%** (after adjusting for 16 trials)
    - Verdict: **not statistically significant**

## Multiple Testing Corrections

When testing many hypotheses simultaneously, adjust p-values:

```python
from validation.multiple_testing import apply_all_corrections

p_values = [0.03, 0.05, 0.12, 0.01, ...]  # One per config

results = apply_all_corrections(p_values)
# results["bonferroni"]        ← Most conservative (FWER control)
# results["holm"]              ← Step-down (FWER, more powerful)
# results["benjamini_hochberg"] ← FDR control (most liberal)
```

| Method | Controls | Use When |
|--------|----------|----------|
| Bonferroni | Family-wise error rate | Very conservative, few tests |
| Holm-Bonferroni | Family-wise error rate | Moderate, ordered step-down |
| Benjamini-Hochberg | False discovery rate | Many tests, accept some false positives |

## The Validation Gauntlet

The recommended validation sequence for any new factor:

```
1. Walk-forward IS/OOS Sharpe           → Is there signal OOS?
2. Bootstrap Sharpe CI                   → Does CI exclude zero?
3. PSR                                   → P(true Sharpe > 0)?
4. DSR (with n_trials)                   → After correction for snooping?
5. Multiple testing on config sweep      → Any config survives BH?
6. Sub-period stability                  → Consistent across half-years?
7. Cost-adjusted Sharpe                  → Profitable after 5 bps?
```

If a factor survives all seven, it's worth keeping. Most won't.
That's the point — failing cheaply and learning from the failure.

See `notebooks/validation.ipynb` for the full 16-config gauntlet.
