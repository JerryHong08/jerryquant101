"""
Statistical Tests — Bootstrap confidence intervals and significance tests.

Provides the statistical machinery to answer "is this Sharpe ratio real?"
beyond point estimates. Key tools:

    - **Bootstrap Sharpe CI**: Non-parametric confidence interval via
      circular block bootstrap (preserves autocorrelation).
    - **Sharpe ratio p-value**: Probability of observing the Sharpe under
      the null hypothesis of zero mean return.
    - **Deflated Sharpe Ratio (DSR)**: Adjusts for multiple testing,
      skewness, and kurtosis (Bailey & de Prado, 2014).
    - **Probabilistic Sharpe Ratio (PSR)**: Probability that true Sharpe
      exceeds a benchmark, accounting for estimation error.

Reference:
    - Bailey & de Prado (2012), "The Sharpe Ratio Efficient Frontier"
    - Bailey & de Prado (2014), "The Deflated Sharpe Ratio"
    - Ledoit & Wolf (2008), "Robust Performance Hypothesis Testing"
    - docs/quant_lab.tex — Part V, Chapter 16 (Validation)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy import stats


@dataclass(frozen=True)
class BootstrapResult:
    """Result of a bootstrap confidence interval estimation.

    Attributes:
        point_estimate: Original statistic computed on the full sample.
        ci_lower: Lower bound of the confidence interval.
        ci_upper: Upper bound of the confidence interval.
        confidence_level: Confidence level (e.g., 0.95).
        n_bootstrap: Number of bootstrap samples drawn.
        bootstrap_distribution: Array of bootstrap statistics (for plotting).
        se: Standard error of the bootstrap distribution.
    """

    point_estimate: float
    ci_lower: float
    ci_upper: float
    confidence_level: float
    n_bootstrap: int
    bootstrap_distribution: np.ndarray
    se: float

    @property
    def ci_width(self) -> float:
        """Width of the confidence interval."""
        return self.ci_upper - self.ci_lower

    @property
    def significant_at_zero(self) -> bool:
        """True if the CI excludes zero."""
        return self.ci_lower > 0 or self.ci_upper < 0

    def __repr__(self) -> str:
        return (
            f"Bootstrap(estimate={self.point_estimate:.3f}, "
            f"CI=[{self.ci_lower:.3f}, {self.ci_upper:.3f}], "
            f"SE={self.se:.3f}, "
            f"{'significant' if self.significant_at_zero else 'not significant'})"
        )


def _annualized_sharpe(returns: np.ndarray, annualization: int = 252) -> float:
    """Annualized Sharpe ratio from daily returns."""
    if len(returns) < 2 or np.std(returns, ddof=1) < 1e-12:
        return 0.0
    return float(np.mean(returns) / np.std(returns, ddof=1) * np.sqrt(annualization))


def bootstrap_sharpe_ci(
    returns: np.ndarray,
    n_bootstrap: int = 10_000,
    confidence_level: float = 0.95,
    block_size: int = 5,
    annualization: int = 252,
    seed: int | None = 42,
) -> BootstrapResult:
    """Bootstrap confidence interval for annualized Sharpe ratio.

    Uses circular block bootstrap to preserve serial dependence in returns.
    Block bootstrap resamples contiguous blocks of length ``block_size``
    rather than individual observations, maintaining autocorrelation
    structure.

    Args:
        returns: 1-D array of daily returns.
        n_bootstrap: Number of bootstrap resamples.
        confidence_level: CI level (e.g., 0.95 for 95%).
        block_size: Length of contiguous blocks for circular block bootstrap.
            Recommended: 5 (one week) for daily returns.
        annualization: Annualization factor (252 for daily).
        seed: Random seed for reproducibility.

    Returns:
        BootstrapResult with point estimate, CI, and distribution.
    """
    returns = np.asarray(returns, dtype=np.float64)
    n = len(returns)

    if n < 10:
        raise ValueError(f"Need at least 10 returns, got {n}")

    rng = np.random.default_rng(seed)
    point_estimate = _annualized_sharpe(returns, annualization)

    # Circular block bootstrap
    n_blocks = int(np.ceil(n / block_size))
    boot_sharpes = np.empty(n_bootstrap)

    for i in range(n_bootstrap):
        # Sample random block start positions (circular)
        block_starts = rng.integers(0, n, size=n_blocks)
        indices = np.concatenate(
            [np.arange(s, s + block_size) % n for s in block_starts]
        )[:n]
        boot_returns = returns[indices]
        boot_sharpes[i] = _annualized_sharpe(boot_returns, annualization)

    alpha = 1 - confidence_level
    ci_lower = float(np.percentile(boot_sharpes, 100 * alpha / 2))
    ci_upper = float(np.percentile(boot_sharpes, 100 * (1 - alpha / 2)))
    se = float(np.std(boot_sharpes, ddof=1))

    return BootstrapResult(
        point_estimate=point_estimate,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        confidence_level=confidence_level,
        n_bootstrap=n_bootstrap,
        bootstrap_distribution=boot_sharpes,
        se=se,
    )


def sharpe_pvalue(
    returns: np.ndarray,
    annualization: int = 252,
) -> dict:
    """Two-sided p-value for the null hypothesis that Sharpe = 0.

    Uses the asymptotic distribution of the Sharpe ratio estimator:
    SR ~ N(SR*, sqrt((1 + SR*²/2 - γ₃·SR* + (γ₄-1)/4·SR*²) / T))

    where γ₃ = skewness, γ₄ = kurtosis, T = sample size.

    This is the Lo (2002) adjusted standard error.

    Args:
        returns: 1-D array of daily returns.
        annualization: Annualization factor.

    Returns:
        Dict with sharpe, se, t_stat, p_value.
    """
    returns = np.asarray(returns, dtype=np.float64)
    n = len(returns)

    if n < 10:
        return {"sharpe": 0.0, "se": np.inf, "t_stat": 0.0, "p_value": 1.0}

    sr = _annualized_sharpe(returns, annualization)
    sr_daily = (
        np.mean(returns) / np.std(returns, ddof=1) if np.std(returns, ddof=1) > 0 else 0
    )

    # Skewness and excess kurtosis
    skew = float(stats.skew(returns))
    kurt = float(stats.kurtosis(returns))  # excess kurtosis

    # Lo (2002) standard error of Sharpe ratio
    se_daily = np.sqrt(
        (1 + 0.5 * sr_daily**2 - skew * sr_daily + (kurt / 4) * sr_daily**2) / n
    )
    se_annual = se_daily * np.sqrt(annualization)

    if se_annual < 1e-12:
        return {"sharpe": sr, "se": 0.0, "t_stat": np.inf, "p_value": 0.0}

    t_stat = sr / se_annual
    p_value = float(2 * (1 - stats.norm.cdf(abs(t_stat))))

    return {
        "sharpe": sr,
        "se": se_annual,
        "t_stat": t_stat,
        "p_value": p_value,
        "skewness": skew,
        "excess_kurtosis": kurt,
        "n_obs": n,
    }


def probabilistic_sharpe_ratio(
    returns: np.ndarray,
    benchmark_sr: float = 0.0,
    annualization: int = 252,
) -> dict:
    """Probabilistic Sharpe Ratio (PSR) — Bailey & de Prado (2012).

    Probability that the true Sharpe ratio exceeds ``benchmark_sr``,
    given sample size, skewness, and kurtosis.

    PSR = Φ((SR* - SR_benchmark) / SE(SR*))

    Args:
        returns: 1-D array of daily returns.
        benchmark_sr: Benchmark Sharpe to beat (default 0).
        annualization: Annualization factor.

    Returns:
        Dict with psr (probability), sharpe, benchmark_sr, se.
    """
    result = sharpe_pvalue(returns, annualization)
    sr = result["sharpe"]
    se = result["se"]

    if se < 1e-12:
        psr = 1.0 if sr > benchmark_sr else 0.0
    else:
        psr = float(stats.norm.cdf((sr - benchmark_sr) / se))

    return {
        "psr": psr,
        "sharpe": sr,
        "benchmark_sr": benchmark_sr,
        "se": se,
        "n_obs": result["n_obs"],
    }


def deflated_sharpe_ratio(
    returns: np.ndarray,
    n_trials: int,
    annualization: int = 252,
) -> dict:
    """Deflated Sharpe Ratio (DSR) — Bailey & de Prado (2014).

    Adjusts the Sharpe ratio for the number of independent trials
    (strategy configurations) tested, accounting for the expected
    maximum Sharpe under the null.

    DSR = PSR(SR*, SR_expected_max)

    where SR_expected_max ≈ sqrt(V[SR]) × ((1 - γ) × Φ⁻¹(1 - 1/N) + γ × Φ⁻¹(1 - 1/(N·e)))
    and γ ≈ 0.5772 (Euler-Mascheroni constant).

    Args:
        returns: 1-D array of daily returns of the *best* strategy.
        n_trials: Number of strategy configurations tested
            (e.g., 16 for 4 methods × 4 frequencies).
        annualization: Annualization factor.

    Returns:
        Dict with dsr, sharpe, expected_max_sr, n_trials, se.
    """
    if n_trials < 1:
        raise ValueError(f"n_trials must be >= 1, got {n_trials}")

    result = sharpe_pvalue(returns, annualization)
    sr = result["sharpe"]
    se = result["se"]

    # Expected maximum Sharpe under the null (all trials have SR=0)
    # Using the approximation from Bailey & de Prado (2014)
    gamma = 0.5772156649  # Euler-Mascheroni constant

    if n_trials == 1:
        expected_max_sr = 0.0
    else:
        z1 = stats.norm.ppf(1 - 1 / n_trials) if n_trials > 1 else 0
        z2 = stats.norm.ppf(1 - 1 / (n_trials * np.e)) if n_trials > 1 else 0
        expected_max_sr = se * ((1 - gamma) * z1 + gamma * z2)

    # DSR = probability that true SR exceeds the expected max
    if se < 1e-12:
        dsr = 1.0 if sr > expected_max_sr else 0.0
    else:
        dsr = float(stats.norm.cdf((sr - expected_max_sr) / se))

    return {
        "dsr": dsr,
        "sharpe": sr,
        "expected_max_sr": expected_max_sr,
        "n_trials": n_trials,
        "se": se,
        "p_value_unadjusted": result["p_value"],
    }
