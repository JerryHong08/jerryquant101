"""
Return Distribution Analysis — Normality tests, tail analysis, QQ-plot data.

This module answers: "are these returns Gaussian?"  The answer is almost always
"no" for equity returns, but *how* they deviate (fat tails, skewness) determines
which risk models are appropriate.

Key functions:
    - normality_tests: Jarque-Bera + Shapiro-Wilk in one call
    - qq_data: Generate QQ-plot coordinates (theoretical vs empirical quantiles)
    - gaussian_comparison: Histogram bin data + fitted Gaussian for overlay plots
    - distribution_summary: All-in-one distribution diagnostic

Usage:
    from risk.return_analysis import normality_tests, distribution_summary

    results = normality_tests(returns)
    print(f"Jarque-Bera p-value: {results['jarque_bera_pvalue']:.4f}")

    summary = distribution_summary(returns)

Reference: guidance/quant_lab.pdf — Part IV, Chapter 13 (Risk Measures),
           specifically the lab module on return distributions.
"""

from typing import Dict, Optional, Tuple

import numpy as np

# ── Normality Tests ───────────────────────────────────────────────────────────


def normality_tests(returns: np.ndarray) -> Dict[str, float]:
    """
    Run standard normality tests on a return series.

    Two complementary tests:
        1. **Jarque-Bera**: Tests whether skewness and kurtosis jointly match
           a Gaussian.  Good for detecting fat tails.  Works well for large
           samples (n > 100).
        2. **Shapiro-Wilk**: A more powerful omnibus test for normality.
           Better for smaller samples but capped at n=5000 by scipy.

    Args:
        returns: Array of period returns.

    Returns:
        Dictionary with:
            - jarque_bera_stat: JB test statistic
            - jarque_bera_pvalue: p-value (< 0.05 → reject Gaussian)
            - shapiro_stat: Shapiro-Wilk test statistic
            - shapiro_pvalue: p-value (< 0.05 → reject Gaussian)
            - is_normal_jb: Whether JB fails to reject at 5%
            - is_normal_sw: Whether Shapiro-Wilk fails to reject at 5%

    Note:
        For equity returns, both tests will almost always reject normality.
        The value is in understanding *how much* the distribution deviates,
        not whether it deviates.
    """
    from scipy.stats import jarque_bera, shapiro

    returns = _validate_returns(returns)

    jb_stat, jb_pval = jarque_bera(returns)

    # Shapiro-Wilk is limited to 5000 samples; subsample if needed
    if len(returns) > 5000:
        rng = np.random.default_rng(42)
        sw_sample = rng.choice(returns, size=5000, replace=False)
    else:
        sw_sample = returns

    sw_stat, sw_pval = shapiro(sw_sample)

    return {
        "jarque_bera_stat": float(jb_stat),
        "jarque_bera_pvalue": float(jb_pval),
        "shapiro_stat": float(sw_stat),
        "shapiro_pvalue": float(sw_pval),
        "is_normal_jb": jb_pval > 0.05,
        "is_normal_sw": sw_pval > 0.05,
    }


# ── QQ-Plot Data ──────────────────────────────────────────────────────────────


def qq_data(returns: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate QQ-plot data: theoretical quantiles vs sample quantiles.

    A QQ-plot compares the empirical distribution to a theoretical Gaussian.
    - Points on the diagonal → Gaussian.
    - S-curve → fat tails.
    - Curvature at one end → skewness.

    Args:
        returns: Array of period returns.

    Returns:
        Tuple of (theoretical_quantiles, sample_quantiles).
        Plot theoretical on x-axis, sample on y-axis.

    Example:
        theo, samp = qq_data(returns)
        plt.scatter(theo, samp, s=5, alpha=0.5)
        plt.plot([theo.min(), theo.max()], [theo.min(), theo.max()], 'r--')
    """
    from scipy.stats import norm

    returns = _validate_returns(returns)
    sorted_returns = np.sort(returns)
    n = len(sorted_returns)

    # Theoretical quantiles using the Hazen plotting position
    probabilities = (np.arange(1, n + 1) - 0.5) / n
    theoretical = norm.ppf(probabilities)

    # Standardize sample to same scale as theoretical
    mu, sigma = np.mean(returns), np.std(returns, ddof=1)
    if sigma > 1e-10:
        standardized = (sorted_returns - mu) / sigma
    else:
        standardized = sorted_returns - mu

    return theoretical, standardized


# ── Gaussian Comparison ───────────────────────────────────────────────────────


def gaussian_comparison(returns: np.ndarray, n_bins: int = 80) -> Dict[str, np.ndarray]:
    """
    Generate histogram + fitted Gaussian curve data for overlay plots.

    This powers the classic "return distribution vs Gaussian" chart that
    shows fat tails visually.

    Args:
        returns: Array of period returns.
        n_bins: Number of histogram bins.

    Returns:
        Dictionary with:
            - bin_edges: Histogram bin edges (n_bins + 1,)
            - bin_centers: Bin center points (n_bins,)
            - hist_density: Normalized histogram values (n_bins,)
            - gaussian_pdf: Fitted Gaussian PDF at bin centers (n_bins,)
            - mean: Sample mean
            - std: Sample std
    """
    from scipy.stats import norm

    returns = _validate_returns(returns)
    mu = np.mean(returns)
    sigma = np.std(returns, ddof=1)

    # Histogram (density-normalized)
    hist_vals, bin_edges = np.histogram(returns, bins=n_bins, density=True)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # Fitted Gaussian
    gaussian_pdf = norm.pdf(bin_centers, loc=mu, scale=sigma)

    return {
        "bin_edges": bin_edges,
        "bin_centers": bin_centers,
        "hist_density": hist_vals,
        "gaussian_pdf": gaussian_pdf,
        "mean": mu,
        "std": sigma,
    }


# ── Tail Analysis ─────────────────────────────────────────────────────────────


def tail_analysis(
    returns: np.ndarray,
    confidence_levels: Optional[list] = None,
) -> Dict[str, object]:
    """
    Comprehensive tail analysis at multiple confidence levels.

    Compares historical vs parametric VaR/CVaR at 90%, 95%, 99% to
    show how the Gaussian assumption breaks down in the tails.

    Args:
        returns: Array of period returns.
        confidence_levels: List of confidence levels (default: [0.90, 0.95, 0.99]).

    Returns:
        Dictionary with:
            - confidence_levels: The confidence levels tested
            - var_historical: List of historical VaR values
            - var_parametric: List of parametric VaR values
            - cvar_historical: List of historical CVaR values
            - cvar_parametric: List of parametric CVaR values
            - var_ratio: List of historical/parametric VaR ratios
            - cvar_ratio: List of historical/parametric CVaR ratios
    """
    from risk.risk_metrics import (
        cvar_historical,
        cvar_parametric,
        var_historical,
        var_parametric,
    )

    returns = _validate_returns(returns)

    if confidence_levels is None:
        confidence_levels = [0.90, 0.95, 0.99]

    result = {
        "confidence_levels": confidence_levels,
        "var_historical": [],
        "var_parametric": [],
        "cvar_historical": [],
        "cvar_parametric": [],
        "var_ratio": [],
        "cvar_ratio": [],
    }

    for cl in confidence_levels:
        h_var = var_historical(returns, cl)
        p_var = var_parametric(returns, cl)
        h_cvar = cvar_historical(returns, cl)
        p_cvar = cvar_parametric(returns, cl)

        result["var_historical"].append(h_var)
        result["var_parametric"].append(p_var)
        result["cvar_historical"].append(h_cvar)
        result["cvar_parametric"].append(p_cvar)
        result["var_ratio"].append(h_var / p_var if p_var > 1e-10 else np.inf)
        result["cvar_ratio"].append(h_cvar / p_cvar if p_cvar > 1e-10 else np.inf)

    return result


# ── Summary ───────────────────────────────────────────────────────────────────


def distribution_summary(returns: np.ndarray) -> Dict[str, object]:
    """
    All-in-one distribution diagnostic.

    Combines normality tests, distribution statistics, and tail analysis
    into a single report.

    Args:
        returns: Array of period returns.

    Returns:
        Dictionary with keys:
            - n: Number of observations
            - mean: Sample mean
            - std: Sample std
            - skewness: Sample skewness
            - excess_kurtosis: Excess kurtosis
            - normality: Dict from normality_tests()
            - tail: Dict from tail_analysis()
    """
    from risk.risk_metrics import return_kurtosis, return_skewness

    returns = _validate_returns(returns)

    return {
        "n": len(returns),
        "mean": float(np.mean(returns)),
        "std": float(np.std(returns, ddof=1)),
        "skewness": return_skewness(returns),
        "excess_kurtosis": return_kurtosis(returns),
        "normality": normality_tests(returns),
        "tail": tail_analysis(returns),
    }


# ── Internal ──────────────────────────────────────────────────────────────────


def _validate_returns(returns: np.ndarray) -> np.ndarray:
    """Validate and clean a return array."""
    returns = np.asarray(returns, dtype=np.float64).ravel()
    mask = np.isfinite(returns)
    if not mask.all():
        returns = returns[mask]
    if len(returns) < 2:
        raise ValueError(
            f"Need at least 2 valid return observations, got {len(returns)}"
        )
    return returns
