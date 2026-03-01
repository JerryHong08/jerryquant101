"""
Risk Metrics — VaR, CVaR, drawdown, and distribution statistics.

Core risk measurement functions that answer: "how bad can it get?"

Three categories:
    1. Tail risk: VaR (historical + parametric), CVaR (Expected Shortfall)
    2. Drawdown: Maximum drawdown, drawdown series
    3. Distribution: Skewness, excess kurtosis, tail ratio

All functions operate on a 1-D numpy array of returns (simple or log).

Usage:
    from risk.risk_metrics import var_historical, cvar_historical, max_drawdown

    returns = portfolio_returns.to_numpy()
    var_95 = var_historical(returns, confidence=0.95)
    cvar_95 = cvar_historical(returns, confidence=0.95)
    mdd = max_drawdown(returns)

Reference: docs/quant_lab.tex — Part IV, Chapter 13 (Risk Measures)
"""

from typing import Optional

import numpy as np

# ── Tail Risk ─────────────────────────────────────────────────────────────────


def var_historical(returns: np.ndarray, confidence: float = 0.95) -> float:
    """
    Historical Value at Risk.

    The empirical quantile of the return distribution — no distributional
    assumption required.  This is the most common VaR method in practice
    because it naturally captures fat tails and skewness.

    Args:
        returns: Array of period returns (e.g. daily simple returns).
        confidence: Confidence level (0.95 = 95% VaR).

    Returns:
        VaR as a positive number (loss magnitude).
        Example: 0.025 means "5% chance of losing more than 2.5%."

    Note:
        Convention: VaR is returned as a positive number representing loss.
        A return of -3% at the 5th percentile → VaR = 0.03.
    """
    returns = _validate_returns(returns)
    quantile = np.percentile(returns, (1 - confidence) * 100)
    return -quantile  # Flip sign: loss is positive


def var_parametric(
    returns: np.ndarray,
    confidence: float = 0.95,
    mean: Optional[float] = None,
    std: Optional[float] = None,
) -> float:
    """
    Parametric (Gaussian) Value at Risk.

    Assumes returns are normally distributed.  Fast and analytically tractable,
    but **underestimates tail risk** for equity returns which have fat tails
    and negative skew.

    Args:
        returns: Array of period returns (used to estimate mean/std if not given).
        confidence: Confidence level.
        mean: Override for the mean return (optional).
        std: Override for the return volatility (optional).

    Returns:
        VaR as a positive number (loss magnitude).

    Warning:
        Gaussian VaR underestimates tail risk.  Always compare with historical
        VaR to measure the gap.  The ratio historical/parametric VaR > 1
        indicates fat tails.
    """
    from scipy.stats import norm

    returns = _validate_returns(returns)
    mu = mean if mean is not None else np.mean(returns)
    sigma = std if std is not None else np.std(returns, ddof=1)

    z = norm.ppf(1 - confidence)  # Negative z for left tail
    return -(mu + z * sigma)  # Flip sign: loss is positive


def cvar_historical(returns: np.ndarray, confidence: float = 0.95) -> float:
    """
    Historical Conditional Value at Risk (Expected Shortfall).

    The expected loss *given that* the loss exceeds VaR.  CVaR is a
    **coherent risk measure** — unlike VaR, it satisfies subadditivity:
        CVaR(A + B) ≤ CVaR(A) + CVaR(B)

    This means diversification always reduces CVaR, which is not guaranteed
    for VaR.  This is a key interview point.

    Args:
        returns: Array of period returns.
        confidence: Confidence level (0.95 = look at worst 5% of days).

    Returns:
        CVaR as a positive number (expected loss in the tail).
    """
    returns = _validate_returns(returns)
    cutoff = np.percentile(returns, (1 - confidence) * 100)
    tail_returns = returns[returns <= cutoff]
    if len(tail_returns) == 0:
        return var_historical(returns, confidence)
    return -np.mean(tail_returns)


def cvar_parametric(
    returns: np.ndarray,
    confidence: float = 0.95,
    mean: Optional[float] = None,
    std: Optional[float] = None,
) -> float:
    """
    Parametric (Gaussian) CVaR (Expected Shortfall).

    Under the Gaussian assumption the conditional expected return
    in the left tail is:

        E[R | R ≤ q_α] = μ − σ · φ(z_α) / (1 − α)

    where φ is the standard normal PDF and z_α = Φ⁻¹(1 − α).
    CVaR (a positive loss magnitude) is the negation of that quantity.

    Args:
        returns: Array of period returns.
        confidence: Confidence level.
        mean: Override for the mean return (optional).
        std: Override for the return volatility (optional).

    Returns:
        CVaR as a positive number.
    """
    from scipy.stats import norm

    returns = _validate_returns(returns)
    mu = mean if mean is not None else np.mean(returns)
    sigma = std if std is not None else np.std(returns, ddof=1)

    z = norm.ppf(1 - confidence)
    # E[R | R ≤ q_α] = μ − σ·φ(z)/Φ(z),  Φ(z) = 1−α
    conditional_mean = mu - sigma * norm.pdf(z) / (1 - confidence)
    return -conditional_mean


# ── Drawdown ──────────────────────────────────────────────────────────────────


def drawdown_series(returns: np.ndarray) -> np.ndarray:
    """
    Compute the drawdown series from a return series.

    Drawdown at time t = (cumulative wealth at t) / (peak wealth up to t) - 1.
    Always ≤ 0.

    Args:
        returns: Array of period returns.

    Returns:
        Array of drawdowns (same length as returns). Values are ≤ 0.
    """
    returns = _validate_returns(returns)
    cumulative = np.cumprod(1 + returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = cumulative / running_max - 1
    return drawdowns


def max_drawdown(returns: np.ndarray) -> float:
    """
    Maximum drawdown — the largest peak-to-trough decline.

    Args:
        returns: Array of period returns.

    Returns:
        Maximum drawdown as a positive number.
        Example: 0.15 means the strategy lost 15% from peak to trough.
    """
    dd = drawdown_series(returns)
    return -np.min(dd)


# ── Distribution Statistics ───────────────────────────────────────────────────


def return_skewness(returns: np.ndarray) -> float:
    """
    Sample skewness of returns.

    Equity returns typically have **negative skewness** — large losses are more
    common than large gains.  This is one reason Gaussian VaR underestimates
    tail risk.

    Args:
        returns: Array of period returns.

    Returns:
        Skewness (negative = left tail heavier).
    """
    from scipy.stats import skew

    returns = _validate_returns(returns)
    return float(skew(returns, bias=False))


def return_kurtosis(returns: np.ndarray) -> float:
    """
    Excess kurtosis of returns.

    Excess kurtosis = kurtosis − 3 (so Gaussian = 0).
    Equity returns typically have **positive excess kurtosis** (fat tails).

    κ > 0 means more extreme events than a Gaussian predicts.  This is the
    other reason Gaussian VaR underestimates tail risk.

    Args:
        returns: Array of period returns.

    Returns:
        Excess kurtosis (positive = fat tails).
    """
    from scipy.stats import kurtosis

    returns = _validate_returns(returns)
    return float(kurtosis(returns, bias=False, fisher=True))


def tail_ratio(returns: np.ndarray, percentile: float = 5.0) -> float:
    """
    Tail ratio: right tail / left tail.

    Measures asymmetry of extreme events:
        tail_ratio = |percentile(returns, 100 - p)| / |percentile(returns, p)|

    - > 1.0: Right tail (gains) fatter than left tail (losses). Good.
    - < 1.0: Left tail (losses) fatter than right tail (gains). Bad.
    - = 1.0: Symmetric tails.

    Args:
        returns: Array of period returns.
        percentile: The tail percentile to compare (default 5%).

    Returns:
        Tail ratio (right/left).
    """
    returns = _validate_returns(returns)
    right = np.abs(np.percentile(returns, 100 - percentile))
    left = np.abs(np.percentile(returns, percentile))
    if left < 1e-10:
        return np.inf
    return right / left


# ── Summary ───────────────────────────────────────────────────────────────────


def risk_summary(
    returns: np.ndarray,
    confidence: float = 0.95,
    annualization_factor: int = 252,
) -> dict:
    """
    Comprehensive risk summary for a return series.

    Computes all key risk metrics in one call.

    Args:
        returns: Array of daily returns.
        confidence: Confidence level for VaR/CVaR.
        annualization_factor: Trading days per year (252 for daily).

    Returns:
        Dictionary with keys:
            - mean_return: Annualized mean return
            - volatility: Annualized volatility
            - sharpe: Annualized Sharpe ratio (assuming rf=0)
            - skewness: Sample skewness
            - excess_kurtosis: Excess kurtosis
            - var_historical: Historical VaR at given confidence
            - var_parametric: Parametric VaR at given confidence
            - cvar_historical: Historical CVaR at given confidence
            - cvar_parametric: Parametric CVaR at given confidence
            - var_ratio: Historical VaR / Parametric VaR (>1 = fat tails)
            - max_drawdown: Maximum drawdown
            - tail_ratio: Right tail / left tail at 5%
            - n_observations: Number of return observations
    """
    returns = _validate_returns(returns)
    n = len(returns)
    mu = np.mean(returns)
    sigma = np.std(returns, ddof=1)

    ann_mu = mu * annualization_factor
    ann_sigma = sigma * np.sqrt(annualization_factor)
    sharpe = ann_mu / ann_sigma if ann_sigma > 1e-10 else 0.0

    h_var = var_historical(returns, confidence)
    p_var = var_parametric(returns, confidence)
    h_cvar = cvar_historical(returns, confidence)
    p_cvar = cvar_parametric(returns, confidence)

    var_r = h_var / p_var if p_var > 1e-10 else np.inf

    return {
        "mean_return": ann_mu,
        "volatility": ann_sigma,
        "sharpe": sharpe,
        "skewness": return_skewness(returns),
        "excess_kurtosis": return_kurtosis(returns),
        "var_historical": h_var,
        "var_parametric": p_var,
        "cvar_historical": h_cvar,
        "cvar_parametric": p_cvar,
        "var_ratio": var_r,
        "max_drawdown": max_drawdown(returns),
        "tail_ratio": tail_ratio(returns),
        "n_observations": n,
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
