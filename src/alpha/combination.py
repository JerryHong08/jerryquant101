"""
Factor Combination — merge multiple alpha signals into a composite.

No single factor is strong enough.  Combination exploits diversification
across weak signals: if factors are imperfectly correlated, the composite
signal has higher IR than any individual factor.

Methods (ordered by complexity):
    1. Equal-weight — simplest, no estimation risk
    2. IC-weight — weight ∝ historical mean IC
    3. Mean-variance — maximize Sharpe on the IC covariance matrix
    4. Risk-parity — equal risk contribution, robust

Usage:
    from alpha.combination import combine_factors

    composite = combine_factors(
        factors=[factor_a, factor_b, factor_c],
        method="ic_weight",
        ic_series_list=[ic_a, ic_b, ic_c],  # required for ic_weight / mv
    )

Reference: guidance/quant_lab.pdf — Part III, Chapter 12 (Factor Combination)
"""

from typing import List, Optional

import numpy as np
import polars as pl


def combine_factors(
    factors: List[pl.DataFrame],
    method: str = "equal_weight",
    ic_series_list: Optional[List[pl.DataFrame]] = None,
    risk_aversion: float = 1.0,
    value_col: str = "value",
    date_col: str = "date",
    ticker_col: str = "ticker",
) -> pl.DataFrame:
    """
    Combine multiple factor signals into a single composite.

    Each factor DataFrame must have columns (date, ticker, value) and should
    be preprocessed (winsorized, z-scored / rank-normalized).

    Args:
        factors: List of factor DataFrames, each with (date, ticker, value).
        method: Combination method.  One of:
            - "equal_weight": w_k = 1/K
            - "ic_weight": w_k ∝ mean(IC_k)
            - "mean_variance": mean-variance optimization on IC covariance
            - "risk_parity": w_k ∝ 1/std(IC_k)
        ic_series_list: List of IC DataFrames (date, ic), one per factor.
                        Required for all methods except "equal_weight".
        risk_aversion: Lambda parameter for mean-variance optimization.
        value_col: Signal column name in factor DataFrames.
        date_col: Date column name.
        ticker_col: Ticker column name.

    Returns:
        Composite signal DataFrame with columns (date, ticker, value).
    """
    k = len(factors)
    if k == 0:
        raise ValueError("At least one factor required.")
    if k == 1:
        return factors[0]

    # Compute weights based on method
    weights = _compute_weights(
        method=method,
        k=k,
        ic_series_list=ic_series_list,
        risk_aversion=risk_aversion,
    )

    # Rename value columns to avoid collision
    renamed = []
    for i, f in enumerate(factors):
        renamed.append(
            f.select(
                [
                    pl.col(date_col),
                    pl.col(ticker_col),
                    pl.col(value_col).alias(f"factor_{i}"),
                ]
            )
        )

    # Join all factors on (date, ticker)
    merged = renamed[0]
    for i in range(1, k):
        merged = merged.join(renamed[i], on=[date_col, ticker_col], how="inner")

    # Weighted sum: composite = sum(w_k * factor_k)
    factor_cols = [f"factor_{i}" for i in range(k)]
    composite_expr = pl.lit(0.0)
    for i, col_name in enumerate(factor_cols):
        composite_expr = composite_expr + pl.col(col_name) * weights[i]

    result = merged.select(
        [
            pl.col(date_col),
            pl.col(ticker_col),
            composite_expr.alias(value_col),
        ]
    )

    return result


def _compute_weights(
    method: str,
    k: int,
    ic_series_list: Optional[List[pl.DataFrame]],
    risk_aversion: float,
) -> List[float]:
    """
    Compute combination weights from the IC time series.

    Returns a list of k float weights (normalized to sum to 1).
    """
    if method == "equal_weight":
        return [1.0 / k] * k

    # All other methods require IC series
    if ic_series_list is None or len(ic_series_list) != k:
        raise ValueError(
            f"Method '{method}' requires ic_series_list with {k} DataFrames."
        )

    # Extract IC arrays
    ic_arrays = []
    for ic_df in ic_series_list:
        arr = ic_df["ic"].drop_nulls().to_numpy()
        ic_arrays.append(arr)

    if method == "ic_weight":
        return _ic_weight(ic_arrays)
    elif method == "mean_variance":
        return _mean_variance_weight(ic_arrays, risk_aversion)
    elif method == "risk_parity":
        return _risk_parity_weight(ic_arrays)
    else:
        raise ValueError(
            f"Unknown method '{method}'. "
            "Use 'equal_weight', 'ic_weight', 'mean_variance', or 'risk_parity'."
        )


def _ic_weight(ic_arrays: List[np.ndarray]) -> List[float]:
    """
    Weight proportional to mean IC.

    w_k ∝ max(mean(IC_k), 0) — negative IC factors get zero weight.
    """
    means = [max(float(np.mean(arr)), 0.0) for arr in ic_arrays]
    total = sum(means)
    if total == 0:
        # Fallback to equal weight if all ICs are non-positive
        k = len(means)
        return [1.0 / k] * k
    return [m / total for m in means]


def _mean_variance_weight(
    ic_arrays: List[np.ndarray], risk_aversion: float
) -> List[float]:
    """
    Mean-variance optimization on IC covariance.

    w* = argmax_w (w^T mu - lambda/2 * w^T Sigma w)
    Closed-form: w* = (1/lambda) * Sigma^{-1} * mu

    Constrained to long-only (w_k >= 0), normalized to sum to 1.
    """
    k = len(ic_arrays)

    # Align IC series to same length (trim to shortest)
    min_len = min(len(arr) for arr in ic_arrays)
    aligned = np.column_stack([arr[:min_len] for arr in ic_arrays])

    mu = np.mean(aligned, axis=0)
    sigma = np.cov(aligned, rowvar=False)

    # Regularize: add ridge to diagonal for numerical stability
    sigma += np.eye(k) * 1e-6

    try:
        sigma_inv = np.linalg.inv(sigma)
        raw_weights = (1.0 / risk_aversion) * sigma_inv @ mu
    except np.linalg.LinAlgError:
        # Fallback to equal weight if singular
        return [1.0 / k] * k

    # Long-only constraint: clip negative weights
    raw_weights = np.maximum(raw_weights, 0.0)

    total = np.sum(raw_weights)
    if total == 0:
        return [1.0 / k] * k

    return (raw_weights / total).tolist()


def _risk_parity_weight(ic_arrays: List[np.ndarray]) -> List[float]:
    """
    Risk parity: weight inversely proportional to IC volatility.

    w_k ∝ 1 / std(IC_k)

    Does not require estimating expected IC — only volatility.
    More robust than mean-variance when IC estimates are noisy.
    """
    stds = [float(np.std(arr, ddof=1)) for arr in ic_arrays]

    # Avoid division by zero
    inv_stds = [1.0 / s if s > 1e-10 else 0.0 for s in stds]
    total = sum(inv_stds)

    if total == 0:
        k = len(stds)
        return [1.0 / k] * k

    return [w / total for w in inv_stds]
