"""
Portfolio Pipeline — end-to-end factor → weights → returns.

Extracts the ~80-line boilerplate that was duplicated across
cost_analysis.ipynb, validation.ipynb, risk_analysis.ipynb, and
alpha_iteration.ipynb into reusable, tested functions.

Pipeline stages:
    1. ``compute_daily_returns()``  — OHLCV → daily return series
    2. ``compute_next_day_returns()`` — daily returns → next-day (tradable) returns
    3. ``build_factor_pipeline()`` — OHLCV → preprocessed composite factor
    4. ``build_sizing_methods()`` — composite factor → 4 sizing method weights
    5. ``resample_weights()`` — reduce rebalancing frequency
    6. ``compute_portfolio_return()`` — weights × returns → portfolio return series
    7. ``run_alpha_pipeline()`` — all-in-one: OHLCV → dict of portfolio returns

Usage:
    from portfolio.pipeline import run_alpha_pipeline

    results = run_alpha_pipeline(
        ohlcv,
        factor_names=["bbiboll", "vol_ratio"],
        sizing_method="Half-Kelly",
        rebal_every_n=5,
    )
    portfolio_returns = results["portfolio_returns"]
    sharpe = results["sharpe"]

Reference: docs/quant_lab.tex — Part III–IV
"""

from __future__ import annotations

from typing import Callable, Dict, List, Literal, Optional

import numpy as np
import polars as pl

from alpha.combination import combine_factors
from alpha.preprocessing import preprocess_factor
from constants import (
    DATE_COL,
    OHLCV_DATE_COL,
    RETURN_COL,
    TICKER_COL,
    TRADING_DAYS_PER_YEAR,
    VALUE_COL,
    WEIGHT_COL,
)

# ── Stage 1: Daily Returns ────────────────────────────────────────────────────


def compute_daily_returns(
    ohlcv: pl.DataFrame,
    price_col: str = "close",
    ohlcv_date_col: str = OHLCV_DATE_COL,
    ticker_col: str = TICKER_COL,
) -> pl.DataFrame:
    """Compute daily simple returns from OHLCV data.

    Args:
        ohlcv: DataFrame with at least (date, ticker, close) columns.
        price_col: Column name for the closing price.
        ohlcv_date_col: Date column in the OHLCV data (defaults to "timestamps").
        ticker_col: Ticker column name.

    Returns:
        DataFrame with columns (date, ticker, daily_return).
        The date column is renamed to the standard ``DATE_COL`` convention.
    """
    return (
        ohlcv.sort([ticker_col, ohlcv_date_col])
        .with_columns(
            (pl.col(price_col) / pl.col(price_col).shift(1).over(ticker_col) - 1).alias(
                RETURN_COL
            )
        )
        .filter(pl.col(RETURN_COL).is_not_null() & pl.col(RETURN_COL).is_finite())
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(ticker_col),
                pl.col(RETURN_COL),
            ]
        )
    )


# ── Stage 2: Next-Day Returns ────────────────────────────────────────────────


def compute_next_day_returns(
    daily_returns: pl.DataFrame,
    date_col: str = DATE_COL,
    ticker_col: str = TICKER_COL,
    return_col: str = RETURN_COL,
) -> pl.DataFrame:
    """Shift returns by -1 to get the *next-day* return for each signal date.

    This is the tradable return: today's signal → tomorrow's return.

    Args:
        daily_returns: Output of ``compute_daily_returns()``.

    Returns:
        DataFrame with columns (date, ticker, next_day_return).
    """
    return (
        daily_returns.sort([ticker_col, date_col])
        .with_columns(
            pl.col(return_col).shift(-1).over(ticker_col).alias("next_day_return")
        )
        .filter(pl.col("next_day_return").is_not_null())
        .select([date_col, ticker_col, "next_day_return"])
    )


# ── Stage 3: Factor Pipeline ─────────────────────────────────────────────────


def _compute_bbiboll_factor(
    ohlcv: pl.DataFrame,
    ohlcv_date_col: str = OHLCV_DATE_COL,
) -> pl.DataFrame:
    """Extract BBIBOLL deviation factor from OHLCV data.

    Computes (close - BBI) / deviation as the raw signal, then
    winsorizes and z-scores cross-sectionally.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    from strategy.indicators.registry import get_indicator

    bbiboll_fn = get_indicator("bbiboll")
    ohlcv_bb = bbiboll_fn(ohlcv)

    raw = (
        ohlcv_bb.with_columns(
            ((pl.col("close") - pl.col("bbi")) / pl.col("dev")).alias(VALUE_COL)
        )
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(raw, winsorize_pct=0.01, method="zscore", neutralize=[])


def _compute_vol_ratio_factor(
    ohlcv: pl.DataFrame,
    short_window: int = 5,
    long_window: int = 20,
    ohlcv_date_col: str = OHLCV_DATE_COL,
) -> pl.DataFrame:
    """Compute volatility ratio factor: vol_5d / vol_20d.

    High ratio = recent vol spike relative to trend → contrarian signal.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    raw = (
        ohlcv.sort([TICKER_COL, ohlcv_date_col])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over(TICKER_COL))
            .log()
            .alias("log_ret")
        )
        .with_columns(
            [
                pl.col("log_ret")
                .rolling_std(window_size=short_window)
                .over(TICKER_COL)
                .alias("vol_short"),
                pl.col("log_ret")
                .rolling_std(window_size=long_window)
                .over(TICKER_COL)
                .alias("vol_long"),
            ]
        )
        .with_columns((pl.col("vol_short") / pl.col("vol_long")).alias(VALUE_COL))
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(raw, winsorize_pct=0.01, method="zscore", neutralize=[])


def _compute_momentum_factor(
    ohlcv: pl.DataFrame,
    lookback: int = 20,
    ohlcv_date_col: str = OHLCV_DATE_COL,
) -> pl.DataFrame:
    """Compute momentum factor: lookback-day log return.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    raw = (
        ohlcv.sort([TICKER_COL, ohlcv_date_col])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(lookback).over(TICKER_COL))
            .log()
            .alias(VALUE_COL)
        )
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(raw, winsorize_pct=0.01, method="zscore", neutralize=[])


# Factor registry: name → compute function
_FACTOR_REGISTRY: dict[str, Callable[..., pl.DataFrame]] = {
    "bbiboll": _compute_bbiboll_factor,
    "vol_ratio": _compute_vol_ratio_factor,
    "momentum": _compute_momentum_factor,
}


def register_factor(name: str, fn: Callable[..., pl.DataFrame]) -> None:
    """Register a custom factor computation function.

    Args:
        name: Factor name (lowercase).
        fn: Callable that takes ``(ohlcv, **kwargs)`` and returns a
            preprocessed factor DataFrame ``(date, ticker, value)``.
    """
    _FACTOR_REGISTRY[name.lower()] = fn


def list_factors() -> list[str]:
    """Return names of all registered factors."""
    return sorted(_FACTOR_REGISTRY.keys())


def build_factor_pipeline(
    ohlcv: pl.DataFrame,
    factor_names: list[str] | None = None,
    combination_method: str = "equal_weight",
    **kwargs,
) -> pl.DataFrame:
    """Build a composite factor signal from OHLCV data.

    Args:
        ohlcv: Raw OHLCV DataFrame (must have timestamps, ticker, close).
        factor_names: Names of factors to compute and combine.
            Default: ``["bbiboll", "vol_ratio"]``.
        combination_method: Method for ``combine_factors()`` —
            "equal_weight", "ic_weight", "mean_variance", "risk_parity".

    Returns:
        Composite factor DataFrame with columns (date, ticker, value).
    """
    if factor_names is None:
        factor_names = ["bbiboll", "vol_ratio"]

    factors = []
    for name in factor_names:
        key = name.lower()
        if key not in _FACTOR_REGISTRY:
            available = ", ".join(sorted(_FACTOR_REGISTRY.keys()))
            raise KeyError(
                f"Unknown factor '{name}'. Available: {available}. "
                f"Use register_factor() to add custom factors."
            )
        factor_fn = _FACTOR_REGISTRY[key]
        factors.append(factor_fn(ohlcv, **kwargs))

    if len(factors) == 1:
        return factors[0]

    return combine_factors(factors=factors, method=combination_method)


# ── Stage 4: Portfolio Weights ────────────────────────────────────────────────


def build_sizing_methods(
    composite: pl.DataFrame,
    ohlcv: pl.DataFrame,
    daily_returns: pl.DataFrame,
    n_long: int = 10,
    n_short: int = 10,
    target_vol: float = 0.10,
    kelly_lookback: int = 60,
    kelly_max_position: float = 0.10,
    vol_window: int = 20,
) -> dict[str, pl.DataFrame]:
    """Build all 4 sizing method weight DataFrames.

    Args:
        composite: Factor signal DataFrame (date, ticker, value).
        ohlcv: Raw OHLCV data (for volatility estimation).
        daily_returns: Output of ``compute_daily_returns()``.
        n_long: Number of long positions per method.
        n_short: Number of short positions per method.
        target_vol: Target annualized volatility for vol-target method.
        kelly_lookback: Look-back window for Half-Kelly.
        kelly_max_position: Max absolute position size for Half-Kelly.
        vol_window: Window for realized volatility estimation.

    Returns:
        Dict mapping method name → weight DataFrame (date, ticker, weight).
    """
    from risk.position_sizing import (
        compute_realized_volatility,
        size_equal_weight,
        size_half_kelly,
        size_inverse_volatility,
        size_volatility_target,
    )

    vol_estimates = compute_realized_volatility(ohlcv, window=vol_window)
    returns_for_kelly = daily_returns.rename({RETURN_COL: "return"})

    return {
        "Equal-Weight": size_equal_weight(composite, n_long=n_long, n_short=n_short),
        "Inverse-Vol": size_inverse_volatility(
            composite, vol_estimates, n_long=n_long, n_short=n_short
        ),
        f"Vol-Target ({target_vol:.0%})": size_volatility_target(
            composite,
            vol_estimates,
            target_vol=target_vol,
            n_long=n_long,
            n_short=n_short,
        ),
        "Half-Kelly": size_half_kelly(
            composite,
            returns_for_kelly,
            lookback=kelly_lookback,
            max_position=kelly_max_position,
        ),
    }


# ── Stage 5: Rebalancing ─────────────────────────────────────────────────────


def resample_weights(
    weights: pl.DataFrame,
    rebal_every_n: int = 5,
    date_col: str = DATE_COL,
    ticker_col: str = TICKER_COL,
    weight_col: str = WEIGHT_COL,
) -> pl.DataFrame:
    """Forward-fill weights to reduce rebalancing frequency.

    Only rebalance every ``rebal_every_n`` trading days. On non-rebalance
    days, carry forward the most recent weights.

    Args:
        weights: DataFrame with columns (date, ticker, weight).
        rebal_every_n: Rebalance every N trading days (1 = daily, 5 = weekly).

    Returns:
        Weight DataFrame with the same schema but fewer distinct
        weight-change dates.
    """
    if rebal_every_n <= 1:
        return weights  # daily = no resampling needed

    # Get unique sorted dates
    unique_dates = (
        weights.select(date_col).unique().sort(date_col).with_row_index("idx")
    )

    # Mark rebalancing dates (every N-th)
    rebal_dates = unique_dates.filter(pl.col("idx") % rebal_every_n == 0)

    # Get weights only on rebalancing dates
    rebal_weights = weights.join(rebal_dates.select(date_col), on=date_col, how="inner")

    # For each non-rebalance date, map it to the most recent rebalance date
    rebal_date_list = rebal_dates[date_col].to_list()
    all_dates = unique_dates[date_col].to_list()

    # Build mapping: each date → most recent rebalance date at or before it
    date_mapping = []
    rebal_idx = 0
    for d in all_dates:
        # Advance rebal_idx if next rebalance date is at or before d
        while (
            rebal_idx + 1 < len(rebal_date_list) and rebal_date_list[rebal_idx + 1] <= d
        ):
            rebal_idx += 1
        if rebal_idx < len(rebal_date_list) and rebal_date_list[rebal_idx] <= d:
            date_mapping.append(
                {date_col: d, "_rebal_date": rebal_date_list[rebal_idx]}
            )

    if not date_mapping:
        return weights

    mapping_df = pl.DataFrame(date_mapping)

    # Join: for each (date, _rebal_date), get the weights from the rebalance date
    result = (
        mapping_df.join(
            rebal_weights.rename({date_col: "_rebal_date"}),
            on="_rebal_date",
            how="inner",
        )
        .drop("_rebal_date")
        .select([date_col, ticker_col, weight_col])
    )

    return result


# ── Stage 6: Portfolio Returns ────────────────────────────────────────────────


def compute_portfolio_return(
    weights_df: pl.DataFrame,
    returns_df: pl.DataFrame,
    ret_col: str = "next_day_return",
    date_col: str = DATE_COL,
    ticker_col: str = TICKER_COL,
    weight_col: str = WEIGHT_COL,
) -> pl.DataFrame:
    """Compute weighted portfolio returns from weights and per-stock returns.

    Args:
        weights_df: DataFrame with columns (date, ticker, weight).
        returns_df: DataFrame with columns (date, ticker, <ret_col>).
        ret_col: Name of the return column in returns_df.

    Returns:
        DataFrame with columns (date, port_return), sorted by date.
    """
    return (
        weights_df.join(returns_df, on=[date_col, ticker_col], how="inner")
        .with_columns((pl.col(weight_col) * pl.col(ret_col)).alias("weighted_return"))
        .group_by(date_col)
        .agg(pl.col("weighted_return").sum().alias("port_return"))
        .sort(date_col)
    )


# ── Stage 7: All-in-One Pipeline ─────────────────────────────────────────────


def run_alpha_pipeline(
    ohlcv: pl.DataFrame,
    factor_names: list[str] | None = None,
    sizing_method: str = "Half-Kelly",
    combination_method: str = "equal_weight",
    rebal_every_n: int = 5,
    n_long: int = 10,
    n_short: int = 10,
    target_vol: float = 0.10,
    annualization: int = TRADING_DAYS_PER_YEAR,
) -> dict:
    """End-to-end pipeline: OHLCV → portfolio returns + metrics.

    This is the single function that replaces the 80-line boilerplate
    duplicated across cost_analysis.ipynb, validation.ipynb, and
    risk_analysis.ipynb.

    Args:
        ohlcv: Raw OHLCV DataFrame (must have timestamps, ticker, close).
        factor_names: Factors to compute. Default: ["bbiboll", "vol_ratio"].
        sizing_method: Which sizing to use. One of:
            "Equal-Weight", "Inverse-Vol", "Vol-Target (10%)", "Half-Kelly".
        combination_method: Factor combination method.
        rebal_every_n: Rebalance every N days (1=daily, 5=weekly).
        n_long: Number of long positions.
        n_short: Number of short positions.
        target_vol: Target annual vol for vol-target sizing.
        annualization: Trading days per year.

    Returns:
        Dict with keys:
            - composite: Composite factor DataFrame
            - daily_returns: Daily return DataFrame
            - next_day_returns: Next-day return DataFrame
            - sizing_methods: Dict of all 4 sizing method weights
            - weights: Selected weight DataFrame (after resampling)
            - portfolio_returns: Portfolio return DataFrame (date, port_return)
            - returns_array: 1-D numpy array of portfolio returns
            - sharpe: Annualized Sharpe ratio
            - annual_return: Annualized mean return
            - annual_vol: Annualized volatility
            - n_days: Number of trading days
    """
    # --- Stage 1-2: Returns ---
    daily_returns = compute_daily_returns(ohlcv)
    next_day_returns = compute_next_day_returns(daily_returns)

    # --- Stage 3: Factor ---
    composite = build_factor_pipeline(
        ohlcv,
        factor_names=factor_names,
        combination_method=combination_method,
    )

    # --- Stage 4: Sizing ---
    sizing_methods = build_sizing_methods(
        composite,
        ohlcv,
        daily_returns,
        n_long=n_long,
        n_short=n_short,
        target_vol=target_vol,
    )

    if sizing_method not in sizing_methods:
        available = ", ".join(sorted(sizing_methods.keys()))
        raise KeyError(
            f"Unknown sizing method '{sizing_method}'. Available: {available}"
        )

    weights = sizing_methods[sizing_method]

    # --- Stage 5: Rebalance ---
    weights = resample_weights(weights, rebal_every_n=rebal_every_n)

    # --- Stage 6: Portfolio returns ---
    port_returns = compute_portfolio_return(weights, next_day_returns)
    returns_array = port_returns["port_return"].to_numpy()

    # --- Metrics ---
    mu = float(np.mean(returns_array))
    sigma = float(np.std(returns_array, ddof=1))
    sharpe = mu / sigma * np.sqrt(annualization) if sigma > 1e-10 else 0.0

    return {
        "composite": composite,
        "daily_returns": daily_returns,
        "next_day_returns": next_day_returns,
        "sizing_methods": sizing_methods,
        "weights": weights,
        "portfolio_returns": port_returns,
        "returns_array": returns_array,
        "sharpe": float(sharpe),
        "annual_return": float(mu * annualization),
        "annual_vol": float(sigma * np.sqrt(annualization)),
        "n_days": len(returns_array),
    }
