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

import numpy as np
import polars as pl

from alpha.combination import combine_factors
from constants import (
    DATE_COL,
    OHLCV_DATE_COL,
    RETURN_COL,
    TICKER_COL,
    TRADING_DAYS_PER_YEAR,
    VALUE_COL,
    WEIGHT_COL,
)
from portfolio.alpha_config import AlphaConfig, FactorConfig
from portfolio.factors import get_factor_fn, list_factors, register_factor

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
# Factor functions live in ``portfolio.factors``.
# ``register_factor`` and ``list_factors`` are re-exported for convenience.


def build_factor_pipeline(
    ohlcv: pl.DataFrame,
    factor_names: list[str] | None = None,
    combination_method: str = "equal_weight",
    config: AlphaConfig | None = None,
    next_day_returns: pl.DataFrame | None = None,
    **kwargs,
) -> pl.DataFrame:
    """Build a composite factor signal from OHLCV data.

    Args:
        ohlcv: Raw OHLCV DataFrame (must have timestamps, ticker, close).
        factor_names: Names of factors to compute and combine.
            Default: ``["bbiboll", "vol_ratio"]``.
        combination_method: Method for ``combine_factors()`` —
            "equal_weight", "ic_weight", "mean_variance", "risk_parity".
        config: ``AlphaConfig`` — if provided, ``factor_names`` and
            ``combination_method`` are taken from it, and per-factor
            preprocessing params are passed through.
        next_day_returns: Next-day returns DataFrame (date, ticker,
            next_day_return).  Required for IC-based combination methods.

    Returns:
        Composite factor DataFrame with columns (date, ticker, value).
    """
    if config is not None:
        factor_names = config.factor_names
        combination_method = config.combination_method

    if factor_names is None:
        factor_names = ["bbiboll", "vol_ratio"]

    factors = []
    for name in factor_names:
        factor_fn = get_factor_fn(name)
        fc = config.get_factor_config(name.lower()) if config else None
        factors.append(factor_fn(ohlcv, factor_config=fc, **kwargs))

    if len(factors) == 1:
        return factors[0]

    # ── IC-based combination: compute IC series per factor ──
    ic_series_list = None
    if combination_method != "equal_weight":
        ic_series_list = _compute_ic_series_list(factors, next_day_returns, config)

    risk_aversion = config.risk_aversion if config else 1.0

    return combine_factors(
        factors=factors,
        method=combination_method,
        ic_series_list=ic_series_list,
        risk_aversion=risk_aversion,
    )


def _compute_ic_series_list(
    factors: list[pl.DataFrame],
    next_day_returns: pl.DataFrame | None,
    config: AlphaConfig | None,
) -> list[pl.DataFrame]:
    """Compute IC time series for each factor (needed for non-equal-weight combination).

    IC_t = rank_corr(factor_t, next_day_return_t) across tickers.
    """
    if next_day_returns is None:
        raise ValueError(
            "IC-based combination methods (ic_weight, mean_variance, risk_parity) "
            "require next_day_returns. Pass it via run_alpha_pipeline() or "
            "build_factor_pipeline(next_day_returns=...)."
        )

    ic_series_list = []
    for factor_df in factors:
        # Join factor values with next-day returns on (date, ticker)
        merged = factor_df.join(
            next_day_returns,
            on=[DATE_COL, TICKER_COL],
            how="inner",
        )

        # Compute rank correlation per date
        ic_per_date = (
            merged.group_by(DATE_COL)
            .agg(
                pl.corr(
                    pl.col(VALUE_COL).rank(), pl.col("next_day_return").rank()
                ).alias("ic")
            )
            .sort(DATE_COL)
            .filter(pl.col("ic").is_not_null() & pl.col("ic").is_finite())
        )
        ic_series_list.append(ic_per_date)

    return ic_series_list


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
    config: AlphaConfig | None = None,
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

    # Pull params from config if provided, else use function defaults
    if config is not None:
        n_long = config.n_long
        n_short = config.n_short
        target_vol = config.target_vol
        kelly_lookback = config.kelly_lookback
        kelly_max_position = config.kelly_max_position
        vol_window = config.vol_window

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

    # Cast mapping columns to match the original date dtype (e.g. datetime[ns] vs [μs])
    orig_dtype = weights.schema[date_col]
    if mapping_df.schema[date_col] != orig_dtype:
        mapping_df = mapping_df.cast({date_col: orig_dtype, "_rebal_date": orig_dtype})

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
    config: AlphaConfig | None = None,
    *,
    # ── Legacy kwargs (used if config is None) ──
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

    Accepts either an ``AlphaConfig`` object (preferred) or individual
    keyword arguments (backward-compatible).

    Args:
        ohlcv: Raw OHLCV DataFrame (must have timestamps, ticker, close).
        config: ``AlphaConfig`` — if provided, all other kwargs are ignored.
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
            - config: The AlphaConfig used (for reproducibility)
    """
    # ── Build config from kwargs if not provided ──
    if config is None:
        config = AlphaConfig(
            factor_names=factor_names or ["bbiboll", "vol_ratio"],
            combination_method=combination_method,
            sizing_method=sizing_method,
            rebal_every_n=rebal_every_n,
            n_long=n_long,
            n_short=n_short,
            target_vol=target_vol,
            annualization=annualization,
        )

    # --- Stage 1-2: Returns ---
    daily_returns = compute_daily_returns(ohlcv)
    next_day_returns = compute_next_day_returns(daily_returns)

    # --- Stage 3: Factor ---
    composite = build_factor_pipeline(
        ohlcv,
        config=config,
        next_day_returns=next_day_returns,
    )

    # --- Stage 4: Sizing ---
    sizing_methods = build_sizing_methods(
        composite,
        ohlcv,
        daily_returns,
        config=config,
    )

    if config.sizing_method not in sizing_methods:
        available = ", ".join(sorted(sizing_methods.keys()))
        raise KeyError(
            f"Unknown sizing method '{config.sizing_method}'. Available: {available}"
        )

    weights = sizing_methods[config.sizing_method]

    # --- Stage 5: Rebalance ---
    weights = resample_weights(weights, rebal_every_n=config.rebal_every_n)

    # --- Stage 6: Portfolio returns ---
    port_returns = compute_portfolio_return(weights, next_day_returns)
    returns_array = port_returns["port_return"].to_numpy()

    # --- Metrics ---
    mu = float(np.mean(returns_array))
    sigma = float(np.std(returns_array, ddof=1))
    sharpe = mu / sigma * np.sqrt(config.annualization) if sigma > 1e-10 else 0.0

    return {
        "composite": composite,
        "daily_returns": daily_returns,
        "next_day_returns": next_day_returns,
        "sizing_methods": sizing_methods,
        "weights": weights,
        "portfolio_returns": port_returns,
        "returns_array": returns_array,
        "sharpe": float(sharpe),
        "annual_return": float(mu * config.annualization),
        "annual_vol": float(sigma * np.sqrt(config.annualization)),
        "n_days": len(returns_array),
        "config": config,
    }
