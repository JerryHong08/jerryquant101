"""
Factor Preprocessing — winsorize, normalize, neutralize.

Raw signals are messy: outliers, heterogeneous scales, sector biases.
This module applies the standard preprocessing pipeline that every factor
goes through before it's ready for combination and portfolio construction.

Pipeline order:
    1. Winsorize — cap extremes at (pct, 1-pct) cross-sectionally
    2. Normalize — z-score or rank-normalize cross-sectionally
    3. Neutralize — remove sector or size effects

Usage:
    from alpha.preprocessing import preprocess_factor

    clean = preprocess_factor(
        raw_signal,
        sectors=sector_df,       # optional
        neutralize=["sector"],   # or [] for no neutralization
        method="zscore",         # or "rank"
    )

Reference: docs/quant_lab.tex — Part III, Chapter 11 (Factor Construction)
"""

from typing import List, Optional

import polars as pl


def winsorize(
    df: pl.DataFrame,
    value_col: str = "value",
    date_col: str = "date",
    pct: float = 0.01,
) -> pl.DataFrame:
    """
    Cap extreme values at (pct, 1-pct) percentiles cross-sectionally.

    Applied before z-scoring to prevent outliers from dominating the mean
    and standard deviation.

    Args:
        df: DataFrame with columns (date, ticker, value).
        value_col: Column name for the signal values.
        date_col: Column to group by (each date is independent).
        pct: Lower percentile for clipping (e.g. 0.01 = 1st percentile).

    Returns:
        DataFrame with outliers capped.
    """
    return df.with_columns(
        pl.col(value_col)
        .clip(
            lower_bound=pl.col(value_col).quantile(pct).over(date_col),
            upper_bound=pl.col(value_col).quantile(1.0 - pct).over(date_col),
        )
        .alias(value_col)
    )


def zscore_normalize(
    df: pl.DataFrame,
    value_col: str = "value",
    date_col: str = "date",
) -> pl.DataFrame:
    """
    Cross-sectional z-score normalization.

    z_{i,t} = (x_{i,t} - mean_t) / std_t

    Ensures zero mean, unit variance across the stock universe at each date.

    Args:
        df: DataFrame with (date, ticker, value).
        value_col: Column to normalize.
        date_col: Column to group by.

    Returns:
        DataFrame with z-scored values.
    """
    return df.with_columns(
        (
            (pl.col(value_col) - pl.col(value_col).mean().over(date_col))
            / pl.col(value_col).std().over(date_col)
        ).alias(value_col)
    )


def rank_normalize(
    df: pl.DataFrame,
    value_col: str = "value",
    date_col: str = "date",
) -> pl.DataFrame:
    """
    Cross-sectional rank normalization to [-1, 1].

    More robust to outliers than z-scoring.  Maps the rank percentile
    linearly to the [-1, 1] range.

    Args:
        df: DataFrame with (date, ticker, value).
        value_col: Column to normalize.
        date_col: Column to group by.

    Returns:
        DataFrame with rank-normalized values in [-1, 1].
    """
    return df.with_columns(
        (
            (pl.col(value_col).rank().over(date_col) - 1)
            / (pl.col(value_col).count().over(date_col) - 1)
            * 2
            - 1
        ).alias(value_col)
    )


def sector_neutralize(
    df: pl.DataFrame,
    sectors: pl.DataFrame,
    value_col: str = "value",
    date_col: str = "date",
    ticker_col: str = "ticker",
    sector_col: str = "sector",
) -> pl.DataFrame:
    """
    Sector neutralization — demean the factor within each sector.

    z_neutral = z_{i,t} - mean(z_{sector(i), t})

    Removes sector-level effects so the factor captures stock-specific alpha,
    not sector rotation.

    Args:
        df: DataFrame with (date, ticker, value).
        sectors: DataFrame with (ticker, sector).
        value_col: Column to neutralize.
        date_col: Date column.
        ticker_col: Ticker column.
        sector_col: Sector column in the sectors DataFrame.

    Returns:
        DataFrame with sector-neutralized values.
    """
    joined = df.join(sectors, on=ticker_col, how="left")

    # Group key for demeaning: (date, sector)
    group_key = [date_col, sector_col]

    neutralized = joined.with_columns(
        (pl.col(value_col) - pl.col(value_col).mean().over(group_key)).alias(value_col)
    ).drop(sector_col)

    return neutralized


def preprocess_factor(
    raw_signal: pl.DataFrame,
    sectors: Optional[pl.DataFrame] = None,
    winsorize_pct: float = 0.01,
    method: str = "zscore",
    neutralize: Optional[List[str]] = None,
    value_col: str = "value",
    date_col: str = "date",
    ticker_col: str = "ticker",
) -> pl.DataFrame:
    """
    Full factor preprocessing pipeline.

    Steps:
        1. Drop rows with null/infinite signal values
        2. Winsorize at (pct, 1-pct) cross-sectionally
        3. Normalize (z-score or rank)
        4. Neutralize for specified factors (sector)

    Args:
        raw_signal: DataFrame with columns (date, ticker, value).
        sectors: DataFrame with (ticker, sector).  Required if
                 neutralize includes "sector".
        winsorize_pct: Percentile for winsorization (default: 0.01 = 1%).
        method: Normalization method — "zscore" or "rank".
        neutralize: List of neutralization targets.  Supported: ["sector"].
                    Pass None or [] to skip neutralization.
        value_col: Column name for signal values.
        date_col: Column name for dates.
        ticker_col: Column name for tickers.

    Returns:
        Preprocessed factor DataFrame with the same schema as input.

    Example:
        >>> clean = preprocess_factor(
        ...     raw_signal,
        ...     sectors=sector_df,
        ...     neutralize=["sector"],
        ...     method="zscore",
        ... )
    """
    if neutralize is None:
        neutralize = []

    # Step 0: Remove invalid rows
    df = raw_signal.filter(
        pl.col(value_col).is_not_null() & pl.col(value_col).is_finite()
    )

    # Step 1: Winsorize
    if winsorize_pct > 0:
        df = winsorize(df, value_col=value_col, date_col=date_col, pct=winsorize_pct)

    # Step 2: Normalize
    if method == "zscore":
        df = zscore_normalize(df, value_col=value_col, date_col=date_col)
    elif method == "rank":
        df = rank_normalize(df, value_col=value_col, date_col=date_col)
    else:
        raise ValueError(f"Unknown method '{method}'. Use 'zscore' or 'rank'.")

    # Step 3: Neutralize
    if "sector" in neutralize:
        if sectors is None:
            raise ValueError("sectors DataFrame required for sector neutralization.")
        df = sector_neutralize(
            df,
            sectors,
            value_col=value_col,
            date_col=date_col,
            ticker_col=ticker_col,
        )

    return df
