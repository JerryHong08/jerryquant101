"""
Forward Returns — Compute N-day forward returns for the stock universe.

This is the "dependent variable" for factor evaluation.  Every factor analysis
starts here: you need to know what each stock returned over the next N days
so you can correlate that with today's signal value.

Usage:
    from alpha.forward_returns import compute_forward_returns

    # ohlcv: LazyFrame from stock_load_process() with (ticker, timestamps, close, ...)
    returns_df = compute_forward_returns(
        ohlcv.collect(),
        horizons=[1, 5, 10, 20],
    )
    # returns_df schema: (date, ticker, forward_return_1d, forward_return_5d, ...)

Reference: guidance/quant_lab.pdf — Part III, Chapter 9 (Factor Evaluation)
"""

from typing import List

import polars as pl


def compute_forward_returns(
    prices: pl.DataFrame,
    horizons: List[int] = [1, 5, 10, 20],
    price_col: str = "close",
    date_col: str = "timestamps",
    ticker_col: str = "ticker",
) -> pl.DataFrame:
    """
    Compute forward returns at multiple horizons for each stock-date.

    For each (date, ticker) pair, the forward return at horizon h is:
        forward_return_hd = close_{t+h} / close_t - 1

    Args:
        prices: DataFrame with at least (date_col, ticker_col, price_col).
                Must be sorted by (ticker, date) — will sort if not.
        horizons: List of forecast horizons in trading days (e.g., [1, 5, 10, 20]).
        price_col: Column name for the price (default: "close").
        date_col: Column name for the date (default: "timestamps").
        ticker_col: Column name for the ticker (default: "ticker").

    Returns:
        DataFrame with columns:
            - date: trading date
            - ticker: stock ticker
            - forward_return_1d, forward_return_5d, ... (one per horizon)

    Example:
        >>> prices = pl.DataFrame({
        ...     "timestamps": ["2025-01-02", "2025-01-03", "2025-01-06"],
        ...     "ticker": ["AAPL", "AAPL", "AAPL"],
        ...     "close": [100.0, 102.0, 105.0],
        ... })
        >>> compute_forward_returns(prices, horizons=[1, 2])
    """
    # Ensure sorted by (ticker, date)
    df = prices.select([date_col, ticker_col, price_col]).sort([ticker_col, date_col])

    # Compute forward returns via shift(-h) within each ticker group
    forward_cols = []
    for h in horizons:
        col_name = f"forward_return_{h}d"
        forward_cols.append(
            (
                pl.col(price_col).shift(-h).over(ticker_col) / pl.col(price_col) - 1
            ).alias(col_name)
        )

    result = df.with_columns(forward_cols).rename({date_col: "date"})

    # Drop the price column — only keep date, ticker, and forward returns
    result = result.drop(price_col)

    return result


def compute_log_forward_returns(
    prices: pl.DataFrame,
    horizons: List[int] = [1, 5, 10, 20],
    price_col: str = "close",
    date_col: str = "timestamps",
    ticker_col: str = "ticker",
) -> pl.DataFrame:
    """
    Compute log forward returns: ln(close_{t+h} / close_t).

    Log returns are additive across time and closer to normal distribution,
    preferred for statistical analysis.

    Args:
        Same as compute_forward_returns.

    Returns:
        Same schema as compute_forward_returns, but with log returns.
    """
    df = prices.select([date_col, ticker_col, price_col]).sort([ticker_col, date_col])

    forward_cols = []
    for h in horizons:
        col_name = f"forward_return_{h}d"
        forward_cols.append(
            (pl.col(price_col).shift(-h).over(ticker_col) / pl.col(price_col))
            .log()
            .alias(col_name)
        )

    result = df.with_columns(forward_cols).rename({date_col: "date"})
    result = result.drop(price_col)

    return result
