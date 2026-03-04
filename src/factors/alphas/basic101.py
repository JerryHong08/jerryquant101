"""BBIBOLL deviation factor."""

from __future__ import annotations

import polars as pl

from constants import OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor


@register_factor("bbiboll")
def _compute_bbiboll_factor(ohlcv: pl.DataFrame, **kwargs) -> pl.DataFrame:
    """Raw signal: (close − BBI) / deviation."""
    from indicators.registry import get_indicator

    ohlcv_bb = get_indicator("bbiboll")(ohlcv)
    return ohlcv_bb.with_columns(
        ((pl.col("close") - pl.col("bbi")) / pl.col("dev")).alias(VALUE_COL)
    )


@register_factor("momentum")
def _compute_momentum_factor(
    ohlcv: pl.DataFrame, *, lookback: int = 20, **kwargs
) -> pl.DataFrame:
    """Raw signal: lookback-day log return."""
    return ohlcv.sort([TICKER_COL, OHLCV_DATE_COL]).with_columns(
        (pl.col("close") / pl.col("close").shift(lookback).over(TICKER_COL))
        .log()
        .alias(VALUE_COL)
    )


@register_factor("vol_ratio")
def _compute_vol_ratio_factor(
    ohlcv: pl.DataFrame,
    *,
    short_window: int = 5,
    long_window: int = 20,
    **kwargs,
) -> pl.DataFrame:
    """Raw signal: short-window vol / long-window vol."""
    return (
        ohlcv.sort([TICKER_COL, OHLCV_DATE_COL])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over(TICKER_COL))
            .log()
            .alias("log_ret")
        )
        .with_columns(
            pl.col("log_ret")
            .rolling_std(window_size=short_window)
            .over(TICKER_COL)
            .alias("vol_short"),
            pl.col("log_ret")
            .rolling_std(window_size=long_window)
            .over(TICKER_COL)
            .alias("vol_long"),
        )
        .with_columns((pl.col("vol_short") / pl.col("vol_long")).alias(VALUE_COL))
    )
