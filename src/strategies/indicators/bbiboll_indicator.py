import numpy as np
import polars as pl
import talib

from .registry import register


@register("bbiboll", grouped=True)
def calculate_bbiboll(
    df: pl.DataFrame,
    boll_length: int = 11,
    boll_multiple: int = 6,
    pct_window: int = 252,
) -> pl.DataFrame:
    """
    Calculate BBIBOLL indicator.

    Args:
        df: ohlcv
        boll_length: rolling_window of dev(std)
        boll_multiple: multipler of dev

    Returns:
        ohlcv with (
            bbi: multiple timespans MA mean
            dev: std of rolling_window of boll_length * boll_multiple
            upr: bbi + dev
            dwn: bbi - dev
            dev_pct:
            longterm_dev_pct_rank: dev rank over entire history
    """

    close_prices = df.select("close").to_numpy().flatten()

    # calculate moving averages for bbi
    ma3 = talib.SMA(close_prices, timeperiod=3)
    ma6 = talib.SMA(close_prices, timeperiod=6)
    ma12 = talib.SMA(close_prices, timeperiod=12)
    ma24 = talib.SMA(close_prices, timeperiod=24)

    # BBI = (MA3 + MA6 + MA12 + MA24) / 4
    bbi = (ma3 + ma6 + ma12 + ma24) / 4

    # DEV (BBI std * multipler)
    dev = talib.STDDEV(bbi, timeperiod=boll_length) * boll_multiple

    # upr and dwm
    upr = bbi + dev
    dwn = bbi - dev

    # add to dataframe
    result = df.with_columns(
        [
            pl.Series("bbi", bbi),
            pl.Series("dev", dev),
            pl.Series("upr", upr),
            pl.Series("dwn", dwn),
        ]
    )

    min_periods = min(50, pct_window // 3)

    dev_ranks = []
    rank_ends = []
    dev_values = result.select("dev").to_numpy().flatten()

    for i in range(len(dev_values)):
        start_idx = max(0, i - pct_window + 1) if i >= min_periods else 0
        window_values = dev_values[start_idx : i + 1]

        if len(window_values) >= min_periods:
            # calculate rank in window
            rank = np.sum(window_values <= dev_values[i])
            dev_ranks.append(rank)
        else:
            dev_ranks.append(np.nan)

    result = result.with_columns(
        [
            pl.Series("dev_pct", dev_ranks),
        ]
    )

    result = result.with_columns(
        [pl.col("dev").rank("ordinal").over("ticker").alias("longterm_dev_pct_rank")]
    )

    return result
