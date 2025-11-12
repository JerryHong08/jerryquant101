# indicators/base.py
from typing import Callable

import polars as pl


def apply_grouped(
    df: pl.DataFrame, func: Callable[[pl.DataFrame, dict], pl.DataFrame], **params
) -> pl.DataFrame:
    """
    ohlcv_data schema
    Schema([
        ('ticker', String),
        ('timestamps', Datetime(time_unit='ns',
        time_zone='America/New_York')),
        ('volume', Int64),
        ('open', Float64),
        ('close', Float64),
        ('high', Float64),
        ('low', Float64),
        ('window_start', Int64),
        ('transactions', UInt32),
        ('split_date', Date),
        ('split_ratio', Float64)
    ])

    """
    return (
        df.sort(["ticker", "timestamps"])
        .group_by("ticker")
        .map_groups(lambda g: func(g, **params))
    )
