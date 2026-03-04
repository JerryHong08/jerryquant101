"""alpha101 factors."""

from __future__ import annotations

import polars as pl

from constants import OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor


@register_factor("alpha101")
def alpha001(ohlcv: pl.DataFrame) -> pl.DataFrame:
    """

    rank(ts_argmax(signed_power(close/delay(close,1)-1, 2), 5))

    Args:
        ohlcv (pl.DataFrame): ohlcv data, DataFrame with columns [date, ticker, open, high, low, close, volume]

    Returns:
        pl.DataFrame: [date, ticker, value]
    """
    pass


def alpha002(ohlcv: pl.DataFrame) -> pl.DataFrame:
    pass
