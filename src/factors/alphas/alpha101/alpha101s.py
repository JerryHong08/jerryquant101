from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig


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
