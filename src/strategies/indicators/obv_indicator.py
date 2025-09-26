# indicators/obv.py
import polars as pl
import talib

from .registry import register


@register("obv", grouped=True)
def calculate_obv(df: pl.DataFrame) -> pl.DataFrame:
    close = df["close"].to_numpy()
    volume = df["volume"].to_numpy().astype("float64")
    obv = talib.OBV(close, volume)
    return df.with_columns(pl.Series("obv", obv))
