import polars as pl
import talib

from .registry import register


@register("obv", grouped=True)
def calculate_obv(df: pl.DataFrame) -> pl.DataFrame:
    close = df["close"].cast(pl.Float64).to_numpy()
    volume = df["volume"].cast(pl.Float64).to_numpy().astype("float64")
    obv = talib.OBV(close, volume)
    return df.with_columns(pl.Series("obv", obv))
