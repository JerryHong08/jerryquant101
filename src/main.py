import polars as pl

df = pl.read_parquet("I:IRXday.parquet")

df = df.with_columns(
    pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone("UTC")
)

df = df.sort("timestamp")

print(df.head())
print(df.tail())
