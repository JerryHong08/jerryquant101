import polars as pl

# df = pl.read_parquet('I:IRXday_yfinance.parquet')
# df = pl.read_parquet('I:IRXday.parquet')
df = pl.read_parquet("I:SPXday_yfinance.parquet")

df = df.with_columns(
    pl.from_epoch("timestamp", time_unit="ms").dt.replace_time_zone("America/New_York")
).sort("timestamp")

print(df.head())
print(df.tail())
