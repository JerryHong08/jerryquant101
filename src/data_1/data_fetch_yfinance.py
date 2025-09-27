import polars as pl
import yfinance as yf

# ^IRX
# ^SPX
ticker = ["^IRX"]
start = "2015-01-01"
end = "2025-09-30"
data = yf.download(ticker, start=start, end=end, interval="1d")
data = data.reset_index()
data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
data = data[["Date", "Open", "High", "Low", "Close"]]
df = pl.from_pandas(data)
df = df.with_columns(
    pl.col("Date").alias("date"),
    pl.col("Open").alias("open"),
    pl.col("High").alias("high"),
    pl.col("Low").alias("low"),
    pl.col("Close").alias("close"),
).select(
    pl.col(["open", "high", "low", "close"]),
    (pl.col("date").dt.timestamp("ms")).alias("timestamp"),
)

df.write_parquet(
    f"I:{ticker[0].replace('^', '')}day.parquet",
    compression="zstd",
    compression_level=3,
)

print(
    df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("datetime")
    ).tail()
)
