import glob
import warnings
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
import seolpyo_mplchart as mc

# warnings.filterwarnings("ignore", message=".*Font family.*not found.*")
matplotlib.rcParams["font.family"] = "DejaVu Sans"

# splits data
splits_dir = "data/raw/us_stocks_sip/splits/splits.parquet"
splits_error_dir = "data/raw/us_stocks_sip/splits/splits_error.parquet"

splits_original = pl.read_parquet(splits_dir)
splits_errors = pl.read_parquet(splits_error_dir)

splits = splits_original.filter(~pl.col("id").is_in(splits_errors["id"].implode()))


def splits_adjust(lf, splits, price_decimals: int = 4):
    # 获取数据范围
    date_range = lf.select(
        [
            pl.col("timestamps").min().alias("date_min"),
            pl.col("timestamps").max().alias("date_max"),
        ]
    ).collect()

    # Check if we have data
    if date_range.height == 0 or date_range[0, 0] is None:
        return lf  # No data to adjust

    date_min = date_range[0, 0]
    date_max = date_range[0, 1]

    tickers = lf.select(pl.col("ticker").unique()).collect().to_series(0).to_list()

    # If no tickers found, return original data
    if not tickers:
        return lf

    splits_filtered = splits.filter(
        (pl.col("ticker").is_in(tickers))
        & (
            pl.col("execution_date")
            .str.to_date()
            .is_between(
                date_min.date() - pl.duration(days=1),
                date_max.date() + pl.duration(days=1),
            )
        )
    )

    if splits_filtered.height > 0:
        splits_processed = splits_filtered.with_columns(
            [
                (pl.col("execution_date").str.to_date() - pl.duration(days=1)).alias(
                    "split_date"
                ),
                (pl.col("split_from") / pl.col("split_to")).alias("split_ratio"),
            ]
        ).select(["ticker", "split_date", "split_ratio"])

        splits_with_factor = (
            splits_processed.sort(["ticker", "split_date"], descending=[False, True])
            .with_columns(
                pl.col("split_ratio")
                .cum_prod()
                .over("ticker")
                .alias("cumulative_split_ratio")
            )
            .sort(["ticker", "split_date"])
        )

        # 由于 Polars join_asof 需要对齐日期类型，可以先添加辅助列
        lf = (
            lf.with_columns(pl.col("timestamps").dt.date().alias("date_only"))
            .join_asof(
                splits_with_factor.lazy(),
                left_on="date_only",
                right_on="split_date",
                by="ticker",
                strategy="forward",
            )
            .with_columns(
                pl.col("cumulative_split_ratio").fill_null(1.0).alias("factor")
            )
            .with_columns(
                [
                    (pl.col("open") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("open"),
                    (pl.col("high") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("high"),
                    (pl.col("low") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("low"),
                    (pl.col("close") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("close"),
                    (pl.col("volume") / pl.col("factor"))
                    .round(0)
                    .cast(pl.Int64)
                    .alias("volume"),
                ]
            )
            .drop(["date_only", "cumulative_split_ratio", "factor"])
        )

    return lf


def resample_ohlcv(lf, timeframe):
    """
    将分钟数据重采样到指定时间框架

    Args:
        lf: Polars LazyFrame with OHLCV data
        timeframe: '5m', '15m', '30m', '1h', '4h', '1d' 等

    Returns:
        Resampled LazyFrame
    """
    return (
        lf.sort(["ticker", "timestamps"])
        .group_by_dynamic(
            "timestamps",
            every=timeframe,
            closed="left",
            group_by="ticker",  # Updated parameter name
        )
        .agg(
            [
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
                pl.col("transactions").sum().alias("transactions"),
            ]
        )
        .filter(pl.col("volume") > 0)  # 过滤掉没有交易的时间段
    )


# ================================
# Function Definitions Start Here
# ================================


def data_dir_calculate(start_date: str, end_date: str):
    """
    Calculate data directory paths based on date range.

    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'

    Returns:
        List of parquet file paths
    """
    from datetime import datetime

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    data_dir = []

    for y in range(start_dt.year, end_dt.year + 1):
        start_month = start_dt.month if y == start_dt.year else 1
        end_month = end_dt.month if y == end_dt.year else 12

        for m in range(start_month, end_month + 1):
            month_str = f"{m:02d}"  # Format as two digits
            file_pattern = (
                f"data/lake/us_stocks_sip/minute_aggs_v1/{y}/{month_str}/*.parquet"
            )
            data_dir.extend(glob.glob(file_pattern))

    return data_dir


def load_stock_minute_aggs(
    start_date: str = None,
    end_date: str = None,
    timeframe: str = "1m",
    ticker: str = None,
    whole_market_time: bool = False,
) -> pl.DataFrame:
    """
    Get a range of specific timeframe tickers data based on minute_aggs timeframe data.

    Args:
        start_date: Begin date '2025-04-01'
        end_date: End date '2025-08-01'
        timeframe: Interval ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
        ticker: Ticker symbol(s), comma-separated (if None, return whole market data)
        whole_market_time: If False, filter to regular trading hours (9:30-16:00)

    Returns:
        Market ticker historical dataframe

    Examples:
        ::
            df = load_stock_minute_aggs(
                start_date='2025-04-01',
                end_date='2025-08-20',
                timeframe='1h',
                ticker='TQQQ',
                whole_market_time=False
            )
            print(df.head())
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    # Validate inputs
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")

    # Validate timeframe
    valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
    if timeframe not in valid_timeframes:
        raise ValueError(f"timeframe must be one of {valid_timeframes}")

    # Get data directory paths
    data_dir = data_dir_calculate(start_date, end_date)

    if not data_dir:
        raise ValueError(
            f"No data files found for date range {start_date} to {end_date}"
        )

    # Parse ticker parameter
    tickers = [t.strip().upper() for t in ticker.split(",")] if ticker else None

    # Build lazy frame with optional ticker filtering
    if tickers:
        lf = pl.scan_parquet(data_dir).filter(pl.col("ticker").is_in(tickers))
    else:
        lf = pl.scan_parquet(data_dir)

    # Convert timestamps and add timezone
    lf = lf.with_columns(
        pl.from_epoch(pl.col("window_start"), time_unit="ns")
        .dt.convert_time_zone("America/New_York")
        .alias("timestamps")
    )

    # Parse date range for filtering
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    # End date should be the start of the next day to include the entire end_date
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("America/New_York")
    ) + timedelta(days=1)

    # Filter by date range
    lf = lf.filter((pl.col("timestamps") >= start_dt) & (pl.col("timestamps") < end_dt))

    # Filter to regular trading hours if requested
    if not whole_market_time:
        lf = lf.filter(
            (pl.col("timestamps").dt.time() >= time(9, 30))
            & (pl.col("timestamps").dt.time() < time(16, 0))
        )

    # Apply stock splits adjustment
    lf_adj = splits_adjust(lf, splits, price_decimals=4)

    # Resample to requested timeframe if not 1m
    if timeframe != "1m":
        lf_adj = resample_ohlcv(lf_adj, timeframe)

    return lf_adj.collect()


# -----------------PLOT-------------------
timeframe = "30m"
ticker = "TQQQ"
df = load_stock_minute_aggs(
    start_date="2025-06-02",
    end_date="2025-07-03",
    timeframe=timeframe,
    ticker=ticker,
    whole_market_time=True,
).to_pandas()

# print(df.head())

format_candleinfo_en = """\
{dt}

close:      {close}
rate:        {rate}
compare: {compare}
open:      {open}({rate_open})
high:       {high}({rate_high})
low:        {low}({rate_low})
volume:  {volume}({rate_volume})\
"""
format_volumeinfo_en = """\
{dt}

volume:      {volume}
volume rate: {rate_volume}
compare:     {compare}\
"""


class Chart(mc.SliderChart):
    digit_price = 3
    digit_volume = 1

    unit_price = "$"
    unit_volume = "Vol"
    format_ma = "ma{}"
    format_candleinfo = format_candleinfo_en
    format_volumeinfo = format_volumeinfo_en


c = Chart()
c.watermark = f"{ticker}-{timeframe}"  # watermark

c.date = "timestamps"
c.Open = "open"
c.high = "high"
c.low = "low"
c.close = "close"
c.volume = "volume"

c.set_data(df)

mc.show()  # same as matplotlib.pyplot.show()
