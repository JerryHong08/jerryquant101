import glob
import os
import warnings
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl

from quant101.core_2.config import data_dir
from quant101.core_2.plotter import plot_candlestick

# warnings.filterwarnings("ignore", message=".*Font family.*not found.*")
matplotlib.rcParams["font.family"] = "DejaVu Sans"

# splits data
# splits_dir = "data/raw/us_stocks_sip/splits/splits.parquet"
# splits_error_dir = "data/raw/us_stocks_sip/splits/splits_error.parquet"
# /mnt/blackdisk/quant_data/polygon_data

splits_dir = os.path.join(data_dir, "raw/us_stocks_sip/splits/splits.parquet")
splits_error_dir = os.path.join(
    data_dir, "raw/us_stocks_sip/splits/splits_error.parquet"
)

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

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    data_dirs = []

    for y in range(start_dt.year, end_dt.year + 1):
        start_month = start_dt.month if y == start_dt.year else 1
        end_month = end_dt.month if y == end_dt.year else 12

        for m in range(start_month, end_month + 1):
            month_str = f"{m:02d}"  # Format as two digits
            file_pattern = (
                f"{data_dir}lake/us_stocks_sip/minute_aggs_v1/{y}/{month_str}/*.parquet"
            )
            data_dirs.extend(glob.glob(file_pattern))

    return data_dirs


def generate_full_timestamp(start_date, end_date, timeframe):

    # 纽约证券交易所 (美国)
    xnys = xcals.get_calendar("XNYS")
    snys_schedule = xnys.schedule.loc[start_date:end_date]

    # 转换为 Polars DataFrame
    df_schedule = pl.from_pandas(snys_schedule.reset_index())

    df_schedule = df_schedule.with_columns(
        [
            pl.col("open").dt.convert_time_zone("America/New_York"),
            pl.col("close").dt.convert_time_zone("America/New_York"),
        ]
    )

    # 为每个交易日生成时间戳
    all_timestamps = []

    for row in df_schedule.iter_rows(named=True):
        open_time = row["open"]
        close_time = row["close"]

        # 根据 timeframe 生成当日的所有交易时间戳
        if timeframe == "1d":
            # 日K线只需要一个时间戳，使用交易日的午夜时间
            trade_date = open_time.date()
            daily_timestamp = datetime.combine(trade_date, time(0, 0)).replace(
                tzinfo=ZoneInfo("America/New_York")
            )
            all_timestamps.append(daily_timestamp)
        else:
            # 分钟、小时级别的时间戳
            day_timestamps = (
                pl.select(
                    pl.datetime_range(
                        open_time,
                        close_time,
                        interval=timeframe,
                        closed="left",  # 不包括收盘时间
                    ).alias("timestamps")
                )
                .get_column("timestamps")
                .to_list()
            )
            all_timestamps.extend(day_timestamps)

    # 创建完整的时间戳DataFrame
    generated_trade_timestamp = (
        pl.DataFrame({"timestamps": all_timestamps})
        .with_columns(
            pl.col("timestamps")
            .dt.cast_time_unit("ns")
            .dt.convert_time_zone("America/New_York")
        )
        .sort("timestamps")
    )

    return generated_trade_timestamp


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
    lf = lf.filter(
        (pl.col("timestamps") >= start_dt) & (pl.col("timestamps") <= end_dt)
    )

    # Filter to regular trading hours if requested
    if not whole_market_time:
        lf = lf.filter(
            (pl.col("timestamps").dt.time() >= time(9, 30))
            & (pl.col("timestamps").dt.time() < time(16, 0))
        )

    # Apply stock splits adjustment
    lf = splits_adjust(lf, splits, price_decimals=4)

    # Resample to requested timeframe if not 1m
    if timeframe != "1m":
        lf_adj = resample_ohlcv(lf, timeframe).collect()
    else:
        lf_adj = lf.collect()

    # -------------fill missing timestamp-----------------

    if lf_adj.height == 0:
        return lf_adj

    # 按ticker分组处理
    result_dfs = []

    for ticker_name in lf_adj["ticker"].unique():
        ticker_df = lf_adj.filter(pl.col("ticker") == ticker_name)

        if ticker_df.height == 0:
            continue

        # 获取数据的时间范围
        min_time = ticker_df["timestamps"].min()
        max_time = ticker_df["timestamps"].max()

        # 创建完整的交易时间序列 - 基于实际交易日的健壮方法

        # 首先获取数据中实际存在的交易日
        actual_trading_days = (
            ticker_df.select(pl.col("timestamps").dt.date().alias("trade_date"))
            .unique()
            .sort("trade_date")
            .get_column("trade_date")
            .to_list()
        )

        if not actual_trading_days:
            continue

        time_points = []

        for trade_date in actual_trading_days:
            if whole_market_time:
                # 盘前盘后时间：4:00 AM - 8:00 PM EDT
                day_start = datetime.combine(trade_date, time(4, 0)).replace(
                    tzinfo=ZoneInfo("America/New_York")
                )
                day_end = datetime.combine(trade_date, time(20, 0)).replace(
                    tzinfo=ZoneInfo("America/New_York")
                )
            else:
                # 正常交易时间：9:30 AM - 4:00 PM EDT
                day_start = datetime.combine(trade_date, time(9, 30)).replace(
                    tzinfo=ZoneInfo("America/New_York")
                )
                day_end = datetime.combine(trade_date, time(16, 0)).replace(
                    tzinfo=ZoneInfo("America/New_York")
                )

            # 对于重采样的数据，需要调整起始时间以匹配重采样后的时间戳格式
            if timeframe != "1m":
                if timeframe == "1d":
                    # 1日K线时间戳从午夜开始
                    day_start = datetime.combine(trade_date, time(0, 0)).replace(
                        tzinfo=ZoneInfo("America/New_York")
                    )
                    day_end = day_start + timedelta(days=1)
                else:
                    continue

            # 创建当天的时间序列
            day_times = (
                pl.select(
                    pl.datetime_range(
                        day_start, day_end, interval=timeframe, closed="left"
                    ).alias("timestamps")
                )
                .get_column("timestamps")
                .to_list()
            )

            time_points.extend(day_times)

        # 保留完整的时间序列，但确保在合理的日期范围内
        # 只按日期过滤，不按具体时间过滤，这样可以保留完整的交易时段用于前向填充
        min_date = min_time.date()
        max_date = max_time.date()
        time_points = [t for t in time_points if min_date <= t.date() <= max_date]

        # 创建时间序列DataFrame
        if not time_points:
            continue  # 如果没有有效时间点，跳过这个ticker

        time_range = pl.DataFrame({"timestamps": time_points}).with_columns(
            # 确保时间戳类型匹配
            pl.col("timestamps")
            .dt.cast_time_unit("ns")
            .dt.convert_time_zone("America/New_York")
        )

        # 添加ticker列
        time_range = time_range.with_columns(pl.lit(ticker_name).alias("ticker"))

        # 左连接以保留所有时间点
        filled_df = time_range.join(ticker_df, on=["ticker", "timestamps"], how="left")

        # 前向填充价格数据
        # 对于没有交易的时间点，OHLC都应该等于前一个收盘价
        filled_df = (
            filled_df.with_columns(
                [
                    # 首先前向填充close价格
                    pl.col("close")
                    .forward_fill()
                    .alias("close_filled")
                ]
            )
            .with_columns(
                [
                    # 对于有实际交易的点，保持原值；对于null点，使用前向填充的close值
                    pl.when(pl.col("open").is_not_null())
                    .then(pl.col("open"))
                    .otherwise(pl.col("close_filled"))
                    .alias("open"),
                    pl.when(pl.col("high").is_not_null())
                    .then(pl.col("high"))
                    .otherwise(pl.col("close_filled"))
                    .alias("high"),
                    pl.when(pl.col("low").is_not_null())
                    .then(pl.col("low"))
                    .otherwise(pl.col("close_filled"))
                    .alias("low"),
                    pl.when(pl.col("close").is_not_null())
                    .then(pl.col("close"))
                    .otherwise(pl.col("close_filled"))
                    .alias("close"),
                    # volume和transactions用0填充缺失值
                    pl.col("volume").fill_null(0),
                    pl.col("transactions").fill_null(0),
                ]
            )
            .drop("close_filled")
        )

        # 处理开头可能仍然为null的情况（当时间序列从交易日开始但数据更晚开始时）
        # 找到第一个有效的价格数据，用它填充之前的null值
        first_valid_idx = (
            filled_df.with_row_index()
            .filter(pl.col("close").is_not_null())
            .select("index")
            .head(1)
        )

        if first_valid_idx.height > 0:
            first_idx = first_valid_idx["index"][0]
            first_close = filled_df[first_idx, "close"]

            # 使用第一个有效收盘价填充前面的null值
            filled_df = filled_df.with_columns(
                [
                    pl.col("open").fill_null(first_close),
                    pl.col("high").fill_null(first_close),
                    pl.col("low").fill_null(first_close),
                    pl.col("close").fill_null(first_close),
                ]
            )

        result_dfs.append(filled_df)

    # 合并所有ticker的数据
    if result_dfs:
        result = pl.concat(result_dfs)
        return result
    else:
        return lf_adj


# -----------------PLOT-------------------
# valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
timeframe = "1d"
ticker = "AAPL"  # 使用流动性更好的股票以便看到更明显的蜡烛图效果
start_date = "2015-01-01"
end_date = "2025-08-19"

if __name__ == "__main__":
    df = load_stock_minute_aggs(
        start_date=start_date,
        end_date=end_date,
        timeframe=timeframe,
        ticker=ticker,
        whole_market_time=False,
    )
    print(df.shape)
    print(df.head())

    generated_timestamp = generate_full_timestamp(start_date, end_date, timeframe)
    print(generated_timestamp.shape)
    print(generated_timestamp.head())

    diff1 = generated_timestamp.filter(
        ~pl.col("timestamps").is_in(df["timestamps"].implode())
    )
    diff2 = df.filter(
        ~pl.col("timestamps").is_in(generated_timestamp["timestamps"].implode())
    )
    with pl.Config(tbl_rows=40):
        print(diff1, diff2)

    # plot_candlestick(df.to_pandas(), ticker, timeframe)

    # ---------------------------
    # - 查找时间戳
    # # target_date = datetime.strptime('2018-08-07', '%Y-%m-%d').date()
    target_date = datetime(2025, 7, 7, 12, 0).replace(
        tzinfo=ZoneInfo("America/New_York")
    )

    print(f"\n=== 查找 {target_date} 的时间戳 ===")

    # 在 generated_timestamp 中查找这一天的时间戳
    # gen_target_day = generated_timestamp.filter(
    #     (pl.col("timestamps").dt.date() == target_date.date()) & (pl.col("timestamps").dt.hour() == target_date.hour)
    # )
    # print(f"generated_timestamp 中 {target_date} 的时间戳:")
    # print(gen_target_day)

    # 在 df 中查找这一天的时间戳
    df_target_day = df.filter(
        (
            (pl.col("timestamps").dt.date() == target_date.date())
            & (
                pl.col("timestamps")
                .dt.hour()
                .is_in([target_date.hour, target_date.hour + 1])
            )
            & (pl.col("volume") != 0)
        )
        | (
            (pl.col("timestamps").dt.date() == (target_date.date() + timedelta(days=4)))
            & (pl.col("timestamps").dt.hour() == 9)
            & (pl.col("timestamps").dt.minute().is_in([30, 31, 32]))
        )
    )
    print(f"\ndf 中 {target_date} 的时间戳:")
    with pl.Config(tbl_rows=100):
        print(
            df_target_day.select(
                [
                    "ticker",
                    "volume",
                    "open",
                    "close",
                    "high",
                    "low",
                    "timestamps",
                    "transactions",
                ]
            ).tail(8)
        )
