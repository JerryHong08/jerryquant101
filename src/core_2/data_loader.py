import glob
import hashlib
import json
import os
from datetime import datetime, time, timedelta

import duckdb
import exchange_calendars as xcals
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas
import polars as pl
import s3fs
from dotenv import load_dotenv

from core_2.config import data_dir, splits_data
from core_2.plotter import plot_candlestick
from utils.tickers_name_alignment_polars import get_mapped_tickers

load_dotenv()

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

# 创建 S3 文件系统对象
fs = s3fs.S3FileSystem(
    key=ACCESS_KEY_ID,
    secret=SECRET_ACCESS_KEY,
    endpoint_url="https://files.polygon.io",
    client_kwargs={"region_name": "us-east-1"},
)

all_tickers = get_mapped_tickers().lazy()


def s3_data_dir_calculate(asset: str, data_type: str, start_date: str, end_date: str):
    """
    Calculate data directory paths based on asset type, timeframe, date range.

    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'

    Returns:
        List of S3 file paths
    """

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    data_dirs = []

    for y in range(start_dt.year, end_dt.year + 1):
        start_month = start_dt.month if y == start_dt.year else 1
        end_month = end_dt.month if y == end_dt.year else 12

        for m in range(start_month, end_month + 1):
            month_str = f"{m:02d}"  # Format as two digits
            # S3 路径
            s3_prefix = f"flatfiles/{asset}/{data_type}/{y}/{month_str}/"
            file_extension = "csv.gz"

            print(f"Searching in S3 path: {s3_prefix}")

            try:
                # 列出该路径下的所有文件
                all_files = fs.ls(s3_prefix)
                # 过滤出正确扩展名的文件
                month_all_file = [
                    f"s3://{f}" for f in all_files if f.endswith(file_extension)
                ]

                print(f"Found {len(month_all_file)} files")

                if m in [start_month, end_month]:
                    filtered_files = []
                    for file in month_all_file:
                        # 从 s3://flatfiles/... 中提取文件名
                        file_name = os.path.basename(file).split(".")[0]
                        try:
                            file_date = datetime.strptime(file_name, "%Y-%m-%d").date()
                        except ValueError:
                            continue  # skip files that don't match the date format
                        if (m == start_month and file_date < start_dt.date()) or (
                            m == end_month and file_date > end_dt.date()
                        ):
                            continue  # skip files outside the range
                        filtered_files.append(file)
                    data_dirs.extend(filtered_files)
                else:
                    data_dirs.extend(month_all_file)
            except Exception as e:
                print(f"Error accessing S3 path {s3_prefix}: {e}")
                continue

    return data_dirs


def data_dir_calculate(
    asset: str, data_type: str, start_date: str, end_date: str, lake: bool = True
):
    """
    Calculate data directory paths based on asset type, timeframe, date range.

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
            file_pattern = f"{data_dir}/{'lake' if lake else 'raw'}/{asset}/{data_type}/{y}/{month_str}/*.{'parquet' if lake else 'csv.gz'}"
            month_all_file = glob.glob(file_pattern)
            if m in [start_month, end_month]:
                filtered_files = []
                for file in month_all_file:
                    _, file_name = os.path.split(file)
                    file_name = file_name.split(".")[0]
                    try:
                        file_date = datetime.strptime(file_name, "%Y-%m-%d").date()
                    except ValueError:
                        continue  # skip files that don't match the date format
                    if (m == start_month and file_date < start_dt.date()) or (
                        m == end_month and file_date > end_dt.date()
                    ):
                        continue  # skip files outside the range
                    filtered_files.append(file)
                data_dirs.extend(filtered_files)
            else:
                data_dirs.extend(month_all_file)
    return data_dirs


def generate_full_timestamp(start_date, end_date, timeframe, full_hour: bool = False):
    """
    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: Start date in format 'YYYY-MM-DD'
        timeframe: '1m', '5m', '15m', '30m', '1h', '1d'
        full_hour: For intraday data, whether to generate full hours (4:00-20:00) or only regular trading hours (9:30-16:00)
    Return:
        Schema([('timestamps', Datetime(time_unit='us', time_zone='America/New_York'))])
    """

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

    if timeframe == "1d":

        # 直接使用交易所日历中的交易日，而不是生成连续日期
        trading_days_df = df_schedule.select(
            pl.col("open").dt.date().alias("trade_date")
        )

        # 创建时间戳DataFrame，使用交易日
        generated_trade_timestamp = trading_days_df.with_columns(
            pl.col("trade_date")
            .cast(pl.Datetime)
            .dt.replace(hour=0, minute=0, second=0, microsecond=0)
            .dt.replace_time_zone("America/New_York")
            .alias("timestamps")
        ).select("timestamps")

    else:
        # 为每个交易日生成时间戳
        all_timestamps = []

        for row in df_schedule.iter_rows(named=True):
            is_half_day = row["close"].hour == 13

            # 确定时间段
            if not full_hour:
                time_segments = [(row["open"], row["close"])]
            elif is_half_day:
                # 半日分两段
                time_segments = [
                    (
                        row["open"].replace(hour=4, minute=0, second=0),
                        row["open"].replace(hour=13, minute=0, second=0),
                    ),
                    (
                        row["open"].replace(hour=16, minute=0, second=0),
                        row["open"].replace(hour=17, minute=0, second=0),
                    ),
                ]
            else:
                # 正常全天
                time_segments = [
                    (
                        row["open"].replace(hour=4, minute=0, second=0),
                        row["close"].replace(hour=20, minute=0, second=0),
                    )
                ]

            # 生成所有时间段的时间戳
            for start_time, end_time in time_segments:
                timestamps = (
                    pl.select(
                        pl.datetime_range(
                            start_time,
                            end_time,
                            interval="1m",
                            closed="left",
                            time_unit="ns",
                        ).alias("timestamps")
                    )
                    .get_column("timestamps")
                    .to_list()
                )
                all_timestamps.extend(timestamps)

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


def resample_ohlcv(lf, timeframe):
    """
    手动resample美股不同交易时段，不受DST影响，支持盘前盘后交易。
    基于时间桶的方法而非group_by_dynamic。
    """
    import re

    def parse_timeframe(tf):
        """解析时间间隔，返回分钟数"""
        match = re.match(r"(\d+)([mhd])", tf.lower())
        if not match:
            raise ValueError(f"Invalid timeframe: {tf}")

        value, unit = int(match.group(1)), match.group(2)

        if unit == "m":
            return value
        elif unit == "h":
            return value * 60
        elif unit == "d":
            return value * 24 * 60
        else:
            raise ValueError(f"Unsupported unit: {unit}")

    def get_session_start(timestamp_col, session_type="regular"):
        """获取不同交易时段的起始时间"""
        if session_type == "regular":
            # 正常交易时间 9:30
            return pl.concat_str(
                [timestamp_col.dt.date().cast(pl.Utf8), pl.lit(" 09:30:00")]
            ).str.strptime(pl.Datetime(time_zone="America/New_York"), strict=True)

        elif session_type == "premarket":
            # 盘前交易 4:00
            return pl.concat_str(
                [timestamp_col.dt.date().cast(pl.Utf8), pl.lit(" 04:00:00")]
            ).str.strptime(pl.Datetime(time_zone="America/New_York"), strict=True)

        elif session_type == "afterhours":
            # 盘后交易 16:00
            return pl.concat_str(
                [timestamp_col.dt.date().cast(pl.Utf8), pl.lit(" 16:00:00")]
            ).str.strptime(pl.Datetime(time_zone="America/New_York"), strict=True)

    bar_minutes = parse_timeframe(timeframe)
    print(f"Resampling to {bar_minutes} minutes intervals")

    lf = lf.sort(["ticker", "timestamps"])

    # 交易日（按纽约时间）
    lf = lf.with_columns(
        [
            pl.col("timestamps").dt.date().alias("trade_date"),
            pl.col("timestamps").dt.hour().alias("hour"),
            pl.col("timestamps").dt.minute().alias("minute"),
        ]
    )

    # 识别交易时段
    lf = lf.with_columns(
        [
            pl.when((pl.col("hour") >= 4) & (pl.col("hour") < 9))
            .then(pl.lit("premarket"))
            .when((pl.col("hour") == 9) & (pl.col("minute") < 30))
            .then(pl.lit("premarket"))
            .when((pl.col("hour") == 9) & (pl.col("minute") >= 30))
            .then(pl.lit("regular"))
            .when((pl.col("hour") > 9) & (pl.col("hour") < 16))
            .then(pl.lit("regular"))
            .when((pl.col("hour") >= 16) & (pl.col("hour") < 20))
            .then(pl.lit("afterhours"))
            .otherwise(pl.lit("other"))
            .alias("session")
        ]
    )
    # 为每个交易时段计算相对于该时段起始的分钟数和桶编号
    lf = lf.with_columns(
        [
            # 盘前时段：从4:00开始计算
            pl.when(pl.col("session") == "premarket")
            .then(
                (
                    (
                        pl.col("timestamps")
                        - get_session_start(pl.col("timestamps"), "premarket")
                    )
                    .dt.total_minutes()
                    .cast(pl.Int64)
                )
            )
            # 正常时段：从9:30开始计算
            .when(pl.col("session") == "regular")
            .then(
                (
                    (
                        pl.col("timestamps")
                        - get_session_start(pl.col("timestamps"), "regular")
                    )
                    .dt.total_minutes()
                    .cast(pl.Int64)
                )
            )
            # 盘后时段：从16:00开始计算
            .when(pl.col("session") == "afterhours")
            .then(
                (
                    (
                        pl.col("timestamps")
                        - get_session_start(pl.col("timestamps"), "afterhours")
                    )
                    .dt.total_minutes()
                    .cast(pl.Int64)
                )
            )
            .otherwise(pl.lit(0))
            .alias("minutes_from_session_start")
        ]
    )

    # 计算桶编号（每个时段独立计算）
    lf = lf.with_columns(
        [(pl.col("minutes_from_session_start") // bar_minutes).alias("bar_id")]
    )
    # 计算每个桶的起始时间
    lf = lf.with_columns(
        [
            pl.when(pl.col("session") == "premarket")
            .then(
                get_session_start(pl.col("timestamps"), "premarket")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .when(pl.col("session") == "regular")
            .then(
                get_session_start(pl.col("timestamps"), "regular")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .when(pl.col("session") == "afterhours")
            .then(
                get_session_start(pl.col("timestamps"), "afterhours")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .otherwise(pl.col("timestamps"))  # fallback
            .alias("bar_start")
        ]
    )

    # 按时段、日期、桶编号分组聚合
    resampled = (
        lf.group_by(["ticker", "trade_date", "session", "bar_id", "bar_start"])
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
        .sort(["ticker", "trade_date", "bar_start"])
        .drop(["trade_date", "session", "bar_id"])  # 清理辅助列
        .rename({"bar_start": "timestamps"})
    )

    return resampled.lazy()


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

    # filter out the data that need to be adjusted
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
            .group_by(["ticker", "split_date"])
            .agg(  # this for some tickers like ["SESN", "CARM"] tickers changed name and reversed splits twice in a day.
                pl.col("cumulative_split_ratio").last().alias("cumulative_split_ratio"),
            )
            .sort(["ticker", "split_date"])
        )

        print(splits_with_factor.head())

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


def generate_cache_key(
    tickers, timeframe, asset, data_type, start_date, end_date, full_hour
):
    """Generate a unique cache key based on parameters"""
    cache_params = {
        "tickers": sorted([t for t in tickers if t is not None]) if tickers else None,
        "timeframe": timeframe,
        "asset": asset,
        "data_type": data_type,
        "start_date": start_date,
        "end_date": end_date,
        "full_hour": full_hour,
    }

    # Convert to JSON string and create hash
    params_str = json.dumps(cache_params, sort_keys=True, default=str)
    cache_key = hashlib.md5(params_str.encode()).hexdigest()
    return cache_key


def get_cache_path(asset, data_type, cache_key):
    """Get the cache file path"""
    cache_dir = os.path.join(data_dir, "processed", asset, data_type)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"cache_{cache_key}.parquet")


def save_cache_metadata(cache_path, params):
    """Save cache metadata for debugging"""
    metadata_path = cache_path.replace(".parquet", "_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(params, f, indent=2, default=str)


def tickers_alignment(tickers):
    tickers = tickers.join(
        all_tickers.select(
            ["ticker", "tickers", "group_id", "latest_ticker", "all_delisted_utc"]
        ).filter(pl.col("group_id").is_not_null()),
        on="ticker",
        how="left",
    )
    return tickers


def data_loader(
    asset: str = "us_stocks_sip",
    data_type: str = "day_aggs_v1",
    start_date: str = "",
    end_date: str = "",
    use_s3: bool = False,
    duck_db=False,
):
    if not duck_db:
        if use_s3:
            lake_file_paths = s3_data_dir_calculate(
                asset=asset,
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            lake_file_paths = data_dir_calculate(
                asset=asset,
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                # lake=False
            )

        print("1. data path loaded.")

        # local lake/*/.parquet
        if all(f.endswith(".parquet") for f in lake_file_paths):
            lf = pl.scan_parquet(lake_file_paths)
        # s3 */.csv.gz
        elif use_s3:
            lf = pl.scan_csv(
                lake_file_paths,
                storage_options={
                    "aws_access_key_id": ACCESS_KEY_ID,
                    "aws_secret_access_key": SECRET_ACCESS_KEY,
                    "aws_endpoint": "https://files.polygon.io",
                    "aws_region": "us-east-1",
                },
            )
        # local raw/*/.csv.gz
        else:
            lf = pl.scan_csv(lake_file_paths)
    else:
        # 连接数据库（内存模式）
        con = duckdb.connect()
        if use_s3:
            # 加载 DuckDB 的 S3 插件
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")

            # 配置 S3 连接参数
            con.execute("SET s3_region='us-east-1';")
            con.execute("SET s3_endpoint='files.polygon.io';")
            # -- 重点：Polygon flat files 的 endpoint
            con.execute(f"SET s3_access_key_id='{ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{SECRET_ACCESS_KEY}';")
            con.execute("SET s3_url_style='path';")
            # -- 避免走 virtual-host 风格 URL

            # 直接在远程 S3 上跑 SQL

            # Calculate the appropriate file patterns based on date range
            s3_paths = s3_data_dir_calculate(asset, data_type, start_date, end_date)

            query = f"""
            SELECT *
            FROM read_csv_auto({s3_paths})
            ;
            """
        else:
            local_paths = data_dir_calculate(
                asset, data_type, start_date, end_date, lake=True
            )
            # Format as array for DuckDB
            local_paths_array = "['" + "', '".join(local_paths) + "']"
            query = f"""
            SELECT *
            FROM read_parquet({local_paths_array})
            ;
            """
        print(query)
        lf = con.execute(query).fetchdf()

    return lf


def splits_figi_alignment(df):
    print("Processing figi alignment...")
    df = (
        df.lazy()
        .join(
            all_tickers.select(["ticker", "group_id", "latest_ticker"]),
            on="ticker",
            how="left",
        )
        .drop("ticker")
        .rename({"latest_ticker": "ticker"})
    )

    aligned_lf = df

    return aligned_lf.drop("group_id")


def ohlcv_figi_alignment(lf):
    """
    增强版的 FIGI 对齐函数，处理同一 group_id 下多个 ticker 的重叠数据
    """
    # 首先添加 group_id 和 ticker 顺序信息
    lf = lf.join(
        all_tickers.select(
            [
                "ticker",
                "group_id",
                "latest_ticker",
                "tickers",
                "all_last_updated_utc",
                "all_delisted_utc",
            ]
        ),
        on="ticker",
        how="left",
    )

    # 找出有多个 ticker 的 group_id 组
    multi_ticker_groups = (
        lf.group_by("group_id")
        .agg(
            [
                pl.col("ticker").n_unique().alias("ticker_count"),
                pl.col("ticker").unique().alias("unique_tickers"),
            ]
        )
        .filter(pl.col("ticker_count") > 1)
        .select("group_id")
    )

    # 分离单 ticker 和多 ticker 的数据
    single_ticker_data = lf.join(
        multi_ticker_groups,
        on="group_id",
        how="anti",  # 反连接，获取不在多ticker组中的数据
    )

    multi_ticker_data = lf.join(
        multi_ticker_groups,
        on="group_id",
        how="semi",  # 半连接，获取在多ticker组中的数据
    )

    # 处理多 ticker 组的重叠数据
    def process_multi_ticker_group(group_df):
        """处理单个 group_id 组内的重叠数据"""

        # 直接从预排序的 tickers 列表中获取顺序
        first_row = group_df.row(0)
        ticker_order = first_row[group_df.columns.index("tickers")]  # 获取 tickers 列表
        last_updated_list = group_df.select("all_last_updated_utc").row(0)[0]
        delisted_list = group_df.select("all_delisted_utc").row(0)[0]

        # print(f"Ticker 顺序: {ticker_order}")  # 调试信息

        processed_data = []
        last_end_date = None

        for i, ticker in enumerate(ticker_order):
            ticker_data = group_df.filter(pl.col("ticker") == ticker).sort("timestamps")

            if ticker_data.height == 0:
                continue

            # 取 cutoff
            lu = last_updated_list[i]
            de = delisted_list[i]

            cutoff_candidates = [d for d in (lu, de) if d is not None]
            cutoff = min(cutoff_candidates) if cutoff_candidates else None
            cutoff = (
                datetime.fromisoformat(cutoff.replace("Z", "+00:00")).date()
                if cutoff is not None
                else None
            )
            if last_end_date is not None:
                # 移除与前一个 ticker 重叠的数据
                ticker_data = ticker_data.filter(
                    pl.col("timestamps").dt.date() > last_end_date
                )

            if cutoff is not None:
                ticker_data = ticker_data.filter(
                    pl.col("timestamps").dt.date() <= cutoff
                )

            if ticker_data.height > 0:
                processed_data.append(ticker_data)
                # 更新最后日期
                last_end_date = ticker_data.select(
                    pl.col("timestamps").dt.date().max()
                ).item()

        # 合并所有处理后的数据
        if processed_data:
            combined = pl.concat(processed_data)
            # 统一使用 latest_ticker
            latest_ticker = group_df.select("latest_ticker").row(0)[0]
            combined = combined.with_columns(pl.lit(latest_ticker).alias("ticker"))
            return combined
        else:
            return pl.DataFrame(schema=group_df.schema)

    # 对每个多 ticker 组应用处理函数
    multi_ticker_data_collected = multi_ticker_data.collect()
    if multi_ticker_data_collected.height > 0:
        processed_groups = []

        for group_id in (
            multi_ticker_data_collected.select("group_id").unique().to_series()
        ):
            group_data = multi_ticker_data_collected.filter(
                pl.col("group_id") == group_id
            )
            processed_group = process_multi_ticker_group(group_data)
            if processed_group.height > 0:
                processed_groups.append(processed_group)

        if processed_groups:
            processed_multi_ticker_data = pl.concat(processed_groups)
        else:
            processed_multi_ticker_data = pl.DataFrame(
                schema=multi_ticker_data_collected.schema
            )
    else:
        processed_multi_ticker_data = pl.DataFrame(schema=lf.collect_schema())

    # 处理单 ticker 数据（简单重命名）
    single_ticker_data_collected = single_ticker_data.collect()
    if single_ticker_data_collected.height > 0:
        processed_single_ticker_data = (
            single_ticker_data_collected.with_columns(
                pl.col("latest_ticker").alias("new_ticker")
            )
            .drop("ticker")
            .rename({"new_ticker": "ticker"})
        )
    else:
        processed_single_ticker_data = pl.DataFrame(schema=lf.schema)

    # 合并所有数据
    if (
        processed_multi_ticker_data.height > 0
        and processed_single_ticker_data.height > 0
    ):
        # print(processed_multi_ticker_data.head())
        # print(processed_single_ticker_data.head())
        # Ensure both DataFrames have the same column order
        common_columns = [
            col
            for col in processed_single_ticker_data.columns
            if col in processed_multi_ticker_data.columns
        ]
        processed_single_ticker_data = processed_single_ticker_data.select(
            common_columns
        )
        processed_multi_ticker_data = processed_multi_ticker_data.select(common_columns)
        final_data = pl.concat(
            [processed_single_ticker_data, processed_multi_ticker_data]
        )
    elif processed_multi_ticker_data.height > 0:
        final_data = processed_multi_ticker_data
    elif processed_single_ticker_data.height > 0:
        final_data = processed_single_ticker_data
    else:
        final_data = pl.DataFrame(schema=lf.schema)

    # 清理列
    columns_to_keep = [
        col
        for col in final_data.columns
        if col
        not in [
            "group_id",
            "latest_ticker",
            "tickers",
            "all_last_updated_utc",
            "all_delisted_utc",
        ]
    ]

    return final_data.select(columns_to_keep).sort(["ticker", "timestamps"])


def stock_load_process(
    tickers: str = None,
    timeframe: str = "1d",
    asset: str = "us_stocks_sip",
    data_type: str = "day_aggs_v1",
    start_date: str = "",
    end_date: str = "",
    full_hour: bool = False,
    use_s3: bool = False,
    use_cache: bool = True,
    duck_db: bool = False,
):
    aligned_tickers = tickers_alignment(pl.DataFrame({"ticker": tickers}).lazy())

    skipped = (
        pl.read_csv("low_volume_tickers.csv", truncate_ragged_lines=True)
        .filter(
            (pl.col("max_duration_days") > 50) | (pl.col("avg_turnover") < 60000),
        )
        .select(pl.col("ticker").unique())
    )

    skipped = tickers_alignment(skipped.lazy())

    print(
        f"there are {len(skipped.collect())} tickers to skip due to specious low volume."
    )
    aligned_tickers = aligned_tickers.join(skipped, on="ticker", how="anti")

    tickers = (
        aligned_tickers.select("tickers")
        .collect()
        .to_series()
        .explode()
        .unique()
        .to_list()
    )

    # Generate cache key
    cache_key = generate_cache_key(
        tickers, timeframe, asset, data_type, start_date, end_date, full_hour
    )
    cache_path = get_cache_path(asset, data_type, cache_key)

    # Check if cache exists and use_cache is True
    if use_cache and os.path.exists(cache_path):
        print(f"Loading from cache: {cache_path}")
        try:
            cached_data = pl.scan_parquet(cache_path)
            print("Cache loaded successfully.")
            print(f"Cache Size: {cached_data.collect().estimated_size('mb'):.2f} MB")
            print(f"Cache rows: {len(cached_data.collect()):,}")
            return cached_data
        except Exception as e:
            print(f"Failed to load cache: {e}, proceeding with normal data loading...")

    print("Processing data from source...")

    # 解析timeframe以确定数据源
    import re

    match = re.match(r"(\d+)([mhd]|w|mo|q|y)", timeframe.lower())
    if not match:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    value, unit = int(match.group(1)), match.group(2)
    is_daily_or_above = unit in ["d", "w", "mo", "q", "y"]

    lf = data_loader(
        asset=asset,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        use_s3=use_s3,
        duck_db=duck_db,
    )

    lf = lf.with_columns(
        pl.from_epoch(pl.col("window_start"), time_unit="ns")
        .dt.convert_time_zone("America/New_York")
        .alias("timestamps")
    ).sort("ticker", "timestamps")

    if tickers == None:
        tickers = lf.select("ticker").unique().collect()["ticker"]
        print(f"All tickers count: {len(tickers)}")
    elif len(tickers) == 0:
        raise ValueError("Tickers list is empty after alignment.")
    else:
        print(f"Selected tickers count: {len(tickers)}")
        lf = lf.filter(pl.col("ticker").is_in(tickers))

    print("2. data loaded.")

    lf = ohlcv_figi_alignment(lf).lazy()
    splits_data_figi = splits_figi_alignment(splits_data)
    # print(splits_data_figi.filter(pl.col("ticker").is_in(['TNFA'])).sort("ticker").head(10))

    lf = splits_adjust(lf.lazy(), splits_data_figi.collect(), price_decimals=4)

    print("3. splits adjusted.")

    ticker_date_ranges = (
        lf.group_by("ticker")
        .agg(
            [
                pl.col("timestamps").min().alias("first_trade_time"),
                pl.col("timestamps").max().alias("last_trade_time"),
            ]
        )
        .collect()
    )

    all_time_ranges = []

    for row in ticker_date_ranges.iter_rows(named=True):
        ticker_name = row["ticker"]
        first_time = row["first_trade_time"].strftime("%Y-%m-%d")
        last_time = row["last_trade_time"].strftime("%Y-%m-%d")

        if is_daily_or_above:
            generated_timestamp = generate_full_timestamp(
                first_time, last_time, timeframe="1d", full_hour=full_hour
            )
        else:
            generated_timestamp = generate_full_timestamp(
                first_time, last_time, timeframe="1m", full_hour=full_hour
            )

        ticker_time_range = pl.DataFrame(
            {
                "ticker": [ticker_name] * len(generated_timestamp),
                "timestamps": generated_timestamp["timestamps"].to_list(),
            }
        )

        all_time_ranges.append(ticker_time_range)

    print("4. timeframe prepare 1.")

    if all_time_ranges:
        time_range_lf = (
            pl.concat(all_time_ranges)
            .with_columns(pl.col("timestamps").dt.cast_time_unit("ns"))
            .lazy()
        )

    # 先在1 min timeframe上forward fill，再resample到目标timeframe
    # 所有操作都在 LazyFrame 中进行
    lf_full = (
        time_range_lf.join(lf, on=["ticker", "timestamps"], how="left")
        .with_columns([pl.col("close").forward_fill().alias("close_filled")])
        .with_columns(
            [
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
                pl.col("volume").fill_null(0),
                pl.col("transactions").fill_null(0),
            ]
        )
        .drop("close_filled")
    )

    print("6. timeframe fillna.")

    if timeframe not in ("1m", "1d"):
        lf_full = resample_ohlcv(lf_full, timeframe)

    print("7. resample done.")

    # lf_full = lf_full.drop(["split_date", "window_start", "split_ratio"])

    # Save to cache if use_cache is True
    # if use_cache and not use_s3:
    try:
        print(f"Saving to cache: {cache_path}")
        data = lf_full.collect()
        data.write_parquet(cache_path)

        print(f"Cache Size: {data.estimated_size('mb'):.2f} MB")
        print(f"Cache rows: {len(data):,}")

        # Save metadata for debugging
        cache_params = {
            "tickers": tickers,
            "timeframe": timeframe,
            "asset": asset,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date,
            "full_hour": full_hour,
            "cache_key": cache_key,
            "created_at": datetime.now().isoformat(),
        }
        save_cache_metadata(cache_path, cache_params)
        print("Cache saved successfully.")

        # Return lazy frame of cached data
        return pl.scan_parquet(cache_path)
    except Exception as e:
        print(f"Failed to save cache: {e}, returning processed data...")

    return lf_full


if __name__ == "__main__":
    tickers = ["VIA"]
    # tickers = ['LCID','TNFA', 'MYMD', 'NVDA', 'FFIE', 'FFAI']
    # tickers = None
    with pl.Config(tbl_cols=50, tbl_width_chars=1000):
        print(all_tickers.filter(pl.col("ticker") == tickers[0]).collect())

    timeframe = "1d"  # timeframe: '1m', '3m', '5m', '10m', '15m', '20m', '30m', '45m', '1h', '2h', '3h', '4h', '1d' 等
    asset = "us_stocks_sip"
    data_type = "day_aggs_v1" if timeframe == "1d" else "minute_aggs_v1"
    start_date = "2015-01-01"
    end_date = "2025-09-19"
    full_hour = False
    plot = True
    # plot = False
    ticker_plot = tickers[0]

    lf_result = stock_load_process(
        tickers=tickers,
        timeframe=timeframe,
        asset=asset,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        full_hour=full_hour,
        use_cache=False,
    ).collect()

    # print(lf_result.filter(pl.col('timestamps').cast(pl.Datetime("us")).is_between(pl.datetime(2024, 6, 3), pl.datetime(2024, 6, 11))))

    with pl.Config(tbl_cols=50):
        print(
            lf_result.filter(
                (pl.col("ticker") == ticker_plot)
                # & (pl.col("timestamps").dt.date().is_between(pl.date(2023, 3, 6), pl.date(2023, 3, 11)))
            )
        )
        # print(lf_result.filter(pl.col("ticker") == ticker_plot).tail())

    if plot:
        plot_candlestick(
            lf_result.filter(pl.col("ticker") == ticker_plot).to_pandas(),
            ticker_plot,
            timeframe,
        )

    # from strategies.indicators.obv_indicator import calculate_obv
    # print(calculate_obv(lf_result).sort('timestamps').tail())
