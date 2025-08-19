import warnings
from datetime import datetime, timedelta

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
import seolpyo_mplchart as mc

# warnings.filterwarnings("ignore", message=".*Font family.*not found.*")
matplotlib.rcParams["font.family"] = "DejaVu Sans"

# 数据路径（7月份所有 parquet 文件）
# valid columns: ["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]
data_dir = "data/lake/us_stocks_sip/minute_aggs_v1/2025/07/*.parquet"

# splits data
splits_dir = "data/raw/us_stocks_sip/splits/splits.parquet"
splits_error_dir = "data/raw/us_stocks_sip/splits/splits_error.parquet"

splits_original = pl.read_parquet(splits_dir)
splits_errors = pl.read_parquet(splits_error_dir)

splits = splits_original.filter(~pl.col("id").is_in(splits_errors["id"].implode()))

print(splits.sort(["execution_date"]).filter(pl.col("ticker") == "BIVI"))
# splits.head()
# ┌─────────────────────────────────┬────────────────┬────────────┬──────────┬────────┐
# │ id                              ┆ execution_date ┆ split_from ┆ split_to ┆ ticker │
# │ ---                             ┆ ---            ┆ ---        ┆ ---      ┆ ---    │
# │ str                             ┆ str            ┆ f64        ┆ f64      ┆ str    │
# ╞═════════════════════════════════╪════════════════╪════════════╪══════════╪════════╡
# │ Pef962e8ce572df20933cdaac3a2d2… ┆ 1978-10-25     ┆ 2.0        ┆ 3.0      ┆ AMD    │
# │ Pdf33a5344081ff35ae801b4923f1a… ┆ 1979-10-24     ┆ 2.0        ┆ 3.0      ┆ AMD    │
# │ Pfc705a25e20e6ea7c233b413c053a… ┆ 1980-10-23     ┆ 1.0        ┆ 2.0      ┆ AMD    │
# │ P020697a24101807bc62e730e553fb… ┆ 1982-10-27     ┆ 2.0        ┆ 3.0      ┆ AMD    │
# │ Pe8be4f2e92217f0b107d842099c39… ┆ 1983-08-22     ┆ 1.0        ┆ 2.0      ┆ AMD    │
# └─────────────────────────────────┴────────────────┴────────────┴──────────┴────────┘


def splits_adjust(lf, splits, price_decimals: int = 4):
    # 获取数据范围
    date_min = lf.select(pl.col("datetime").min()).collect()[0, 0]
    date_max = lf.select(pl.col("datetime").max()).collect()[0, 0]

    tickers = lf.select(pl.col("ticker").unique()).collect().to_series(0).to_list()

    splits_filtered = splits.filter(
        (pl.col("ticker").is_in(tickers))
        & (
            pl.col("execution_date")
            .str.to_date()
            .is_between(date_min - pl.duration(days=1), date_max + pl.duration(days=1))
        )
    )

    if splits_filtered.is_empty():
        lf = lf.with_columns(
            [
                pl.col("open").alias("open_adj"),
                pl.col("high").alias("high_adj"),
                pl.col("low").alias("low_adj"),
                pl.col("close").alias("close_adj"),
                pl.col("volume").alias("volume_adj"),
            ]
        )
    else:
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
        lf_adj = (
            lf.with_columns(pl.col("datetime").dt.date().alias("date_only"))
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
                    .alias("open_adj"),
                    (pl.col("high") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("high_adj"),
                    (pl.col("low") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("low_adj"),
                    (pl.col("close") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("close_adj"),
                    (pl.col("volume") / pl.col("factor"))
                    .round(0)
                    .cast(pl.Int64)
                    .alias("volume_adj"),
                ]
            )
            .drop("date_only")
        )

    return lf_adj


lf = pl.scan_parquet(data_dir).with_columns(
    pl.from_epoch(pl.col("window_start"), time_unit="ns")
    .dt.convert_time_zone("America/New_York")
    .alias("datetime")
)
# .filter(pl.col("ticker") == "BIVI")

# splits event process for historical data
lf_adj = splits_adjust(lf, splits, price_decimals=4)

df_test = lf.filter(pl.col("ticker") == "BIVI").collect()
df_adj_test = (
    lf_adj.filter(pl.col("ticker") == "BIVI")
    .select(
        [
            "ticker",
            "volume_adj",
            "open_adj",
            "high_adj",
            "low_adj",
            "close_adj",
            "transactions",
            "datetime",
        ]
    )
    .sort(["datetime"])
    .collect()
)

# print(f"before head:{df_test.head(1)}")
# print(f"before tail:{df_test.tail(1)}")

# print(f"after head:{df_adj_test.head(1)}")
# print(f"after tail:{df_adj_test.tail(1)}")


def resample_ohlcv(df, timeframe):
    """
    将分钟数据重采样到指定时间框架
    timeframe: '5m', '15m', '30m', '1h', '4h', '1d' 等
    """
    return (
        df.sort("datetime")
        .group_by_dynamic("datetime", every=timeframe, closed="left")  # 左闭合区间
        .agg(
            [
                pl.col("open_adj").first().alias("open"),
                pl.col("high_adj").max().alias("high"),
                pl.col("low_adj").min().alias("low"),
                pl.col("close_adj").last().alias("close"),
                pl.col("volume_adj").sum().alias("volume"),
                pl.col("transactions").sum().alias("transactions"),
            ]
        )
        .filter(pl.col("volume") > 0)  # 过滤掉没有交易的时间段
    )


# ----------------------------------------
# 选择要聚合的时间框架
timeframe = "15m"  # 可以改为: "5m", "15m", "30m", "1h", "4h", "1d"

# 重采样数据
df_resampled = resample_ohlcv(df_adj_test, timeframe).to_pandas()

# -----------------PLOT-------------------
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
c.watermark = f'{df_adj_test.select(pl.col("ticker").unique()).to_series().to_list()[0]}-{timeframe}'  # watermark

c.date = "datetime"
c.Open = "open"
c.high = "high"
c.low = "low"
c.close = "close"
c.volume = "volume"

c.set_data(df_resampled)

mc.show()  # same as matplotlib.pyplot.show()
