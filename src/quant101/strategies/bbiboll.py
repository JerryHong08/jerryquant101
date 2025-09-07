import datetime
import glob
import json
import os
import sys

import matplotlib.pyplot as plt
import polars as pl

from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import stock_load_process
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll
from quant101.utils.compute import calculate_signal_duration, prepare_trades


def only_common_stocks():
    all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    tickers = all_tickers.filter(
        (pl.col("type").is_in(["CS", "ADRC"]))
        & (
            (pl.col("active") == True)
            | (
                pl.col("delisted_utc").is_not_null()
                & (
                    pl.col("delisted_utc")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    > datetime.date(2023, 1, 1)
                )
            )
        )
    ).select(pl.col(["ticker", "delisted_utc"]))

    print(f"Using {all_tickers_file}, total {len(tickers)} active tickers")

    return tickers


def compute_signals(data_df, tickers):
    # ticker 上市日 & 退市日
    selected_tickers = tickers.with_columns(
        [
            pl.col("delisted_utc")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.date()
            .alias("delisted_date")
        ]
    )

    # indicator
    bbiboll = calculate_bbiboll(data_df).with_columns(
        (pl.col("volume") * pl.col("close")).alias("turnover")
    )

    with pl.Config(tbl_cols=20, tbl_rows=2000):
        print(bbiboll.filter(pl.col("ticker") == "NVDA"))

    bbiboll = bbiboll.join(
        selected_tickers.select(["ticker", "delisted_date"]), on="ticker", how="left"
    )

    # signals generate
    signals = (
        (
            bbiboll.filter(
                pl.col("bbi").is_not_null()
                & (pl.col("dev_pct") <= 1)
                & (pl.col("timestamps").dt.date() >= datetime.date(2023, 2, 13))
                & (
                    pl.col("delisted_date").is_null()
                    | (pl.col("timestamps").dt.date() <= pl.col("delisted_date"))
                )
                & (pl.col("ticker") == "NVDA")
            )
        )
        .select(["timestamps", "ticker"])
        .with_columns(pl.lit(1).alias("signal"))
    )

    return signals


if __name__ == "__main__":
    # ========Config===========
    # tickers = ['NVDA','TSLA','FIG']
    tickers = only_common_stocks()
    timeframe = "1d"
    data_type = "day_aggs_v1" if timeframe == "1d" else "minute_aggs_v1"
    start_date = "2022-01-01"
    end_date = "2025-09-05"

    # load data
    data_df = stock_load_process(
        tickers=tickers.to_series().to_list(),
        timeframe=timeframe,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
    ).collect()

    print(data_df.shape)
    print(f"Memory size of lf_result: {data_df.estimated_size('mb'):.2f} MB")

    signals = compute_signals(data_df, tickers)

    trades, portfolio = prepare_trades(data_df, signals)

    spx = pl.read_parquet("I:SPXday20150101_20250905.parquet")
    spx = spx.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")  # 从毫秒时间戳转换
        .dt.convert_time_zone("America/New_York")
        .dt.replace(hour=0, minute=0, second=0)
        .cast(pl.Datetime("ns", "America/New_York"))  # 转换为纳秒精度以匹配portfolio
        .alias("date")
    )

    with pl.Config(tbl_cols=20, tbl_rows=500):
        print(f"siganls:{signals}")
        print(f"trades:{trades.head()}")
        print(f"portfolio:{portfolio.head()}")
        print(f"spx:{spx.head()}")

    # 计算SPX基准收益（归一化到起始点）
    spx = spx.with_columns(
        (
            pl.col("close")
            / pl.col("close").first()
            * portfolio["equity_curve"].first()
        ).alias("spx_normalized")
    )

    aligned_data = portfolio.join(
        spx.select(["date", "close", "spx_normalized"]), on="date", how="inner"
    )

    # print(trades)
    plt.plot(
        aligned_data["date"],
        aligned_data["equity_curve"],
        label="Strategy Equity Curve",
        linewidth=2,
    )
    plt.plot(
        aligned_data["date"],
        aligned_data["spx_normalized"],
        label="SPX",
        linewidth=2,
        alpha=0.7,
    )
    plt.title("Equity Curve")
    plt.show()
