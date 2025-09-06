import datetime
import glob
import json
import os
import sys

import matplotlib.pyplot as plt
import polars as pl

from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import data_loader
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll
from quant101.utils.compute import calculate_signal_duration, prepare_trades

if __name__ == "__main__":
    # tickers = ['NVDA','TSLA','FIG']
    tickers = None
    timeframe = "1d"
    asset = "us_stocks_sip"
    data_type = "day_aggs_v1" if timeframe == "1d" else "minute_aggs_v1"
    start_date = "2022-01-01"
    end_date = "2025-09-04"
    full_hour = False

    all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    tickers = (
        all_tickers.filter(
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
        )
        .select(pl.col("ticker"))
        .to_series()
        .to_list()
    )
    print(f"Using {all_tickers_file}, total {len(tickers)} active tickers")

    # ticker 上市日 & 退市日
    all_tickers = all_tickers.with_columns(
        [
            pl.col("delisted_utc")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.date()
            .alias("delisted_date")
        ]
    )

    lf_result = data_loader(
        tickers=tickers,
        timeframe=timeframe,
        asset=asset,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        full_hour=full_hour,
        use_cache=True,  # 启用缓存
    ).collect()

    print(lf_result.shape)
    print(f"Memory size of lf_result: {lf_result.estimated_size('mb'):.2f} MB")

    # 只选择当天有成交记录的股票进行计算
    last_date = lf_result.select(pl.col("timestamps")).max().item()
    tickers_with_data = (
        lf_result.filter(pl.col("timestamps") == last_date)
        .select(pl.col("ticker"))
        .unique()
    )
    print(tickers_with_data.shape)

    bbiboll = calculate_bbiboll(
        lf_result.filter(pl.col("ticker").is_in(tickers_with_data["ticker"].to_list()))
    ).with_columns((pl.col("volume") * pl.col("close")).alias("turnover"))

    bbiboll = bbiboll.join(
        all_tickers.select(["ticker", "delisted_date"]), on="ticker", how="left"
    )

    signals = (
        (
            # bbiboll.filter(pl.col('ticker') == 'NVDA' & pl.col('bbi').is_not_null())
            bbiboll.filter(
                pl.col("bbi").is_not_null()
                & (pl.col("dev_pct") <= 1)
                & (pl.col("timestamps").dt.date() >= datetime.date(2023, 1, 1))
                & (
                    pl.col("delisted_date").is_null()
                    | (pl.col("timestamps").dt.date() <= pl.col("delisted_date"))
                )
            )
        )
        .select(["timestamps", "ticker"])
        .with_columns(pl.lit(1).alias("signal"))
    )

    result = signals.join(
        all_tickers.select(["ticker", "type", "primary_exchange", "active"]),
        on="ticker",
        how="left",
    )
    # ).filter(pl.col('ticker') == 'MLGO')

    # signal_durations, avg_durations = calculate_signal_duration(signals)

    # with pl.Config(tbl_cols=20, tbl_rows=500):
    #     print(result)

trades, portfolio = prepare_trades(lf_result, result)

print(trades)
plt.plot(portfolio["date"], portfolio["equity_curve"])
plt.title("Equity Curve")
plt.show()
