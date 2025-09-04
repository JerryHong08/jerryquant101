import datetime

import polars as pl

from quant101.core_2.data_loader import data_loader
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll

if __name__ == "__main__":
    # tickers = ['NVDA','TSLA','FIG']
    tickers = None
    timeframe = "1d"  # timeframe: '1m', '3m', '5m', '10m', '15m', '20m', '30m', '45m', '1h', '2h', '3h', '4h', '1d' 等
    asset = "us_stocks_sip"
    data_type = "day_aggs_v1" if timeframe == "1d" else "minute_aggs_v1"
    start_date = "2022-01-01"
    end_date = "2025-08-10"
    full_hour = False
    # plot = True
    plot = False
    ticker_plot = "NVDA"

    lf_result = data_loader(
        tickers=tickers,
        timeframe=timeframe,
        asset=asset,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        full_hour=full_hour,
    ).collect()

    last_date = lf_result.select(pl.col("timestamps")).max().item()
    tickers_with_data = (
        lf_result.filter(pl.col("timestamps") == last_date)
        .select(pl.col("ticker"))
        .unique()
    )

    print(tickers_with_data)

    bbiboll = calculate_bbiboll(
        lf_result.filter(pl.col("ticker").is_in(tickers_with_data["ticker"].to_list()))
    )

    print(bbiboll.shape)

    result = (
        # bbiboll.filter(pl.col('ticker') == 'NVDA' & pl.col('bbi').is_not_null())
        bbiboll.filter(
            (pl.col("timestamps").dt.date() == datetime.date(2025, 8, 8))
            & pl.col("bbi").is_not_null()
            & (pl.col("dev_pct") <= 20)
        ).select(
            [
                col
                for col in bbiboll.columns
                if col
                not in [
                    "open",
                    "high",
                    "low",
                    "split_date",
                    "split_ratio",
                    "window_start",
                ]
            ]
        )
        # .sort('timestamps')
        .sort("dev_pct")
        # .head(20)
    )

    result.write_csv("bbiboll_20250808.csv")

    with pl.Config(tbl_cols=20, tbl_rows=2000):
        print(result)

    if plot:
        plot_candlestick(
            lf_result.filter(pl.col("ticker") == "NVDA").to_pandas(),
            ticker_plot,
            timeframe,
        )
