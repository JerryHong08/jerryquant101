from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import redis

from backtesting.backtest_pre_data import only_common_stocks
from utils.longbridge_utils import update_watchlist

r = redis.Redis(host="localhost", port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe("market_snapshot")


print("Analyzer listening...")

for message in pubsub.listen():
    if message["type"] == "message":
        json_data = message["data"]
        df = pl.read_json(json_data)

        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                "America/New_York"
            )
        )
        print("Received snapshot:", df.shape)

        # filter only common stock and rank by percent_change
        updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
            "%Y%m%d%H%M%S"
        )  # 20250930170603
        filter_date = (
            updated_time[:4] + "-" + updated_time[4:6] + "-" + updated_time[6:8]
        )  # 2025-09-30
        selected_tickers = only_common_stocks(filter_date).drop(
            "active", "composite_figi"
        )
        df = selected_tickers.join(df, on="ticker", how="inner").sort(
            "percent_change", descending=True
        )

        with pl.Config(tbl_rows=20, tbl_cols=50):
            print(df.head(20))

        # rank by percent_change, rank change, since exploded, float shares. to be added

        # if new tickers in top 20, update watchlist
        # 选top20更新watchlist
        top_20 = df.select("ticker").to_series().to_list()[:20]
        # update_watchlist(watchlist_name="market_mover", tickers=top_20)
