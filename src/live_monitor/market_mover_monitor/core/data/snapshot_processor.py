from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from utils.backtest_utils.backtest_utils import only_common_stocks


class snapshot_processor:
    def __init__(self):
        pass

    def data_process(self, df):
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                "America/New_York"
            )
        )

        # Filter only common stocks and sort by percent_change
        updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
            "%Y%m%d%H%M%S"
        )

        filter_date = f"{updated_time[:4]}-{updated_time[4:6]}-{updated_time[6:8]}"
        try:
            df = (
                only_common_stocks(filter_date)
                .drop("active", "composite_figi")
                .join(df, on="ticker", how="inner")
                .sort("percent_change", descending=True)
            )
        except Exception as e:
            print(f"Error filtering common stocks: {e}")
            # Fallback: just sort by percent_change
            df = df.sort("percent_change", descending=True)

        if len(self.last_df) != len(df):
            filled_df = (
                pl.concat([self.last_df, df], how="vertical")
                .sort("timestamp")
                .group_by(["ticker"])
                .agg(
                    pl.col("timestamp").last(),
                    pl.col("current_price").last(),
                    pl.col("percent_change").last(),
                    pl.col("accumulated_volume").last(),
                    pl.col("prev_close").last(),
                    pl.col("prev_volume").last(),
                )
            ).sort("percent_change", descending=True)
            print(
                f"df need fullfilled: fullfilled_df:{len(filled_df)} recieved df: {len(df)}"
            )
        else:
            print(f"df dont't need fullfilled, original length: {len(df)}")
            filled_df = df

        # from pl.Dataframe to chart data
        self.data_manager.update_from_realtime(filled_df)
        self.last_df = filled_df

        # read chart data
        chart_data = self.data_manager.get_chart_data()
        print(
            f"Chart data summary: {len(chart_data.get('datasets', []))} datasets, "
            f"{len(chart_data.get('timestamps', []))} timestamps, "
            f"{len(chart_data.get('highlights', []))} highlights"
        )

        if chart_data.get("datasets"):
            sample_dataset = chart_data["datasets"][0]
            print(
                f"Sample dataset: {sample_dataset['label']}, "
                f"{len(sample_dataset['data'])} data points, "
                f"rank: {sample_dataset.get('rank', 'N/A')}"
            )

        # Broadcast to all connected clients
        self.socketio.emit("chart_update", chart_data)

        print(
            f"Processed snapshot with {len(df)} stocks, "
            f"broadcasting to {len(self.connected_clients)} clients"
        )
