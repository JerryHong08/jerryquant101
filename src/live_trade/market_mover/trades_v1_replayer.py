import os
from datetime import datetime

import polars as pl
import redis

from cores.config import data_dir
from utils.data_utils.path_loader import DataPathFetcher

r = redis.Redis(host="localhost", port=6379, db=0)


def load_previous_data(replay_date: str):
    """
    Load previous data like prev_close, prev_volume from 'us_stocks_sip/day_aggsv1/{replay_date-timedelta(1)}.parquet'
    """
    pass


# ticker accumulated_volume current_price timestamp


def read_trades_v1(replay_date: str, speed_multiplier: float = 1.0):
    """
    1. load path
    2. rank and tranform to market_mover schema (combine prev and current)
    # ┌─────────┬────────────────────┬────────────────────┬───────────────┬────────────┬───────────────┬─────────────┐
    # │ ticker  │   percent_change   │ accumulated_volume │ current_price │ prev_close │   timestamp   │ prev_volume │
    # │ varchar │       double       │       double       │    double     │   double   │     int64     │   double    │
    # ├─────────┼────────────────────┼────────────────────┼───────────────┼────────────┼───────────────┼─────────────┤
    # │ MASK    │ 6.4687819856704225 │            25161.0 │        0.5201 │     0.4885 │ 1760605200000 │    329645.0 │
    # │ GDL     │                0.0 │                0.0 │           0.0 │       8.47 │             0 │     16711.0 │
    # │ IGC     │                0.0 │                0.0 │           0.0 │     0.4128 │             0 │    773819.0 │
    # │ JUNW    │                0.0 │                0.0 │           0.0 │      32.75 │             0 │     12706.0 │
    # │ SWBI    │                0.0 │                0.0 │           0.0 │      10.08 │             0 │    625793.0 │
    # └─────────┴────────────────────┴────────────────────┴───────────────┴────────────┴───────────────┴─────────────┘
    3. push up to redis port in regular routine (args: speed and time)
    """
    replay_date = datetime.strptime(replay_date, "%Y%m%d").strftime("%Y-%m-%d")
    trades_v1_dir = os.path.join(data_dir, "lake")
    # print(trades_v1_dir)

    path_fetcher = DataPathFetcher(
        asset="us_stocks_sip",
        data_type="trades_v1",
        start_date=replay_date,
        end_date=replay_date,
        lake=True,
        s3=False,
    )
    paths = path_fetcher.data_dir_calculate()
    # print(paths)

    lf = pl.scan_parquet(paths).sort("ticker", "sip_timestamp")

    schema = lf.collect_schema()
    print(schema)

    with pl.Config(tbl_cols=15):
        print(lf.collect(engine="streaming"))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="us_stocks_sip trades_v1 replayer")
    parser.add_argument("--date", default="20251015", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Speed multiplier (default: 1.0)"
    )

    args = parser.parse_args()

    print(f"Replaying market snapshots for date: {args.date}")
    print(f"Speed: {args.speed}x")
    if args.date:
        read_trades_v1(args.date, args.speed)
