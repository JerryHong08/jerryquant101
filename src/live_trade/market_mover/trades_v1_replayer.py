import os
from datetime import datetime

import polars as pl

from cores.config import data_dir
from utils.data_utils.path_loader import DataPathFetcher


def load_previous_data():
    # prev_close prev_volume
    pass


# ticker accumulated_volume current_price timestamp


def read_trades_v1(replay_date: str, speed_multiplier: float = 1.0):
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
    print(lf.explain(optimized=True))

    schema = lf.collect_schema()
    print(schema)

    with pl.Config(tbl_cols=15):
        print(lf.collect(streaming=True))


# 1. load path
# 2. process to market_mover schema (prev and current)
# 3. push up regular routine (speed and time)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="us_stocks_sip trades_v1 replayer")
    parser.add_argument("--date", default="20251003", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Speed multiplier (default: 1.0)"
    )

    args = parser.parse_args()

    print(f"Replaying market snapshots for date: {args.date}")
    print(f"Speed: {args.speed}x")

    read_trades_v1(args.date, args.speed)

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
