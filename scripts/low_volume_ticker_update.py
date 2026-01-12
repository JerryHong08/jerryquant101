"""
Low Volume Ticker Tracker - Event Stream Processing Model

Core Principles:
1. Maintain state machine for each ticker (normal <-> low liquidity)
2. Process event stream day by day, update state
3. Support incremental updates (only process new data)
4. Auto-save history and current state
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

from cores.data_loader import stock_load_process
from utils.backtest_utils.backtest_utils import get_common_stocks


class LowVolumeTrackerEventStream:
    """
    Low liquidity tracker using event stream processing model

    Data Structure:
    1. state.parquet - Current state (one row per ticker)
    2. history.parquet - Complete history (all low liquidity periods)
    3. low_volume_tickers.csv - Summary view (for watchlist)
    """

    def __init__(
        self,
        data_dir: Path = Path("."),
        low_volume_threshold: int = 0,
        min_duration_days: int = 1,
    ):
        self.data_dir = data_dir
        self.low_volume_threshold = low_volume_threshold
        self.min_duration_days = min_duration_days

        self.state_file = data_dir / "low_volume_state.parquet"
        self.history_file = data_dir / "low_volume_history.parquet"
        self.csv_file = data_dir / "low_volume_tickers.csv"

        print(f"📊 LowVolumeTracker initialized:")
        print(f"   Volume threshold: <= {low_volume_threshold}")
        print(f"   Min duration: {min_duration_days} days")

    def initialize(
        self, start_date: str = "2015-01-01", end_date: Optional[str] = None
    ):
        """
        Initialize: full processing from scratch

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"\n🔄 Initializing from {start_date} to {end_date}...")

        print("⏳ Loading OHLCV data...")
        tickers = get_common_stocks(filter_date=start_date).collect()

        ohlcv_data = stock_load_process(
            tickers=tickers.to_series().to_list(),
            timeframe="1d",
            start_date=start_date,
            end_date=end_date,
            use_cache=True,
            skip_low_volume=False,
        ).with_columns(pl.col("timestamps").dt.date().alias("date"))

        print(
            f"✅ Loaded {ohlcv_data.select(pl.col('ticker').n_unique()).collect().item()} tickers"
        )

        state, history = self._process_event_stream(ohlcv_data)

        self._save_state(state)
        self._save_history(history)
        self._export_csv()

        print(f"✅ Initialization complete!")

    def _process_event_stream(
        self, ohlcv_data: pl.LazyFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Core: Event stream processing

        Logic:
        1. Sort by (ticker, date)
        2. Determine if each row is low volume
        3. Update state machine:
           - Normal -> Low volume: Start new period
           - Low volume -> Low volume: Extend period
           - Low volume -> Normal: Close period, record to history

        Args:
            ohlcv_data: OHLCV data

        Returns:
            (state_df, history_df)
        """
        print("🔄 Processing event stream...")

        events = (
            ohlcv_data.with_columns(
                (pl.col("volume") <= self.low_volume_threshold).alias("is_low_volume")
            )
            .collect()
            .sort(["ticker", "date"])
        )

        ticker_states = {}

        history_buffer = []

        total_events = len(events)
        for i, row in enumerate(events.iter_rows(named=True)):
            ticker = row["ticker"]
            date = row["date"]
            is_low_volume = row["is_low_volume"]

            if (i + 1) % 100000 == 0:
                print(f"   Processed {i+1}/{total_events} events...")

            if ticker not in ticker_states:
                ticker_states[ticker] = {
                    "is_low_volume": False,
                    "period_start": None,
                    "period_days": 0,
                    "max_period_days": 0,
                    "last_check_date": None,
                }

            state = ticker_states[ticker]
            prev_is_low = state["is_low_volume"]

            # is_low_volume stands for latest is_low_volume state.
            if is_low_volume:
                # prev_is_low stands for last is_low_volume state, transferred from ticker_states[ticker]
                if prev_is_low:
                    # it means it's still low_volume ticker.
                    state["period_days"] += 1
                    state["max_period_days"] = max(
                        state["max_period_days"], state["period_days"]
                    )
                else:
                    # it means it's a new low_volume ticker
                    state["is_low_volume"] = True
                    state["period_start"] = date
                    state["period_days"] = 1
                    state["max_period_days"] = max(state["max_period_days"], 1)
            else:
                if prev_is_low:
                    # it means it's not low_volume anymore, so close it.
                    # write into history_buffer
                    if state["period_days"] >= self.min_duration_days:
                        history_buffer.append(
                            {
                                "ticker": ticker,
                                "period_start": state["period_start"],
                                "period_end": state["last_check_date"],
                                "duration_days": state["period_days"],
                                "status": "closed",
                            }
                        )

                    state["is_low_volume"] = False
                    state["period_start"] = None
                    state["period_days"] = 0

            state["last_check_date"] = date

        for ticker, state in ticker_states.items():
            if (
                state["is_low_volume"]
                and state["period_days"] >= self.min_duration_days
            ):
                history_buffer.append(
                    {
                        "ticker": ticker,
                        "period_start": state["period_start"],
                        "period_end": state["last_check_date"],
                        "duration_days": state["period_days"],
                        "status": "ongoing",
                    }
                )
        state_df = pl.DataFrame(
            data=[
                {
                    "ticker": ticker,
                    "is_currently_low_volume": state["is_low_volume"],
                    "current_period_start": state["period_start"],
                    "current_period_days": state["period_days"],
                    "max_period_days": state["max_period_days"],
                    "last_check_date": state["last_check_date"],
                }
                for ticker, state in ticker_states.items()
            ],
            schema={
                "ticker": pl.String,
                "is_currently_low_volume": pl.Boolean,
                "current_period_start": pl.Date,  # ✅ 显式指定为 Date 类型
                "current_period_days": pl.Int64,
                "max_period_days": pl.Int64,
                "last_check_date": pl.Date,  # ✅ 显式指定为 Date 类型
            },
        )

        if history_buffer:
            history_df = pl.DataFrame(
                data=history_buffer,
                schema={
                    "ticker": pl.String,
                    "period_start": pl.Date,
                    "period_end": pl.Date,
                    "duration_days": pl.Int64,
                    "status": pl.String,
                },
            )
        else:
            history_df = pl.DataFrame(
                schema={
                    "ticker": pl.String,
                    "period_start": pl.Date,
                    "period_end": pl.Date,
                    "duration_days": pl.Int64,
                    "status": pl.String,
                }
            )

        print(f"✅ Processed {total_events} events")
        print(
            f"   Current low volume tickers: {state_df.filter(pl.col('is_currently_low_volume')).height}"
        )
        print(f"   New history records: {history_df.height}")

        return state_df, history_df

    def _save_state(self, state_df: pl.DataFrame):
        """Save current state"""
        state_df.write_parquet(self.state_file)
        print(f"💾 Saved state to {self.state_file}")

    def _save_history(self, history_df: pl.DataFrame):
        """Save history records (overwrite)"""
        if history_df.height > 0:
            history_df.write_parquet(self.history_file)
            print(f"💾 Saved history to {self.history_file}")

    def _export_csv(self):
        """
        Export CSV summary view (compatible with original format)

        Format:
        - ticker
        - max_duration_days
        - max_duration_start_date
        - max_duration_end_date
        - avg_turnover
        - notes (loaded from old CSV)
        - source
        - cut_off_date
        - counts
        """
        print("📄 Exporting CSV...")

        state = pl.read_parquet(self.state_file)

        if not self.history_file.exists():
            print("⚠️  No history file, skipping CSV export")
            return

        history = pl.read_parquet(self.history_file)

        max_durations = history.group_by("ticker").agg(
            [
                pl.col("duration_days").max().alias("max_duration_days"),
                pl.col("period_start")
                .filter(pl.col("duration_days") == pl.col("duration_days").max())
                .sort()
                .last()
                .alias("max_duration_start_date"),
                pl.col("period_end")
                .filter(pl.col("duration_days") == pl.col("duration_days").max())
                .sort()
                .last()
                .alias("max_duration_end_date"),
            ]
        )

        print("   Calculating avg_turnover...")
        tickers = max_durations.select("ticker").to_series().to_list()

        ohlcv_data = stock_load_process(
            tickers=tickers,
            timeframe="1d",
            start_date="2015-01-01",
            end_date=datetime.now().strftime("%Y-%m-%d"),
            use_cache=True,
            skip_low_volume=False,
        )

        avg_turnover = (
            ohlcv_data.filter(pl.col("volume") > self.low_volume_threshold)
            .with_columns((pl.col("volume") * pl.col("close")).alias("turnover"))
            .group_by("ticker")
            .agg(pl.col("turnover").mean().alias("avg_turnover").cast(pl.Int64))
            .collect()
        )

        result = max_durations.join(avg_turnover, on="ticker", how="left")

        try:
            old_csv = pl.read_csv(self.csv_file, truncate_ragged_lines=True)
            result = result.join(
                old_csv.select(["ticker", "notes", "source", "cut_off_date", "counts"]),
                on="ticker",
                how="left",
            )
            print("   Loaded notes from previous CSV")
        except FileNotFoundError:
            result = result.with_columns(
                [
                    pl.lit(None).alias("notes"),
                    pl.lit(None).alias("source"),
                    pl.lit(None).alias("cut_off_date"),
                    pl.lit(None).alias("counts"),
                ]
            )
            print("   No previous CSV found, initializing notes columns")
        result = result.sort(
            ["avg_turnover", "max_duration_days", "ticker"],
            descending=[True, True, False],
        )

        result.write_csv(self.csv_file)
        print(f"✅ Exported {result.height} tickers to {self.csv_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Low Volume Ticker Tracker")
    parser.add_argument(
        "--start-date",
        default="2015-01-01",
        help="Start date for initialization (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date for initialization (YYYY-MM-DD), defaults to today",
    )

    args = parser.parse_args()

    tracker = LowVolumeTrackerEventStream(
        data_dir=Path("."), low_volume_threshold=0, min_duration_days=1
    )

    tracker.initialize(start_date=args.start_date, end_date=args.end_date)
