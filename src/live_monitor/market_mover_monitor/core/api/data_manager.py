"""
Data Manager for Market Mover Web Analyzer
Handles historical data loading and real-time data updates
"""

import asyncio
import concurrent.futures
import glob
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import polars as pl
from prometheus_client import Summary, start_http_server

from cores.config import cache_dir, float_shares_dir
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from utils.backtest_utils.backtest_utils import only_common_stocks

start_http_server(9090)  # localhost:9090/metrics

PROCESS_LATENCY = Summary(
    "datamanager_process_latency_seconds", "Time spent processing snapshot"
)


class DataManager:
    """Manages stock data for real-time visualization"""

    def __init__(self, max_history_points: int = 5000):
        self.max_history_points = max_history_points
        self.stock_data: Dict[str, Dict] = {}
        self.top_20_history: List[List[str]] = []  # Track top 20 changes over time
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    @PROCESS_LATENCY.time()
    def update_from_realtime(self, df: pl.DataFrame):
        """Update data from real-time DataFrame"""
        try:

            timestamp_value = None
            if hasattr(df, "select") and "timestamp" in df.columns:
                timestamp_value = df["timestamp"].max()
                print(
                    f"DEBUG: timestamp_value type: {type(timestamp_value)}, value: {timestamp_value}"
                )

            # Convert timestamp to datetime
            if timestamp_value is not None:
                if isinstance(timestamp_value, (int, float)):
                    # Handle numeric timestamps (assume milliseconds if > 1e10, else seconds)
                    if timestamp_value > 1e10:
                        timestamp = datetime.fromtimestamp(
                            timestamp_value / 1000, tz=ZoneInfo("America/New_York")
                        )
                    else:
                        timestamp = datetime.fromtimestamp(
                            timestamp_value, tz=ZoneInfo("America/New_York")
                        )
                elif isinstance(timestamp_value, str):
                    # Parse string timestamp
                    try:
                        # Try ISO format first
                        timestamp = datetime.fromisoformat(
                            timestamp_value.replace("Z", "+00:00")
                        )
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(
                                tzinfo=ZoneInfo("America/New_York")
                            )
                        else:
                            timestamp = timestamp.astimezone(
                                ZoneInfo("America/New_York")
                            )
                    except ValueError:
                        # Try parsing as timestamp
                        timestamp_float = float(timestamp_value)
                        if timestamp_float > 1e10:
                            timestamp = datetime.fromtimestamp(
                                timestamp_float / 1000, tz=ZoneInfo("America/New_York")
                            )
                        else:
                            timestamp = datetime.fromtimestamp(
                                timestamp_float, tz=ZoneInfo("America/New_York")
                            )
                elif isinstance(timestamp_value, datetime):
                    # Already a datetime object - this shouldn't happen with DataFrame JSON but handle it
                    if timestamp_value.tzinfo is None:
                        timestamp = timestamp_value.replace(
                            tzinfo=ZoneInfo("America/New_York")
                        )
                    else:
                        timestamp = timestamp_value.astimezone(
                            ZoneInfo("America/New_York")
                        )
                else:
                    print(f"DEBUG: Unsupported timestamp type: {type(timestamp_value)}")
                    timestamp = datetime.now(ZoneInfo("America/New_York"))

                print(f"DEBUG: Final timestamp: {timestamp}")
            else:
                print("DEBUG: No timestamp found, using current time")
                timestamp = datetime.now(ZoneInfo("America/New_York"))

            # Process the snapshot data - pass DataFrame directly
            self._process_snapshot(df, timestamp)

        except Exception as e:
            print(f"Error processing Redis message: {e}")
            import traceback

            traceback.print_exc()

    def _process_snapshot(
        self, df: pl.DataFrame, timestamp: datetime, is_historical: bool = False
    ) -> None:
        """Process a single data snapshot"""
        # Sort by percent_change to get current rankings
        df_sorted = df.sort("percent_change", descending=True)
        current_top_20 = df_sorted.head(20)

        # Track top 20 changes
        current_top_20_tickers = current_top_20.select("ticker").to_series().to_list()
        self.top_20_history.append(current_top_20_tickers)

        # Keep only recent history to manage memory
        # print(f"Debug: current {len(self.top_20_history)}")
        if len(self.top_20_history) > self.max_history_points:
            self.top_20_history = self.top_20_history[-self.max_history_points :]

        # Update stock data
        for i, row in enumerate(current_top_20.iter_rows(named=True)):
            ticker = row["ticker"]
            current_rank = i + 1

            if ticker not in self.stock_data:
                # New stock entry
                self.stock_data[ticker] = {
                    "timestamps": [],
                    "percent_changes": [],
                    "current_rank": current_rank,
                    "previous_rank": None,
                    "rank_history": [],
                    "first_appearance": timestamp,
                    "is_new_entrant": not is_historical,
                    "rank_velocity": 0.0,
                    "alpha": 1.0,
                    "highlight": not is_historical,  # Highlight new entrants in real-time
                    "metadata": {
                        "current_price": row.get("current_price", 0.0),
                        "prev_close": row.get("prev_close", 0.0),
                        "volume": row.get("accumulated_volume", 0.0),
                        "float_shares": 0.0,
                    },
                }
            else:
                # Update existing stock
                stock_info = self.stock_data[ticker]
                stock_info["previous_rank"] = stock_info["current_rank"]
                stock_info["current_rank"] = current_rank

                # Calculate rank velocity (negative means moving up in ranking)
                if stock_info["previous_rank"] is not None:
                    rank_change = stock_info["previous_rank"] - current_rank
                    stock_info["rank_velocity"] = rank_change

                    # Highlight stocks with rapid rank improvement
                    if not is_historical and rank_change >= 5:  # Moved up 5+ positions
                        stock_info["highlight"] = True

                # stock_info["alpha"] = self._calculate_alpha(current_rank)
                stock_info["is_new_entrant"] = False

                # Update metadata
                stock_info["metadata"].update(
                    {
                        "current_price": row.get("current_price", 0.0),
                        "prev_close": row.get("prev_close", 0.0),
                        "volume": row.get("accumulated_volume", 0.0),
                    }
                )

            # Add data points
            self.stock_data[ticker]["timestamps"].append(timestamp)
            self.stock_data[ticker]["percent_changes"].append(row["percent_change"])
            self.stock_data[ticker]["rank_history"].append(current_rank)

            # Limit history size
            if len(self.stock_data[ticker]["timestamps"]) > self.max_history_points:
                self.stock_data[ticker]["timestamps"] = self.stock_data[ticker][
                    "timestamps"
                ][-self.max_history_points :]
                self.stock_data[ticker]["percent_changes"] = self.stock_data[ticker][
                    "percent_changes"
                ][-self.max_history_points :]
                self.stock_data[ticker]["rank_history"] = self.stock_data[ticker][
                    "rank_history"
                ][-self.max_history_points :]
                # Clean up stocks that are no longer in top 20

        self._cleanup_old_stocks(current_top_20_tickers)

    def _cleanup_old_stocks(self, current_top_20: List[str]) -> None:
        """Remove stocks that haven't been in top 20 for a while"""
        stocks_to_remove = []

        for ticker in self.stock_data:
            # print(f"Current top 20 tickers: {ticker}")
            if ticker not in current_top_20:
                # Mark for removal if not in top 20 for too long
                # Keep some history for smooth transitions
                # if len(self.stock_data[ticker]["timestamps"]) > 10:
                last_timestamp = self.stock_data[ticker]["timestamps"][-1]
                if (
                    datetime.now(ZoneInfo("America/New_York")) - last_timestamp
                ).total_seconds() > 300:  # 5 minutes
                    stocks_to_remove.append(ticker)

        # print(f"Removing {len(stocks_to_remove)} old stocks: {stocks_to_remove}")
        for ticker in stocks_to_remove:
            del self.stock_data[ticker]

    def get_chart_data(self) -> Dict:
        """Get data formatted for chart visualization"""
        chart_data = {"datasets": [], "timestamps": [], "highlights": []}

        # Get all unique timestamps and sort them
        all_timestamps = set()
        for stock_data in self.stock_data.values():
            all_timestamps.update(stock_data["timestamps"])

        sorted_timestamps = sorted(list(all_timestamps))
        chart_data["timestamps"] = [ts.isoformat() for ts in sorted_timestamps]

        # Process each stock
        for ticker, stock_data in self.stock_data.items():
            if not stock_data["timestamps"]:
                continue

            dataset = {
                "label": ticker,
                "data": [],
                "borderColor": self._get_stock_color(ticker, stock_data),
                "backgroundColor": self._get_stock_color(
                    ticker, stock_data, with_alpha=False
                ),
                "borderWidth": 3 if stock_data["highlight"] else 1,
                "pointRadius": 2,
                "tension": 0.1,
                "rank": stock_data["current_rank"],
                "alpha": stock_data["alpha"],
                "highlight": stock_data["highlight"],
                "velocity": stock_data["rank_velocity"],
                "metadata": stock_data["metadata"],
            }

            # Map data points to timeline - only include actual data points
            for i, ts in enumerate(stock_data["timestamps"]):
                if i < len(stock_data["percent_changes"]):
                    dataset["data"].append(
                        {"x": ts.isoformat(), "y": stock_data["percent_changes"][i]}
                    )

            chart_data["datasets"].append(dataset)

            # Add to highlights if needed
            if stock_data["highlight"]:
                chart_data["highlights"].append(
                    {
                        "ticker": ticker,
                        "rank": stock_data["current_rank"],
                        "velocity": stock_data["rank_velocity"],
                        "is_new": stock_data["is_new_entrant"],
                    }
                )

        return chart_data

    def toggle_stock_highlight(self, ticker: str, highlight: bool) -> bool:
        """Toggle highlight status for a specific stock"""
        if ticker in self.stock_data:
            self.stock_data[ticker]["highlight"] = highlight
            print(f"Updated highlight for {ticker}: {highlight}")
            return True
        return False

    def get_stock_detail(self, ticker: str):
        """get local data firts + return a async future"""
        provider = FloatSharesProvider()

        local_float = provider.fetch_from_local(ticker)

        if local_float:
            self.stock_data[ticker]["metadata"]["float_shares"] = local_float.data[
                0
            ].float_shares

        # create a async web fetcher
        future = self.executor.submit(self._fetch_extra_float_sync, ticker)

        return self.stock_data[ticker], future

    def _fetch_extra_float_sync(self, ticker: str) -> Optional[Dict]:
        """thread pool wrap func"""
        try:
            # create new task in this thread pool
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self._fetch_extra_float_async(ticker))
                return result
            finally:
                loop.close()
        except Exception as e:
            print(f"Error fetching extra float data for {ticker}: {e}")
            return None

    async def _fetch_extra_float_async(self, ticker: str) -> Optional[Dict]:
        """fecth web float data"""
        try:
            web_data = await FloatSharesProvider.fetch_from_web(ticker)
            if not web_data or not web_data.data:
                print(f"Warning: {ticker} no web float data found.")
                return None

            extra = {
                "float_sources": [
                    {
                        d.source: {
                            "float": d.float_shares,
                            "short%": d.short_percent,
                            "outstanding": d.outstanding_shares,
                        }
                    }
                    for d in web_data.data
                ]
            }
            print(f"Debug extra:\n{ticker}\n{extra}")

            if ticker in self.stock_data:
                self.stock_data[ticker]["metadata"]["extra"] = extra

            return extra
        except Exception as e:
            print(f"Error in _fetch_extra_float_async for {ticker}: {e}")
            return None

    def _get_stock_color(
        self, ticker: str, stock_data: Dict, with_alpha: bool = False
    ) -> str:
        """Generate color for stock based on rank and status"""
        rank = stock_data["current_rank"]
        is_highlighted = stock_data["highlight"]
        alpha = stock_data["alpha"]

        if is_highlighted:
            # Special colors for highlighted stocks
            if stock_data["is_new_entrant"]:
                print("find new entrant:", ticker)
                base_color = "255, 0, 0"  # Red for new entrants
            else:
                base_color = "255, 165, 0"  # Orange for fast movers
        else:
            # Color gradient based on rank
            # Top ranks: Blue to Green, Lower ranks: Yellow to Red
            if rank <= 5:
                base_color = f"{127 + (rank - 1) * 28}, {(rank - 1) * 51}, 255"
            elif rank <= 10:
                base_color = (
                    f"{100 + (rank-5) * 30}, 255, {200 - (rank-5) * 30}"  # Green shades
                )
            elif rank <= 15:
                base_color = f"200, {200 - (rank-10) * 20}, {120 - (rank-10) * 15}"  # Lighter yellow to orange
            else:
                base_color = (
                    f"{10 + (20-rank) * 32}, {60 + (20-rank) * 32}, 255"  # Blue shades
                )
            # print('find top rank:', ticker, stock_data['current_rank'], base_color)

        if with_alpha:
            return f"rgba({base_color}, {alpha * 0.3})"  # Lighter for fill
        else:
            return f"rgba({base_color}, {alpha})"

    def get_top_stocks(self, limit: int = 20) -> List[Tuple[str, Dict]]:
        """Get top stocks sorted by current rank"""
        ranked_stocks = [(ticker, data) for ticker, data in self.stock_data.items()]
        ranked_stocks.sort(key=lambda x: x[1]["current_rank"])
        return ranked_stocks[:limit]

    # ----------------- for replayer data -----------------------
    def initialize_from_history(self, date: str) -> None:
        """Load historical data for the given date"""
        year = date[:4]
        month = date[4:6]
        day = date[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, day)

        if not os.path.exists(market_mover_dir):
            print(f"No historical data found for {date}")
            return

        all_files = glob.glob(os.path.join(market_mover_dir, "*_market_snapshot.csv"))
        all_files.sort()

        print(f"Loading {len(all_files)} historical files...")

        for file_path in all_files:
            try:
                df = pl.read_csv(file_path)
                timestamp = self._extract_timestamp_from_filename(file_path)
                if timestamp.hour < 4:  # Skip data before pre-market data
                    print(f"Skipping pre-market data at {timestamp}")
                    continue
                filter_date = f"{year}-{month}-{day}"
                df = (
                    only_common_stocks(filter_date)
                    .drop("active", "composite_figi")
                    .join(df, on="ticker", how="inner")
                    .sort("percent_change", descending=True)
                ).filter(
                    (pl.col("percent_change") > 0)
                    & (pl.col("accumulated_volume") > 1000)
                )
                self._process_snapshot(df, timestamp, is_historical=True)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")

        print(f"Loaded historical data for {len(self.stock_data)} stocks")

    def _extract_timestamp_from_filename(self, filename: str) -> datetime:
        """Extract timestamp from filename"""
        basename = os.path.basename(filename)
        timestamp_str = basename.split("_")[0]

        year = int(timestamp_str[:4])
        month = int(timestamp_str[4:6])
        day = int(timestamp_str[6:8])
        hour = int(timestamp_str[8:10])
        minute = int(timestamp_str[10:12])
        second = int(timestamp_str[12:14])

        dt = datetime(year, month, day, hour, minute, second)
        return dt.replace(tzinfo=ZoneInfo("America/New_York"))
