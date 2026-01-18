"""
Data Manager for Market Mover Web Analyzer
Handles snapshot processing, membership management, and state tracking.

Architecture:
- Redis Stream: stores all snapshot events with rank, price, volume metrics
- Redis HSET: stores state_cursor (last_state_updated_time per ticker) for recovery/replay
- InfluxDB market_snapshot: stores all subscribed tickers' historical snapshot data
- InfluxDB movers_state: stores state change events for each ticker
"""

import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import influxdb_client
import polars as pl
import redis
from influxdb_client.client.write_api import SYNCHRONOUS

from live_monitor.market_mover_monitor.core.data.transforms import (
    _parse_transfrom_timetamp,
)
from live_monitor.market_mover_monitor.core.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class SnapshotAnalyzer:
    """
    Manages stock snapshot data processing with decoupled responsibilities:
    - Rank computation and membership management (Redis Stream + Set)
    - Historical data storage (InfluxDB market_snapshot)
    - State tracking and cursor management (Redis HSET + InfluxDB movers_state)
    """

    TOP_N = 20  # Number of top movers to track

    def __init__(
        self,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
    ):
        self.run_mode = "replay" if replay_date else "live"
        self.db_date = (
            replay_date
            if replay_date
            else datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        )
        self.db_id = self._derive_db_id(self.db_date, suffix_id)

        # ---------- InfluxDB Configuration ----------
        token = os.environ.get("INFLUXDB_TOKEN")
        self.org = "jerryhong"
        self.bucket = "jerrymmm"
        url = "http://localhost:8086"

        self._influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.org
        )
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._influx_client.query_api()

        # ---------- Redis Configuration ----------
        self.r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

        # Stream: stores processed snapshot events with metrics (output stream)
        self.STREAM_NAME = f"market_snapshot_processed:{self.db_date}"
        # HSET: stores state_cursor (last_state_updated_time per ticker)
        self.HSET_NAME = f"state_cursor:{self.db_date}"
        # Set: tracks all tickers that have ever been in top 20 (for subscription)
        self.SUBSCRIBED_SET_NAME = f"movers_subscribed_set:{self.db_date}"

        # In-memory state cache for state change detection
        self._ticker_states: Dict[str, Dict] = {}

        # Volume tracking for relative volume calculation
        # Structure: {ticker: [(timestamp, accumulated_volume), ...]}
        # Stores recent volume data points for last 5 minutes
        self._volume_history: Dict[str, List[Tuple[datetime, float]]] = defaultdict(
            list
        )

        # Reload state and volume history from InfluxDB
        self._reload_states_from_influx()
        self._reload_volume_history_from_influx()

        logger.info(
            f"__init__ - SnapshotAnalyzer initialized: mode={self.run_mode}, "
            f"__init__ - db_id={self.db_id}, STREAM={self.STREAM_NAME}, HSET={self.HSET_NAME}"
        )

    @staticmethod
    def _derive_db_id(db_date: Optional[str], override: Optional[str]) -> str:
        if db_date and override:
            return f"{db_date}_{override}"
        if override:
            return override
        if db_date:
            return db_date
        return "na"

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def process_snapshot(self, df: pl.DataFrame, is_historical: bool = False) -> Dict:
        """
        Main entry point: process a snapshot DataFrame.

        Flow:
        1. Parse timestamp and compute competition ranks
        2. Compute derived metrics (change, relativeVolume, etc.)
        3. Update subscription set for top N tickers
        4. Write to Redis Stream and InfluxDB for all subscribed tickers
        5. For each ticker, check state changes and update cursor

        Returns:
            Dict with processing summary (for logging/debugging)
        """
        # Step 0: Parse timestamp
        timestamp = self._extract_timestamp(df)

        # Step 1: Compute ranks (competition ranking)
        ranked_df = self._compute_ranks(df)

        # Step 2: Compute derived metrics
        enriched_df = self._compute_derived_metrics(ranked_df, timestamp)

        # Step 3: Update subscription set - add new top N tickers
        current_top_n = enriched_df.head(self.TOP_N)
        new_subscriptions = self._update_subscription_set(current_top_n, timestamp)

        # Step 4: Get all subscribed tickers and write their data
        all_subscribed = self._get_all_subscribed_tickers()
        self._write_to_stream_and_influx(enriched_df, all_subscribed, timestamp)

        # Step 5: Update states and track changes
        state_changes = self._process_state_updates(
            enriched_df, all_subscribed, timestamp, is_historical
        )

        return {
            "timestamp": timestamp.isoformat(),
            "new_subscriptions": new_subscriptions,
            "total_subscribed": len(all_subscribed),
            "state_changes": state_changes,
        }

    def get_subscribed_tickers(self) -> List[str]:
        """Get all currently subscribed tickers from Redis Set."""
        return self._get_all_subscribed_tickers()

    def get_state_cursor(self, ticker: str) -> Optional[str]:
        """Get the last state update time for a ticker from Redis HSET."""
        return self.r.hget(self.HSET_NAME, ticker)

    def get_all_state_cursors(self) -> Dict[str, str]:
        """Get all state cursors for recovery/replay."""
        return self.r.hgetall(self.HSET_NAME)

    def get_ticker_state(self, ticker: str) -> Optional[Dict]:
        """Get current in-memory state for a ticker."""
        return self._ticker_states.get(ticker)

    # =========================================================================
    # RANK COMPUTATION
    # =========================================================================

    def _extract_timestamp(self, df: pl.DataFrame) -> datetime:
        """Extract and parse timestamp from DataFrame."""
        if "timestamp" in df.columns:
            timestamp_value = df["timestamp"].max()
        else:
            timestamp_value = None
        return _parse_transfrom_timetamp(timestamp_value)

    def _compute_ranks(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Sort by percent_change and add competition ranking.
        Ties get same rank, next rank skips.
        Example: 50%, 50%, 40%, 30% -> ranks 1, 1, 3, 4
        """
        return df.sort("percent_change", descending=True).with_columns(
            pl.col("percent_change")
            .rank(method="min", descending=True)
            .cast(pl.Int32)
            .alias("competition_rank")
        )

    # =========================================================================
    # DERIVED METRICS COMPUTATION
    # =========================================================================

    def _compute_derived_metrics(
        self, ranked_df: pl.DataFrame, timestamp: datetime
    ) -> pl.DataFrame:
        """
        Compute derived metrics for each ticker:
        - change: current_price - prev_close
        - relativeVolumeDaily: accumulated_volume / prev_volume (if prev_volume exists)
        - relativeVolume5min: current_5min_volume / avg_5min_volume
        """
        # Compute change = current_price - prev_close
        df = ranked_df.with_columns(
            (pl.col("current_price") - pl.col("prev_close")).alias("change")
        )

        # Compute relativeVolumeDaily if prev_volume exists
        if "prev_volume" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("prev_volume") > 0)
                .then(pl.col("accumulated_volume") / pl.col("prev_volume"))
                .otherwise(0.0)
                .alias("relativeVolumeDaily")
            )
        else:
            df = df.with_columns(pl.lit(0.0).alias("relativeVolumeDaily"))

        # Compute relativeVolume5min - needs row-by-row processing
        relative_5min_values = []
        for row in df.iter_rows(named=True):
            ticker = row["ticker"]
            accumulated_volume = row.get("accumulated_volume", 0.0)
            rel_5min = self._compute_relative_volume_5min(
                ticker, timestamp, accumulated_volume
            )
            relative_5min_values.append(rel_5min)

        df = df.with_columns(pl.Series("relativeVolume5min", relative_5min_values))

        return df

    def _compute_relative_volume_5min(
        self, ticker: str, timestamp: datetime, accumulated_volume: float
    ) -> float:
        """
        Compute 5-minute relative volume:
        relativeVolume5min = last_1min_volume / last_5min_avg_volume

        - last_1min_volume: volume traded in the last 1 minute
        - last_5min_avg_volume: average volume per minute over the last 5 minutes
        """
        # Add current data point to history
        self._volume_history[ticker].append((timestamp, accumulated_volume))

        # Keep only data points from the last 6 minutes (for buffer)
        cutoff_time = timestamp - timedelta(minutes=6)
        self._volume_history[ticker] = [
            (ts, vol) for ts, vol in self._volume_history[ticker] if ts >= cutoff_time
        ]

        history = self._volume_history[ticker]
        if len(history) < 2:
            # Not enough data yet, return 1.0 (neutral)
            return 1.0

        # Calculate last 1 minute volume
        one_min_ago = timestamp - timedelta(minutes=1)
        volume_1min_ago = None
        for ts, vol in reversed(history):
            if ts <= one_min_ago:
                volume_1min_ago = vol
                break

        if volume_1min_ago is None:
            # Use earliest available data point
            volume_1min_ago = history[0][1]

        last_1min_volume = max(0.0, accumulated_volume - volume_1min_ago)

        # Calculate last 5 minute average volume (per minute)
        five_min_ago = timestamp - timedelta(minutes=5)
        volume_5min_ago = None
        for ts, vol in history:
            if ts <= five_min_ago:
                volume_5min_ago = vol
            else:
                break

        if volume_5min_ago is None:
            # Use earliest available data point
            volume_5min_ago = history[0][1]
            # Calculate time span from earliest point
            earliest_ts = history[0][0]
            time_span_minutes = max(
                1.0, (timestamp - earliest_ts).total_seconds() / 60.0
            )
        else:
            time_span_minutes = 5.0

        last_5min_total_volume = max(0.0, accumulated_volume - volume_5min_ago)
        last_5min_avg_volume = last_5min_total_volume / time_span_minutes

        if ticker == "IVP":
            logger.debug(
                f"_compute_relative_volume_5min - Ticker: {ticker}, last_1min_volume: {last_1min_volume}, last_5min_avg_volume: {last_5min_avg_volume}"
            )

        if last_5min_avg_volume > 0:
            return last_1min_volume / last_5min_avg_volume

        # Not enough data yet, return 1.0 (neutral)
        return 1.0

    # =========================================================================
    # SUBSCRIPTION MANAGEMENT (Redis Set)
    # =========================================================================

    def _update_subscription_set(
        self, current_top_n: pl.DataFrame, timestamp: datetime
    ) -> List[str]:
        """
        Update Redis Set for subscription tracking.
        Tickers that enter top N are added to the set (never removed).

        Returns list of newly subscribed tickers.
        """
        new_subscriptions = []

        for row in current_top_n.iter_rows(named=True):
            ticker = row["ticker"]
            # SADD returns 1 if member was added, 0 if already existed
            added = self.r.sadd(self.SUBSCRIBED_SET_NAME, ticker)
            if added:
                new_subscriptions.append(ticker)
                logger.debug(
                    f"_update_subscription_set - New subscription: {ticker} at {timestamp.isoformat()}"
                )

        if new_subscriptions:
            logger.info(
                f"_update_subscription_set - New subscriptions: {new_subscriptions}"
            )

        return new_subscriptions

    def _get_all_subscribed_tickers(self) -> List[str]:
        """Get all subscribed tickers from Redis Set."""
        return list(self.r.smembers(self.SUBSCRIBED_SET_NAME))

    def get_top_n_tickers(self, n: int = 20) -> List[str]:
        """
        Get top N tickers by rank from the last snapshot in Redis Stream.
        Reads the most recent message and returns tickers with rank <= n.
        """
        import json

        # Read the last entry from stream
        entries = self.r.xrevrange(self.STREAM_NAME, count=1)

        if not entries:
            return []

        # Parse the latest message
        entry_id, fields = entries[0]
        data_json = fields.get("data")
        if not data_json:
            return []

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return []

        # Filter tickers with rank <= n and sort by rank
        current_membership = [
            (item["symbol"], item["rank"])
            for item in tickers_data
            if item.get("rank", 999) <= n
        ]
        current_membership.sort(key=lambda x: x[1])
        return [ticker for ticker, rank in current_membership]

    # =========================================================================
    # REDIS STREAM & INFLUXDB STORAGE
    # =========================================================================

    def _write_to_stream_and_influx(
        self,
        enriched_df: pl.DataFrame,
        subscribed_tickers: List[str],
        timestamp: datetime,
    ) -> None:
        """
        Write snapshot data to both Redis Stream and InfluxDB.

        Redis Stream: market_snapshot_processed:{date_suffix}
        - Single message per snapshot containing all subscribed tickers as JSON

        InfluxDB Measurement: market_snapshot
        Tags: symbol, run_mode, db_id
        Fields: rank, price, change, changePercent, volume,
                relativeVolume5min, relativeVolumeDaily
        Time: snapshot timestamp
        """
        # Create lookup dict from DataFrame for O(1) access
        df_dict = {row["ticker"]: row for row in enriched_df.iter_rows(named=True)}

        influx_points = []
        timestamp_iso = timestamp.isoformat()

        # Build stream data for all subscribed tickers
        stream_tickers_data = []

        for ticker in subscribed_tickers:
            row = df_dict.get(ticker)
            if row is None:
                # Ticker not in current snapshot (may have dropped out of top movers)
                continue

            rank = int(row.get("competition_rank", 0))
            price = float(row.get("current_price", 0.0))
            change = float(row.get("change", 0.0))
            change_percent = float(row.get("percent_change", 0.0))
            volume = float(row.get("accumulated_volume", 0))
            relative_volume_5min = float(row.get("relativeVolume5min", 1.0))
            relative_volume_daily = float(row.get("relativeVolumeDaily", 0.0))

            # Add to stream data list
            stream_tickers_data.append(
                {
                    "symbol": ticker,
                    "rank": rank,
                    "price": price,
                    "change": change,
                    "changePercent": change_percent,
                    "volume": volume,
                    "relativeVolume5min": relative_volume_5min,
                    "relativeVolumeDaily": relative_volume_daily,
                }
            )

            # Build InfluxDB point
            point = (
                influxdb_client.Point("market_snapshot")
                .tag("symbol", ticker)
                .tag("run_mode", self.run_mode)
                .tag("db_id", self.db_id)
                .field("rank", rank)
                .field("price", price)
                .field("change", change)
                .field("changePercent", change_percent)
                .field("volume", volume)
                .field("relativeVolume5min", relative_volume_5min)
                .field("relativeVolumeDaily", relative_volume_daily)
                .field("prev_close", float(row.get("prev_close", 0.0)))
                .time(timestamp)
            )
            influx_points.append(point)

        # Write single stream message with all tickers' data as JSON
        if stream_tickers_data:
            import json

            stream_message = {
                "timestamp": timestamp_iso,
                "data": json.dumps(stream_tickers_data),
            }
            self.r.xadd(self.STREAM_NAME, stream_message, maxlen=100)

        if influx_points:
            self._write_api.write(
                bucket=self.bucket, org=self.org, record=influx_points
            )
            logger.debug(
                f"_write_to_stream_and_influx - Wrote {len(influx_points)} points to Redis Stream and InfluxDB"
            )

        # Set 19-hour expiration on the stream if not already set
        if self.r.ttl(self.STREAM_NAME) < 0:
            self.r.expire(self.STREAM_NAME, 19 * 3600)

    # =========================================================================
    # STATE ENGINE
    # =========================================================================

    def _process_state_updates(
        self,
        ranked_df: pl.DataFrame,
        subscribed_tickers: List[str],
        timestamp: datetime,
        is_historical: bool,
    ) -> List[Dict]:
        """
        For each subscribed ticker, compute state and detect changes.
        If state changed, write to InfluxDB and update cursor.

        Returns list of state change events.
        """
        df_dict = {row["ticker"]: row for row in ranked_df.iter_rows(named=True)}

        state_changes = []

        for ticker in subscribed_tickers:
            row = df_dict.get(ticker)
            if row is None:
                continue

            # Compute current state
            current_state = self._compute_ticker_state(ticker, row, timestamp)

            # Check if state changed
            if self._has_state_changed(ticker, current_state):
                # Write state to InfluxDB
                self._write_state_to_influx(ticker, current_state, timestamp)

                # Update in-memory cache
                self._ticker_states[ticker] = current_state

                state_changes.append(
                    {
                        "ticker": ticker,
                        "state": current_state.get("state"),
                        "rank": current_state.get("rank"),
                        "timestamp": timestamp.isoformat(),
                    }
                )

                # if not is_historical:
                #     logger.info(
                #         f"_process_state_updates - State change: {ticker} -> {current_state.get('state')}"
                #     )

            # Update cursor in Redis HSET no matter state changed or not
            self._update_state_cursor(ticker, timestamp)

        logger.debug(
            f"_process_state_updates - Updated state cursor: {timestamp.isoformat()} for {len(subscribed_tickers)} tickers"
        )

        return state_changes

    def _compute_ticker_state(
        self, ticker: str, row: Dict, timestamp: datetime
    ) -> Dict:
        """
        Compute the current state for a ticker.

        State categories (to be expanded):
        - "rising": rank improved significantly
        - "falling": rank dropped significantly
        - "stable": rank relatively unchanged
        - "new_entrant": just entered top N
        - "exited": no longer in current snapshot

        TODO: Add more sophisticated state logic in the future
        """
        current_rank = row.get("competition_rank", 999)
        percent_change = row.get("percent_change", 0.0)

        # Get previous state for velocity calculation
        prev_state = self._ticker_states.get(ticker, {})
        prev_rank = prev_state.get("rank", current_rank)

        # Calculate rank velocity (positive = improving, negative = falling)
        rank_velocity = prev_rank - current_rank

        # Determine state (simplified logic - to be expanded)
        if ticker not in self._ticker_states:
            state = "new_entrant"
        elif rank_velocity >= 5:
            state = "rising_fast"
        elif rank_velocity >= 2:
            state = "rising"
        elif rank_velocity <= -5:
            state = "falling_fast"
        elif rank_velocity <= -2:
            state = "falling"
        else:
            state = "stable"

        return {
            "state": state,
            "rank": current_rank,
            "prev_rank": prev_rank,
            "rank_velocity": rank_velocity,
            "percent_change": percent_change,
            "timestamp": timestamp,
        }

    def _has_state_changed(self, ticker: str, current_state: Dict) -> bool:
        """
        Detect if the state has meaningfully changed.
        Currently checks: state category or significant rank change.

        TODO: Add more sophisticated change detection logic
        """
        if ticker not in self._ticker_states:
            return True  # New ticker, always record initial state

        prev_state = self._ticker_states[ticker]

        # State category changed
        if prev_state.get("state") != current_state.get("state"):
            return True

        # Significant rank change (even within same category)
        rank_diff = abs(prev_state.get("rank", 0) - current_state.get("rank", 0))
        if rank_diff >= 3:
            return True

        return False

    def _write_state_to_influx(
        self, ticker: str, state: Dict, timestamp: datetime
    ) -> None:
        """
        Write state change event to InfluxDB.

        Measurement: movers_state
        Tags: symbol, run_mode, db_id
        Fields: state, rank, percent_change, rank_velocity
        Time: state_change timestamp
        """
        point = (
            influxdb_client.Point("movers_state")
            .tag("symbol", ticker)
            .tag("run_mode", self.run_mode)
            .tag("db_id", self.db_id)
            .field("state", state.get("state", "unknown"))
            .field("rank", int(state.get("rank", 0)))
            .field("percent_change", float(state.get("percent_change", 0.0)))
            .field("rank_velocity", int(state.get("rank_velocity", 0)))
            .time(timestamp)
        )

        self._write_api.write(bucket=self.bucket, org=self.org, record=point)
        # logger.debug(
        #     f"_write_state_to_influx - Wrote state change {state.get('state', 'unknown')} for {ticker} to InfluxDB movers_state"
        # )

    def _update_state_cursor(self, ticker: str, timestamp: datetime) -> None:
        """
        Update the state cursor in Redis HSET.
        Used for recovery/replay to know where to resume state computation.
        """
        cursor_value = timestamp.isoformat()
        self.r.hset(self.HSET_NAME, ticker, cursor_value)
        # logger.debug(f"Updated state cursor: {ticker} = {cursor_value}")

    # =========================================================================
    # RECOVERY / REPLAY SUPPORT
    # =========================================================================

    def _reload_states_from_influx(self) -> None:
        """
        Reload ticker states from InfluxDB movers_state on initialization.
        Queries the latest state for each subscribed ticker.

        In replay mode, uses cursor timestamps from Redis HSET to determine time range.
        In live mode, uses -1d as time range.
        """
        subscribed = self._get_all_subscribed_tickers()
        if not subscribed:
            logger.info(
                "_reload_states_from_influx - No subscribed tickers to reload states for"
            )
            return

        # Get cursor-based time range for replay mode
        range_start, range_end = self._get_reload_time_range()

        logger.info(
            f"Reloading states for {len(subscribed)} subscribed tickers from InfluxDB "
            f"(range: {range_start} to {range_end})..."
        )

        for ticker in subscribed:
            query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {range_start}, stop: {range_end})
                |> filter(fn: (r) => r["_measurement"] == "movers_state")
                |> filter(fn: (r) => r["symbol"] == "{ticker}")
                |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
                |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: 1)
            """
            try:
                tables = self._query_api.query(query, org=self.org)
                for table in tables:
                    for record in table.records:
                        self._ticker_states[ticker] = {
                            "state": record.values.get("state", "unknown"),
                            "rank": int(record.values.get("rank", 999)),
                            "percent_change": float(
                                record.values.get("percent_change", 0.0)
                            ),
                            "rank_velocity": int(record.values.get("rank_velocity", 0)),
                            "timestamp": record.get_time(),
                        }
                        break  # Only need the latest
            except Exception as e:
                logger.error(
                    f"_reload_states_from_influx - Error reloading state for {ticker}: {e}"
                )

        logger.info(
            f"_reload_states_from_influx - Reloaded states for {len(self._ticker_states)} tickers"
        )

    def _reload_volume_history_from_influx(self) -> None:
        """
        Reload volume history from InfluxDB market_snapshot on initialization.
        Queries the last 6 minutes of volume data for each subscribed ticker.

        In replay mode, uses cursor timestamps from Redis HSET to determine time range.
        In live mode, uses -6m as time range.
        """
        subscribed = self._get_all_subscribed_tickers()
        if not subscribed:
            logger.info("No subscribed tickers to reload volume history for")
            return

        # Get cursor-based time range for replay mode (need 6 min before cursor)
        range_start, range_end = self._get_reload_time_range(lookback_minutes=6)

        logger.info(
            f"Reloading volume history for {len(subscribed)} subscribed tickers from InfluxDB "
            f"(range: {range_start} to {range_end})..."
        )

        for ticker in subscribed:
            query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {range_start}, stop: {range_end})
                |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
                |> filter(fn: (r) => r["symbol"] == "{ticker}")
                |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
                |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: false)
            """
            try:
                tables = self._query_api.query(query, org=self.org)
                for table in tables:
                    for record in table.records:
                        ts = record.get_time()
                        volume = record.values.get(
                            "accumulated_volume"
                        ) or record.values.get("volume", 0)
                        if volume is not None:
                            self._volume_history[ticker].append((ts, float(volume)))
            except Exception as e:
                logger.error(
                    f"_reload_volume_history_from_influx - Error reloading volume history for {ticker}: {e}"
                )

        tickers_with_history = sum(1 for v in self._volume_history.values() if v)
        logger.info(
            f"_reload_volume_history_from_influx - Reloaded volume history for {tickers_with_history} tickers"
        )

    def _get_reload_time_range(self, lookback_minutes: int = 0) -> Tuple[str, str]:
        """
        Get the time range for reloading data from InfluxDB.

        In replay mode:
            - Gets min cursor timestamp from Redis HSET
            - Returns range from (cursor - lookback_minutes) to cursor
        In live mode:
            - Returns relative range based on lookback_minutes or default

        Args:
            lookback_minutes: Minutes to look back from cursor (for volume history)

        Returns:
            Tuple of (range_start, range_end) as InfluxDB time strings
        """
        if self.run_mode == "replay":
            # Get cursor timestamps from Redis HSET
            cursors = self.get_all_state_cursors()

            if cursors:
                # Find the minimum (earliest) cursor timestamp
                min_ts = None
                for ticker, cursor_ts in cursors.items():
                    try:
                        ts = datetime.fromisoformat(cursor_ts)
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                    except (ValueError, TypeError):
                        continue

                if min_ts:
                    # Calculate range based on cursor
                    if lookback_minutes > 0:
                        range_start = (
                            min_ts - timedelta(minutes=lookback_minutes)
                        ).isoformat()
                    else:
                        # For state reload, start from beginning of replay date
                        range_start = f"{self.db_date[:4]}-{self.db_date[4:6]}-{self.db_date[6:8]}T00:00:00Z"

                    range_end = min_ts.isoformat()
                    return range_start, range_end

            # No cursors found in replay mode, use full day range
            range_start = (
                f"{self.db_date[:4]}-{self.db_date[4:6]}-{self.db_date[6:8]}T00:00:00Z"
            )
            range_end = "now()"
            return range_start, range_end

        else:
            # Live mode: use relative time ranges
            if lookback_minutes > 0:
                return f"-{lookback_minutes}m", "now()"
            else:
                return "-1d", "now()"

    def recover_states_from_cursors(self) -> Dict[str, List]:
        """
        Recover ticker states by reading from InfluxDB since each ticker's cursor.
        Used after engine restart.

        Returns dict of {ticker: [replay_points]}
        """
        cursors = self.get_all_state_cursors()
        recovered = {}

        for ticker, cursor_ts in cursors.items():
            points = self._query_snapshots_since(ticker, cursor_ts)
            recovered[ticker] = points
            logger.info(
                f"recover_states_from_cursors - Recovered {len(points)} snapshots for {ticker} since {cursor_ts}"
            )

        return recovered

    def _query_snapshots_since(self, ticker: str, since_ts: str) -> List[Dict]:
        """
        Query InfluxDB for market_snapshot data since a given timestamp.
        """
        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {since_ts})
            |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
            |> filter(fn: (r) => r["symbol"] == "{ticker}")
            |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
            |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        try:
            tables = self._query_api.query(query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    results.append(
                        {
                            "time": record.get_time(),
                            "percent_change": record.values.get("percent_change"),
                            "current_price": record.values.get("current_price"),
                            "accumulated_volume": record.values.get(
                                "accumulated_volume"
                            ),
                            "rank": record.values.get("rank"),
                        }
                    )
            return results
        except Exception as e:
            logger.error(
                f"_query_snapshots_since - Error querying snapshots for {ticker}: {e}"
            )
            return []

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("close - SnapshotAnalyzer closed")
