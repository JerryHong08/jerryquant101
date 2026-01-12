"""
Data Manager for Market Mover Web Analyzer
Handles snapshot processing, membership management, and state tracking.

Architecture:
- Redis ZSET: stores all subscribed tickers (ever appeared in top 20) with their first appearance time as score
- Redis HSET: stores state_cursor (last_state_updated_time per ticker) for recovery/replay
- InfluxDB market_snapshot: stores all subscribed tickers' historical snapshot data
- InfluxDB movers_state: stores state change events for each ticker
"""

import logging
import os
from datetime import datetime
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
    - Rank computation and membership management (Redis ZSET)
    - Historical data storage (InfluxDB market_snapshot)
    - State tracking and cursor management (Redis HSET + InfluxDB movers_state)
    """

    TOP_N = 20  # Number of top movers to track

    def __init__(
        self,
        replay_date: Optional[str] = None,
        replay_id: Optional[str] = None,
    ):
        replay_mode = bool(replay_date)
        self.run_mode = "replay" if replay_mode else "live"
        self.replay_id = self._derive_replay_id(replay_date, replay_id)
        self.replay_date = replay_date

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

        if replay_mode:
            logger.info(f"Replay mode activated for date: {replay_date}")
            date_suffix = replay_date
        else:
            date_suffix = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")

        # ZSET: stores subscribed tickers with first_appearance_ts as score
        self.ZSET_NAME = f"movers_subscribed:{date_suffix}"
        # HSET: stores state_cursor (last_state_updated_time per ticker)
        self.HSET_NAME = f"state_cursor:{date_suffix}"

        # In-memory state cache for state change detection
        self._ticker_states: Dict[str, Dict] = {}

        logger.info(
            f"SnapshotAnalyzer initialized: mode={self.run_mode}, "
            f"replay_id={self.replay_id}, ZSET={self.ZSET_NAME}, HSET={self.HSET_NAME}"
        )

    @staticmethod
    def _derive_replay_id(replay_date: Optional[str], override: Optional[str]) -> str:
        if replay_date and override:
            return f"{replay_date}_{override}"
        if override:
            return override
        if replay_date:
            return replay_date
        return "na"

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def process_snapshot(self, df: pl.DataFrame, is_historical: bool = False) -> Dict:
        """
        Main entry point: process a snapshot DataFrame.

        Flow:
        1. Parse timestamp and compute competition ranks
        2. Update ZSET membership for top N tickers
        3. Write all subscribed tickers' data to InfluxDB
        4. For each ticker, check state changes and update cursor

        Returns:
            Dict with processing summary (for logging/debugging)
        """
        # Step 0: Parse timestamp
        timestamp = self._extract_timestamp(df)

        # Step 1: Compute ranks (competition ranking)
        ranked_df = self._compute_ranks(df)

        # Step 2: Update membership - add new top N tickers to subscription
        current_top_n = ranked_df.head(self.TOP_N)
        new_subscriptions = self._update_membership(current_top_n, timestamp)

        # Step 3: Get all subscribed tickers and write their data to InfluxDB
        all_subscribed = self._get_all_subscribed_tickers()
        self._write_snapshot_to_influx(ranked_df, all_subscribed, timestamp)

        # Step 4: Update states and track changes
        state_changes = self._process_state_updates(
            ranked_df, all_subscribed, timestamp, is_historical
        )

        return {
            "timestamp": timestamp.isoformat(),
            "new_subscriptions": new_subscriptions,
            "total_subscribed": len(all_subscribed),
            "state_changes": state_changes,
        }

    def get_subscribed_tickers(self) -> List[str]:
        """Get all currently subscribed tickers from Redis ZSET."""
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
    # MEMBERSHIP MANAGEMENT (Redis ZSET)
    # =========================================================================

    def _update_membership(
        self, current_top_n: pl.DataFrame, timestamp: datetime
    ) -> List[str]:
        """
        Update Redis ZSET with new top N tickers.
        Score = first appearance timestamp (epoch seconds).
        Returns list of newly subscribed tickers.
        """
        new_subscriptions = []
        ts_epoch = timestamp.timestamp()

        for row in current_top_n.iter_rows(named=True):
            ticker = row["ticker"]
            # NX: only add if not exists (preserves first appearance time)
            added = self.r.zadd(self.ZSET_NAME, {ticker: ts_epoch}, nx=True)
            if added:
                new_subscriptions.append(ticker)
                logger.debug(f"New subscription: {ticker} at {timestamp.isoformat()}")

        if new_subscriptions:
            logger.info(f"New subscriptions: {new_subscriptions}")

        return new_subscriptions

    def _get_all_subscribed_tickers(self) -> List[str]:
        """Get all tickers from Redis ZSET (sorted by first appearance time)."""
        # ZRANGE returns members sorted by score (first appearance time)
        return self.r.zrange(self.ZSET_NAME, 0, -1)

    def get_ticker_first_appearance(self, ticker: str) -> Optional[datetime]:
        """Get the first appearance time for a ticker."""
        score = self.r.zscore(self.ZSET_NAME, ticker)
        if score is not None:
            return datetime.fromtimestamp(score, tz=ZoneInfo("America/New_York"))
        return None

    # =========================================================================
    # INFLUXDB SNAPSHOT STORAGE
    # =========================================================================

    def _write_snapshot_to_influx(
        self,
        ranked_df: pl.DataFrame,
        subscribed_tickers: List[str],
        timestamp: datetime,
    ) -> None:
        """
        Write snapshot data for all subscribed tickers to InfluxDB.

        Measurement: market_snapshot
        Tags: symbol, run_mode, replay_id
        Fields: percent_change, current_price, accumulated_volume, rank
        Time: snapshot timestamp
        """
        # Create lookup dict from DataFrame for O(1) access
        df_dict = {row["ticker"]: row for row in ranked_df.iter_rows(named=True)}

        points = []
        for ticker in subscribed_tickers:
            row = df_dict.get(ticker)
            if row is None:
                # Ticker not in current snapshot (may have dropped out of top movers)
                continue

            point = (
                influxdb_client.Point("market_snapshot")
                .tag("symbol", ticker)
                .tag("run_mode", self.run_mode)
                .tag("replay_id", self.replay_id)
                .field("percent_change", float(row.get("percent_change", 0.0)))
                .field("current_price", float(row.get("current_price", 0.0)))
                .field("accumulated_volume", float(row.get("accumulated_volume", 0)))
                .field("prev_close", float(row.get("prev_close", 0.0)))
                .field("rank", int(row.get("competition_rank", 0)))
                .time(timestamp)
            )
            points.append(point)

        if points:
            self._write_api.write(bucket=self.bucket, org=self.org, record=points)
            logger.debug(f"Wrote {len(points)} points to InfluxDB market_snapshot")

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

                # Update cursor in Redis HSET
                self._update_state_cursor(ticker, timestamp)

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

                if not is_historical:
                    logger.info(
                        f"State change: {ticker} -> {current_state.get('state')}"
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
        Tags: symbol, run_mode, replay_id
        Fields: state, rank, percent_change, rank_velocity
        Time: state_change timestamp
        """
        point = (
            influxdb_client.Point("movers_state")
            .tag("symbol", ticker)
            .tag("run_mode", self.run_mode)
            .tag("replay_id", self.replay_id)
            .field("state", state.get("state", "unknown"))
            .field("rank", int(state.get("rank", 0)))
            .field("percent_change", float(state.get("percent_change", 0.0)))
            .field("rank_velocity", int(state.get("rank_velocity", 0)))
            .time(timestamp)
        )

        self._write_api.write(bucket=self.bucket, org=self.org, record=point)
        logger.debug(
            f"Wrote state change {state.get('state', 'unknown')} for {ticker} to InfluxDB movers_state"
        )

    def _update_state_cursor(self, ticker: str, timestamp: datetime) -> None:
        """
        Update the state cursor in Redis HSET.
        Used for recovery/replay to know where to resume state computation.
        """
        cursor_value = timestamp.isoformat()
        self.r.hset(self.HSET_NAME, ticker, cursor_value)
        logger.debug(f"Updated state cursor: {ticker} = {cursor_value}")

    # =========================================================================
    # RECOVERY / REPLAY SUPPORT
    # =========================================================================

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
                f"Recovered {len(points)} snapshots for {ticker} since {cursor_ts}"
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
            |> filter(fn: (r) => r["replay_id"] == "{self.replay_id}")
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
            logger.error(f"Error querying snapshots for {ticker}: {e}")
            return []

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("SnapshotAnalyzer closed")
