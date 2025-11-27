import json
import os
import socket
import time
from datetime import datetime
from threading import Thread
from zoneinfo import ZoneInfo

import polars as pl
import redis
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

from utils.backtest_utils.backtest_utils import only_common_stocks


class redis_engine:
    def __init__(self, data_callback=None, replay=False):
        self.replay_mode = replay

        # ----------redis stream-----------
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        self.STREAM_NAME = f"market_snapshot_stream:{today}"
        self.CONSUMER_GROUP = "market_consumers"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        # Create consumer group if not exists
        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        self.last_df = pl.DataFrame()
        self.data_callback = data_callback  # Callback function for processed data

    # -------------------------------------------------------------------------
    def _redis_stream_listener(self):
        print(f"Starting Redis Stream consumer {self.CONSUMER_NAME}...")
        print(f"Replay mode: {self.replay_mode}")

        # REPLAY HISTORY VIA XRANGE
        if self.replay_mode:
            print(">>> Replaying today's historical messages via XRANGE ...")

            try:
                all_history = self.redis_client.xrange(
                    self.STREAM_NAME, min="0", max="+"
                )
                print(f">>> Found {len(all_history)} historical messages")

                for message_id, message_data in all_history:
                    self._process_message(message_id, message_data, ack=False)

                print(
                    f">>> Replay finished! Processed {len(all_history)} historical messages. Now switching to real-time mode."
                )

            except Exception as e:
                print(f"Error during replay: {e}")

            self.replay_mode = False

        # REAL-TIME CONSUMPTION
        while True:
            try:
                self.redis_client.ping()

                messages = self.redis_client.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            self._process_message(message_id, message_data, ack=True)
                else:
                    print("No new messages, waiting...")

            except KeyboardInterrupt:
                print("Stopping Redis Stream listener...")
                break
            except Exception as e:
                print(f"Redis Stream error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(5)

        print("Redis Stream listener stopped")

    def _process_message(self, message_id, message_data, ack=True):
        try:
            json_data = message_data[b"data"]
            df = pl.read_json(json_data)
            filtered_df = self.data_process(df)

            self.last_df = filtered_df

            if self.data_callback:
                self.data_callback(filtered_df, df)

            if ack:
                self.redis_client.xack(
                    self.STREAM_NAME, self.CONSUMER_GROUP, message_id
                )

        except Exception as e:
            print(f"Error processing message {message_id}: {e}")
            import traceback

            traceback.print_exc()

    def data_process(self, df: pl.DataFrame):
        """Process received DataFrame"""
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

        return filled_df

    def get_stream_info(self):
        """get Stream info"""
        return self.redis_client.xinfo_stream(self.STREAM_NAME)

    def get_pending_messages(self):
        """Get pending msg"""
        return self.redis_client.xpending_range(self.STREAM_NAME, self.CONSUMER_GROUP)

    def cleanup_old_messages(self, max_messages=10000):
        """Clean up old msg to limit the Stream length"""
        self.redis_client.xtrim(self.STREAM_NAME, maxlen=max_messages)


if __name__ == "__main__":
    rd = redis_engine()
    redis_thread = Thread(target=rd._redis_stream_listener, daemon=True)
    redis_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
