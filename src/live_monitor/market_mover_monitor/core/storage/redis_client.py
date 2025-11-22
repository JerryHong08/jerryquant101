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


class redis_engine:
    def __init__(self):
        # ----------redis api--------

        # Redis setup
        # self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
        # self.pubsub = self.redis_client.pubsub()
        # self.pubsub.subscribe("market_snapshot")

        # ----------redis stream-----------

        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
        self.STREAM_NAME = "market_snapshot_stream"
        self.CONSUMER_GROUP = "market_consumers"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _redis_stream_listener(self):
        """Redis Stream consumer group listener"""
        print(f"Starting Redis Stream consumer {self.CONSUMER_NAME}...")

        while True:
            try:
                # read msg from consumer
                messages = self.redis_client.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.STREAM_NAME: ">"},  # '>' stands for new msg only
                    count=1,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            print(
                                f"received message from redis stream at {datetime.now(ZoneInfo('America/New_York'))}"
                            )

                            try:
                                json_data = message_data[b"data"]
                                df = pl.read_json(json_data)

                                self.data_process(df)

                                # make sure msg processed.
                                self.redis_client.xack(
                                    self.STREAM_NAME, self.CONSUMER_GROUP, message_id
                                )

                            except Exception as e:
                                print(f"Error processing message {message_id}: {e}")

            except Exception as e:
                print(f"Redis Stream error: {e}")
                time.sleep(1)

    def _redis_listener(self):
        """Redis message listener running in separate thread"""
        print("Starting Redis listener...")

        for message in self.pubsub.listen():
            print(
                f'received message from redis at {datetime.now(ZoneInfo("America/New_York"))}'
            )
            if message["type"] == "message":
                try:
                    json_data = message["data"]
                    df = pl.read_json(json_data)

                    self.data_process(df)

                except Exception as e:
                    print(f"Error processing Redis message: {e}")
                    self.socketio.emit(
                        "error", {"message": f"Data processing error: {str(e)}"}
                    )

    def get_stream_info(self):
        """get Stream info"""
        return self.redis_client.xinfo_stream(self.STREAM_NAME)

    def get_pending_messages(self):
        """Get pending msg"""
        return self.redis_client.xpending_range(self.STREAM_NAME, self.CONSUMER_GROUP)

    def cleanup_old_messages(self, max_messages=10000):
        """Clean up old msg to limit the Stream length"""
        self.redis_client.xtrim(self.STREAM_NAME, maxlen=max_messages)
