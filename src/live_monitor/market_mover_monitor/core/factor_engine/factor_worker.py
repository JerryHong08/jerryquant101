# Factor Worker

import os
import socket
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import redis


class FactorEngine:
    def __int__(self, replay_date=None):
        replay_mode = False
        if replay_date:  # YYYYMMDD
            replay_mode = True

        # ----------redis stream-----------
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

        if replay_mode:
            self.STREAM_NAME = f"market_snapshot_stream_replay:{replay_date}"
            # self.factor_redis_hash_name = f"factors:{ticker}"
        else:
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

    def _factor_redis_stream_listener(self):
        print(f"INFO: Starting Factor Redis Stream consumer {self.CONSUMER_NAME}...")
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

                            print(f"Debug: message_data: {message_data}")

                            # self.process_msg(message_id, message_data, ack=True)
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
