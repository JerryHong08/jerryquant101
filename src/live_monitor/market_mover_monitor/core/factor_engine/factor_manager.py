import asyncio
import json
import logging
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock, Thread
from zoneinfo import ZoneInfo

import redis
from dotenv import load_dotenv

from live_monitor.market_mover_monitor.core.collector.polygon_manager import (
    PolygonWebSocketManager,
)
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from live_monitor.market_mover_monitor.core.data.providers.news_fetcher import (
    MoomooStockResolver,
)
from live_monitor.market_mover_monitor.core.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

load_dotenv()


@dataclass
class TickerContext:
    """Ticker Context"""

    symbol: str

    quote_window: list = field(default_factory=list)
    trade_window: list = field(default_factory=list)

    trade_ts_window: list = field(default_factory=list)
    aggressor_window: list = field(default_factory=list)

    belief_state: dict = field(default_factory=dict)

    last_calc_ts: float = 0.0
    lock: Lock = field(default_factory=Lock)

    float_shares: float = None
    available_shares: float = None
    news_fetch_time: datetime = None
    news_list: list = None

    def add_quote(self, quote):
        with self.lock:
            self.quote_window.append(quote)
            if len(self.quote_window) > 1000:
                self.quote_window.pop(0)

    def add_trade(self, trade):
        with self.lock:
            self.trade_window.append(trade)
            self.trade_ts_window.append(trade["timestamp"])

            if self.quote_window:
                last_q = self.quote_window[-1]
                if trade["price"] >= last_q["ask"]:
                    self.aggressor_window.append(1)  # Buy
                elif trade["price"] <= last_q["bid"]:
                    self.aggressor_window.append(-1)  # Sell
                else:
                    self.aggressor_window.append(0)  # Neutral

            if len(self.trade_window) > 1000:
                self.trade_window.pop(0)
                self.trade_ts_window.pop(0)
                self.aggressor_window.pop(0)


class FactorManager:
    """Factor Manager"""

    def __init__(self, replay_date=None):
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

        # ------- Redis Stream Name -------
        if replay_date:
            today_stream = f"factor_tasks_replay:{replay_date}"
        else:
            today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
            today_stream = f"factor_tasks:{today}"

        self.STREAM_NAME = today_stream
        self.CONSUMER_GROUP = "factor_tasks_consumer"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # ------- Runtime State -------
        self.active_tickers = set()
        self.contexts = {}

        self.ws_manager = PolygonWebSocketManager(api_key=os.getenv("POLYGON_API_KEY"))

        # thread pool for CPU-bound factor compute
        self.pool = ThreadPoolExecutor(max_workers=8)

        # start an event loop asyncio, for websocket listening
        self.ws_loop = asyncio.new_event_loop()
        Thread(target=self._start_ws_loop, daemon=True).start()

    def _start_ws_loop(self):
        """start WebSocket event loop"""
        asyncio.set_event_loop(self.ws_loop)
        self.ws_loop.run_until_complete(self.ws_manager.stream_forever())

    def _tasks_listener(self):
        """Redis Stream Listener"""
        logger.info(f"_tasks_listener -  Start consumer: {self.CONSUMER_NAME}")

        while True:
            try:
                messages = self.redis_client.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.STREAM_NAME: ">"},
                    count=10,
                    block=2000,
                )

                if not messages:
                    continue

                for stream_name, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        action = msg_data.get(b"action", b"").decode()
                        symbol = msg_data.get(b"ticker", b"").decode().upper()
                        logger.info(
                            f"_tasks_listener - action: {action} symbol: {symbol}"
                        )

                        if action == "add":
                            self.add_ticker(symbol)
                            ctx = TickerContext(symbol)
                            self.contexts[symbol] = ctx

                            self.pool.submit(self._load_float_shares, ctx)
                            self.pool.submit(self._load_stock_news, ctx)

                        elif action == "remove":
                            self.remove_ticker(symbol)

                        self.redis_client.xack(
                            self.STREAM_NAME, self.CONSUMER_GROUP, msg_id
                        )

            except Exception as e:
                logger.error("_tasks_listener - Redis:", e)
                time.sleep(5)

    def _load_float_shares(self, ctx: TickerContext):
        provider = FloatSharesProvider()
        try:
            float_data = provider.fetch_from_local(ctx.symbol)
            with ctx.lock:
                ctx.float_shares = float_data.data[0].float_shares
            logger.info(
                f"_load_float_shares - Loaded float_shares for {ctx.symbol}: {ctx.float_shares}"
            )
        except Exception as e:
            logger.error(
                f"_load_float_shares - Failed to load float_shares for {ctx.symbol}: {e}"
            )

    def _load_stock_news(self, ctx: TickerContext):
        news_fetcher = MoomooStockResolver()
        try:
            stock_info = news_fetcher.get_stock_info(ctx.symbol)
            if stock_info:
                news_articles = news_fetcher.get_news_momo(ctx.symbol, pageSize=5)
                if news_articles:
                    logger.info(f"_load_stock_news - news_result: {news_articles}")

                    with ctx.lock:
                        # ctx.news_fetch_time = news_result['news_data']['server_time']
                        ctx.news_fetch_time = datetime.now(ZoneInfo("America/New_York"))
                        ctx.news_list = news_articles

                    # logger.info(f"_load_stock_news - stock latest news updated: {ctx.symbol}")
                    # logger.debug(f"_load_stock_news - stock latest news debug: \n news_fetch_time: {ctx.news_fetch_time} \n news_list: {[[article.title, article.published_time] for article in ctx.news_list[:3]]}")
                    time_passed = (
                        ctx.news_fetch_time - ctx.news_list[0].published_time
                    ).total_seconds() / 60
                    logger.debug(
                        f"_load_stock_news - stock latest news published time til now has passed: {time_passed} mins"
                    )
            else:
                logger.error(f"_load_stock_news - Could not find {ctx.symbol}")

        except Exception as e:
            with ctx.lock:
                pass

            logger.error(
                f"_load_stock_news - Latest news updated failed for {ctx.symbol}: {e}"
            )

    def add_ticker(self, symbol):
        """Add ticker"""
        if symbol in self.active_tickers:
            return

        logger.info(f"add_ticker - Add ticker {symbol}")

        self.active_tickers.add(symbol)

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_manager", symbols=[symbol], events=["Q", "T"]
            ),
            self.ws_loop,
        )
        streams_keys = ["Q." + symbol, "T." + symbol]
        for stream_key in streams_keys:
            asyncio.run_coroutine_threadsafe(
                self._consume_stream_key(stream_key), self.ws_loop
            )

    def remove_ticker(self, symbol):
        """Remove ticker"""
        if symbol not in self.active_tickers:
            return

        logger.info(f"remove_ticker - Remove ticker {symbol}")

        self.active_tickers.remove(symbol)
        self.contexts.pop(symbol, None)

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.unsubscribe(
                websocket_client="factor_manager", symbol=symbol, events=["Q", "T"]
            ),
            self.ws_loop,
        )

    async def _consume_stream_key(self, stream_key):
        """comsume WebSocket queue"""
        logger.debug(f"_consume_stream_key - Consume stream key {stream_key}")
        q = self.ws_manager.queues.get(stream_key)
        if q is None:
            return

        while True:
            data = await q.get()
            # logger.debug(f"_consume_stream_key - Consume stream data {data}")
            self.on_tick(stream_key, data)

    def on_tick(self, stream_key, data):
        """Quote handler"""
        if stream_key == f"Q.{data['symbol']}":
            ctx = self.contexts.get(data["symbol"])
            if not ctx:
                return

            ctx.add_quote(data)

        if stream_key == f"T.{data['symbol']}":
            ctx = self.contexts.get(data["symbol"])
            if not ctx:
                return

            ctx.add_trade(data)

        # thread pool factor compute
        self.pool.submit(self.compute_factors, ctx)

    def compute_belief_dynamics(self, ctx: TickerContext):
        if len(ctx.trade_ts_window) < 20:
            return None

        ts = ctx.trade_ts_window[-20:]
        intervals = [ts[i] - ts[i - 1] for i in range(1, len(ts))]

        mean_interval = sum(intervals) / len(intervals)
        trade_rate = 1.0 / mean_interval if mean_interval > 0 else 0

        first = intervals[: len(intervals) // 2]
        second = intervals[len(intervals) // 2 :]

        accel = (sum(first) / len(first)) - (sum(second) / len(second))

        aggressiveness = sum(ctx.aggressor_window[-20:]) / 20

        return {
            "trade_rate": round(trade_rate, 4),
            "accel": round(accel, 4),
            "aggressiveness": round(aggressiveness, 4),
        }

    def compute_factors(self, ctx: TickerContext):
        "Compute factors"
        now = time.time()

        # 200ms for less computing, but not now
        # if now - ctx.last_calc_ts < 0.2:
        #     return

        ctx.last_calc_ts = now
        belief = self.compute_belief_dynamics(ctx)
        if not belief:
            return

        ctx.belief_state = belief

        # with ctx.lock:
        #     if not ctx.quote_window:
        #         return

        #     last = ctx.quote_window[-1]
        #     spread = last["ask"] - last["bid"]

        # TODO: add Redis push later
        # self.redis_client.hset(
        #     f"factor:{ctx.symbol}",
        #     mapping={"mid_price": mid, "ts": now}
        # )

        human_time = datetime.fromtimestamp(ctx.last_calc_ts).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

        logger.debug(
            f"compute_factors - {ctx.symbol} "
            f"factor:belief_state  {ctx.belief_state},"
            f"ts: {human_time},"
            f"float_shares: {ctx.float_shares},"
        )

    def start(self):
        """Start manager"""
        Thread(target=self._tasks_listener, daemon=True).start()
        logger.info("start - FactorManager started.")


if __name__ == "__main__":
    """Run"""
    import argparse

    parser = argparse.ArgumentParser(description="Factor Manager")

    parser.add_argument(
        "--replay-date", help="receive specific replay date data in format YYYYMMDD"
    )
    args = parser.parse_args()

    if args.replay_date:
        try:
            datetime.strptime(args.replay_date, "%Y%m%d")
        except ValueError:
            parser.error("replay-date must be in format YYYYMMDD (e.g., 20240115)")

    fm = FactorManager(replay_date=args.replay_date)
    fm.start()

    while True:
        time.sleep(1)
