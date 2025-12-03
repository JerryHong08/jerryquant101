import asyncio
import json
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
from live_monitor.market_mover_monitor.core.data.providers.borrow_fee import (
    BorrowFeeProvider,
)
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from live_monitor.market_mover_monitor.core.data.providers.news_fetcher import (
    MoomooStockResolver,
)

load_dotenv()


@dataclass
class TickerContext:
    """Ticker Context"""

    symbol: str
    window: list = field(default_factory=list)
    last_calc_ts: float = 0.0
    lock: Lock = field(default_factory=Lock)
    float_shares: float = None
    available_shares: float = None
    borrow_fee: float = None
    borrow_fee_update_time: datetime = None

    def add_quote(self, quote):
        with self.lock:
            self.window.append(quote)
            if len(self.window) > 1000:
                self.window.pop(0)


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
        print(f"[INFO][Manager] Start consumer: {self.CONSUMER_NAME}")

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
                        print(f"[DEBUG][Manager] symbol: {symbol}")

                        if action == "add":
                            self.add_ticker(symbol)
                            ctx = TickerContext(symbol)
                            self.contexts[symbol] = ctx

                            self.pool.submit(self._load_float_shares, ctx)
                            self.pool.submit(self._load_borrow_fees, ctx)

                        elif action == "remove":
                            self.remove_ticker(symbol)

                        self.redis_client.xack(
                            self.STREAM_NAME, self.CONSUMER_GROUP, msg_id
                        )

            except Exception as e:
                print("[Error][Manager] Redis:", e)
                time.sleep(5)

    def _load_float_shares(self, ctx: TickerContext):
        provider = FloatSharesProvider()
        try:
            float_data = provider.fetch_from_local(ctx.symbol)
            with ctx.lock:
                ctx.float_shares = float_data.data[0].float_shares
            print(
                f"[INFO][Manager] Loaded float_shares for {ctx.symbol}: {ctx.float_shares}"
            )
        except Exception as e:
            print(f"[ERROR][Manager] Failed to load float_shares for {ctx.symbol}: {e}")

    def _load_borrow_fees(self, ctx: TickerContext):
        provider = BorrowFeeProvider()
        try:
            borrow_data = provider.extract_realtime_borrow_fee(ctx.symbol)

            with ctx.lock:
                print(f"[DEBUG][Manager] borrow_data: {borrow_data}")
                ctx.available_shares = borrow_data["available_shares"]
                ctx.borrow_fee = borrow_data["borrow_fee"]
                ctx.borrow_fee_update_time = datetime.strptime(
                    borrow_data["update_time"], "%Y-%m-%d %I:%M:%S %p EST"
                )

            print(f"[INFO] Borrow fee updated: {ctx.symbol}")

        except Exception as e:
            with ctx.lock:
                ctx.available_shares = None
                ctx.borrow_fee = None
                ctx.borrow_fee_update_time = None

            print(f"[ERROR] Borrow fee failed for {ctx.symbol}: {e}")

    def _load_stock_news(self, ctx: TickerContext):
        news_fetcher = MoomooStockResolver()
        try:
            stock_info = news_fetcher.get_stock_info(ctx.symbol)
            if stock_info:
                news_result = news_fetcher.get_news(ctx.symbol, pageSize=5)
                print(f"[DEBUG][Manager] news_result: {news_result}")
                print(json.dumps(news_result, indent=2, ensure_ascii=False))

                with ctx.lock:
                    # ctx.news_fetch_time = news_result['news_data']['server_time']
                    # ctx.news_list = news_result['news_data']['list']
                    pass

                print(f"[INFO] stock latest news updated: {ctx.symbol}")
            else:
                print(f"[ERROR] Could not find {ctx.symbol}")

        except Exception as e:
            with ctx.lock:
                pass

            print(f"[ERROR] Latest news updated failed for {ctx.symbol}: {e}")

    def add_ticker(self, symbol):
        """Add ticker"""
        if symbol in self.active_tickers:
            return

        print(f"[INFO][Manager] Add ticker {symbol}")

        self.active_tickers.add(symbol)

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_manager", symbols=[symbol]
            ),
            self.ws_loop,
        )

        asyncio.run_coroutine_threadsafe(self._consume_symbol(symbol), self.ws_loop)

    def remove_ticker(self, symbol):
        """Remove ticker"""
        if symbol not in self.active_tickers:
            return

        print(f"[INFO][Manager] Remove ticker {symbol}")

        self.active_tickers.remove(symbol)
        self.contexts.pop(symbol, None)

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.unsubscribe(
                websocket_client="factor_manager", symbol=symbol
            ),
            self.ws_loop,
        )

    async def _consume_symbol(self, symbol):
        """comsume WebSocket queue"""
        q = self.ws_manager.queues.get(symbol)
        if q is None:
            return

        while True:
            quote = await q.get()
            self.on_quote(symbol, quote)

    def on_quote(self, symbol, quote):
        """Quote handler"""
        ctx = self.contexts.get(symbol)
        if not ctx:
            return

        ctx.add_quote(quote)

        # thread pool factor compute
        self.pool.submit(self.compute_factors, ctx)

    def compute_factors(self, ctx: TickerContext):
        "Compute factors"
        now = time.time()

        # 200ms for less computing, but not now
        # if now - ctx.last_calc_ts < 0.2:
        #     return

        ctx.last_calc_ts = now

        with ctx.lock:
            if not ctx.window:
                return

            last = ctx.window[-1]
            # print(f'[DEBUG][Manager] last: {last}')
            mid = (last["bid"] + last["ask"]) / 2

        # TODO: Redis push add later
        # self.redis_client.hset(
        #     f"factor:{ctx.symbol}",
        #     mapping={"mid_price": mid, "ts": now}
        # )

        human_time = datetime.fromtimestamp(ctx.last_calc_ts).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
        print(
            f"[DEBUG][Manager] factor:mid {mid},"
            f"ts: {human_time},"
            f"float_shares: {ctx.float_shares},"
            f"borrow_fee: {ctx.borrow_fee}"
        )

    def start(self):
        """Start manager"""
        Thread(target=self._tasks_listener, daemon=True).start()
        print("[INFO][Manager] FactorManager started.")


if __name__ == "__main__":
    """Run"""
    fm = FactorManager()
    fm.start()

    while True:
        time.sleep(1)
