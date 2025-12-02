# core/factor_engine/factor_engine.py
import json
import time
from datetime import datetime

import polars as pl
import redis

from live_monitor.market_mover_monitor.core.factor_engine.factor_base import FactorBase


class FactorEngine:
    def __init__(self, contexts, active_tickers):
        self.contexts = contexts
        self.active_tickers = active_tickers

    def run(self):
        while True:
            tickers = list(self.active_tickers)

            for t in tickers:
                ctx = self.contexts.get(t)
                if ctx is None:
                    continue
                if ctx.static is None:
                    continue

                factors = self.compute_all(ctx)

                redis.hset(f"factor:{ctx.date}:{t}", mapping=factors)

                if self.should_publish(ctx.last_output, factors):
                    redis.publish(
                        "factor_updates",
                        json.dumps(
                            {
                                "ticker": t,
                                "factors": factors,
                            }
                        ),
                    )

                ctx.last_output = factors

            time.sleep(0.2)
