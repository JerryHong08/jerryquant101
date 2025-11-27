# core/factor_engine/factors/cross_hotness_score.py
from live_monitor.market_mover_monitor.core.factor_engine.factor_base import FactorBase


class FirstEntryFactor(FactorBase):
    def __init__(self):
        self.history = set()

    def compute(self, snapshot):
        ticker = snapshot["symbol"]
        in_top = snapshot.get("rank") <= 20

        if in_top and ticker not in self.history:
            self.history.add(ticker)
            return {"first_time_rank20": True}
        return {"first_time_rank20": False}
