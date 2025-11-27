# core/factor_engine/factors/fundamental_score.py
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from live_monitor.market_mover_monitor.core.factor_engine.factor_base import FactorBase


class FundamentalFactor(FactorBase):
    def __init__(self):
        self.provider = FloatSharesProvider()

    def compute(self, snapshot):
        ticker = snapshot["symbol"]
        info = self.provider.fetch_from_local(ticker)
        return {
            "float_shares": info.float_shares,
            # "country": info.country,
            "is_low_float": info.float_shares < 15000000,  # example
        }
