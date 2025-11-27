# core/factor_engine/factor_engine.py
from datetime import datetime

import polars as pl

from live_monitor.market_mover_monitor.core.factor_engine.factor_base import FactorBase


class FactorEngine:
    def __init__(self):
        self.factors: list[FactorBase] = []

    def register(self, factor: FactorBase):
        self.factors.append(factor)

    def compute_all(self, snapshot: dict) -> dict:
        results = {}
        for f in self.factors:
            try:
                result = f.compute(snapshot)
                if result:
                    results.update(result)
            except Exception as e:
                print(f"[Factor Error] {type(f).__name__}: {e}")
        return results
