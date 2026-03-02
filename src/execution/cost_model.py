"""
Cost Model — Transaction cost estimation for backtesting.

Three models, from simplest to most realistic:

    1. FixedCostModel:      Flat cost per dollar traded (e.g. 5 bps).
    2. SpreadCostModel:     Half-spread cost — you pay the bid-ask spread.
    3. SqrtImpactCostModel: Square-root market impact — cost grows with
                             trade size relative to daily volume.

These can be composed via CompositeCostModel (e.g. spread + impact).

Usage:
    from execution.cost_model import FixedCostModel, CompositeCostModel

    model = FixedCostModel(cost_bps=5.0)
    cost = model.estimate(trade_value=10000)  # → 5.0

    # More realistic: spread + impact
    model = CompositeCostModel([
        SpreadCostModel(half_spread_bps=2.0),
        SqrtImpactCostModel(eta=0.1),
    ])

Reference: docs/quant_lab.tex — Part IV, Chapter 15 (Execution & Costs)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
import polars as pl

# ── Abstract Base ─────────────────────────────────────────────────────────────


class CostModel(ABC):
    """
    Abstract base class for transaction cost models.

    All cost models implement a single interface:
        estimate(trade_value, **kwargs) → cost in dollars

    Convention:
        - trade_value: Absolute dollar value of the trade (always positive).
        - Returns: Cost in dollars (always positive).
        - To get cost as a fraction: cost / trade_value.
    """

    @abstractmethod
    def estimate(
        self,
        trade_value: float,
        *,
        daily_volume: Optional[float] = None,
        price: Optional[float] = None,
    ) -> float:
        """
        Estimate the cost of a single trade.

        Args:
            trade_value: Absolute dollar value being traded.
            daily_volume: Average daily dollar volume (for impact models).
            price: Current stock price (for tick-size models).

        Returns:
            Cost in dollars (positive).
        """
        ...

    def estimate_array(
        self,
        trade_values: np.ndarray,
        *,
        daily_volumes: Optional[np.ndarray] = None,
        prices: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        """
        Vectorized cost estimation for an array of trades.

        Default implementation loops over estimate(); subclasses can override
        for better performance.

        Args:
            trade_values: Array of absolute dollar trade values.
            daily_volumes: Array of daily dollar volumes (optional).
            prices: Array of stock prices (optional).

        Returns:
            Array of costs in dollars.
        """
        n = len(trade_values)
        costs = np.empty(n)
        for i in range(n):
            kwargs = {}
            if daily_volumes is not None:
                kwargs["daily_volume"] = daily_volumes[i]
            if prices is not None:
                kwargs["price"] = prices[i]
            costs[i] = self.estimate(trade_values[i], **kwargs)
        return costs

    @abstractmethod
    def describe(self) -> str:
        """Return a human-readable description of the cost model."""
        ...


# ── Fixed Cost Model ──────────────────────────────────────────────────────────


class FixedCostModel(CostModel):
    """
    Fixed proportional cost — a flat fraction of trade value.

    This is the simplest model:
        cost = trade_value × (cost_bps / 10_000)

    Good for:
        - Quick sensitivity analysis ("what if costs are X bps?")
        - Brokerage commissions that scale with trade value

    Not so good for:
        - Large trades where market impact dominates
        - Illiquid stocks where the spread varies

    Args:
        cost_bps: Cost in basis points (1 bp = 0.01%).
                  Typical values: 1–5 bps for liquid large-cap,
                  10–30 bps for small-cap or illiquid.
    """

    def __init__(self, cost_bps: float = 5.0):
        if cost_bps < 0:
            raise ValueError(f"cost_bps must be non-negative, got {cost_bps}")
        self.cost_bps = cost_bps
        self._cost_frac = cost_bps / 10_000

    def estimate(
        self,
        trade_value: float,
        *,
        daily_volume: Optional[float] = None,
        price: Optional[float] = None,
    ) -> float:
        return abs(trade_value) * self._cost_frac

    def estimate_array(
        self,
        trade_values: np.ndarray,
        *,
        daily_volumes: Optional[np.ndarray] = None,
        prices: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        return np.abs(trade_values) * self._cost_frac

    def describe(self) -> str:
        return f"FixedCost({self.cost_bps:.1f} bps)"


# ── Spread Cost Model ─────────────────────────────────────────────────────────


class SpreadCostModel(CostModel):
    """
    Half-spread cost — you cross the bid-ask spread on each trade.

    When you buy, you pay the ask; when you sell, you receive the bid.
    The cost per trade is approximately half the bid-ask spread.

        cost = trade_value × (half_spread_bps / 10_000)

    In reality, the spread varies by stock, time of day, and market
    conditions.  Using a constant is a first approximation.

    Args:
        half_spread_bps: Half the bid-ask spread in basis points.
                         Typical: 1–2 bps for AAPL/MSFT,
                         5–15 bps for mid-cap, 20+ for micro-cap.

    Note:
        For a more realistic model, you'd estimate per-stock spreads
        from NBBO data and vary them by stock and date.  That's a
        Phase 6 improvement if we get there.
    """

    def __init__(self, half_spread_bps: float = 2.0):
        if half_spread_bps < 0:
            raise ValueError(
                f"half_spread_bps must be non-negative, got {half_spread_bps}"
            )
        self.half_spread_bps = half_spread_bps
        self._cost_frac = half_spread_bps / 10_000

    def estimate(
        self,
        trade_value: float,
        *,
        daily_volume: Optional[float] = None,
        price: Optional[float] = None,
    ) -> float:
        return abs(trade_value) * self._cost_frac

    def estimate_array(
        self,
        trade_values: np.ndarray,
        *,
        daily_volumes: Optional[np.ndarray] = None,
        prices: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        return np.abs(trade_values) * self._cost_frac

    def describe(self) -> str:
        return f"SpreadCost({self.half_spread_bps:.1f} bps half-spread)"


# ── Square-Root Market Impact Model ───────────────────────────────────────────


class SqrtImpactCostModel(CostModel):
    r"""
    Square-root market impact model (Almgren-style).

    The most widely used market impact model in production:

        impact = η · σ · √(trade_value / ADV)

    where:
        η (eta) = impact coefficient (dimensionless, typically 0.05–0.30)
        σ = daily volatility of the stock
        ADV = average daily dollar volume

    The key insight: impact grows as the square root of participation rate,
    not linearly.  Trading 4× the volume only costs 2× as much in impact.

    Why square root?
        - Empirically confirmed across equity markets (Almgren et al. 2005,
          Bershova & Rakhlin 2013).
        - Intuitive: the first share you trade moves the price less than
          the last share, because order book depth is consumed gradually.

    Args:
        eta: Impact coefficient.  Higher = more expensive.
             Typical: 0.05–0.10 for liquid large-cap,
             0.15–0.30 for small-cap.
        default_volatility: Daily vol used if per-stock vol not provided.
        default_adv: Default ADV in dollars if not provided.

    Note:
        Without per-stock volatility and volume, this falls back to
        the defaults — effectively a proportional cost.  For full
        accuracy, provide daily_volume and compute σ from recent data.
    """

    def __init__(
        self,
        eta: float = 0.10,
        default_volatility: float = 0.02,
        default_adv: float = 50_000_000.0,
    ):
        if eta < 0:
            raise ValueError(f"eta must be non-negative, got {eta}")
        self.eta = eta
        self.default_volatility = default_volatility
        self.default_adv = default_adv

    def estimate(
        self,
        trade_value: float,
        *,
        daily_volume: Optional[float] = None,
        price: Optional[float] = None,
        volatility: Optional[float] = None,
    ) -> float:
        adv = daily_volume if daily_volume is not None else self.default_adv
        sigma = volatility if volatility is not None else self.default_volatility
        if adv <= 0:
            return 0.0
        participation = abs(trade_value) / adv
        impact_frac = self.eta * sigma * np.sqrt(participation)
        return abs(trade_value) * impact_frac

    def estimate_array(
        self,
        trade_values: np.ndarray,
        *,
        daily_volumes: Optional[np.ndarray] = None,
        prices: Optional[np.ndarray] = None,
        volatilities: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        adv = (
            daily_volumes
            if daily_volumes is not None
            else np.full(len(trade_values), self.default_adv)
        )
        sigma = (
            volatilities
            if volatilities is not None
            else np.full(len(trade_values), self.default_volatility)
        )
        adv = np.maximum(adv, 1e-8)
        participation = np.abs(trade_values) / adv
        impact_frac = self.eta * sigma * np.sqrt(participation)
        return np.abs(trade_values) * impact_frac

    def describe(self) -> str:
        return (
            f"SqrtImpact(η={self.eta:.2f}, "
            f"σ_default={self.default_volatility:.3f}, "
            f"ADV_default=${self.default_adv:,.0f})"
        )


# ── Composite Cost Model ─────────────────────────────────────────────────────


class CompositeCostModel(CostModel):
    """
    Compose multiple cost models — the total cost is the sum.

    Real trading costs have multiple components:
        1. Brokerage commission (fixed)
        2. Bid-ask spread (spread)
        3. Market impact (sqrt)

    CompositeCostModel lets you combine them:

        model = CompositeCostModel([
            SpreadCostModel(half_spread_bps=2.0),
            SqrtImpactCostModel(eta=0.10),
        ])

    Args:
        models: List of CostModel instances.  Costs are summed.
    """

    def __init__(self, models: list[CostModel]):
        if not models:
            raise ValueError("CompositeCostModel requires at least one model")
        self.models = models

    def estimate(
        self,
        trade_value: float,
        *,
        daily_volume: Optional[float] = None,
        price: Optional[float] = None,
        **kwargs,
    ) -> float:
        total = 0.0
        for model in self.models:
            total += model.estimate(trade_value, daily_volume=daily_volume, price=price)
        return total

    def estimate_array(
        self,
        trade_values: np.ndarray,
        *,
        daily_volumes: Optional[np.ndarray] = None,
        prices: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        total = np.zeros(len(trade_values))
        for model in self.models:
            total += model.estimate_array(
                trade_values, daily_volumes=daily_volumes, prices=prices
            )
        return total

    def describe(self) -> str:
        parts = [m.describe() for m in self.models]
        return "Composite(" + " + ".join(parts) + ")"
