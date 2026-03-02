"""
Execution & Cost Modeling — Transaction cost estimation and impact analysis.

Sub-modules:
    cost_model: Cost estimation (fixed, spread, square-root market impact)
    cost_analysis: Portfolio-level cost analytics (turnover, net-of-cost returns,
                   Sharpe vs cost sensitivity, breakeven cost)

Convention:
    - Weights DataFrames: (date, ticker, weight) — same as position_sizing output
    - Returns DataFrames: (date, ticker, daily_return) or (date, ticker, next_day_return)
    - Costs returned as positive fractions (0.001 = 10 bps)
    - All analysis uses next-day returns to avoid look-ahead bias

Reference: docs/quant_lab.tex — Part IV, Chapter 15 (Execution & Costs)
"""

from execution.cost_analysis import (
    breakeven_cost,
    compute_net_returns,
    compute_turnover,
    sharpe_vs_cost_curve,
)
from execution.cost_model import (
    CompositeCostModel,
    CostModel,
    FixedCostModel,
    SpreadCostModel,
    SqrtImpactCostModel,
)

__all__ = [
    # cost_model
    "CostModel",
    "FixedCostModel",
    "SpreadCostModel",
    "SqrtImpactCostModel",
    "CompositeCostModel",
    # cost_analysis
    "compute_turnover",
    "compute_net_returns",
    "sharpe_vs_cost_curve",
    "breakeven_cost",
]
