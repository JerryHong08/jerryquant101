"""
Cost Analysis — Portfolio-level transaction cost analytics.

This module connects cost models to portfolio weights and returns to answer:
    1. How much turnover does each sizing method generate?
    2. What is the net-of-cost Sharpe ratio?
    3. At what cost level does the strategy break even?
    4. How does Sharpe degrade as costs increase?

Key functions:
    - compute_turnover: Daily turnover from weight changes
    - compute_net_returns: Apply cost model to gross returns
    - sharpe_vs_cost_curve: Sweep over cost levels
    - breakeven_cost: Find cost level where Sharpe = 0

Usage:
    from execution.cost_analysis import compute_turnover, breakeven_cost

    turnover = compute_turnover(weights_df)
    be = breakeven_cost(gross_returns, turnover_series)
    print(f"Strategy breaks even at {be*10_000:.1f} bps")

Reference: docs/quant_lab.tex — Part IV, Chapter 15 (Execution & Costs)
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import polars as pl

from execution.cost_model import CostModel, FixedCostModel

# ── Turnover ──────────────────────────────────────────────────────────────────


def compute_turnover(weights: pl.DataFrame) -> pl.DataFrame:
    """
    Compute daily one-way turnover from a weights DataFrame.

    Turnover is defined as:
        turnover_t = (1/2) × Σ_i |w_{i,t} - w_{i,t-1}|

    This is **one-way** turnover — the fraction of the portfolio that
    is traded each day.  Two-way turnover = 2 × one-way.

    For a long-short portfolio with sum(|w|) = 1, turnover ranges from
    0 (no change) to 1 (complete portfolio replacement).

    Args:
        weights: DataFrame with columns (date, ticker, weight).

    Returns:
        DataFrame with columns (date, turnover).
        First date is dropped (no prior weights to compare).

    Note:
        High turnover is the enemy of factor strategies.  A factor with
        Sharpe 0.5 and 200% annual turnover can easily become negative
        after costs.  The rule of thumb:

            Net Sharpe ≈ Gross Sharpe − (turnover × cost_per_trade × √252)
    """
    _validate_weights(weights)

    # Get sorted dates
    dates = weights.select("date").unique().sort("date")
    date_list = dates["date"].to_list()

    if len(date_list) < 2:
        return pl.DataFrame({"date": [], "turnover": []})

    # For each consecutive pair of dates, compute turnover
    turnover_records = []
    for i in range(1, len(date_list)):
        prev_date = date_list[i - 1]
        curr_date = date_list[i]

        prev_w = (
            weights.filter(pl.col("date") == prev_date)
            .select(["ticker", "weight"])
            .rename({"weight": "w_prev"})
        )
        curr_w = (
            weights.filter(pl.col("date") == curr_date)
            .select(["ticker", "weight"])
            .rename({"weight": "w_curr"})
        )

        # Full outer join to handle stocks entering/leaving
        merged = prev_w.join(curr_w, on="ticker", how="full", coalesce=True)
        merged = merged.with_columns(
            pl.col("w_prev").fill_null(0.0),
            pl.col("w_curr").fill_null(0.0),
        )

        # One-way turnover = 0.5 * Σ|Δw|
        delta_sum = merged.select(
            ((pl.col("w_curr") - pl.col("w_prev")).abs().sum() / 2.0).alias("turnover")
        )
        turnover_records.append(
            {"date": curr_date, "turnover": delta_sum["turnover"][0]}
        )

    return pl.DataFrame(turnover_records)


# ── Net-of-Cost Returns ──────────────────────────────────────────────────────


def compute_net_returns(
    gross_returns: np.ndarray,
    turnover: np.ndarray,
    cost_model: CostModel,
    portfolio_value: float = 1.0,
) -> np.ndarray:
    """
    Compute net-of-cost returns from gross returns and turnover.

    The cost on each day is:
        cost_t = cost_model.estimate(turnover_t × portfolio_value)

    This is subtracted from gross returns:
        net_return_t = gross_return_t − cost_t / portfolio_value

    Args:
        gross_returns: Array of daily gross portfolio returns.
        turnover: Array of daily one-way turnover (same length).
        cost_model: A CostModel instance.
        portfolio_value: Assumed portfolio notional (default $1).

    Returns:
        Array of daily net returns.

    Note:
        For a FixedCostModel with cost_bps=5, the daily cost is simply
        turnover × 5/10_000.  This is the most common approximation.
    """
    if len(gross_returns) != len(turnover):
        raise ValueError(
            f"gross_returns ({len(gross_returns)}) and turnover ({len(turnover)}) "
            f"must have the same length"
        )

    # Dollar value traded each day
    trade_values = np.abs(turnover) * portfolio_value
    costs = cost_model.estimate_array(trade_values)
    cost_fracs = costs / portfolio_value

    return gross_returns - cost_fracs


# ── Sharpe vs Cost Curve ──────────────────────────────────────────────────────


def sharpe_vs_cost_curve(
    gross_returns: np.ndarray,
    turnover: np.ndarray,
    cost_bps_range: Optional[np.ndarray] = None,
    annualization_factor: int = 252,
) -> dict:
    """
    Compute Sharpe ratio as a function of cost level.

    Sweeps over a range of fixed-cost levels and computes the net Sharpe
    at each.  This answers: "how much cost can my strategy tolerate?"

    The curve should be approximately linear for fixed-cost models:
        Sharpe(c) ≈ Sharpe_gross − c × avg_turnover × √252 / σ

    Args:
        gross_returns: Array of daily gross returns.
        turnover: Array of daily one-way turnover (same length).
        cost_bps_range: Array of cost levels in bps to sweep.
                        Default: [0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 50].
        annualization_factor: Trading days per year.

    Returns:
        Dictionary with:
            - cost_bps: Array of cost levels tested
            - net_sharpe: Sharpe at each cost level
            - net_annual_return: Annualized return at each cost level
            - gross_sharpe: The gross (zero-cost) Sharpe for reference
            - avg_daily_turnover: Mean daily one-way turnover
            - annual_turnover: Annualized one-way turnover (× 252)
    """
    if cost_bps_range is None:
        cost_bps_range = np.array([0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 50], dtype=float)

    avg_to = float(np.mean(turnover))

    results = {
        "cost_bps": cost_bps_range,
        "net_sharpe": np.empty(len(cost_bps_range)),
        "net_annual_return": np.empty(len(cost_bps_range)),
        "gross_sharpe": _sharpe(gross_returns, annualization_factor),
        "avg_daily_turnover": avg_to,
        "annual_turnover": avg_to * annualization_factor,
    }

    for i, bps in enumerate(cost_bps_range):
        model = FixedCostModel(cost_bps=bps)
        net = compute_net_returns(gross_returns, turnover, model)
        results["net_sharpe"][i] = _sharpe(net, annualization_factor)
        results["net_annual_return"][i] = float(np.mean(net)) * annualization_factor

    return results


# ── Breakeven Cost ────────────────────────────────────────────────────────────


def breakeven_cost(
    gross_returns: np.ndarray,
    turnover: np.ndarray,
    annualization_factor: int = 252,
    precision_bps: float = 0.1,
    max_bps: float = 200.0,
) -> float:
    """
    Find the cost level (in bps) where the strategy's Sharpe = 0.

    Uses binary search over cost levels.

    If the gross Sharpe is already ≤ 0, returns 0.0 (strategy is dead
    before costs).

    Args:
        gross_returns: Array of daily gross returns.
        turnover: Array of daily one-way turnover.
        annualization_factor: Trading days per year.
        precision_bps: Search precision in bps.
        max_bps: Maximum cost level to search.

    Returns:
        Breakeven cost in basis points.  The strategy is profitable
        only if actual costs are below this level.

    Interview context:
        "My factor has a gross Sharpe of 0.3 and breaks even at 8 bps.
         With realistic costs of 3–5 bps for large-cap US equities,
         there's a 3–5 bps margin of safety."
    """
    gross_sharpe = _sharpe(gross_returns, annualization_factor)
    if gross_sharpe <= 0:
        return 0.0

    # Binary search
    lo, hi = 0.0, max_bps

    while (hi - lo) > precision_bps:
        mid = (lo + hi) / 2
        model = FixedCostModel(cost_bps=mid)
        net = compute_net_returns(gross_returns, turnover, model)
        s = _sharpe(net, annualization_factor)
        if s > 0:
            lo = mid
        else:
            hi = mid

    return (lo + hi) / 2


# ── Internal ──────────────────────────────────────────────────────────────────


def _sharpe(returns: np.ndarray, annualization_factor: int = 252) -> float:
    """Annualized Sharpe ratio (rf=0)."""
    mu = np.mean(returns)
    sigma = np.std(returns, ddof=1)
    if sigma < 1e-10:
        return 0.0
    return mu / sigma * np.sqrt(annualization_factor)


def _validate_weights(weights: pl.DataFrame) -> None:
    """Validate that weights DataFrame has the required columns."""
    required = {"date", "ticker", "weight"}
    missing = required - set(weights.columns)
    if missing:
        raise ValueError(
            f"Weights DataFrame missing columns: {missing}. "
            f"Expected (date, ticker, weight), got {weights.columns}"
        )
