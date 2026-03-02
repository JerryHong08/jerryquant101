"""
Portfolio Tracker — simulate portfolio equity from daily weights and returns.

Pure computation: no I/O, no plotting, no strategy logic.  Tracks:
    - Daily portfolio return (weights × stock returns)
    - Equity curve (cumulative product of 1 + r)
    - Turnover (daily weight changes)
    - Position history

This is the core simulation engine that both ``WeightBacktester`` (new)
and the old ``BacktestEngine`` can delegate to.

Usage:
    tracker = PortfolioTracker(initial_capital=100.0)
    result = tracker.run(weights_df, returns_df)
    print(result.portfolio_daily)
    print(result.total_turnover)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import polars as pl

from constants import (
    DATE_COL,
    RETURN_COL,
    TICKER_COL,
    TRADING_DAYS_PER_YEAR,
    WEIGHT_COL,
)


@dataclass(frozen=True)
class TrackingResult:
    """Immutable container for portfolio tracking output.

    Attributes:
        portfolio_daily: DataFrame(date, portfolio_return, equity_curve).
            Compatible with ``PerformanceAnalyzer.calculate_performance_metrics()``.
        turnover: DataFrame(date, turnover) — daily one-way turnover.
        total_turnover: Sum of daily turnovers.
        n_days: Trading days in simulation.
        position_count: DataFrame(date, n_long, n_short, n_total).
    """

    portfolio_daily: pl.DataFrame
    turnover: pl.DataFrame
    total_turnover: float
    n_days: int
    position_count: pl.DataFrame


class PortfolioTracker:
    """Simulate portfolio equity from weight and return DataFrames.

    Args:
        initial_capital: Starting portfolio value (default 100.0 = percentage).
        cost_bps: One-way transaction cost in basis points (default 0 = no cost).
            Applied proportionally to turnover each day.
    """

    def __init__(
        self,
        initial_capital: float = 100.0,
        cost_bps: float = 0.0,
    ):
        self.initial_capital = initial_capital
        self.cost_bps = cost_bps

    def run(
        self,
        weights: pl.DataFrame,
        returns: pl.DataFrame,
        date_col: str = DATE_COL,
        ticker_col: str = TICKER_COL,
        weight_col: str = WEIGHT_COL,
        return_col: str = "next_day_return",
    ) -> TrackingResult:
        """Run the portfolio simulation.

        Args:
            weights: DataFrame with (date, ticker, weight).
                Weights are positions *before* the trading day; the return
                on that date is applied to those weights.
            returns: DataFrame with (date, ticker, <return_col>).
                Typically next-day returns aligned so that ``date`` in weights
                matches ``date`` in returns.
            date_col: Date column name.
            ticker_col: Ticker column name.
            weight_col: Weight column name in ``weights``.
            return_col: Return column name in ``returns``.

        Returns:
            ``TrackingResult`` with portfolio_daily, turnover, etc.
        """
        # ── Join weights and returns ──
        combined = weights.join(returns, on=[date_col, ticker_col], how="inner")

        if combined.is_empty():
            return self._empty_result()

        # ── Daily portfolio return (weight × return, summed per date) ──
        daily_port = (
            combined.with_columns(
                (pl.col(weight_col) * pl.col(return_col)).alias("weighted_ret")
            )
            .group_by(date_col)
            .agg(pl.col("weighted_ret").sum().alias("portfolio_return"))
            .sort(date_col)
        )

        # ── Apply transaction costs ──
        turnover_df = self._compute_turnover(weights, date_col, ticker_col, weight_col)

        # Align datetime resolution (input data may be ns while turnover_df is μs)
        port_dtype = daily_port[date_col].dtype
        if turnover_df[date_col].dtype != port_dtype:
            turnover_df = turnover_df.cast({date_col: port_dtype})

        if self.cost_bps > 0.0:
            cost_rate = self.cost_bps / 10_000
            daily_port = (
                daily_port.join(turnover_df, on=date_col, how="left")
                .with_columns(
                    (
                        pl.col("portfolio_return")
                        - pl.col("turnover").fill_null(0.0) * cost_rate
                    ).alias("portfolio_return")
                )
                .drop("turnover")
            )

        # ── Equity curve ──
        returns_arr = daily_port["portfolio_return"].to_numpy()
        equity = np.cumprod(1.0 + returns_arr) * (self.initial_capital / 100.0)
        # Normalize so equity_curve starts near 1.0 (matching PerformanceAnalyzer expectation)
        equity_curve = np.cumprod(1.0 + returns_arr)
        daily_port = daily_port.with_columns(pl.Series("equity_curve", equity_curve))

        # ── Position counts ──
        position_count = self._compute_position_counts(
            weights, date_col, ticker_col, weight_col
        )

        # ── Turnover stats ──
        total_turnover = (
            float(turnover_df["turnover"].sum()) if not turnover_df.is_empty() else 0.0
        )

        return TrackingResult(
            portfolio_daily=daily_port,
            turnover=turnover_df,
            total_turnover=total_turnover,
            n_days=len(returns_arr),
            position_count=position_count,
        )

    def _compute_turnover(
        self,
        weights: pl.DataFrame,
        date_col: str,
        ticker_col: str,
        weight_col: str,
    ) -> pl.DataFrame:
        """Compute daily one-way turnover (sum of absolute weight changes).

        Turnover on day t = 0.5 × Σ|w_t(i) - w_{t-1}(i)| across all tickers.
        """
        dates = weights.select(date_col).unique().sort(date_col)
        date_list = dates[date_col].to_list()

        if len(date_list) < 2:
            return pl.DataFrame(
                {date_col: date_list, "turnover": [0.0] * len(date_list)}
            )

        turnovers = [0.0]  # First day: no prior weights
        prev_weights = weights.filter(pl.col(date_col) == date_list[0])

        for d in date_list[1:]:
            curr_weights = weights.filter(pl.col(date_col) == d)

            # Full outer join on ticker to handle tickers appearing/disappearing
            merged = (
                prev_weights.select([ticker_col, pl.col(weight_col).alias("w_prev")])
                .join(
                    curr_weights.select(
                        [ticker_col, pl.col(weight_col).alias("w_curr")]
                    ),
                    on=ticker_col,
                    how="full",
                    coalesce=True,
                )
                .with_columns(
                    pl.col("w_prev").fill_null(0.0),
                    pl.col("w_curr").fill_null(0.0),
                )
            )

            turnover = float((merged["w_curr"] - merged["w_prev"]).abs().sum()) * 0.5
            turnovers.append(turnover)
            prev_weights = curr_weights

        return pl.DataFrame({date_col: date_list, "turnover": turnovers})

    def _compute_position_counts(
        self,
        weights: pl.DataFrame,
        date_col: str,
        ticker_col: str,
        weight_col: str,
    ) -> pl.DataFrame:
        """Count long/short/total positions per day."""
        return (
            weights.group_by(date_col)
            .agg(
                [
                    (pl.col(weight_col) > 1e-8).sum().alias("n_long"),
                    (pl.col(weight_col) < -1e-8).sum().alias("n_short"),
                    (pl.col(weight_col).abs() > 1e-8).sum().alias("n_total"),
                ]
            )
            .sort(date_col)
        )

    def _empty_result(self) -> TrackingResult:
        """Return an empty TrackingResult for degenerate inputs."""
        empty_daily = pl.DataFrame(
            {
                "date": [],
                "portfolio_return": [],
                "equity_curve": [],
            }
        )
        empty_turnover = pl.DataFrame({"date": [], "turnover": []})
        empty_positions = pl.DataFrame(
            {
                "date": [],
                "n_long": [],
                "n_short": [],
                "n_total": [],
            }
        )
        return TrackingResult(
            portfolio_daily=empty_daily,
            turnover=empty_turnover,
            total_turnover=0.0,
            n_days=0,
            position_count=empty_positions,
        )
