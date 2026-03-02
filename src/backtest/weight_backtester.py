"""
Weight-Based Backtester — bridge from alpha signals to backtest analytics.

Accepts portfolio weight DataFrames (from ``portfolio.pipeline``) and
produces the same performance metrics as the legacy ``BacktestEngine``,
without requiring a ``StrategyBase`` subclass.

This is the **alpha → backtest bridge** identified as the #1 architectural
gap in the Phase 4.5 assessment.

Usage:
    from backtest.weight_backtester import WeightBacktester

    bt = WeightBacktester(cost_bps=5.0)
    result = bt.run(weights, next_day_returns, benchmark_data=spy)

    print(f"Sharpe: {result.metrics['Sharpe Ratio']:.3f}")
    bt.print_summary(result)
    bt.export(result, output_dir="results/half_kelly")

Reference: docs/quant_lab.tex — Part III, Chapter 12
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import numpy as np
import polars as pl

from constants import DATE_COL, TICKER_COL, TRADING_DAYS_PER_YEAR, WEIGHT_COL

from .performance_analyzer import PerformanceAnalyzer
from .portfolio_tracker import PortfolioTracker, TrackingResult


class BacktestResult:
    """Container for weight-based backtest results.

    Attributes:
        tracking: Raw tracking output (equity, turnover, positions).
        metrics: Performance metrics dict (from PerformanceAnalyzer).
        config: Configuration dict used for this backtest.
        benchmark_data: Benchmark DataFrame (if provided).
    """

    def __init__(
        self,
        tracking: TrackingResult,
        metrics: Dict[str, Any],
        config: Dict[str, Any],
        benchmark_data: Optional[pl.DataFrame] = None,
    ):
        self.tracking = tracking
        self.metrics = metrics
        self.config = config
        self.benchmark_data = benchmark_data

    @property
    def portfolio_daily(self) -> pl.DataFrame:
        """Convenience: portfolio_daily from tracking."""
        return self.tracking.portfolio_daily

    @property
    def sharpe(self) -> float:
        """Convenience: extract Sharpe from metrics."""
        return float(self.metrics.get("Sharpe Ratio", 0.0))

    @property
    def total_return(self) -> float:
        """Convenience: total return percentage."""
        return float(self.metrics.get("Total Return [%]", 0.0))

    @property
    def max_drawdown(self) -> float:
        """Convenience: max drawdown percentage (negative)."""
        return float(self.metrics.get("Max Drawdown [%]", 0.0))


class WeightBacktester:
    """Backtest a strategy defined by a weight DataFrame.

    Unlike ``BacktestEngine`` which requires a ``StrategyBase`` subclass,
    this class accepts raw weight DataFrames — the output of
    ``portfolio.pipeline.run_alpha_pipeline()`` or any other signal-to-weight
    pipeline.

    Args:
        initial_capital: Starting portfolio value (default 100.0).
        cost_bps: One-way transaction cost in basis points (default 5.0).
        trading_fee_rate: Legacy fee rate for PerformanceAnalyzer (default 0.007).
    """

    def __init__(
        self,
        initial_capital: float = 100.0,
        cost_bps: float = 5.0,
        trading_fee_rate: float = 0.007,
    ):
        self.initial_capital = initial_capital
        self.cost_bps = cost_bps
        self.tracker = PortfolioTracker(
            initial_capital=initial_capital,
            cost_bps=cost_bps,
        )
        self.analyzer = PerformanceAnalyzer(
            initial_capital=initial_capital,
            trading_fee_rate=trading_fee_rate,
        )

    def run(
        self,
        weights: pl.DataFrame,
        returns: pl.DataFrame,
        benchmark_data: Optional[pl.DataFrame] = None,
        return_col: str = "next_day_return",
        name: str = "WeightStrategy",
    ) -> BacktestResult:
        """Run backtest from weights and returns.

        Args:
            weights: DataFrame(date, ticker, weight).
            returns: DataFrame(date, ticker, <return_col>).
            benchmark_data: Optional benchmark DataFrame(date, close)
                for relative performance metrics.
            return_col: Name of the return column in ``returns``.
            name: Strategy name for display purposes.

        Returns:
            ``BacktestResult`` with tracking, metrics, and config.
        """
        # ── Track portfolio ──
        tracking = self.tracker.run(
            weights,
            returns,
            return_col=return_col,
        )

        # ── Preprocess benchmark ──
        # PerformanceAnalyzer expects benchmark with 'benchmark_return' column
        # (cumulative value, not daily returns).  If caller passes raw
        # close prices, derive it automatically.
        bench_for_analyzer = self._prepare_benchmark(benchmark_data)

        # ── Compute performance metrics ──
        # PerformanceAnalyzer expects a trades DataFrame — pass empty since
        # weight-based strategies don't produce discrete trade records.
        empty_trades = pl.DataFrame(
            {
                "ticker": [],
                "buy_date": [],
                "sell_date": [],
                "buy_price": [],
                "sell_price": [],
                "return": [],
            }
        )

        metrics = self.analyzer.calculate_performance_metrics(
            portfolio_daily=tracking.portfolio_daily,
            trades=empty_trades,
            benchmark_data=bench_for_analyzer,
        )

        # ── Augment metrics with turnover info ──
        metrics["Total Turnover"] = tracking.total_turnover
        metrics["Avg Daily Turnover"] = tracking.total_turnover / max(
            1, tracking.n_days
        )
        metrics["Cost (bps)"] = self.cost_bps

        config = {
            "strategy_name": name,
            "initial_capital": self.initial_capital,
            "cost_bps": self.cost_bps,
            "n_days": tracking.n_days,
        }

        return BacktestResult(
            tracking=tracking,
            metrics=metrics,
            config=config,
            benchmark_data=benchmark_data,
        )

    def run_from_pipeline(
        self,
        pipeline_result: dict,
        benchmark_data: Optional[pl.DataFrame] = None,
        name: str = "AlphaPipeline",
    ) -> BacktestResult:
        """Run backtest directly from ``run_alpha_pipeline()`` output.

        Args:
            pipeline_result: Output dict from ``portfolio.pipeline.run_alpha_pipeline()``.
            benchmark_data: Optional benchmark data.
            name: Strategy name.

        Returns:
            ``BacktestResult``.
        """
        return self.run(
            weights=pipeline_result["weights"],
            returns=pipeline_result["next_day_returns"],
            benchmark_data=benchmark_data,
            return_col="next_day_return",
            name=name,
        )

    def print_summary(self, result: BacktestResult) -> None:
        """Print performance summary to console."""
        self.analyzer.print_performance_summary(
            result.metrics,
            result.config.get("strategy_name", "Strategy"),
        )

    def export(
        self,
        result: BacktestResult,
        output_dir: str = "backtest_results",
    ) -> None:
        """Export backtest results to disk.

        Creates:
            - ``portfolio_daily.csv``
            - ``turnover.csv``
            - ``position_counts.csv``
            - ``metrics.txt``
            - ``config.txt``

        Args:
            result: Output of ``run()`` or ``run_from_pipeline()``.
            output_dir: Directory to write files to.
        """
        os.makedirs(output_dir, exist_ok=True)
        name = result.config.get("strategy_name", "strategy")

        # Portfolio daily
        port_path = os.path.join(output_dir, f"{name}_portfolio_daily.csv")
        daily = result.portfolio_daily
        if "date" in daily.columns:
            try:
                daily = daily.with_columns(pl.col("date").dt.date().alias("date"))
            except Exception:
                pass
        daily.write_csv(port_path)

        # Turnover
        turnover_path = os.path.join(output_dir, f"{name}_turnover.csv")
        result.tracking.turnover.write_csv(turnover_path)

        # Position counts
        pos_path = os.path.join(output_dir, f"{name}_position_counts.csv")
        result.tracking.position_count.write_csv(pos_path)

        # Metrics
        metrics_path = os.path.join(output_dir, f"{name}_metrics.txt")
        with open(metrics_path, "w", encoding="utf-8") as f:
            f.write(f"{name} — Weight-Based Backtest Metrics\n")
            f.write("=" * 50 + "\n\n")
            for key, value in result.metrics.items():
                f.write(f"{key}: {value}\n")

        # Config
        config_path = os.path.join(output_dir, f"{name}_config.txt")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(f"{name} — Backtest Configuration\n")
            f.write("=" * 50 + "\n\n")
            for key, value in result.config.items():
                f.write(f"{key}: {value}\n")

        print(f"Results exported to {output_dir}/")

    def compare(
        self,
        results: Dict[str, BacktestResult],
    ) -> pl.DataFrame:
        """Compare multiple backtest results side by side.

        Args:
            results: Dict mapping strategy name → BacktestResult.

        Returns:
            DataFrame with one row per strategy, key metrics as columns.
        """
        rows = []
        for name, r in results.items():
            rows.append(
                {
                    "Strategy": name,
                    "Total Return [%]": r.metrics.get("Total Return [%]", 0.0),
                    "Sharpe Ratio": r.metrics.get("Sharpe Ratio", 0.0),
                    "Max Drawdown [%]": r.metrics.get("Max Drawdown [%]", 0.0),
                    "Avg Daily Turnover": r.metrics.get("Avg Daily Turnover", 0.0),
                    "Cost (bps)": r.metrics.get("Cost (bps)", 0.0),
                    "N Days": r.tracking.n_days,
                }
            )
        return pl.DataFrame(rows)

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _prepare_benchmark(
        benchmark_data: Optional[pl.DataFrame],
    ) -> Optional[pl.DataFrame]:
        """Ensure benchmark has ``benchmark_return`` column for PerformanceAnalyzer.

        Accepts either:
            * DataFrame with ``benchmark_return`` already present — pass through.
            * DataFrame with ``close`` — derive cumulative return (close / close[0]).

        Returns None if input is None or empty.
        """
        if benchmark_data is None or benchmark_data.is_empty():
            return None
        if "benchmark_return" in benchmark_data.columns:
            return benchmark_data
        if "close" in benchmark_data.columns:
            first_close = benchmark_data["close"][0]
            return benchmark_data.with_columns(
                (pl.col("close") / first_close).alias("benchmark_return")
            )
        return benchmark_data
