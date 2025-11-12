from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import polars as pl


class PerformanceAnalyzer:
    """
    Backtest performance analyzer for calculating various performance metrics
    """

    def __init__(self, initial_capital: float = 100.0):
        self.initial_capital = initial_capital

    def calculate_performance_metrics(
        self,
        portfolio_daily: pl.DataFrame,
        trades: pl.DataFrame,
        benchmark_data: Optional[pl.DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Calculate detailed backtest performance metrics

        Args:
            portfolio_daily: Daily portfolio performance DataFrame containing [date, portfolio_return, equity_curve]
            trades: Trading records DataFrame
            benchmark_data: Benchmark data DataFrame containing [date, benchmark_return]

        Returns:
            Performance metrics dictionary
        """
        if portfolio_daily.is_empty():
            return self._empty_metrics()

        # Basic statistics
        start_date = portfolio_daily["date"].min()
        end_date = portfolio_daily["date"].max()
        period_days = (end_date - start_date).total_seconds() / (24 * 3600)

        start_value = self.initial_capital
        end_value = (
            portfolio_daily["equity_curve"].tail(1).item() * self.initial_capital
        )

        # Return calculation
        total_return = (end_value / start_value - 1) * 100

        # Benchmark return
        benchmark_return = 0.0
        if benchmark_data is not None and not benchmark_data.is_empty():
            benchmark_aligned = portfolio_daily.join(
                benchmark_data, on="date", how="inner"
            )
            if not benchmark_aligned.is_empty():
                benchmark_start = benchmark_aligned["benchmark_return"].head(1).item()
                benchmark_end = benchmark_aligned["benchmark_return"].tail(1).item()
                benchmark_return = (benchmark_end / benchmark_start - 1) * 100

        # Maximum drawdown
        equity_curve = portfolio_daily["equity_curve"].to_numpy()
        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        max_drawdown = np.min(drawdown) * 100

        # Maximum drawdown duration
        max_dd_duration = self._calculate_max_drawdown_duration(equity_curve)

        # Trading statistics
        trade_stats = self._calculate_trade_stats(trades)

        returns = portfolio_daily["portfolio_return"].drop_nulls()

        risk_metrics = self._calculate_risk_metrics(returns)

        # Trading fees calculation (assume 0.7% per trade)
        total_fees = len(trades) * end_value * 0.007

        # Exposure (assume fully invested)
        max_gross_exposure = 100.0

        metrics = {
            "Start": start_date,
            "End": end_date,
            "Period": f"{int(period_days)} days 00:00:00",
            "Start Value": start_value,
            "End Value": end_value,
            "Total Return [%]": total_return,
            "Benchmark Return [%]": benchmark_return,
            "Max Gross Exposure [%]": max_gross_exposure,
            "Total Fees Paid": total_fees,
            "Max Drawdown [%]": abs(max_drawdown),
            "Max Drawdown Duration": max_dd_duration,
            **trade_stats,
            **risk_metrics,
        }

        return metrics

    def _calculate_trade_stats(self, trades: pl.DataFrame) -> Dict[str, Any]:
        """Calculate trading statistics"""
        if trades.is_empty():
            return {
                "Total Trades": 0,
                "Total Closed Trades": 0,
                "Total Open Trades": 0,
                "Open Trade PnL": 0.0,
                "Win Rate [%]": 0.0,
                "Best Trade [%]": 0.0,
                "Worst Trade [%]": 0.0,
                "Avg Winning Trade [%]": 0.0,
                "Avg Losing Trade [%]": 0.0,
                "Avg Winning Trade Duration": "0 days 00:00:00",
                "Avg Losing Trade Duration": "0 days 00:00:00",
                "Profit Factor": 0.0,
                "Expectancy": 0.0,
            }

        # Calculate trade duration
        trades_with_duration = trades.with_columns(
            [
                ((pl.col("sell_date") - pl.col("buy_date")).dt.total_days()).alias(
                    "duration_days"
                ),
                (pl.col("return") * 100).alias("return_pct"),
            ]
        )

        total_trades = len(trades_with_duration)
        closed_trades = trades_with_duration.filter(pl.col("sell_date").is_not_null())
        total_closed = len(closed_trades)
        total_open = total_trades - total_closed

        if total_closed == 0:
            return {
                "Total Trades": total_trades,
                "Total Closed Trades": 0,
                "Total Open Trades": total_open,
                "Open Trade PnL": 0.0,
                "Win Rate [%]": 0.0,
                "Best Trade [%]": 0.0,
                "Worst Trade [%]": 0.0,
                "Avg Winning Trade [%]": 0.0,
                "Avg Losing Trade [%]": 0.0,
                "Avg Winning Trade Duration": "0 days 00:00:00",
                "Avg Losing Trade Duration": "0 days 00:00:00",
                "Profit Factor": 0.0,
                "Expectancy": 0.0,
            }

        returns = closed_trades["return_pct"].to_numpy()
        durations = closed_trades["duration_days"].to_numpy()

        # Win/loss statistics
        winning_trades = returns[returns > 0]
        losing_trades = returns[returns < 0]

        win_rate = len(winning_trades) / total_closed * 100 if total_closed > 0 else 0
        best_trade = np.max(returns) if len(returns) > 0 else 0
        worst_trade = np.min(returns) if len(returns) > 0 else 0

        avg_winning = np.mean(winning_trades) if len(winning_trades) > 0 else 0
        avg_losing = np.mean(losing_trades) if len(losing_trades) > 0 else 0

        # Duration statistics
        winning_durations = durations[returns > 0]
        losing_durations = durations[returns < 0]

        avg_win_duration = (
            f"{int(np.mean(winning_durations))} days 00:00:00"
            if len(winning_durations) > 0
            else "0 days 00:00:00"
        )
        avg_lose_duration = (
            f"{int(np.mean(losing_durations))} days 00:00:00"
            if len(losing_durations) > 0
            else "0 days 00:00:00"
        )

        # Profit factor and expectancy
        total_profit = np.sum(winning_trades) if len(winning_trades) > 0 else 0
        total_loss = abs(np.sum(losing_trades)) if len(losing_trades) > 0 else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float("inf")

        expectancy = np.mean(returns) if len(returns) > 0 else 0

        return {
            "Total Trades": total_trades,
            "Total Closed Trades": total_closed,
            "Total Open Trades": total_open,
            "Open Trade PnL": 0.0,  # Assume no unrealized PnL
            "Win Rate [%]": win_rate,
            "Best Trade [%]": best_trade,
            "Worst Trade [%]": worst_trade,
            "Avg Winning Trade [%]": avg_winning,
            "Avg Losing Trade [%]": avg_losing,
            "Avg Winning Trade Duration": avg_win_duration,
            "Avg Losing Trade Duration": avg_lose_duration,
            "Profit Factor": profit_factor,
            "Expectancy": expectancy,
        }

    def _calculate_risk_metrics(self, returns: pl.Series) -> Dict[str, float]:
        """Calculate risk metrics"""
        if returns.is_empty():
            return {
                "Sharpe Ratio": 0.0,
                "Calmar Ratio": 0.0,
                "Omega Ratio": 0.0,
                "Sortino Ratio": 0.0,
            }

        returns_array = returns.to_numpy()

        annual_vol = np.std(returns_array) * np.sqrt(252)

        # Sharpe ratio
        sharpe_ratio = returns_array.mean() * 252 / annual_vol if annual_vol > 0 else 0

        annual_return = np.mean(returns_array) * 252
        # Sortino ratio (downside volatility)
        negative_returns = returns_array[returns_array < 0]
        downside_vol = (
            np.std(negative_returns) * np.sqrt(252)
            if len(negative_returns) > 0
            else annual_vol
        )
        sortino_ratio = annual_return / downside_vol if downside_vol > 0 else 0

        # Calmar ratio (simplified, would need max_drawdown)
        calmar_ratio = 0.0  # Simplified handling, actually needs max_drawdown

        # Omega ratio (simplified calculation)
        positive_returns = returns_array[returns_array > 0]
        negative_returns = returns_array[returns_array < 0]
        omega_ratio = (
            (np.sum(positive_returns) / abs(np.sum(negative_returns)))
            if len(negative_returns) > 0
            else float("inf")
        )

        return {
            "Sharpe Ratio": sharpe_ratio,
            "Calmar Ratio": calmar_ratio,
            "Omega Ratio": omega_ratio,
            "Sortino Ratio": sortino_ratio,
        }

    def _calculate_max_drawdown_duration(self, equity_curve: np.ndarray) -> str:
        """Calculate maximum drawdown duration"""
        peak = np.maximum.accumulate(equity_curve)
        is_drawdown = equity_curve < peak

        if not np.any(is_drawdown):
            return "0 days 00:00:00"

        # Find consecutive drawdown periods
        drawdown_starts = np.where(np.diff(np.concatenate(([False], is_drawdown))))[0]
        drawdown_ends = np.where(np.diff(np.concatenate((is_drawdown, [False]))))[0]

        if len(drawdown_starts) == 0:
            return "0 days 00:00:00"

        max_duration = 0
        for start, end in zip(drawdown_starts, drawdown_ends):
            duration = end - start
            max_duration = max(max_duration, duration)

        return f"{max_duration} days 00:00:00"

    def _empty_metrics(self) -> Dict[str, Any]:
        """Return empty performance metrics"""
        return {
            "Start": None,
            "End": None,
            "Period": "0 days 00:00:00",
            "Start Value": self.initial_capital,
            "End Value": self.initial_capital,
            "Total Return [%]": 0.0,
            "Benchmark Return [%]": 0.0,
            "Max Gross Exposure [%]": 0.0,
            "Total Fees Paid": 0.0,
            "Max Drawdown [%]": 0.0,
            "Max Drawdown Duration": "0 days 00:00:00",
            "Total Trades": 0,
            "Total Closed Trades": 0,
            "Total Open Trades": 0,
            "Open Trade PnL": 0.0,
            "Win Rate [%]": 0.0,
            "Best Trade [%]": 0.0,
            "Worst Trade [%]": 0.0,
            "Avg Winning Trade [%]": 0.0,
            "Avg Losing Trade [%]": 0.0,
            "Avg Winning Trade Duration": "0 days 00:00:00",
            "Avg Losing Trade Duration": "0 days 00:00:00",
            "Profit Factor": 0.0,
            "Expectancy": 0.0,
            "Sharpe Ratio": 0.0,
            "Calmar Ratio": 0.0,
            "Omega Ratio": 0.0,
            "Sortino Ratio": 0.0,
        }

    def print_performance_summary(
        self, metrics: Dict[str, Any], strategy_name: str = "Strategy"
    ):
        """Print performance summary"""
        print(f"\n{'='*60}")
        print(f"{strategy_name} Backtest Results")
        print(f"{'='*60}")

        for key, value in metrics.items():
            if isinstance(value, float):
                if abs(value) > 1000:
                    print(f"{key:<35} {value:>20,.2f}")
                else:
                    print(f"{key:<35} {value:>20.6f}")
            else:
                print(f"{key:<35} {str(value):>20}")

        print(f"{'='*60}\n")
