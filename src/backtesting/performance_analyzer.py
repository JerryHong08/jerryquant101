"""
回测性能分析器 - 计算详细的策略表现指标
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import polars as pl

from backtesting.backtest_pre_data import load_irx_data


class PerformanceAnalyzer:
    """
    回测性能分析器，计算各种性能指标
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
        计算详细的回测性能指标

        Args:
            portfolio_daily: 每日组合表现DataFrame，包含 [date, portfolio_return, equity_curve]
            trades: 交易记录DataFrame
            benchmark_data: 基准数据DataFrame，包含 [date, benchmark_return]

        Returns:
            性能指标字典
        """
        if portfolio_daily.is_empty():
            return self._empty_metrics()

        # 基础统计
        start_date = portfolio_daily["date"].min()
        end_date = portfolio_daily["date"].max()
        period_days = (end_date - start_date).total_seconds() / (24 * 3600)

        start_value = self.initial_capital
        end_value = (
            portfolio_daily["equity_curve"].tail(1).item() * self.initial_capital
        )

        # 收益率计算
        total_return = (end_value / start_value - 1) * 100

        # 基准收益率
        benchmark_return = 0.0
        if benchmark_data is not None and not benchmark_data.is_empty():
            benchmark_aligned = portfolio_daily.join(
                benchmark_data, on="date", how="inner"
            )
            if not benchmark_aligned.is_empty():
                benchmark_start = benchmark_aligned["benchmark_return"].head(1).item()
                benchmark_end = benchmark_aligned["benchmark_return"].tail(1).item()
                benchmark_return = (benchmark_end / benchmark_start - 1) * 100

        # 最大回撤
        equity_curve = portfolio_daily["equity_curve"].to_numpy()
        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        max_drawdown = np.min(drawdown) * 100

        # 最大回撤持续时间
        max_dd_duration = self._calculate_max_drawdown_duration(equity_curve)

        # 交易统计
        trade_stats = self._calculate_trade_stats(trades)

        # 风险指标
        daily_irx = load_irx_data(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )

        if daily_irx is not None:
            print(
                f"IRX date range: {daily_irx['date'].min()} to {daily_irx['date'].max()}"
            )

            portfolio_with_irx = portfolio_daily.join(
                daily_irx.select(["date", "irx_rate"]), on="date", how="left"
            )

            returns = portfolio_with_irx["portfolio_return"]
            irx_aligned = portfolio_with_irx["irx_rate"]

            print(f"Valid data points after alignment: {len(portfolio_with_irx)}")
        else:
            returns = portfolio_daily["portfolio_return"].drop_nulls()
            irx_aligned = None

        # irx_aligned = None
        risk_metrics = self._calculate_risk_metrics(returns, irx_aligned)

        # trade fees culculate (assume 0.7% per trade)
        total_fees = len(trades) * end_value * 0.007

        # 暴露度 (假设满仓)
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
        """计算交易统计指标"""
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

        # 计算交易持续时间
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

        # 盈亏统计
        winning_trades = returns[returns > 0]
        losing_trades = returns[returns < 0]

        win_rate = len(winning_trades) / total_closed * 100 if total_closed > 0 else 0
        best_trade = np.max(returns) if len(returns) > 0 else 0
        worst_trade = np.min(returns) if len(returns) > 0 else 0

        avg_winning = np.mean(winning_trades) if len(winning_trades) > 0 else 0
        avg_losing = np.mean(losing_trades) if len(losing_trades) > 0 else 0

        # 持续时间统计
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

        # 盈亏比和期望值
        total_profit = np.sum(winning_trades) if len(winning_trades) > 0 else 0
        total_loss = abs(np.sum(losing_trades)) if len(losing_trades) > 0 else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float("inf")

        expectancy = np.mean(returns) if len(returns) > 0 else 0

        return {
            "Total Trades": total_trades,
            "Total Closed Trades": total_closed,
            "Total Open Trades": total_open,
            "Open Trade PnL": 0.0,  # 假设没有未平仓收益
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

    def _calculate_risk_metrics(
        self, returns: pl.Series, irx_daily_rate: Optional[pl.DataFrame] = None
    ) -> Dict[str, float]:
        """计算风险指标"""
        if returns.is_empty():
            return {
                "Sharpe Ratio": 0.0,
                "Calmar Ratio": 0.0,
                "Omega Ratio": 0.0,
                "Sortino Ratio": 0.0,
            }

        returns_array = returns.to_numpy()

        if irx_daily_rate is not None:
            print("Calculating risk metrics with IRX data...")
            irx_daily_array = irx_daily_rate.to_numpy()
            # Ensure both arrays have the same length by taking the minimum
            print(len(returns_array), len(irx_daily_array))
            min_length = min(len(returns_array), len(irx_daily_array))

            returns_array = returns_array[:min_length]
            irx_daily_array = irx_daily_array[:min_length]
            execess_return = returns_array - irx_daily_array
        else:
            print("Not found IRX data")
            execess_return = returns_array

        annual_vol = np.std(execess_return) * np.sqrt(252)

        # Sharpe比率
        sharpe_ratio = execess_return.mean() * 252 / annual_vol if annual_vol > 0 else 0

        annual_return = np.mean(returns_array) * 252
        # Sortino比率 (下行波动率)
        negative_returns = returns_array[returns_array < 0]
        downside_vol = (
            np.std(negative_returns) * np.sqrt(252)
            if len(negative_returns) > 0
            else annual_vol
        )
        sortino_ratio = annual_return / downside_vol if downside_vol > 0 else 0

        # Calmar比率 (需要最大回撤)
        calmar_ratio = 0.0  # 简化处理，实际需要传入max_drawdown

        # Omega比率 (简化计算)
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
        """计算最大回撤持续时间"""
        peak = np.maximum.accumulate(equity_curve)
        is_drawdown = equity_curve < peak

        if not np.any(is_drawdown):
            return "0 days 00:00:00"

        # 找到连续回撤期间
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
        """返回空的性能指标"""
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
        """打印性能摘要"""
        print(f"\n{'='*60}")
        print(f"{strategy_name} 回测结果")
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
