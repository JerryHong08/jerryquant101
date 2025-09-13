"""
回测引擎 - 统一的回测执行和结果管理
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl

from .performance_analyzer import PerformanceAnalyzer
from .strategy_base import StrategyBase
from .visualizer import BacktestVisualizer


class BacktestEngine:
    """
    回测引擎，提供统一的回测执行和结果管理
    """

    def __init__(self, initial_capital: float = 100.0):
        self.initial_capital = initial_capital
        self.performance_analyzer = PerformanceAnalyzer(initial_capital)
        self.visualizer = BacktestVisualizer()
        self.results = {}

    def add_strategy(
        self,
        strategy: StrategyBase,
        ohlcv_data: pl.DataFrame,
        tickers: List[str] = None,
    ):
        """
        添加策略到回测引擎

        Args:
            strategy: 策略实例
            ohlcv_data: OHLCV数据
            tickers: 股票列表
        """
        strategy.set_data(ohlcv_data, tickers)
        return strategy

    def run_backtest(
        self,
        strategy: StrategyBase,
        benchmark_data: Optional[pl.DataFrame] = None,
        use_cached_indicators: bool = False,
        save_results: bool = True,
    ) -> Dict[str, Any]:
        """
        运行单个策略的回测

        Args:
            strategy: 策略实例
            benchmark_data: 基准数据
            use_cached_indicators: 是否使用缓存指标
            save_results: 是否保存结果

        Returns:
            完整的回测结果
        """
        print(f"\n开始回测策略: {strategy.name}")
        print("=" * 60)

        # 1. 运行策略回测
        strategy_results = strategy.run_backtest(use_cached_indicators)

        # 2. 计算性能指标
        print("计算性能指标...")
        performance_metrics = self.performance_analyzer.calculate_performance_metrics(
            portfolio_daily=strategy_results["portfolio_daily"],
            trades=strategy_results["trades"],
            benchmark_data=benchmark_data,
        )

        # 3. 组合完整结果
        complete_results = {
            **strategy_results,
            "performance_metrics": performance_metrics,
            "benchmark_data": benchmark_data,
            "backtest_config": {
                "initial_capital": self.initial_capital,
                "use_cached_indicators": use_cached_indicators,
            },
        }

        # 4. 保存结果
        if save_results:
            self.results[strategy.name] = complete_results

        # 5. 打印性能摘要
        self.performance_analyzer.print_performance_summary(
            performance_metrics, strategy.name
        )

        return complete_results

    def run_multiple_strategies(
        self,
        strategies: List[StrategyBase],
        benchmark_data: Optional[pl.DataFrame] = None,
        use_cached_indicators: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        运行多个策略的回测并比较

        Args:
            strategies: 策略列表
            benchmark_data: 基准数据
            use_cached_indicators: 是否使用缓存指标

        Returns:
            所有策略的回测结果
        """
        all_results = {}

        for strategy in strategies:
            results = self.run_backtest(
                strategy=strategy,
                benchmark_data=benchmark_data,
                use_cached_indicators=use_cached_indicators,
                save_results=True,
            )
            all_results[strategy.name] = results

        # 打印策略比较
        self._print_strategy_comparison(all_results)

        return all_results

    def plot_results(
        self,
        strategy_name: str,
        plot_equity: bool = True,
        plot_performance: bool = True,
        plot_monthly: bool = True,
        save_plots: bool = False,
        output_dir: str = "backtest_plots",
    ):
        """
        绘制回测结果图表

        Args:
            strategy_name: 策略名称
            plot_equity: 是否绘制资金曲线
            plot_performance: 是否绘制性能指标
            plot_monthly: 是否绘制月度收益
            save_plots: 是否保存图表
            output_dir: 输出目录
        """
        if strategy_name not in self.results:
            print(f"未找到策略 {strategy_name} 的回测结果")
            return

        results = self.results[strategy_name]

        if save_plots:
            os.makedirs(output_dir, exist_ok=True)

        # 1. 资金曲线图
        if plot_equity:
            save_path = (
                f"{output_dir}/{strategy_name}_equity_curve.png" if save_plots else None
            )
            self.visualizer.plot_equity_curve(
                portfolio_daily=results["portfolio_daily"],
                benchmark_data=results.get("benchmark_data"),
                strategy_name=strategy_name,
                save_path=save_path,
            )

        # 2. 性能指标图
        if plot_performance:
            save_path = (
                f"{output_dir}/{strategy_name}_performance.png" if save_plots else None
            )
            self.visualizer.plot_performance_metrics(
                metrics=results["performance_metrics"],
                strategy_name=strategy_name,
                save_path=save_path,
            )

        # 3. 月度收益热力图
        if plot_monthly:
            save_path = (
                f"{output_dir}/{strategy_name}_monthly_returns.png"
                if save_plots
                else None
            )
            self.visualizer.plot_monthly_returns(
                portfolio_daily=results["portfolio_daily"],
                strategy_name=strategy_name,
                save_path=save_path,
            )

    def plot_candlestick_with_signals(
        self,
        strategy_name: str,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        save_plot: bool = False,
        output_dir: str = "backtest_plots",
    ):
        """
        绘制个股K线图和交易信号

        Args:
            strategy_name: 策略名称
            ticker: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            save_plot: 是否保存图表
            output_dir: 输出目录
        """
        if strategy_name not in self.results:
            print(f"未找到策略 {strategy_name} 的回测结果")
            return

        results = self.results[strategy_name]

        # 获取原始OHLCV数据
        strategy = None
        for name, res in self.results.items():
            if name == strategy_name:
                # 这里需要从策略实例获取原始数据，简化处理
                break

        if save_plot:
            os.makedirs(output_dir, exist_ok=True)
            save_path = f"{output_dir}/{strategy_name}_{ticker}_candlestick.png"
        else:
            save_path = None

        # 注意：这里需要原始OHLCV数据，实际使用时需要传入
        print(f"请使用 visualizer.plot_candlestick_with_signals() 方法直接绘制")
        print(f"需要传入原始OHLCV数据")

    def get_strategy_results(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """获取指定策略的回测结果"""
        return self.results.get(strategy_name)

    def get_all_results(self) -> Dict[str, Dict[str, Any]]:
        """获取所有策略的回测结果"""
        return self.results

    def export_results(self, strategy_name: str, output_dir: str = "backtest_results"):
        """
        导出回测结果到文件

        Args:
            strategy_name: 策略名称
            output_dir: 输出目录
        """
        if strategy_name not in self.results:
            print(f"未找到策略 {strategy_name} 的回测结果")
            return

        results = self.results[strategy_name]
        os.makedirs(output_dir, exist_ok=True)

        # 导出交易记录
        trades_path = f"{output_dir}/{strategy_name}_trades.csv"

        trades = (
            pl.DataFrame(results["trades"])
            .with_columns((pl.col("return") * 100).round(2).alias("return %"))
            .sort("return %", descending=True)
            .drop("return")
        )
        trades.write_csv(trades_path)
        print(f"交易记录已导出到: {trades_path}")

        # 导出每日组合表现
        portfolio_path = f"{output_dir}/{strategy_name}_portfolio_daily.csv"
        results["portfolio_daily"].write_csv(portfolio_path)
        print(f"每日组合表现已导出到: {portfolio_path}")

        # 导出性能指标
        metrics_path = f"{output_dir}/{strategy_name}_metrics.txt"
        with open(metrics_path, "w", encoding="utf-8") as f:
            f.write(f"{strategy_name} 回测性能指标\n")
            f.write("=" * 50 + "\n\n")
            for key, value in results["performance_metrics"].items():
                f.write(f"{key}: {value}\n")
        print(f"性能指标已导出到: {metrics_path}")

    def _print_strategy_comparison(self, all_results: Dict[str, Dict[str, Any]]):
        """打印策略比较表格"""
        if len(all_results) < 2:
            return

        print("\n" + "=" * 80)
        print("策略比较")
        print("=" * 80)

        # 选择关键指标进行比较
        key_metrics = [
            "Total Return [%]",
            "Max Drawdown [%]",
            "Sharpe Ratio",
            "Win Rate [%]",
            "Total Trades",
        ]

        print(f"{'策略名称':<20}", end="")
        for metric in key_metrics:
            print(f"{metric:<15}", end="")
        print()
        print("-" * 80)

        for strategy_name, results in all_results.items():
            metrics = results["performance_metrics"]
            print(f"{strategy_name:<20}", end="")
            for metric in key_metrics:
                value = metrics.get(metric, 0)
                if isinstance(value, float):
                    print(f"{value:<15.2f}", end="")
                else:
                    print(f"{value:<15}", end="")
            print()

        print("=" * 80 + "\n")
