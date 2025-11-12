import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl
import quantstats as qs

from .performance_analyzer import PerformanceAnalyzer
from .strategy_base import StrategyBase
from .visualizer import BacktestVisualizer


class BacktestEngine:

    def __init__(self, initial_capital: float = 100.0):
        self.initial_capital = initial_capital
        self.performance_analyzer = PerformanceAnalyzer(initial_capital)
        self.visualizer = BacktestVisualizer()
        self.results = {}

        # ignore matplotlib font error warnings
        import logging

        logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

    def add_strategy(
        self,
        strategy: StrategyBase,
        ohlcv_data: pl.DataFrame,
        tickers: List[str] = None,
    ):
        """
        Args:
            strategy:
            ohlcv_data:
            tickers:
        """
        strategy.set_data(ohlcv_data, tickers)
        return strategy

    def run_backtest(
        self,
        strategy: StrategyBase,
        benchmark_data: Optional[pl.DataFrame] = None,
        use_cached_indicators: bool = True,
        save_results: bool = True,
    ) -> Dict[str, Any]:
        """
        Args:
            strategy:
            benchmark_data:
            use_cached_indicators:
            save_results:

        Returns:
            complete backtest result
        """
        print(f"\nstart backtest: {strategy.name}")
        print("=" * 60)

        # 1. run backtest
        strategy_results = strategy.run_backtest(use_cached_indicators)

        # 2. calculate metrics
        print("calculate metrics...")
        performance_metrics = self.performance_analyzer.calculate_performance_metrics(
            portfolio_daily=strategy_results["portfolio_daily"],
            trades=strategy_results["trades"],
            benchmark_data=benchmark_data,
        )

        # 3. complete_results
        complete_results = {
            **strategy_results,
            "performance_metrics": performance_metrics,
            "benchmark_data": benchmark_data,
            "backtest_config": {
                "initial_capital": self.initial_capital,
                "use_cached_indicators": use_cached_indicators,
            },
        }

        # 4. save results
        if save_results:
            self.results[strategy.name] = complete_results

        # 5. print metrics
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
        Args:
            strategies:
            benchmark_data:
            use_cached_indicators:

        Returns:
            all strategy results
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
        Args:
            strategy_name:
            plot_equity:
            plot_performance:
            plot_monthly:
            save_plots:
            output_dir:
        """
        if strategy_name not in self.results:
            print(f"not found {strategy_name} backtest result")
            return

        results = self.results[strategy_name]

        if save_plots:
            os.makedirs(output_dir, exist_ok=True)

        # 1. equity curve
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

        # 2. month return heatmap
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

    def get_strategy_results(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        return self.results.get(strategy_name)

    def get_all_results(self) -> Dict[str, Dict[str, Any]]:
        return self.results

    def export_results(
        self, strategy_config, strategy_name: str, output_dir: str = "backtest_results"
    ):
        """
        Args:
            strategy_name:
            output_dir:
        """
        if strategy_name not in self.results:
            print(f"not found {strategy_name} backtest result")
            return

        results = self.results[strategy_name]
        os.makedirs(output_dir, exist_ok=True)

        # export quantstats report
        portfolio_daily = results["portfolio_daily"]
        if not portfolio_daily.is_empty():
            benchmark = results.get("benchmark_data")
            benchmark = benchmark.with_columns(
                (pl.col("close") / pl.col("close").shift(1) - 1).alias("daily_return")
            )
            benchmark = benchmark.with_columns(
                pl.col("date").cast(pl.Date).alias("date")
            )
            benchmark = pd.Series(benchmark["daily_return"], index=benchmark["date"])
            portfolio_daily = portfolio_daily.with_columns(
                pl.col("date").cast(pl.Date).alias("date")
            )

            qs_returns = pd.Series(
                portfolio_daily["portfolio_return"], index=portfolio_daily["date"]
            )
            html_path = f"{output_dir}/{strategy_name}_report.html"
            qs.reports.html(
                qs_returns, benchmark=benchmark, output=html_path, benchmark_title="SPY"
            )

        # export trades and open positions
        trades_path = f"{output_dir}/{strategy_name}_trades.csv"
        open_positions_path = f"{output_dir}/{strategy_name}_open_positions.csv"

        open_positions = (
            pl.DataFrame(results.get("open_positions", []))
            .with_columns(
                pl.col("buy_date").dt.date().alias("buy_date"),
            )
            .sort("buy_date")
        )
        open_positions.write_csv(open_positions_path)
        print(f"open positions exported: {open_positions_path}")

        trades = (
            pl.DataFrame(results["trades"])
            .with_columns((pl.col("return") * 100).round(2).alias("return %"))
            .sort("return %", descending=True)
            .drop("return")
        ).with_columns(
            pl.col("buy_date").dt.date().alias("buy_date"),
            pl.col("sell_date").dt.date().alias("sell_date"),
        )
        trades.write_csv(trades_path)
        print(f"trades exported: {trades_path}")

        portfolio_path = f"{output_dir}/{strategy_name}_portfolio_daily.csv"
        results["portfolio_daily"].with_columns(
            pl.col("date").dt.date().alias("date"),
        ).write_csv(portfolio_path)
        print(f"daily portfolio performance exported: {portfolio_path}")

        metrics_path = f"{output_dir}/{strategy_name}_metrics.txt"
        with open(metrics_path, "w", encoding="utf-8") as f:
            f.write(f"{strategy_name} backtest performance metrics\n")
            f.write("=" * 50 + "\n\n")
            for key, value in results["performance_metrics"].items():
                f.write(f"{key}: {value}\n")
        print(f"backtest performance metrics exported: {metrics_path}")

        # export strategy_config
        strategy_config_path = f"{output_dir}/{strategy_name}_config.txt"
        with open(strategy_config_path, "w", encoding="utf-8") as f:
            f.write(f"{strategy_name} strategy config\n")
            f.write("=" * 50 + "\n\n")
            for key, value in strategy_config.items():
                f.write(f"{key}: {value}\n")
        print(f"Strategy config exported: {strategy_config_path}")

    def _print_strategy_comparison(self, all_results: Dict[str, Dict[str, Any]]):
        if len(all_results) < 2:
            return

        print("\n" + "=" * 80)
        print("Strategies Comparison")
        print("=" * 80)

        key_metrics = [
            "Total Return [%]",
            "Max Drawdown [%]",
            "Sharpe Ratio",
            "Win Rate [%]",
            "Total Trades",
        ]

        print(f"{'Strategy name':<20}", end="")
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
