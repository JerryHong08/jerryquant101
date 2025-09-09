"""
回测可视化模块 - 绘制回测结果图表
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

# 设置中文字体和样式
plt.rcParams["font.sans-serif"] = ["SimHei", "DejaVu Sans", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False
sns.set_style("whitegrid")


class BacktestVisualizer:
    """
    回测结果可视化器
    """

    def __init__(self, figsize: tuple = (12, 8)):
        self.figsize = figsize

    def plot_equity_curve(
        self,
        portfolio_daily: pl.DataFrame,
        benchmark_data: Optional[pl.DataFrame] = None,
        strategy_name: str = "Strategy",
        show_drawdown: bool = True,
        save_path: Optional[str] = None,
    ):
        """
        绘制资金曲线图

        Args:
            portfolio_daily: 每日组合表现DataFrame
            benchmark_data: 基准数据DataFrame
            strategy_name: 策略名称
            show_drawdown: 是否显示回撤
            save_path: 保存路径
        """
        if show_drawdown:
            fig, (ax1, ax2) = plt.subplots(
                2, 1, figsize=self.figsize, height_ratios=[3, 1], sharex=True
            )
        else:
            fig, ax1 = plt.subplots(1, 1, figsize=self.figsize)

        # 转换为pandas用于绘图
        dates = portfolio_daily["date"].to_pandas()
        equity_curve = portfolio_daily["equity_curve"].to_pandas()

        # 绘制策略曲线
        ax1.plot(
            dates,
            equity_curve,
            label=f"{strategy_name} Strategy",
            linewidth=2,
            color="blue",
        )

        # 绘制基准曲线
        if benchmark_data is not None:
            benchmark_dates = benchmark_data["date"].to_pandas()
            benchmark_curve = benchmark_data["benchmark_return"].to_pandas()
            ax1.plot(
                benchmark_dates,
                benchmark_curve,
                label="Benchmark (SPX)",
                linewidth=2,
                color="red",
                alpha=0.7,
            )

        ax1.set_title(f"{strategy_name} Equity Curve", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Cumulative Profit", fontsize=12)
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)

        # 格式化x轴
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))

        # 绘制回撤图
        if show_drawdown:
            equity_array = equity_curve.values
            peak = np.maximum.accumulate(equity_array)
            drawdown = (equity_array - peak) / peak * 100

            ax2.fill_between(dates, drawdown, 0, color="red", alpha=0.3)
            ax2.plot(dates, drawdown, color="red", linewidth=1)
            ax2.set_title("withdraw", fontsize=12)
            ax2.set_ylabel("withdraw (%)", fontsize=10)
            ax2.set_xlabel("Date", fontsize=12)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"资金曲线图已保存到: {save_path}")

        plt.show()

    def plot_candlestick_with_signals(
        self,
        ohlcv_data: pl.DataFrame,
        trades: pl.DataFrame,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        indicators: Optional[pl.DataFrame] = None,
        save_path: Optional[str] = None,
    ):
        """
        绘制K线图和交易信号

        Args:
            ohlcv_data: OHLCV数据
            trades: 交易记录
            ticker: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            indicators: 技术指标数据
            save_path: 保存路径
        """
        # 过滤数据
        ticker_data = ohlcv_data.filter(pl.col("ticker") == ticker)

        if start_date:
            # 使用 str.to_date() 或 strptime 来解析日期字符串
            ticker_data = ticker_data.filter(
                pl.col("timestamps") >= pl.lit(start_date).str.to_date()
            )

        if end_date:
            ticker_data = ticker_data.filter(
                pl.col("timestamps") <= pl.lit(end_date).str.to_date()
            )

        if ticker_data.is_empty():
            print(f"没有找到股票 {ticker} 的数据")
            return

        # 转换为pandas
        df = ticker_data.to_pandas()
        df.set_index("timestamps", inplace=True)
        print(df.index)

        # 创建子图
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(15, 10), height_ratios=[3, 1], sharex=True
        )

        # 绘制K线图
        self._plot_candlesticks(ax1, df)

        # 绘制交易信号
        ticker_trades = trades.filter(pl.col("ticker") == ticker)
        if not ticker_trades.is_empty():
            self._plot_trade_signals(ax1, ticker_trades)

        # 绘制技术指标
        if indicators is not None:
            ticker_indicators = indicators.filter(pl.col("ticker") == ticker)
            if not ticker_indicators.is_empty():
                self._plot_indicators(ax1, ticker_indicators)

        # 绘制成交量
        ax2.bar(df.index, df["volume"], color="gray", alpha=0.3)
        ax2.set_ylabel("Volume", fontsize=10)
        ax2.set_xlabel("Date", fontsize=12)

        ax1.set_title(f"{ticker} Kline and Signals", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Price", fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"K线图已保存到: {save_path}")

        plt.show()

    def _plot_candlesticks(self, ax, df):
        """绘制K线"""
        # 简化的K线绘制
        for i, (date, row) in enumerate(df.iterrows()):
            color = "red" if row["close"] > row["open"] else "green"

            # 绘制影线
            ax.plot(
                [date, date], [row["low"], row["high"]], color="black", linewidth=0.5
            )

            # 绘制实体
            height = abs(row["close"] - row["open"])
            bottom = min(row["close"], row["open"])
            ax.bar(date, height, bottom=bottom, color=color, alpha=0.8, width=0.8)

    def _plot_trade_signals(self, ax, trades: pl.DataFrame):
        """绘制交易信号"""
        trades_pd = trades.to_pandas()

        # 买入信号
        if "buy_date" in trades_pd.columns and "buy_open" in trades_pd.columns:
            buy_dates = trades_pd["buy_date"].dropna()
            buy_prices = trades_pd["buy_open"].dropna()
            ax.scatter(
                buy_dates,
                buy_prices,
                color="green",
                marker="^",
                s=100,
                label="Buy",
                zorder=5,
            )

        # 卖出信号
        if "sell_date" in trades_pd.columns and "sell_open" in trades_pd.columns:
            sell_dates = trades_pd["sell_date"].dropna()
            sell_prices = trades_pd["sell_open"].dropna()
            ax.scatter(
                sell_dates,
                sell_prices,
                color="red",
                marker="v",
                s=100,
                label="Sell",
                zorder=5,
            )

    def _plot_indicators(self, ax, indicators: pl.DataFrame):
        """绘制技术指标"""
        indicators_pd = indicators.to_pandas()
        indicators_pd.set_index("timestamps", inplace=True)

        # 根据可用的指标列绘制
        if "bbi" in indicators_pd.columns:
            ax.plot(
                indicators_pd.index,
                indicators_pd["bbi"],
                label="BBI",
                alpha=0.7,
                linewidth=1,
            )

        if "upr" in indicators_pd.columns:
            ax.plot(
                indicators_pd.index,
                indicators_pd["upr"],
                label="UPR",
                alpha=0.7,
                linewidth=1,
                linestyle="--",
            )

        if "dwn" in indicators_pd.columns:
            ax.plot(
                indicators_pd.index,
                indicators_pd["dwn"],
                label="DWN",
                alpha=0.7,
                linewidth=1,
                linestyle="--",
            )

    def plot_performance_metrics(
        self,
        metrics: Dict[str, Any],
        strategy_name: str = "Strategy",
        save_path: Optional[str] = None,
    ):
        """
        绘制性能指标图表

        Args:
            metrics: 性能指标字典
            strategy_name: 策略名称
            save_path: 保存路径
        """
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

        # 1. 收益率条形图
        returns_data = {
            "Strategy Profit": metrics.get("Total Return [%]", 0),
            "Benchmark Profit": metrics.get("Benchmark Return [%]", 0),
        }
        ax1.bar(
            returns_data.keys(), returns_data.values(), color=["blue", "red"], alpha=0.7
        )
        ax1.set_title("Profit VS", fontweight="bold")
        ax1.set_ylabel("Profit (%)")

        # 2. 风险指标雷达图
        risk_metrics = ["Sharpe Ratio", "Sortino Ratio", "Calmar Ratio"]
        risk_values = [metrics.get(metric, 0) for metric in risk_metrics]

        ax2.bar(risk_metrics, risk_values, color="green", alpha=0.7)
        ax2.set_title("Risk Adjusted Indicator", fontweight="bold")
        ax2.set_ylabel("Ratio")
        ax2.tick_params(axis="x", rotation=45)

        # 3. 交易统计
        trade_stats = {
            "Total Trade": metrics.get("Total Trades", 0),
            "Win trades": int(
                metrics.get("Total Trades", 0) * metrics.get("Win Rate [%]", 0) / 100
            ),
            "Loss trades": int(
                metrics.get("Total Trades", 0)
                * (100 - metrics.get("Win Rate [%]", 0))
                / 100
            ),
        }
        ax3.pie(
            list(trade_stats.values())[1:],
            labels=list(trade_stats.keys())[1:],
            autopct="%1.1f%%",
            colors=["green", "red"],
        )
        ax3.set_title(
            f'Trade Metrics (Total: {trade_stats["Total Trade"]})', fontweight="bold"
        )

        # 4. 关键指标表格
        key_metrics = [
            ("Win Rate", f"{metrics.get('Win Rate [%]', 0):.2f}%"),
            ("Max Drawdown", f"{metrics.get('Max Drawdown [%]', 0):.2f}%"),
            ("Win/Loss", f"{metrics.get('Profit Factor', 0):.2f}"),
            ("Expectancy", f"{metrics.get('Expectancy', 0):.2f}"),
        ]

        ax4.axis("tight")
        ax4.axis("off")
        table = ax4.table(
            cellText=key_metrics,
            colLabels=["metric", "value"],
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        ax4.set_title("key indicator", fontweight="bold")

        plt.suptitle(
            f"{strategy_name} Backtest Analyze", fontsize=16, fontweight="bold"
        )
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"性能分析图已保存到: {save_path}")

        plt.show()

    def plot_monthly_returns(
        self,
        portfolio_daily: pl.DataFrame,
        strategy_name: str = "Strategy",
        save_path: Optional[str] = None,
    ):
        """
        绘制月度收益热力图

        Args:
            portfolio_daily: 每日组合表现
            strategy_name: 策略名称
            save_path: 保存路径
        """
        try:
            # 计算月度收益
            monthly_data = portfolio_daily.with_columns(
                [
                    pl.col("date").dt.year().alias("year"),
                    pl.col("date").dt.month().alias("month"),
                    (pl.col("portfolio_return") + 1).alias("return_factor"),
                ]
            )

            # 转换为pandas pivot表
            monthly_df = monthly_data.to_pandas()

            monthly_returns = (
                monthly_df.groupby(["year", "month"])["return_factor"]
                .apply(lambda x: x.prod() - 1)
                .reset_index()
            )
            monthly_returns.columns = ["year", "month", "monthly_return"]

            monthly_pivot = monthly_returns.pivot(
                index="year", columns="month", values="monthly_return"
            )

            # 创建热力图
            plt.figure(figsize=(12, 8))
            sns.heatmap(
                monthly_pivot * 100,
                annot=True,
                fmt=".2f",
                cmap="RdYlGn",
                center=0,
                cbar_kws={"label": "Mothly Return (%)"},
            )

            plt.title(
                f"{strategy_name} Monthly Return Heatmap",
                fontsize=14,
                fontweight="bold",
            )
            plt.xlabel("Month")
            plt.ylabel("Year")

            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches="tight")
                print(f"月度收益热力图已保存到: {save_path}")

            plt.show()

        except Exception as e:
            print(f"绘制月度收益热力图时出错: {e}")
            print("使用简化版本...")
