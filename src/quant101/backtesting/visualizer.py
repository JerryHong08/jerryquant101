"""
回测可视化模块 - 绘制回测结果图表
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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

    def _normalize_date_string(self, date_obj):
        """标准化日期字符串格式"""
        try:
            if pd.isna(date_obj):
                return None

            # 如果是字符串
            if isinstance(date_obj, str):
                # 尝试提取日期部分 (去掉时间部分)
                if "T" in date_obj:
                    return date_obj.split("T")[0]
                elif " " in date_obj:
                    return date_obj.split(" ")[0]
                else:
                    return date_obj

            # 如果是datetime对象
            elif hasattr(date_obj, "strftime"):
                return date_obj.strftime("%Y-%m-%d")

            # 其他情况
            else:
                date_str = str(date_obj)
                if "T" in date_str:
                    return date_str.split("T")[0]
                elif " " in date_str:
                    return date_str.split(" ")[0]
                else:
                    return date_str

        except Exception:
            return str(date_obj)

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
        绘制K线图和交易信号 (带交互功能)

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

        # 转换为pandas并重置索引为数值索引
        df = ticker_data.to_pandas().sort_values("timestamps").reset_index(drop=True)

        # 创建数值索引映射到日期
        dates = df["timestamps"].values
        date_labels = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]

        # 创建子图
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(15, 10), height_ratios=[3, 1], sharex=True
        )

        # 绘制K线图 (使用数值索引)
        self._plot_candlesticks_compact(ax1, df)

        # 绘制交易信号
        ticker_trades = trades.filter(pl.col("ticker") == ticker)
        if not ticker_trades.is_empty():
            self._plot_trade_signals_compact(ax1, ticker_trades, df)

        # 绘制技术指标
        if indicators is not None:
            ticker_indicators = indicators.filter(pl.col("ticker") == ticker)
            if not ticker_indicators.is_empty():
                print("plot indicators...")
                self._plot_indicators_compact(ax1, ticker_indicators, df)

        # 绘制成交量
        ax2.bar(range(len(df)), df["volume"], color="gray", alpha=0.3)
        ax2.set_ylabel("Volume", fontsize=10)
        ax2.set_xlabel("Date", fontsize=12)

        # 设置x轴标签 (只显示部分日期避免拥挤)
        step = max(1, len(df) // 10)  # 最多显示10个日期标签
        tick_positions = range(0, len(df), step)
        tick_labels = [date_labels[i][:10] for i in tick_positions]

        ax1.set_xticks(tick_positions)
        ax1.set_xticklabels(tick_labels, rotation=45)
        ax2.set_xticks(tick_positions)
        ax2.set_xticklabels(tick_labels, rotation=45)

        ax1.set_title(
            f"{ticker} K-Line Chart with Signals", fontsize=14, fontweight="bold"
        )
        ax1.set_ylabel("Price", fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 添加交互功能
        self._add_interactive_features(fig, ax1, ax2, df, ticker_trades)

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

    def _plot_candlesticks_compact(self, ax, df):
        """绘制紧凑的K线图 (使用数值索引)"""
        for i in range(len(df)):
            row = df.iloc[i]
            color = "red" if row["close"] > row["open"] else "green"

            # 绘制影线
            ax.plot([i, i], [row["low"], row["high"]], color="black", linewidth=0.5)

            # 绘制实体
            height = abs(row["close"] - row["open"])
            bottom = min(row["close"], row["open"])
            ax.bar(i, height, bottom=bottom, color=color, alpha=0.8, width=0.8)

    def _plot_trade_signals_compact(self, ax, trades: pl.DataFrame, df):
        """绘制交易信号 (紧凑版本)"""
        trades_pd = trades.to_pandas()

        # 为买卖信号创建位置映射
        date_to_index = {str(df.iloc[i]["timestamps"]): i for i in range(len(df))}

        # 买入信号
        buy_signals_plotted = False
        if "buy_date" in trades_pd.columns and "buy_open" in trades_pd.columns:
            for _, trade in trades_pd.iterrows():
                if pd.notna(trade["buy_date"]) and pd.notna(trade["buy_open"]):
                    buy_date_str = str(trade["buy_date"])
                    for date_key, index in date_to_index.items():
                        if buy_date_str in date_key:
                            ax.scatter(
                                index,
                                trade["buy_open"],
                                color="green",
                                marker="^",
                                s=100,
                                label="Buy" if not buy_signals_plotted else "",
                                zorder=5,
                            )
                            buy_signals_plotted = True
                            break

        # 卖出信号
        sell_signals_plotted = False
        if "sell_date" in trades_pd.columns and "sell_open" in trades_pd.columns:
            for _, trade in trades_pd.iterrows():
                if pd.notna(trade["sell_date"]) and pd.notna(trade["sell_open"]):
                    sell_date_str = str(trade["sell_date"])
                    for date_key, index in date_to_index.items():
                        if sell_date_str in date_key:
                            ax.scatter(
                                index,
                                trade["sell_open"],
                                color="red",
                                marker="v",
                                s=100,
                                label="Sell" if not sell_signals_plotted else "",
                                zorder=5,
                            )
                            sell_signals_plotted = True
                            break

    def _plot_indicators_compact(self, ax, indicators: pl.DataFrame, df):
        """绘制技术指标 (紧凑版本) - 修复长度不匹配问题"""
        try:
            indicators_pd = indicators.to_pandas()

            # 创建标准化的日期映射
            df_dates = [
                self._normalize_date_string(df.iloc[i]["timestamps"])
                for i in range(len(df))
            ]

            # 为指标数据创建索引映射
            valid_indices = []
            valid_bbi = []
            valid_upr = []
            valid_dwn = []

            for _, row in indicators_pd.iterrows():
                indicator_date = self._normalize_date_string(row["timestamps"])
                if indicator_date and indicator_date in df_dates:
                    index = df_dates.index(indicator_date)
                    valid_indices.append(index)

                    if "bbi" in indicators_pd.columns and pd.notna(row["bbi"]):
                        valid_bbi.append(row["bbi"])
                    else:
                        valid_bbi.append(None)

                    if "upr" in indicators_pd.columns and pd.notna(row["upr"]):
                        valid_upr.append(row["upr"])
                    else:
                        valid_upr.append(None)

                    if "dwn" in indicators_pd.columns and pd.notna(row["dwn"]):
                        valid_dwn.append(row["dwn"])
                    else:
                        valid_dwn.append(None)

            # 绘制指标 - 确保长度匹配
            if len(valid_indices) > 0:
                # 过滤掉None值
                if len(valid_bbi) == len(valid_indices):
                    bbi_clean = [
                        (valid_indices[i], valid_bbi[i])
                        for i in range(len(valid_indices))
                        if valid_bbi[i] is not None
                    ]
                    if bbi_clean:
                        indices, values = zip(*bbi_clean)
                        ax.plot(
                            indices,
                            values,
                            label="BBI",
                            alpha=0.7,
                            linewidth=1.5,
                            color="orange",
                        )

                if len(valid_upr) == len(valid_indices):
                    upr_clean = [
                        (valid_indices[i], valid_upr[i])
                        for i in range(len(valid_indices))
                        if valid_upr[i] is not None
                    ]
                    if upr_clean:
                        indices, values = zip(*upr_clean)
                        ax.plot(
                            indices,
                            values,
                            label="Upper Band",
                            alpha=0.7,
                            linewidth=1,
                            linestyle="--",
                            color="red",
                        )

                if len(valid_dwn) == len(valid_indices):
                    dwn_clean = [
                        (valid_indices[i], valid_dwn[i])
                        for i in range(len(valid_indices))
                        if valid_dwn[i] is not None
                    ]
                    if dwn_clean:
                        indices, values = zip(*dwn_clean)
                        ax.plot(
                            indices,
                            values,
                            label="Lower Band",
                            alpha=0.7,
                            linewidth=1,
                            linestyle="--",
                            color="green",
                        )

                print(
                    f"成功绘制技术指标: OHLCV={len(df)}, 指标={len(valid_indices)} 个匹配点"
                )
            else:
                print(f"警告: 技术指标与OHLCV数据没有匹配的日期")

        except Exception as e:
            print(f"绘制技术指标时出错: {e}")
            import traceback

            traceback.print_exc()

    def _add_interactive_features(self, fig, ax1, ax2, df, trades):
        """添加交互功能：鼠标悬停显示OHLCV和交易信息"""
        # 创建交易信息映射
        trades_pd = trades.to_pandas() if not trades.is_empty() else pd.DataFrame()
        trade_info_map = {}

        if not trades_pd.empty:
            for _, trade in trades_pd.iterrows():
                # 买入信息
                if pd.notna(trade.get("buy_date")) and pd.notna(trade.get("buy_open")):
                    buy_date_str = str(trade["buy_date"])
                    for i in range(len(df)):
                        if buy_date_str in str(df.iloc[i]["timestamps"]):
                            trade_info_map[i] = {
                                "type": "BUY",
                                "price": trade["buy_open"],
                                "date": trade["buy_date"],
                            }
                            break

                # 卖出信息
                if pd.notna(trade.get("sell_date")) and pd.notna(
                    trade.get("sell_open")
                ):
                    sell_date_str = str(trade["sell_date"])
                    for i in range(len(df)):
                        if sell_date_str in str(df.iloc[i]["timestamps"]):
                            trade_info_map[i] = {
                                "type": "SELL",
                                "price": trade["sell_open"],
                                "date": trade["sell_date"],
                            }
                            break

        # 创建信息显示框
        info_text = ax1.text(
            0.02,
            0.98,
            "",
            transform=ax1.transAxes,
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8),
            verticalalignment="top",
            fontsize=10,
        )

        def on_mouse_move(event):
            if event.inaxes == ax1:
                # 获取鼠标位置对应的数据索引
                x_pos = event.xdata
                if x_pos is not None:
                    index = int(round(x_pos))
                    if 0 <= index < len(df):
                        row = df.iloc[index]
                        date_str = str(row["timestamps"])

                        # 基础OHLCV信息
                        info_lines = [
                            f"Date: {date_str[:10]}",
                            f"Open: {row['open']:.2f}",
                            f"High: {row['high']:.2f}",
                            f"Low: {row['low']:.2f}",
                            f"Close: {row['close']:.2f}",
                            f"Volume: {row['volume']:,.0f}",
                        ]

                        # 如果有交易信息，添加交易详情
                        if index in trade_info_map:
                            trade_info = trade_info_map[index]
                            info_lines.append("")
                            info_lines.append(f"*** {trade_info['type']} Signal ***")
                            info_lines.append(f"Price: {trade_info['price']:.2f}")

                        info_text.set_text("\n".join(info_lines))
                    else:
                        info_text.set_text("")
                else:
                    info_text.set_text("")
            else:
                info_text.set_text("")

            fig.canvas.draw_idle()

        # 连接鼠标移动事件
        fig.canvas.mpl_connect("motion_notify_event", on_mouse_move)

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
