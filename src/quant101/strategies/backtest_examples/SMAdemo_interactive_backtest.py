"""
使用新的交互式K线图功能的完整示例
"""

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import polars as pl

from quant101.backtesting import (BacktestEngine, BacktestVisualizer,
                                  StrategyBase)
from quant101.core_2.data_loader import stock_load_process
from quant101.strategies.backtest_examples.bbiboll_backtest_example import (
    load_spx_benchmark, only_common_stocks)


class SimpleMAStrategy(StrategyBase):
    """
    简单移动平均策略示例 - 展示新K线图功能
    """

    def __init__(self, config=None):
        default_config = {"short_window": 5, "long_window": 15, "hold_days": 3}
        if config:
            default_config.update(config)
        super().__init__(name="SimpleMA", config=default_config)

    def calculate_indicators(self, cached=False):
        """计算移动平均指标"""
        indicators = self.ohlcv_data.sort(["ticker", "timestamps"]).with_columns(
            [
                pl.col("close")
                .rolling_mean(window_size=self.config["short_window"])
                .over("ticker")
                .alias("ma_short"),
                pl.col("close")
                .rolling_mean(window_size=self.config["long_window"])
                .over("ticker")
                .alias("ma_long"),
            ]
        )
        return indicators

    def generate_signals(self, indicators):
        """生成交易信号：短均线上穿长均线"""
        signals = (
            indicators.with_columns(
                [
                    pl.col("ma_short").shift(1).over("ticker").alias("ma_short_prev"),
                    pl.col("ma_long").shift(1).over("ticker").alias("ma_long_prev"),
                ]
            )
            .filter(
                pl.col("ma_short").is_not_null()
                & pl.col("ma_long").is_not_null()
                & pl.col("ma_short_prev").is_not_null()
                & pl.col("ma_long_prev").is_not_null()
                & (pl.col("ma_short_prev") <= pl.col("ma_long_prev"))
                & (pl.col("ma_short") > pl.col("ma_long"))
            )
            .select(["timestamps", "ticker"])
            .with_columns(pl.lit(1).alias("signal"))
        )

        return signals

    def trade_rules(self, signals):
        """执行交易规则"""
        if signals.is_empty():
            return pl.DataFrame(), pl.DataFrame()

        prices = (
            self.ohlcv_data.select(["ticker", "timestamps", "open", "close"])
            .sort(["ticker", "timestamps"])
            .with_columns(pl.arange(0, pl.len()).over("ticker").alias("row_id"))
        )

        signal_with_prices = signals.join(
            prices.rename({"timestamps": "signal_date"}),
            left_on=["ticker", "timestamps"],
            right_on=["ticker", "signal_date"],
            how="left",
        )

        hold_days = self.config["hold_days"]
        signal_with_prices = signal_with_prices.with_columns(
            [
                (pl.col("row_id") + 1).alias("buy_row_id"),
                (pl.col("row_id") + 1 + hold_days).alias("sell_row_id"),
            ]
        )

        trades = signal_with_prices.join(
            prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
                {"row_id": "buy_row_id", "timestamps": "buy_date", "open": "buy_open"}
            ),
            on=["ticker", "buy_row_id"],
            how="left",
        ).join(
            prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
                {
                    "row_id": "sell_row_id",
                    "timestamps": "sell_date",
                    "open": "sell_open",
                }
            ),
            on=["ticker", "sell_row_id"],
            how="left",
        )

        trades = (
            trades.filter(
                pl.col("buy_open").is_not_null() & pl.col("sell_open").is_not_null()
            )
            .with_columns(
                ((pl.col("sell_open") / pl.col("buy_open")) - 1).alias("return")
            )
            .select(
                ["ticker", "buy_date", "buy_open", "sell_date", "sell_open", "return"]
            )
            .with_row_index("block_id")
        )

        # 简化的每日组合计算
        all_dates = prices.select("timestamps").unique().sort("timestamps")
        portfolio_returns = []
        # Get the original datetime dtype to preserve it
        original_dtype = all_dates.select("timestamps").dtypes[0]

        for date_row in all_dates.iter_rows():
            date = date_row[0]
            active_trades = trades.filter(
                (pl.col("buy_date") <= date) & (pl.col("sell_date") >= date)
            )

            daily_return = (
                random.uniform(-0.01, 0.01) if len(active_trades) > 0 else 0.0
            )
            portfolio_returns.append(
                {
                    "date": date,
                    "portfolio_return": daily_return,
                    "n_positions": len(active_trades),
                }
            )

        portfolio_daily = pl.DataFrame(portfolio_returns).with_columns(
            # Cast the date column back to original dtype to preserve precision and timezone
            pl.col("date").cast(original_dtype),
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve"),
        )
        return trades, portfolio_daily


def demo_interactive_backtest():
    """演示完整的交互式回测流程"""
    print("🎯 交互式K线图回测演示")
    print("=" * 60)

    # 1. 配置参数
    config = {
        "timeframe": "1d",
        "start_date": "2022-01-01",
        "end_date": "2025-09-05",
        "initial_capital": 100.0,
    }

    # 2. 加载数据
    print("加载市场数据...")
    tickers = only_common_stocks()

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers.to_series().to_list(),
                timeframe=config["timeframe"],
                start_date=config["start_date"],
                end_date=config["end_date"],
            )
            .drop(["split_date", "window_start", "split_ratio"])
            .filter(pl.col("volume") != 0)
            .collect()
        )

        print(f"数据大小: {ohlcv_data.estimated_size('mb'):.2f} MB")
        print(f"数据行数: {len(ohlcv_data):,}")
        print(f"股票数量: {ohlcv_data.select('ticker').n_unique()}")

    except Exception as e:
        print(f"加载市场数据失败: {e}")
        return

    # 3. 加载基准数据
    print("加载基准数据...")
    benchmark_data = load_spx_benchmark(config["start_date"], config["end_date"])

    tickers = ohlcv_data.select("ticker").unique().to_series().to_list()

    print(
        f"   数据期间: {ohlcv_data['timestamps'].min()} 到 {ohlcv_data['timestamps'].max()}"
    )
    print(f"   股票数量: {len(tickers)}")
    print(f"   总数据量: {len(ohlcv_data):,} 行")

    # 2. 创建回测引擎和策略
    print("\n🚀 设置回测引擎...")
    engine = BacktestEngine(initial_capital=10000)

    strategy = SimpleMAStrategy(
        config={"short_window": 5, "long_window": 15, "hold_days": 3}
    )

    engine.add_strategy(strategy, ohlcv_data, tickers)

    # 3. 运行回测
    print("\n⚡ 运行策略回测...")
    results = engine.run_backtest(
        strategy, benchmark_data=benchmark_data, use_cached_indicators=False
    )
    print(results)
    engine.plot_results(
        strategy_name=strategy.name,
        plot_equity=True,
        plot_performance=True,
        plot_monthly=True,
        save_plots=False,
        output_dir="backtest_output",
    )

    # 4. 展示交互式K线图
    print("\n📈 展示交互式K线图功能...")
    visualizer = BacktestVisualizer()

    for ticker in tickers[:1]:
        print(f"\n🎨 绘制 {ticker} 的交互式K线图...")
        print("💡 功能演示:")
        print("   - 紧凑排列，无周末空隙")
        print("   - 鼠标悬停显示OHLCV详情")
        print("   - 交易信号点交互显示")

        try:
            visualizer.plot_candlestick_with_signals(
                ohlcv_data=ohlcv_data,
                trades=results["trades"],
                ticker=ticker,
                start_date="2023-03-01",
                end_date="2025-09-05",
                indicators=results["indicators"],
                save_path=f"demo_{ticker}_interactive.png",
            )

            print(f"   ✅ {ticker} 图表保存为: demo_{ticker}_interactive.png")

        except Exception as e:
            print(f"   ❌ {ticker} 绘制失败: {e}")

    # 5. 总结
    trades = results["trades"]
    portfolio = results["portfolio_daily"]

    print(f"\n📋 回测结果总结:")
    print(f"   交易次数: {len(trades)}")

    if not portfolio.is_empty():
        final_value = portfolio["equity_curve"].tail(1).item() * 10000
        total_return = (final_value / 10000 - 1) * 100
        print(f"   期末价值: ${final_value:,.2f}")
        print(f"   总收益率: {total_return:.2f}%")

    print(f"\n🎊 演示完成！")
    print("📝 使用技巧:")
    print("   1. 在图表上移动鼠标查看详细信息")
    print("   2. 绿色三角形↑ = 买入信号")
    print("   3. 红色倒三角形↓ = 卖出信号")
    print("   4. 信息框显示在图表左上角")
    print("   5. 可以使用matplotlib的缩放和平移工具")


if __name__ == "__main__":
    demo_interactive_backtest()
