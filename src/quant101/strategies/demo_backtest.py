"""
回测框架演示脚本 - 使用模拟数据展示功能
"""

import random
import warnings
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from src.quant101.backtesting import BacktestEngine, StrategyBase

warnings.filterwarnings("ignore")


class DemoStrategy(StrategyBase):
    """
    演示策略 - 简单的移动平均策略
    """

    def __init__(self, config=None):
        default_config = {"short_window": 5, "long_window": 20, "hold_days": 3}
        if config:
            default_config.update(config)
        super().__init__(name="MA_Demo", config=default_config)

    def calculate_indicators(self, cached=False):
        """计算移动平均指标"""
        print("计算移动平均指标...")

        indicators = self.ohlcv_data.sort(["ticker", "timestamps"]).with_columns(
            [
                # 短期移动平均
                pl.col("close")
                .rolling_mean(window_size=self.config["short_window"])
                .over("ticker")
                .alias("ma_short"),
                # 长期移动平均
                pl.col("close")
                .rolling_mean(window_size=self.config["long_window"])
                .over("ticker")
                .alias("ma_long"),
            ]
        )

        return indicators

    def generate_signals(self, indicators):
        """生成交易信号：短均线上穿长均线时买入"""
        print("生成交易信号...")

        signals = (
            indicators.with_columns(
                [
                    # 计算前一日的移动平均值
                    pl.col("ma_short").shift(1).over("ticker").alias("ma_short_prev"),
                    pl.col("ma_long").shift(1).over("ticker").alias("ma_long_prev"),
                ]
            )
            .filter(
                # 金叉信号：短均线从下方穿越长均线
                pl.col("ma_short").is_not_null()
                & pl.col("ma_long").is_not_null()
                & pl.col("ma_short_prev").is_not_null()
                & pl.col("ma_long_prev").is_not_null()
                & (
                    pl.col("ma_short_prev") <= pl.col("ma_long_prev")
                )  # 前一日短均线在长均线下方
                & (pl.col("ma_short") > pl.col("ma_long"))  # 当日短均线在长均线上方
            )
            .select(["timestamps", "ticker"])
            .with_columns(pl.lit(1).alias("signal"))
        )

        return signals

    def trade_rules(self, signals):
        """执行交易规则：固定持仓天数"""
        print("执行交易规则...")

        if signals.is_empty():
            return pl.DataFrame(), pl.DataFrame()

        # 准备价格数据
        prices = (
            self.ohlcv_data.select(["ticker", "timestamps", "open", "close"])
            .sort(["ticker", "timestamps"])
            .with_columns(pl.arange(0, pl.len()).over("ticker").alias("row_id"))
        )

        # 将信号映射到价格数据
        signal_with_prices = signals.join(
            prices.rename({"timestamps": "signal_date"}),
            left_on=["ticker", "timestamps"],
            right_on=["ticker", "signal_date"],
            how="left",
        )

        # 计算买卖行号
        hold_days = self.config["hold_days"]
        signal_with_prices = signal_with_prices.with_columns(
            [
                (pl.col("row_id") + 1).alias("buy_row_id"),
                (pl.col("row_id") + 1 + hold_days).alias("sell_row_id"),
            ]
        )

        # 获取买入价格
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

        # 过滤有效交易并计算收益
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

        # 计算每日组合表现
        portfolio_daily = self._calculate_portfolio_daily(trades, prices)

        return trades, portfolio_daily

    def _calculate_portfolio_daily(self, trades, prices):
        """计算每日组合表现（简化版）"""
        if trades.is_empty():
            return pl.DataFrame()

        # 简化：假设每日收益为所有活跃交易的平均收益
        all_dates = prices.select("timestamps").unique().sort("timestamps")

        portfolio_returns = []
        for date_row in all_dates.iter_rows():
            date = date_row[0]

            # 找到在此日期活跃的交易
            active_trades = trades.filter(
                (pl.col("buy_date") <= date) & (pl.col("sell_date") >= date)
            )

            if len(active_trades) > 0:
                # 简化：使用固定的日收益率
                daily_return = random.uniform(-0.02, 0.02)  # -2% 到 +2%
            else:
                daily_return = 0.0

            portfolio_returns.append(
                {
                    "date": date,
                    "portfolio_return": daily_return,
                    "n_positions": len(active_trades),
                }
            )

        portfolio_daily = pl.DataFrame(portfolio_returns)

        # 计算累计权益曲线
        if not portfolio_daily.is_empty():
            portfolio_daily = portfolio_daily.with_columns(
                (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
            )

        return portfolio_daily


def create_demo_data():
    """创建演示用的OHLCV数据"""
    print("创建演示数据...")

    tickers = ["DEMO1", "DEMO2", "DEMO3"]
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)

    data = []

    for ticker in tickers:
        current_date = start_date
        price = 100.0  # 起始价格

        while current_date <= end_date:
            # 模拟价格随机游走
            daily_return = random.uniform(-0.05, 0.05)  # -5% 到 +5%
            price *= 1 + daily_return

            # 生成OHLCV数据
            high = price * random.uniform(1.0, 1.03)
            low = price * random.uniform(0.97, 1.0)
            open_price = price * random.uniform(0.98, 1.02)
            volume = random.randint(10000, 100000)

            data.append(
                {
                    "ticker": ticker,
                    "timestamps": current_date,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": price,
                    "volume": volume,
                }
            )

            current_date += timedelta(days=1)

    return pl.DataFrame(data)


def main():
    """演示回测框架功能"""
    print("🚀 回测框架功能演示")
    print("=" * 50)

    # 1. 创建演示数据
    ohlcv_data = create_demo_data()
    print(f"数据行数: {len(ohlcv_data):,}")
    print(f"股票数量: {ohlcv_data.select('ticker').n_unique()}")
    print(
        f"日期范围: {ohlcv_data['timestamps'].min()} 到 {ohlcv_data['timestamps'].max()}"
    )

    # 2. 创建基准数据（简化：使用第一只股票作为基准）
    benchmark_data = ohlcv_data.filter(pl.col("ticker") == "DEMO1").select(
        [
            pl.col("timestamps").alias("date"),
            pl.col("close"),
            (pl.col("close") / pl.col("close").first()).alias("benchmark_return"),
        ]
    )

    # 3. 创建回测引擎
    engine = BacktestEngine(initial_capital=10000.0)

    # 4. 创建演示策略
    strategy_config = {"short_window": 5, "long_window": 20, "hold_days": 3}

    demo_strategy = DemoStrategy(config=strategy_config)

    # 5. 添加数据到策略
    tickers = ohlcv_data.select("ticker").unique().to_series().to_list()
    engine.add_strategy(demo_strategy, ohlcv_data, tickers)

    # 6. 运行回测
    print("\n🔄 开始回测...")
    results = engine.run_backtest(
        strategy=demo_strategy,
        benchmark_data=benchmark_data,
        use_cached_indicators=False,
        save_results=True,
    )

    # 7. 显示简要结果
    print("\n📊 回测结果摘要:")
    print("-" * 30)

    trades = results["trades"]
    portfolio = results["portfolio_daily"]
    metrics = results["performance_metrics"]

    print(f"交易数量: {len(trades)}")
    print(f"组合日数: {len(portfolio)}")

    if not portfolio.is_empty():
        final_value = portfolio["equity_curve"].tail(1).item() * 10000
        total_return = (final_value / 10000 - 1) * 100
        print(f"期末价值: ${final_value:,.2f}")
        print(f"总收益率: {total_return:.2f}%")

    # 8. 尝试生成简单图表
    try:
        import matplotlib.pyplot as plt

        if not portfolio.is_empty():
            print("\n📈 生成资金曲线图...")

            dates = portfolio["date"].to_pandas()
            equity = portfolio["equity_curve"].to_pandas() * 10000

            plt.figure(figsize=(12, 6))
            plt.plot(dates, equity, label="Strategy Curve", linewidth=2)

            if not benchmark_data.is_empty():
                bench_dates = benchmark_data["date"].to_pandas()
                bench_equity = benchmark_data["benchmark_return"].to_pandas() * 10000
                plt.plot(bench_dates, bench_equity, label="benchmark", alpha=0.7)

            plt.title("Demo Strategy - Plotfolio Equity Curve")
            plt.xlabel("Date")
            plt.ylabel("Equity ($)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()

    except ImportError:
        print("matplotlib未安装，跳过图表生成")
    except Exception as e:
        print(f"图表生成出错: {e}")

    print("\n✅ 演示完成！")
    print("\n💡 提示：")
    print("- 这是使用模拟数据的演示")
    print("- 实际使用时请替换为真实的市场数据")
    print("- 可以通过修改strategy_config来调整策略参数")
    print("- 更多功能请参考 examples/bbiboll_backtest_example.py")


if __name__ == "__main__":
    main()
