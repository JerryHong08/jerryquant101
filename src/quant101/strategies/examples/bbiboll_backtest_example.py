"""
BBIBOLL策略回测示例 - 使用新的回测框架
"""

import datetime
import os

import polars as pl

from quant101.backtesting.engine import BacktestEngine
from quant101.backtesting.visualizer import BacktestVisualizer
from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import stock_load_process
from quant101.strategies.bbiboll_optimized import BBIBOLLStrategy


def load_spx_benchmark():
    """加载SPX基准数据"""
    try:
        spx = pl.read_parquet("I:SPXday20150101_20250905.parquet")
        spx = spx.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.convert_time_zone("America/New_York")
            .dt.replace(hour=0, minute=0, second=0)
            .cast(pl.Datetime("ns", "America/New_York"))
            .alias("date")
        )

        # 计算基准收益曲线（归一化）
        spx = spx.with_columns(
            (pl.col("close") / pl.col("close").first()).alias("benchmark_return")
        ).select(["date", "close", "benchmark_return"])

        return spx
    except Exception as e:
        print(f"加载SPX基准数据失败: {e}")
        return None


def load_tickers_data():
    """加载股票列表数据"""
    try:
        # 这里简化处理，直接返回指定股票
        return ["FDUS", "NVDA", "TSLA", "AAPL", "MSFT"]
    except Exception as e:
        print(f"加载股票列表失败: {e}")
        return ["FDUS"]


def only_common_stocks():
    all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    tickers = all_tickers.filter(
        (pl.col("type").is_in(["CS", "ADRC"]))
        & (
            (pl.col("active") == True)
            | (
                pl.col("delisted_utc").is_not_null()
                & (
                    pl.col("delisted_utc")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    > datetime.date(2023, 1, 1)
                )
            )
        )
    ).select(pl.col(["ticker", "delisted_utc"]))

    print(f"Using {all_tickers_file}, total {len(tickers)} active tickers")

    return tickers


def main():
    """主函数 - 演示BBIBOLL策略回测"""

    print("BBIBOLL策略回测演示")
    print("=" * 60)

    # 1. 配置参数
    config = {
        "timeframe": "1d",
        "start_date": "2022-01-01",
        "end_date": "2025-09-05",
        "initial_capital": 100.0,
    }

    # 策略配置
    strategy_config = {
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1.0,
        "hold_days": 3,
        "start_date": "2023-02-13",
        "selected_tickers": ["TPET"],  # 可以改为 'random' 随机选择
        # 'selected_tickers': ['random'],  # 可以改为 'random' 随机选择
        "random_count": 7348,
        "min_turnover": 0,
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
    benchmark_data = load_spx_benchmark()

    # 4. 创建回测引擎
    engine = BacktestEngine(initial_capital=config["initial_capital"])

    # 5. 创建策略实例
    print("创建BBIBOLL策略...")

    bbiboll_strategy = BBIBOLLStrategy(config=strategy_config)

    # 6. 添加数据到策略
    engine.add_strategy(bbiboll_strategy, ohlcv_data, tickers)

    # 7. 运行回测
    print("开始回测...")
    results = engine.run_backtest(
        strategy=bbiboll_strategy,
        benchmark_data=benchmark_data,
        use_cached_indicators=True,  # 使用缓存以加快速度
        save_results=True,
    )

    # 8. 绘制结果图表
    print("生成回测图表...")
    try:
        # 资金曲线图
        engine.plot_results(
            strategy_name="BBIBOLL",
            plot_equity=True,
            plot_performance=True,
            plot_monthly=True,
            save_plots=True,
            output_dir="backtest_output",
        )

        # 个股K线图和交易信号（示例）
        visualizer = BacktestVisualizer()
        selected_ticker = strategy_config["selected_tickers"][0]

        print(f"绘制 {selected_ticker} 的K线图和交易信号...")
        visualizer.plot_candlestick_with_signals(
            ohlcv_data=ohlcv_data,
            trades=results["trades"],
            ticker=selected_ticker,
            start_date="2022-01-01",
            end_date="2025-09-05",
            indicators=results.get("indicators"),
            save_path=f"backtest_output/{selected_ticker}_signals.png",
        )

    except Exception as e:
        print(f"绘图过程中出现错误: {e}")

    # 9. 导出结果
    print("导出回测结果...")
    try:
        engine.export_results("BBIBOLL", output_dir="backtest_output")
    except Exception as e:
        print(f"导出结果时出现错误: {e}")

    # 10. 显示关键结果
    print("\n关键结果摘要:")
    print("-" * 40)
    performance = results["performance_metrics"]

    key_metrics = [
        ("总收益率", f"{performance.get('Total Return [%]', 0):.2f}%"),
        ("基准收益率", f"{performance.get('Benchmark Return [%]', 0):.2f}%"),
        ("最大回撤", f"{performance.get('Max Drawdown [%]', 0):.2f}%"),
        ("夏普比率", f"{performance.get('Sharpe Ratio', 0):.4f}"),
        ("胜率", f"{performance.get('Win Rate [%]', 0):.2f}%"),
        ("交易次数", f"{performance.get('Total Trades', 0)}"),
    ]

    for metric, value in key_metrics:
        print(f"{metric:<12}: {value}")

    print(f"\n回测完成！结果已保存到 backtest_output/ 目录")


def run_multiple_strategies_example():
    """运行多策略比较示例"""
    print("\n多策略比较示例")
    print("=" * 60)

    # 加载数据（复用上面的函数）
    tickers = load_tickers_data()

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers,
                timeframe="1d",
                start_date="2022-01-01",
                end_date="2025-09-05",
            )
            .drop(["split_date", "window_start", "split_ratio"])
            .collect()
        )
    except Exception as e:
        print(f"加载数据失败: {e}")
        return

    benchmark_data = load_spx_benchmark()
    engine = BacktestEngine(initial_capital=100.0)

    # 创建不同配置的策略
    strategies = []

    # 策略1：保守型（短持仓）
    conservative_config = {
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 0.5,
        "hold_days": 2,
        "start_date": "2023-02-13",
        "selected_tickers": ["FDUS"],
        "min_turnover": 0,
    }
    conservative_strategy = BBIBOLLStrategy(config=conservative_config)
    conservative_strategy.name = "BBIBOLL_Conservative"
    strategies.append(conservative_strategy)

    # 策略2：激进型（长持仓）
    aggressive_config = {
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1.5,
        "hold_days": 5,
        "start_date": "2023-02-13",
        "selected_tickers": ["FDUS"],
        "min_turnover": 0,
    }
    aggressive_strategy = BBIBOLLStrategy(config=aggressive_config)
    aggressive_strategy.name = "BBIBOLL_Aggressive"
    strategies.append(aggressive_strategy)

    # 添加数据
    for strategy in strategies:
        engine.add_strategy(strategy, ohlcv_data, tickers)

    # 运行多策略回测
    all_results = engine.run_multiple_strategies(
        strategies=strategies, benchmark_data=benchmark_data, use_cached_indicators=True
    )

    print("多策略比较完成！")


if __name__ == "__main__":
    # 运行主回测示例
    main()

    # 可选：运行多策略比较示例
    # run_multiple_strategies_example()
