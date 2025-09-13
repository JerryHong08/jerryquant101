"""
BBIBOLL策略回测示例 - 使用新的回测框架
"""

import os

import polars as pl

from quant101.backtesting.engine import BacktestEngine
from quant101.backtesting.visualizer import BacktestVisualizer
from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import stock_load_process
from quant101.strategies.bbibollStrategy import BBIBOLLStrategy
from quant101.strategies.pre_data import load_spx_benchmark, only_common_stocks


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
        "max_dev_pct": 1,
        "hold_days": 2,
        "start_date": "2023-02-13",
        "selected_tickers": ["DVLT"],  # 可以改为 'random' 随机选择
        # "selected_tickers": ['SILA', 'RS', 'NLSP', 'XWEL', 'RIVN', 'CHKP', 'SANA', 'BAP', 'SBSW', 'FRSH', 'CHW', 'WLY', 'RLYB', 'LUCD', 'ZBH', 'AWK', 'WLMS', 'TFC', 'WPRT', 'WBX', 'TCVA', 'LGHL', 'ABTS', 'PWR', 'FIX', 'INGR', 'MRAI', 'BMRA', 'TALK', 'CTV', 'ADPT', 'WDS', 'INAB', 'LIN', 'MXCT', 'PSNL', 'PLRX', 'AVNW', 'BGSF', 'IQST', 'PMI', 'FWONK', 'MGOL', 'WGS', 'PNC', 'WIRE', 'ULBI', 'SKIL', 'SGFY', 'DMAC', 'APRN', 'JANX', 'ABR', 'HLVX', 'EQT', 'TRUE', 'SLAM', 'EEX', 'ATTO', 'ERIE', 'INFA', 'SMPL', 'NUKK', 'ARTV', 'ALNY', 'KSPI', 'BSX', 'ACRO', 'DUK', 'CBZ', 'ENR', 'CABA', 'CMPR', 'BHVN', 'ACOR', 'CENQ', 'INLF', 'AMRZ', 'TGB', 'GORO', 'SMBC', 'NYMTI', 'WEN', 'TRGP', 'FRME', 'CAPT', 'JNCE', 'RGR', 'EVCM', 'SNWV', 'MAIA', 'INTU', 'DAIC', 'PHI', 'SSNC', 'GDST', 'SBIG', 'ASPA', 'ACOG', 'MDNA'],
        # 'selected_tickers': ['random'],  # 可以改为 'random' 随机选择
        "random_count": 7708,
        "min_turnover": 0,
    }

    # 2. 加载数据
    print("加载市场数据...")
    tickers = only_common_stocks(filter_date=config["start_date"])

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers.to_series().to_list(),
                timeframe=config["timeframe"],
                start_date=config["start_date"],
                end_date=config["end_date"],
            )
            .filter(pl.col("volume") != 0)
            .collect()
        )

        print(f"股票数量: {ohlcv_data.select('ticker').n_unique()}")

    except Exception as e:
        print(f"加载市场数据失败: {e}")
        return

    # 3. 加载基准数据
    print("加载基准数据...")
    benchmark_data = load_spx_benchmark(config["start_date"], config["end_date"])

    # 4. 创建回测引擎
    engine = BacktestEngine(initial_capital=config["initial_capital"])

    # 5. 创建策略实例
    print("创建BBIBOLL策略...")

    strategy = BBIBOLLStrategy(config=strategy_config)

    # 6. 添加数据到策略
    engine.add_strategy(strategy, ohlcv_data, tickers)

    # 7. 运行回测
    print("开始回测...")
    results = engine.run_backtest(
        strategy=strategy,
        benchmark_data=benchmark_data,
        use_cached_indicators=True,  # 使用缓存以加快速度
        save_results=True,
    )

    strategy_name = strategy.name
    output_dir = os.path.join("backtest_output", strategy_name)

    # 8. 绘制结果图表
    print("生成回测图表...")
    selected_ticker = strategy_config["selected_tickers"][0]
    try:
        if (
            len(strategy_config.get("selected_tickers", [])) > 1
            or selected_ticker == "random"
        ):
            # 资金曲线图
            engine.plot_results(
                strategy_name=strategy_name,
                plot_equity=True,
                plot_performance=True,
                plot_monthly=True,
                save_plots=True,
                output_dir=output_dir,
            )

        # 个股K线图和交易信号（示例）
        visualizer = BacktestVisualizer()

        if (
            len(strategy_config.get("selected_tickers", [])) > 1
            or selected_ticker == "random"
        ):
            selected_ticker = (
                results["trades"].select("ticker").unique().to_series().sample(1)[0]
            )

        print(f"绘制 {selected_ticker} 的K线图和交易信号...")
        visualizer.plot_candlestick_with_signals(
            ohlcv_data=ohlcv_data,
            trades=results["trades"],
            ticker=selected_ticker,
            start_date="2022-01-01",
            end_date="2025-09-05",
            indicators=results.get("indicators"),
            save_path=f"{output_dir}/{selected_ticker}_signals.png",
        )

    except Exception as e:
        print(f"绘图过程中出现错误: {e}")

    # 9. 导出结果
    print("导出回测结果...")
    try:
        engine.export_results(strategy_name, output_dir=output_dir)
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

    print(f"\n回测完成！结果已保存到 {output_dir} 目录")


def run_multiple_strategies_example():
    """运行多策略比较示例"""
    print("\n多策略比较示例")
    print("=" * 60)

    # 加载数据（复用上面的函数）
    tickers = only_common_stocks()

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers.to_series().to_list(),
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
        "max_dev_pct": 1,
        "hold_days": 2,
        "start_date": "2023-02-13",
        # "selected_tickers": ["FDUS"],
        "selected_tickers": ["random"],  # 可以改为 'random' 随机选择
        "random_count": 7367,
        "min_turnover": 0,
    }
    conservative_strategy = BBIBOLLStrategy(config=conservative_config)
    conservative_strategy.name = "BBIBOLL_Conservative"
    strategies.append(conservative_strategy)

    # 策略2：激进型（长持仓）
    aggressive_config = {
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1,
        "hold_days": 5,
        "start_date": "2023-02-13",
        "selected_tickers": ["random"],  # 可以改为 'random' 随机选择
        "random_count": 7367,
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
