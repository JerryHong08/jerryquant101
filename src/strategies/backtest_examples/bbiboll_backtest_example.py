"""
BBIBOLL策略回测示例 - 使用新的回测框架
"""

import os

import polars as pl

from backtesting.engine import BacktestEngine
from backtesting.visualizer import BacktestVisualizer
from core_2.config import all_tickers_dir
from core_2.data_loader import stock_load_process
from strategies.bbibollStrategy import BBIBOLLStrategy
from strategies.pre_data import load_spx_benchmark, only_common_stocks


def main():
    """主函数 - 演示BBIBOLL策略回测"""

    print("BBIBOLL策略回测演示")
    print("=" * 60)

    # 1. 配置参数
    config = {
        "timeframe": "1d",
        "start_date": "2022-01-01",
        "end_date": "2025-09-19",
        "initial_capital": 10000.0,
    }

    # 策略配置
    strategy_config = {
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1,
        "hold_days": 5,
        "start_date": "2023-02-13",
        "selected_tickers": ["LCID"],  # 可以改为 'random' 随机选择
        # "selected_tickers": ['AACG', 'AC', 'ACAD', 'ACNT', 'ADD', 'AEHL', 'AEON', 'AHG', 'AIRI', 'AKA', 'AKU', 'ALNY', 'ALPS', 'ALTS', 'AMBR', 'AMLX', 'ARDX', 'AREN', 'ASNS', 'ATXI', 'AUGX', 'AUID', 'AUUD', 'BACC', 'BCLI', 'BFRI', 'BIIB', 'BLUW', 'BMTX', 'BNRE', 'BTOC', 'BVFL', 'CCII', 'CDTX', 'CGTL', 'CLSK', 'CMDBw', 'CMPOV', 'CMREw', 'CNL', 'CNTG', 'CPTN', 'CRD.B', 'CRSP', 'CURR', 'CYTK', 'DERM', 'DISCB', 'DLA', 'DPSI', 'DRDB', 'DRUG', 'DTCK', 'DVLT', 'DWTX', 'DYNT', 'EDN', 'EDTK', 'EDUC', 'EHLDV', 'ELDN', 'ELMD', 'ELWS', 'EQD', 'ESSC', 'EUDA', 'EVAC', 'FENG', 'FFNW', 'FLX', 'FMST', 'FNCB', 'FPAY', 'FRES', 'FTLF', 'FUNC', 'FUSN', 'GERN', 'GFED', 'GH', 'GLE', 'GMHS', 'GNS', 'GOAC', 'GPL', 'GRNQ', 'GTBP', 'GTIM', 'GURE', 'GVH', 'HBANM', 'HCTI', 'HHS', 'HIVE', 'HKIT', 'HROWL', 'HUDI', 'ICPT', 'IMDX', 'IMNM', 'INKT', 'INPXV', 'INTS', 'INVA', 'IPWR', 'ISBA', 'ITRM', 'JRSH', 'JXJT', 'KEN', 'LBGJ', 'LDTC', 'LEAP', 'LGLw', 'LIXT', 'LMNL', 'LSH', 'LSTA', 'LUMO', 'LUXA', 'LWAY', 'LXRX', 'MBINM', 'MDCX', 'MGIH', 'MHH', 'MI', 'MIXT', 'MKZR', 'MLP', 'MLTX', 'MNPR', 'MNTX', 'MTEM', 'MTR', 'MYSE', 'NDAC', 'NERV', 'NH', 'NIXX', 'NKSH', 'NMFCZ', 'NMRD', 'NPAC', 'NTBL', 'NTIC', 'NTWK', 'NVA', 'NVAX', 'NVCN', 'NVIV', 'NVOS', 'NWFL', 'NWTG', 'OBCI', 'OCA', 'OCFT', 'OCS', 'ODYS', 'OLB', 'ONCT', 'ONE', 'ONFO', 'OST', 'OXLCI', 'PC', 'PCAP', 'PCOM', 'PCSA', 'PELI', 'PMD', 'PT', 'PWOD', 'PXDT', 'QXO', 'RADX', 'RDAG', 'RDZN', 'REED', 'RGCO', 'RILYG', 'RKDA', 'ROLR', 'RYM', 'SAI', 'SCVX', 'SEAC', 'SEMw', 'SERA', 'SHIM', 'SJ', 'SMFL', 'SMID', 'SMIT', 'SNAL', 'SNSE', 'SPHA', 'SPPI', 'SPPL', 'SPRC', 'SPRY', 'SQFT', 'SRPT', 'SRXH', 'SVFC', 'SYBX', 'TAIT', 'TCRX', 'TELA', 'TLIS', 'TOPS', 'TVAC', 'UBXG', 'UFAB', 'UFG', 'USCB', 'VECT', 'VEEE', 'VERU', 'VGII', 'VHC', 'VIVK', 'VNME', 'VSTE', 'VTAK', 'VZLA', 'WAI', 'WS', 'WXM', 'XBIT', 'XXII', 'XYF', 'YYGH', 'ZIVO', 'ZKIN', 'ZVRA'],
        # "selected_tickers": ["random"],  # 可以改为 'random' 随机选择
        "random_count": 7709,
        "min_turnover": 0,
        "plot_all": True,
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
        ) and (strategy_config["plot_all"] == False):
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
                # line=False,
                save_path=f"{output_dir}/{selected_ticker}_signals.png",
            )

        elif strategy_config["plot_all"]:
            for selected_ticker in strategy_config.get("selected_tickers", []):
                print(f"绘制 {selected_ticker} 的K线图和交易信号...")
                visualizer.plot_candlestick_with_signals(
                    ohlcv_data=ohlcv_data,
                    trades=results["trades"],
                    ticker=selected_ticker,
                    start_date="2022-01-01",
                    end_date="2025-09-05",
                    indicators=results.get("indicators"),
                    # line=False,
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
