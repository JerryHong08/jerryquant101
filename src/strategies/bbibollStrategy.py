"""
优化后的BBIBOLL策略 - 继承策略基类
"""

import datetime
import os
import random
from typing import Any, Dict, List

import polars as pl

from backtesting.strategy_base import StrategyBase
from strategies.indicators.bbiboll_indicator import calculate_bbiboll


class BBIBOLLStrategy(StrategyBase):
    """
    BBIBOLL策略 - 基于BBI和布林带的量化策略

    策略逻辑:
    1. 计算BBI(多周期移动平均)和布林带指标
    2. 当偏离度 <= 1% 时产生买入信号
    3. 持仓3个交易日后卖出
    """

    def __init__(self, config: Dict[str, Any] = None):
        default_config = {
            "boll_length": 11,
            "boll_multiple": 6,
            "max_dev_pct": 1.0,  # 最大偏离度 1%
            "hold_days": 3,  # 持仓天数
            "start_date": "2023-02-13",
            "selected_tickers": ["random"],  # 可以设置为 'random' 随机选择
            "random_count": 10,  # 随机选择的股票数量
            # "min_turnover": 0,  # 最小成交额筛选
        }

        if config:
            default_config.update(config)

        super().__init__(name="bbiboll", config=default_config)

    def calculate_indicators(self, cached: bool = False) -> pl.DataFrame:
        """
        计算BBIBOLL技术指标

        Args:
            cached: 是否使用缓存的指标数据

        Returns:
            包含技术指标的DataFrame
        """

        if cached:
            cached_indicators = self.load_cached_indicators()
            if cached_indicators is not None:
                return cached_indicators

        print("计算BBIBOLL指标...")

        # compute bbiboll
        indicators = calculate_bbiboll(
            self.ohlcv_data,
            boll_length=self.config["boll_length"],
            boll_multiple=self.config["boll_multiple"],
        )

        # 添加成交额
        indicators = indicators.with_columns(
            (pl.col("volume") * pl.col("close")).alias("turnover")
        )

        # 保存缓存
        self.save_indicators_cache(indicators)

        return indicators

    def generate_signals(self, indicators: pl.DataFrame) -> pl.DataFrame:
        """
        根据BBIBOLL指标生成交易信号

        Args:
            indicators: 技术指标DataFrame

        Returns:
            交易信号DataFrame，包含买入(1)和卖出(-1)信号
        """
        # 选择要交易的股票
        examined_tickers = self._select_tickers(indicators)
        print(f"选择的交易股票: {examined_tickers}")

        # 生成交易信号
        start_date = datetime.datetime.strptime(
            self.config["start_date"], "%Y-%m-%d"
        ).date()

        # 基础数据过滤
        filtered_indicators = indicators.filter(
            (pl.col("ticker").is_in(examined_tickers))
            & (pl.col("bbi").is_not_null())
            & (pl.col("timestamps").dt.date() >= start_date)
        ).sort(["ticker", "timestamps"])

        if filtered_indicators.is_empty():
            print("没有满足基础条件的数据")
            return pl.DataFrame()

        # 条件一：偏离度 <= max_dev_pct
        condition_one = pl.col("dev_pct") <= self.config["max_dev_pct"]

        # 为每行数据添加是否满足买入条件的标记
        data_with_condition = filtered_indicators.with_columns(
            pl.when(condition_one)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("meets_buy_condition")
        )

        # 初始化信号列表
        all_signals = []

        # 按股票分组处理
        for ticker in examined_tickers:
            ticker_data = data_with_condition.filter(pl.col("ticker") == ticker).sort(
                "timestamps"
            )

            if ticker_data.is_empty():
                continue

            ticker_signals = []
            is_holding = False
            buy_price = None
            buy_date = None

            # 遍历每个交易日
            for row in ticker_data.iter_rows(named=True):
                current_date = row["timestamps"]
                current_close = row["close"]
                current_open = row["open"]
                meets_condition = row["meets_buy_condition"]
                current_dev_pct = row["dev_pct"]

                if not is_holding and meets_condition:
                    # 产生买入信号
                    ticker_signals.append(
                        {"ticker": ticker, "timestamps": current_date, "signal": 1}
                    )
                    is_holding = True
                    buy_price = current_close
                    buy_date = current_date

                elif is_holding and buy_price is not None:
                    # 计算收益率（基于买入日收盘价）
                    daily_return = (current_open / buy_price) - 1

                    # 卖出条件判断
                    loss_threshold = -0.20  # 亏损20%
                    profit_threshold = 0.20  # 盈利20%

                    sell_condition = (
                        # 条件1: 亏损超过20% 且不再满足买入条件
                        (
                            daily_return < loss_threshold
                            and current_dev_pct > self.config["max_dev_pct"]
                        )
                        or
                        # 条件2: 盈利超过20%
                        (daily_return > profit_threshold)
                    )

                    if sell_condition:
                        # 产生卖出信号
                        ticker_signals.append(
                            {"ticker": ticker, "timestamps": current_date, "signal": -1}
                        )
                        is_holding = False
                        buy_price = None
                        buy_date = None

            all_signals.extend(ticker_signals)

        if not all_signals:
            print("没有生成任何交易信号")
            return pl.DataFrame()

        # 转换为DataFrame
        signals_df = (
            pl.DataFrame(all_signals)
            .with_columns(
                pl.col("timestamps").dt.cast_time_unit("ns")  # 统一转换为微秒精度
            )
            .sort(["ticker", "timestamps"])
            .rename({"timestamps": "signal_date"})
        )

        # 统计信号数量
        buy_count = signals_df.filter(pl.col("signal") == 1).height
        sell_count = signals_df.filter(pl.col("signal") == -1).height

        print(f"生成买入信号数量: {buy_count}")
        print(f"生成卖出信号数量: {sell_count}")
        print(f"总信号数量: {len(signals_df)}")

        return signals_df

    def trade_rules(self, signals: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        根据信号执行交易规则

        Args:
            signals: 交易信号DataFrame，包含买入(1)和卖出(-1)信号

        Returns:
            tuple: (trades_df, portfolio_daily_df)
        """
        if signals.is_empty():
            print("没有生成任何交易信号")
            return pl.DataFrame(), pl.DataFrame()

        with pl.Config(tbl_rows=20, tbl_cols=50):
            print(signals.head(20))

        # 准备价格数据
        prices = self.ohlcv_data.select(["ticker", "timestamps", "open", "close"]).sort(
            ["ticker", "timestamps"]
        )

        # 分离买入和卖出信号
        buy_signals = (
            signals.filter(pl.col("signal") == 1)
            .select(["ticker", "signal_date"])
            .rename({"signal_date": "buy_signal_date"})
        )
        sell_signals = (
            signals.filter(pl.col("signal") == -1)
            .select(["ticker", "signal_date"])
            .rename({"signal_date": "sell_signal_date"})
        )

        # 为每个买入信号匹配对应的卖出信号
        # 使用 join_asof 来匹配每个买入信号后的第一个卖出信号
        trades = buy_signals.join_asof(
            sell_signals.sort(["ticker", "sell_signal_date"]),
            left_on="buy_signal_date",
            right_on="sell_signal_date",
            by="ticker",
            strategy="forward",
        )

        # 获取买入价格（买入信号日的次日开盘价）
        trades = trades.join(
            prices.select(["ticker", "timestamps", "open"]).rename(
                {"timestamps": "buy_signal_date", "open": "buy_open"}
            ),
            on=["ticker", "buy_signal_date"],
            how="left",
        )

        # 获取卖出价格（卖出信号日的次日开盘价）
        trades = trades.join(
            prices.select(["ticker", "timestamps", "open"]).rename(
                {"timestamps": "sell_signal_date", "open": "sell_open"}
            ),
            on=["ticker", "sell_signal_date"],
            how="left",
        )

        # 过滤有效交易（必须有买入和卖出价格）
        trades = trades.filter(
            pl.col("buy_open").is_not_null()
            & pl.col("sell_open").is_not_null()
            & pl.col("sell_signal_date").is_not_null()
            & (pl.col("buy_signal_date") < pl.col("sell_signal_date"))
        )

        # 计算交易收益
        trades = trades.with_columns(
            ((pl.col("sell_open") / pl.col("buy_open")) - 1).alias("return")
        )

        # 重命名列以保持一致性
        trades = trades.rename(
            {"buy_signal_date": "buy_date", "sell_signal_date": "sell_date"}
        )

        # 展开每笔交易到每个持仓日
        portfolio_daily = self._calculate_daily_portfolio(trades, prices)

        trades_output = trades.select(
            ["ticker", "buy_date", "buy_open", "sell_date", "sell_open", "return"]
        ).sort(["buy_date", "ticker"])

        print(f"生成交易记录数量: {len(trades_output)}")
        print(f"组合每日表现记录数量: {len(portfolio_daily)}")

        return trades_output, portfolio_daily

    def _select_tickers(self, indicators: pl.DataFrame) -> List[str]:
        """选择要交易的股票"""
        selected_tickers = self.config["selected_tickers"]

        if isinstance(selected_tickers, list) and selected_tickers != ["random"]:
            return selected_tickers

        # 随机选择股票
        available_tickers = indicators.select("ticker").unique().to_series().to_list()
        if self.config["random_count"] == None:
            random_count = len(available_tickers)
        else:
            random_count = min(self.config["random_count"], len(available_tickers))
        print(f"可选股票数量: {len(available_tickers)}，随机选择数量: {random_count}")

        if selected_tickers == ["random"] or selected_tickers == "random":
            selected = random.sample(available_tickers, random_count)
        else:
            selected = available_tickers[:random_count]

        return selected

    def _calculate_daily_portfolio(
        self, trades: pl.DataFrame, prices: pl.DataFrame
    ) -> pl.DataFrame:
        """计算每日组合表现"""
        if trades.is_empty():
            return pl.DataFrame()

        # 为每笔交易展开到持仓期间的每一天
        expanded_trades = []

        for row in trades.iter_rows(named=True):
            ticker = row["ticker"]
            buy_date = row["buy_date"]
            sell_date = row["sell_date"]
            buy_price = row["buy_open"]

            # 获取该股票在持仓期间的价格数据
            ticker_prices = prices.filter(
                (pl.col("ticker") == ticker)
                & (pl.col("timestamps") >= buy_date)
                & (pl.col("timestamps") < sell_date)
            ).sort("timestamps")

            # 计算每日收益率
            if not ticker_prices.is_empty():
                ticker_prices_with_return = ticker_prices.with_columns(
                    [
                        pl.lit(ticker).alias("trade_ticker"),
                        pl.lit(buy_date).alias("trade_buy_date"),
                        pl.lit(buy_price).alias("trade_buy_price"),
                        ((pl.col("close") / buy_price) - 1).alias("position_return"),
                    ]
                )

                expanded_trades.append(ticker_prices_with_return)

        if not expanded_trades:
            return pl.DataFrame()

        # 合并所有展开的交易
        all_positions = pl.concat(expanded_trades)

        # 按日期聚合组合收益（等权平均）
        portfolio_daily = (
            all_positions.group_by("timestamps")
            .agg(
                [
                    pl.col("position_return").mean().alias("portfolio_return"),
                    pl.count().alias("n_positions"),
                ]
            )
            .sort("timestamps")
            .rename({"timestamps": "date"})
        )

        # 计算累计权益曲线
        portfolio_daily = portfolio_daily.with_columns(
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
        )

        return portfolio_daily
