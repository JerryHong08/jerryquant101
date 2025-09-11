"""
优化后的BBIBOLL策略 - 继承策略基类
"""

import datetime
import os
import random
from typing import Any, Dict, List

import polars as pl

from quant101.backtesting.strategy_base import StrategyBase
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll


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
            "min_turnover": 0,  # 最小成交额筛选
        }

        if config:
            default_config.update(config)

        super().__init__(name="BBIBOLL", config=default_config)

    def calculate_indicators(self, cached: bool = False) -> pl.DataFrame:
        """
        计算BBIBOLL技术指标

        Args:
            cached: 是否使用缓存的指标数据

        Returns:
            包含技术指标的DataFrame
        """
        cache_file = "bbiboll.parquet"

        if cached and os.path.exists(cache_file):
            print(f"从缓存加载BBIBOLL指标: {cache_file}")
            return pl.read_parquet(cache_file)

        print("计算BBIBOLL指标...")

        # 计算指标
        indicators = calculate_bbiboll(
            self.ohlcv_data,
            boll_length=self.config["boll_length"],
            boll_multiple=self.config["boll_multiple"],
        )

        # 添加成交额
        indicators = indicators.with_columns(
            (pl.col("volume") * pl.col("close")).alias("turnover")
        )

        # 添加退市日期信息（如果tickers包含退市信息）
        if self.tickers is not None and hasattr(self.tickers, "columns"):
            if "delisted_utc" in self.tickers.columns:
                delisted_info = self.tickers.with_columns(
                    [
                        pl.col("delisted_utc")
                        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                        .dt.date()
                        .alias("delisted_date")
                    ]
                )

                indicators = indicators.join(
                    delisted_info.select(["ticker", "delisted_date"]),
                    on="ticker",
                    how="left",
                )

        # 保存缓存
        indicators.write_parquet(cache_file)
        print(f"BBIBOLL指标已缓存到: {cache_file}")

        return indicators

    def generate_signals(self, indicators: pl.DataFrame) -> pl.DataFrame:
        """
        根据BBIBOLL指标生成交易信号

        Args:
            indicators: 技术指标DataFrame

        Returns:
            交易信号DataFrame
        """
        # 选择要交易的股票
        examined_tickers = self._select_tickers(indicators)
        print(f"选择的交易股票: {examined_tickers}")

        # 生成交易信号
        start_date = datetime.datetime.strptime(
            self.config["start_date"], "%Y-%m-%d"
        ).date()

        signals = indicators.filter(
            # 基础过滤条件
            pl.col("bbi").is_not_null()
            & (pl.col("dev_pct") <= self.config["max_dev_pct"])
            & (pl.col("timestamps").dt.date() >= start_date)
            & (pl.col("turnover") >= self.config["min_turnover"])
            & (pl.col("ticker").is_in(examined_tickers))
        )

        # 添加退市股票过滤
        if "delisted_date" in signals.columns:
            signals = signals.filter(
                pl.col("delisted_date").is_null()
                | (pl.col("timestamps").dt.date() <= pl.col("delisted_date"))
            )

        # 提取信号
        signals = signals.select(["timestamps", "ticker"]).with_columns(
            pl.lit(1).alias("signal")
        )

        print(f"生成信号数量: {len(signals)}")
        return signals

    def trade_rules(self, signals: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        根据信号执行交易规则

        Args:
            signals: 交易信号DataFrame

        Returns:
            tuple: (trades_df, portfolio_daily_df)
        """
        if signals.is_empty():
            print("没有生成任何交易信号")
            return pl.DataFrame(), pl.DataFrame()

        # 信号分组和去重
        signals = signals.sort(["ticker", "timestamps"])
        signals = signals.with_columns(
            (pl.col("timestamps").diff().dt.total_days().fill_null(999) > 1)
            .cum_sum()
            .over("ticker")
            .alias("block_id")
        )

        # 每个信号块只取最后一个信号
        last_signals = signals.group_by(["ticker", "block_id"]).agg(
            pl.col("timestamps").max().alias("signal_date")
        )

        # 准备价格数据，添加行号
        prices = self.ohlcv_data.select(["ticker", "timestamps", "open", "close"]).sort(
            ["ticker", "timestamps"]
        )

        prices = prices.with_columns(
            pl.arange(0, pl.len()).over("ticker").alias("row_id")
        )

        # 将信号日期映射到价格数据
        last_signals = last_signals.join(
            prices.rename({"timestamps": "signal_date"}),
            on=["ticker", "signal_date"],
            how="left",
        )

        # 计算买卖行号
        hold_days = self.config["hold_days"]
        last_signals = last_signals.with_columns(
            [
                (pl.col("row_id") + 1).alias("buy_row_id"),
                (pl.col("row_id") + 1 + hold_days).alias("sell_row_id"),
            ]
        )

        # 获取买入价格
        buys = last_signals.join(
            prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
                {"row_id": "buy_row_id", "timestamps": "buy_date", "open": "buy_open"}
            ),
            on=["ticker", "buy_row_id"],
            how="left",
        )

        # 获取卖出价格
        sells = last_signals.join(
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

        # 合并买卖数据
        trades = buys.join(
            sells.select(
                ["ticker", "block_id", "sell_row_id", "sell_date", "sell_open"]
            ),
            on=["ticker", "block_id", "sell_row_id"],
            how="left",
        ).select(
            [
                "ticker",
                "block_id",
                "buy_row_id",
                "buy_date",
                "buy_open",
                "sell_row_id",
                "sell_date",
                "sell_open",
            ]
        )

        # 过滤有效交易
        trades = trades.filter(
            pl.col("buy_row_id").is_not_null()
            & pl.col("sell_row_id").is_not_null()
            & (pl.col("buy_row_id") < pl.col("sell_row_id"))
            & pl.col("buy_open").is_not_null()
            & pl.col("sell_open").is_not_null()
        )

        # 计算交易收益
        trades = trades.with_columns(
            ((pl.col("sell_open") / pl.col("buy_open")) - 1).alias("return")
        )

        # 展开每笔交易到每个持仓日
        portfolio_daily = self._calculate_daily_portfolio(trades, prices)

        trades_output = trades.select(
            [
                "ticker",
                "block_id",
                "buy_row_id",
                "buy_date",
                "buy_open",
                "sell_row_id",
                "sell_date",
                "sell_open",
                "return",
            ]
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
        random_count = min(self.config["random_count"], len(available_tickers))

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

        # 准备价格数据（用于计算每日收益）
        prices_short = prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
            {"row_id": "day_row_id", "timestamps": "date", "open": "open_day"}
        )

        # 展开每笔交易到持仓日
        expanded = trades.join(prices_short, on="ticker", how="left").filter(
            (pl.col("day_row_id") >= pl.col("buy_row_id"))
            & (pl.col("day_row_id") < pl.col("sell_row_id"))
        )

        # 获取下一日开盘价
        expanded = expanded.with_columns(
            (pl.col("day_row_id") + 1).alias("next_row_id")
        )

        prices_next = prices.select(["ticker", "row_id", "open"]).rename(
            {"row_id": "next_row_id", "open": "open_next"}
        )

        expanded = expanded.join(prices_next, on=["ticker", "next_row_id"], how="left")

        # 计算每日收益
        expanded = expanded.with_columns(
            ((pl.col("open_next") / pl.col("open_day")) - 1).alias("daily_return")
        ).filter(pl.col("daily_return").is_not_null())

        # 按日期聚合组合收益（等权平均）
        portfolio_daily = (
            expanded.group_by("date")
            .agg(
                [
                    pl.col("daily_return").mean().alias("portfolio_return"),
                    pl.count().alias("n_positions"),
                ]
            )
            .sort("date")
        )

        # 计算累计权益曲线
        portfolio_daily = portfolio_daily.with_columns(
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
        )

        return portfolio_daily
