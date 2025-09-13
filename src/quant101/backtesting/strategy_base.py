"""
策略基类 - 所有策略都应该继承这个基类
"""

import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import polars as pl

from quant101.core_2.config import cache_dir

strategy_cache_dir = os.path.join(cache_dir, "strategies")


class StrategyBase(ABC):
    """
    策略基类，定义策略接口
    """

    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.ohlcv_data = None
        self.tickers = None
        # 初始化缓存文件路径
        self.cache_file = os.path.join(strategy_cache_dir, f"{self.name}.csv")
        # 确保缓存目录存在
        os.makedirs(strategy_cache_dir, exist_ok=True)

    def load_cached_indicators(self) -> Optional[pl.DataFrame]:
        """加载缓存的指标数据"""
        if os.path.exists(self.cache_file):
            print(f"从缓存加载{self.name}指标: {self.cache_file}")
            return pl.read_csv(self.cache_file)
        return None

    def save_indicators_cache(self, indicators: pl.DataFrame) -> None:
        """保存指标数据到缓存"""
        indicators.write_csv(self.cache_file)
        print(f"{self.name}指标已缓存到: {self.cache_file}")

    def set_data(self, ohlcv_data: pl.DataFrame, tickers: list = None):
        """设置数据"""
        self.ohlcv_data = ohlcv_data
        self.tickers = tickers

    @abstractmethod
    def calculate_indicators(self, cached: bool = False) -> pl.DataFrame:
        """
        计算策略所需的技术指标

        Args:
            cached: 是否使用缓存的指标数据

        Returns:
            包含技术指标的DataFrame
        """
        pass

    @abstractmethod
    def generate_signals(self, indicators: pl.DataFrame) -> pl.DataFrame:
        """
        根据指标生成交易信号

        Args:
            indicators: 技术指标DataFrame

        Returns:
            交易信号DataFrame，包含columns: [ticker, timestamps, signal]
        """
        pass

    @abstractmethod
    def trade_rules(self, signals: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        根据信号执行交易规则，生成交易记录和组合表现

        Args:
            signals: 交易信号DataFrame

        Returns:
            tuple: (trades_df, portfolio_daily_df)
                - trades_df: 每笔交易记录
                - portfolio_daily_df: 每日组合表现
        """
        pass

    def run_backtest(self, use_cached_indicators: bool = False) -> Dict[str, Any]:
        """
        运行完整的回测流程

        Args:
            use_cached_indicators: 是否使用缓存的指标

        Returns:
            回测结果字典
        """
        if self.ohlcv_data is None:
            raise ValueError("必须先设置数据，请调用 set_data() 方法")

        # 1. 计算指标
        print(f"计算 {self.name} 策略指标...")
        indicators = self.calculate_indicators(cached=use_cached_indicators)

        # 2. 生成信号
        print(f"生成 {self.name} 策略信号...")
        signals = self.generate_signals(indicators)

        # 3. 执行交易
        print(f"执行 {self.name} 策略交易...")
        trades, portfolio_daily = self.trade_rules(signals)

        return {
            "strategy_name": self.name,
            "config": self.config,
            "indicators": indicators,
            "signals": signals,
            "trades": trades,
            "portfolio_daily": portfolio_daily,
        }

    def get_strategy_info(self) -> Dict[str, Any]:
        """获取策略信息"""
        return {
            "name": self.name,
            "config": self.config,
            "description": self.__doc__ or f"{self.name} 策略",
        }
