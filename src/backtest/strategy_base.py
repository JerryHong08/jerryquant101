import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import polars as pl

from cores.config import cache_dir
from utils.backtest_utils.backtest_utils import load_irx_data

strategy_cache_dir = os.path.join(cache_dir, "strategies")


class StrategyBase(ABC):
    """
    StrategyBase
    """

    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.ohlcv_data = None
        self.tickers = None

        self.cache_file = os.path.join(strategy_cache_dir, f"{self.name}.csv")

        os.makedirs(strategy_cache_dir, exist_ok=True)

    def load_cached_indicators(self) -> Optional[pl.DataFrame]:
        """load cached indicators"""
        if os.path.exists(self.cache_file):
            print(f"loading {self.name} cached indicators: {self.cache_file}")
            df = pl.read_csv(self.cache_file, try_parse_dates=True)
            # Convert datetime columns to datetime[ns, America/New_York] format
            for col in df.columns:
                if df[col].dtype in [pl.Datetime, pl.Datetime("us"), pl.Datetime("ms")]:
                    df = df.with_columns(
                        pl.col(col)
                        .dt.convert_time_zone("America/New_York")
                        .dt.cast_time_unit("ns")
                    )
            return df
        return None

    def save_indicators_cache(self, indicators: pl.DataFrame) -> None:
        """save indicators to cache"""
        indicators.write_csv(self.cache_file)
        print(f"{self.name} cache saved into: {self.cache_file}")

    def set_data(self, ohlcv_data: pl.DataFrame, tickers: list = None):
        """set ohlcv data for backtest"""
        self.ohlcv_data = ohlcv_data
        self.tickers = tickers

    @abstractmethod
    def calculate_indicators(self, cached: bool = False) -> pl.DataFrame:
        """
        calculate technical indicators

        Args:
            cached: if use cached indicators

        Returns:
            return ohlcv with indicators DataFrame
        """
        pass

    @abstractmethod
    def generate_signals(self, indicators: pl.DataFrame) -> pl.DataFrame:
        """
        Args:
            indicators: ohlcv with indicators DataFrame

        Returns:
            signal DataFrame with columns: [ticker, timestamps, signal]
        """
        pass

    @abstractmethod
    def trade_rules(self, signals: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Args:
            signals: Signal DataFrame

        Returns:
            tuple: (trades_df, portfolio_daily_df)
                - trades_df: 每笔交易记录
                - portfolio_daily_df: 每日组合表现
        """
        pass

    def run_backtest(self, use_cached_indicators: bool = False) -> Dict[str, Any]:
        """
        run backtest

        Args:
            use_cached_indicators: bool

        Returns:
            backtest results dict
        """
        if self.ohlcv_data is None:
            raise ValueError("no ohlcv data, please set_data() first.")

        # 1. calculate indicators
        print(f"calculating {self.name} indicators...")
        indicators = self.calculate_indicators(cached=use_cached_indicators)

        # 2. generate signals
        print(f"generate {self.name} signals...")
        signals = self.generate_signals(indicators)

        # 3. simulate trades
        print(f"simulating {self.name} trades...")
        trades, portfolio_daily, open_positions = self.trade_rules(signals)
        # self.vectorbt_trade(signals)

        start_date = portfolio_daily["date"].min()
        end_date = portfolio_daily["date"].max()
        # 4. adjust portfolio returns with risk-free rate
        if "add_risk_free_rate" in self.config:
            daily_irx = load_irx_data(
                start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )

            if daily_irx is not None:
                print(
                    f"IRX date range: {daily_irx['date'].min()} to {daily_irx['date'].max()}"
                )
                portfolio_daily = portfolio_daily.join(
                    daily_irx.select(["date", "irx_rate"]), on="date", how="left"
                )

                portfolio_daily = portfolio_daily.with_columns(
                    (pl.col("portfolio_return") - pl.col("irx_rate")).alias(
                        "portfolio_return"
                    )
                )
                print("Portfolio returns adjusted with risk-free rate (IRX).")
            else:
                print("No IRX data available for the given date range.")

        return {
            "strategy_name": self.name,
            "config": self.config,
            "indicators": indicators,
            "signals": signals,
            "trades": trades,
            "portfolio_daily": portfolio_daily,
            "open_positions": open_positions,
        }

    def get_strategy_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": self.config,
            "description": self.__doc__ or f"{self.name} Strategy",
        }
