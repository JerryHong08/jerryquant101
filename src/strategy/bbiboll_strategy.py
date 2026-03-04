"""
BBIBOLL Strategy - Inherits from StrategyBase
"""

import datetime
import os
import random
from typing import Any, Dict, List

import polars as pl

from backtest.strategy_base import StrategyBase
from data.loader.data_loader import TimestampGenerator
from strategy.indicators.registry import get_indicator


class BBIBOLLStrategy(StrategyBase):
    """
    BBIBOLL Strategy - Quantitative strategy based on BBI and Bollinger Bands

    Strategy Logic:
    1. Calculate BBI (multi-period moving average) and Bollinger Band indicators
    2. Generate buy signal when deviation_pct <= max_dev_pct
    3. Hold position after buying (no re-entry) until sell conditions are met:
       - Loss exceeds loss_threshold AND buy condition no longer met
       - Profit exceeds profit_threshold
    """

    def __init__(self, config: Dict[str, Any] = None):
        default_config = {
            "result_customized_name": "",
            "boll_length": 11,
            "boll_multiple": 6,
            "max_dev_pct": 3,
            "loss_threshold": -0.3,
            "profit_threshold": 0.3,
            "selected_tickers": ["random"],
            # "min_turnover": 0,
            "random_count": None,
            "plot_all": False,
            "timeframe": "1d",
            "data_start_date": "2020-01-01",
            "trade_start_date": "2021-01-01",
            "end_date": "2025-09-19",
            "initial_capital": 10000.0,
            "add_risk_free_rate": True,
            "silent": False,
        }

        if config:
            default_config.update(config)

        super().__init__(name="bbiboll", config=default_config)

        self.timestamp_gen = TimestampGenerator()

    def calculate_indicators(self, cached: bool = False) -> pl.DataFrame:
        """
        calculate indicators

        Args:
            cached: if use cache

        Returns:
            DataFrame with strategies indicators
        """

        if cached:
            cached_indicators = self.load_cached_indicators()
            if cached_indicators is not None:
                return cached_indicators

        print("calculate indicators...")
        # compute bbiboll
        func = get_indicator("bbiboll")
        indicators = func(
            self.ohlcv_data,
            boll_length=self.config["boll_length"],
            boll_multiple=self.config["boll_multiple"],
        )

        print("✅ indicators calculation completed")

        # add turnover
        indicators = indicators.with_columns(
            (pl.col("volume") * pl.col("close")).alias("turnover")
        )

        self.save_indicators_cache(indicators)

        return indicators

    def generate_signals(self, indicators: pl.DataFrame) -> pl.DataFrame:
        """
        Generate trading signals based on BBIBOLL indicators

        Args:
            indicators: Technical indicators DataFrame

        Returns:
            Trading signals DataFrame with buy (1) and sell (-1) signals
        """
        # filter tickers
        examined_tickers = self._select_tickers(indicators)
        print(f"Selected tickers for trading: {len(examined_tickers)}")

        # reformat time date
        start_date = datetime.datetime.strptime(
            self.config["trade_start_date"], "%Y-%m-%d"
        ).date()

        # basic filter
        filtered_indicators = indicators.filter(
            (pl.col("ticker").is_in(examined_tickers))
            & (pl.col("bbi").is_not_null())
            & (pl.col("timestamps").dt.date() >= start_date)
        ).sort(["ticker", "timestamps"])
        # with pl.Config(tbl_rows=10, tbl_cols=20):
        #     print(f'debug indicator: {filtered_indicators.head()}')

        if filtered_indicators.is_empty():
            print("no indicators found.")
            return pl.DataFrame()

        # Condition 1: deviation_pct <= max_dev_pct
        condition_one = pl.col("dev_pct") <= self.config["max_dev_pct"]

        # Add buy condition flag to each row
        data_with_condition = filtered_indicators.with_columns(
            pl.when(condition_one)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("meets_buy_condition")
        )

        # Initialize signal list
        all_signals = []

        # Process by ticker group
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

            # Iterate each trading day
            for row in ticker_data.iter_rows(named=True):
                current_date = row["timestamps"]
                current_close = row["close"]
                current_open = row["open"]
                meets_condition = row["meets_buy_condition"]
                current_dev_pct = row["dev_pct"]

                if not is_holding and meets_condition:
                    # Generate buy signal
                    ticker_signals.append(
                        {"ticker": ticker, "timestamps": current_date, "signal": 1}
                    )
                    is_holding = True
                    buy_price = current_close
                    buy_date = current_date

                elif is_holding and buy_price is not None:
                    # Calculate return (based on buy day close price)
                    daily_return = (current_open / buy_price) - 1

                    # Sell condition check
                    sell_condition = (
                        # Condition 1: Loss exceeds threshold AND buy condition no longer met
                        (
                            daily_return < self.config["loss_threshold"]
                            and current_dev_pct > self.config["max_dev_pct"]
                        )
                        or
                        # Condition 2: Profit exceeds threshold
                        (daily_return > self.config["profit_threshold"])
                    )

                    if sell_condition:
                        # Generate sell signal
                        ticker_signals.append(
                            {"ticker": ticker, "timestamps": current_date, "signal": -1}
                        )
                        is_holding = False
                        buy_price = None
                        buy_date = None

            all_signals.extend(ticker_signals)

        if not all_signals:
            print("No trading signals generated")
            return pl.DataFrame()

        # reformat
        signals_df = (
            pl.DataFrame(all_signals)
            .with_columns(
                pl.col("timestamps").dt.cast_time_unit(
                    "ns"
                )  # Unify to nanosecond precision
            )
            .sort(["ticker", "timestamps"])
            .rename({"timestamps": "signal_date"})
        )

        # metrics
        buy_count = signals_df.filter(pl.col("signal") == 1).height
        sell_count = signals_df.filter(pl.col("signal") == -1).height

        print(f"Buy signals generated: {buy_count}")
        print(f"Sell signals generated: {sell_count}")
        print(f"Total signals: {len(signals_df)}")

        # shape: (5, 3)
        # ┌────────┬────────────────────────────────┬────────┐
        # │ ticker ┆ signal_date                    ┆ signal │
        # │ ---    ┆ ---                            ┆ ---    │
        # │ str    ┆ datetime[ns, America/New_York] ┆ i64    │
        # ╞════════╪════════════════════════════════╪════════╡
        # │ AA     ┆ 2025-05-05 00:00:00 EDT        ┆ 1      │
        # │ AA     ┆ 2025-05-13 00:00:00 EDT        ┆ -1     │
        # │ AA     ┆ 2025-06-02 00:00:00 EDT        ┆ 1      │
        # │ AA     ┆ 2025-06-11 00:00:00 EDT        ┆ -1     │
        # │ AA     ┆ 2025-06-25 00:00:00 EDT        ┆ 1      │
        # └────────┴────────────────────────────────┴────────┘

        return signals_df

    def trade_rules(self, signals: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """
        Execute trading rules based on signals

        Args:
            signals: Trading signals DataFrame with buy (1) and sell (-1) signals

        Returns:
            tuple: (trades_df, portfolio_daily_df, open_positions_df)
        """
        if signals.is_empty():
            print("No trading signals generated")
            empty_trades = pl.DataFrame(
                schema={
                    "ticker": pl.Utf8,
                    "buy_date": pl.Datetime,
                    "buy_price": pl.Float64,
                    "sell_date": pl.Datetime,
                    "sell_open": pl.Float64,
                    "return": pl.Float64,
                }
            )
            empty_portfolio = pl.DataFrame(
                schema={
                    "date": pl.Datetime,
                    "portfolio_return": pl.Float64,
                    "n_positions": pl.Int32,
                    "equity_curve": pl.Float64,
                }
            )
            empty_open_positions = pl.DataFrame(
                schema={
                    "ticker": pl.Utf8,
                    "buy_date": pl.Datetime,
                    "sell_signal_date": pl.Datetime,
                    "buy_price": pl.Float64,
                    "sell_open": pl.Float64,
                }
            )
            return empty_trades, empty_portfolio, empty_open_positions

        # Prepare price data
        prices = self.ohlcv_data.select(["ticker", "timestamps", "open", "close"]).sort(
            ["ticker", "timestamps"]
        )

        # Separate buy and sell signals
        buy_signals = (
            signals.filter(pl.col("signal") == 1)
            .select(["ticker", "signal_date"])
            .rename({"signal_date": "buy_signal_date"})
        )

        # print(buy_signals.filter(pl.col('buy_signal_date').dt.date() == pl.date(2025, 9, 26)))

        sell_signals = (
            signals.filter(pl.col("signal") == -1)
            .select(["ticker", "signal_date"])
            .rename({"signal_date": "sell_signal_date"})
        )

        # Match each buy signal with its corresponding sell signal
        # Use join_asof to find the first sell signal after each buy
        trades = buy_signals.join_asof(
            sell_signals.sort(["ticker", "sell_signal_date"]),
            left_on="buy_signal_date",
            right_on="sell_signal_date",
            by="ticker",
            strategy="forward",
        )

        # Keep open positions in-memory; avoid writing side-effect debug files.

        # Get buy price (close price on buy signal day)
        trades = trades.join(
            prices.select(["ticker", "timestamps", "close"]).rename(
                {"timestamps": "buy_signal_date", "close": "buy_price"}
            ),
            on=["ticker", "buy_signal_date"],
            how="left",
        )

        # Get sell price (open price on sell signal day)
        trades = trades.join(
            prices.select(["ticker", "timestamps", "open"]).rename(
                {"timestamps": "sell_signal_date", "open": "sell_open"}
            ),
            on=["ticker", "sell_signal_date"],
            how="left",
        )

        open_positions = trades.filter(pl.col("sell_signal_date").is_null())

        if not open_positions.is_empty():
            last_close = (
                prices.sort(["ticker", "timestamps"])
                .group_by("ticker")
                .agg(pl.col("close").last().alias("last_close"))
            )
            open_positions = open_positions.join(last_close, on="ticker", how="left")
            open_positions = open_positions.with_columns(
                ((pl.col("last_close") / pl.col("buy_price")) - 1).alias(
                    "unrealized_return"
                )
            )

        print(f"open positions count: {open_positions.height}")

        # Filter valid trades (must have both buy and sell prices)
        trades = trades.filter(
            pl.col("buy_price").is_not_null()
            & pl.col("sell_open").is_not_null()
            & pl.col("sell_signal_date").is_not_null()
            & (pl.col("buy_signal_date") < pl.col("sell_signal_date"))
        )

        # Calculate trade returns
        trades = trades.with_columns(
            ((pl.col("sell_open") / pl.col("buy_price")) - 1).alias("return")
        )

        # Rename columns for consistency
        trades = trades.rename(
            {"buy_signal_date": "buy_date", "sell_signal_date": "sell_date"}
        )
        open_positions = open_positions.rename({"buy_signal_date": "buy_date"})

        # Expand each trade to daily holding periods
        portfolio_daily = self._calculate_daily_portfolio(trades, prices)

        trades_output = trades.select(
            ["ticker", "buy_date", "buy_price", "sell_date", "sell_open", "return"]
        ).sort(["buy_date", "ticker"])

        print(f"Trade records generated: {len(trades_output)}")
        print(f"Daily portfolio records: {len(portfolio_daily)}")

        return trades_output, portfolio_daily, open_positions

    def _select_tickers(self, indicators: pl.DataFrame) -> List[str]:
        """Select tickers for trading"""
        selected_tickers = self.config["selected_tickers"]

        if isinstance(selected_tickers, list) and selected_tickers != ["random"]:
            return selected_tickers

        # Random ticker selection
        available_tickers = indicators.select("ticker").unique().to_series().to_list()
        if self.config["random_count"] == None:
            random_count = len(available_tickers)
        else:
            random_count = min(self.config["random_count"], len(available_tickers))
        print(
            f"Available tickers: {len(available_tickers)}, selected count: {random_count}"
        )

        if selected_tickers == ["random"] or selected_tickers == "random":
            selected = random.sample(available_tickers, random_count)
        else:
            selected = available_tickers[:random_count]

        return selected

    def _calculate_daily_portfolio(
        self, trades: pl.DataFrame, prices: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Expand trades into daily portfolio performance (pure Polars implementation)

        Args:
            trades: DataFrame with columns: ticker, buy_date, sell_date, buy_price, sell_open, return
            prices: DataFrame with columns: ticker, timestamps, open, close

        Returns:
            portfolio_daily: DataFrame with columns:
                - date: trading day
                - portfolio_return: daily portfolio return
                - n_positions: number of positions held
                - equity_curve: cumulative portfolio value
        """
        if trades.is_empty():
            return pl.DataFrame(
                schema={
                    "date": pl.Datetime,
                    "portfolio_return": pl.Float64,
                    "n_positions": pl.Int32,
                    "equity_curve": pl.Float64,
                }
            )

        # Assign trade_id for join
        trades = trades.with_row_index(name="trade_id")

        # Expand holding periods: keep only prices within each trade's date range
        expanded = (
            prices.join(trades, on="ticker", how="inner")
            .filter(
                (pl.col("timestamps") >= pl.col("buy_date"))
                & (pl.col("timestamps") <= pl.col("sell_date"))
            )
            .select(["trade_id", "ticker", "timestamps", "close"])
            .sort(["trade_id", "timestamps"])
        )

        if expanded.is_empty():
            return pl.DataFrame(
                schema={
                    "date": pl.Datetime,
                    "portfolio_return": pl.Float64,
                    "n_positions": pl.Int32,
                    "equity_curve": pl.Float64,
                }
            )

        # Calculate daily returns (shift within each trade_id group)
        expanded = expanded.with_columns(
            [
                (pl.col("close") / pl.col("close").shift(1) - 1)
                .over("trade_id")
                .alias("daily_return")
            ]
        )

        # Drop the first row of each trade (shift produces null; buy day has no return)
        expanded = expanded.filter(pl.col("daily_return").is_not_null())

        # Aggregate to portfolio level: equal-weight average across concurrent positions
        portfolio = (
            expanded.group_by("timestamps")
            .agg(
                [
                    pl.col("daily_return").mean().alias("portfolio_return"),
                    pl.col("ticker").n_unique().alias("n_positions"),
                ]
            )
            .rename({"timestamps": "date"})
            .sort("date")
        )

        # Calculate equity_curve
        portfolio = portfolio.with_columns(
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
        )

        generated_timestamp = (
            self.timestamp_gen.generate(
                self.config["trade_start_date"], self.config["end_date"], timeframe="1d"
            )
            .with_columns(pl.col("timestamps").dt.cast_time_unit("ns"))
            .rename({"timestamps": "date"})
        )

        complete_portfolio = generated_timestamp.join(portfolio, on="date", how="left")

        # Fill missing values
        complete_portfolio = complete_portfolio.with_columns(
            [
                # Fill null portfolio_return with 0
                pl.col("portfolio_return").fill_null(0.0),
                # Fill null n_positions with 0
                pl.col("n_positions").fill_null(0),
            ]
        )

        # Recalculate complete equity_curve to ensure continuity
        portfolio = complete_portfolio.with_columns(
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
        )

        return portfolio
