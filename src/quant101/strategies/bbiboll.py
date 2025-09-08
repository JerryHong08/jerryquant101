import datetime
import glob
import json
import os
import random
import sys

import matplotlib.pyplot as plt
import polars as pl

from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import stock_load_process
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll
from quant101.utils.compute import calculate_signal_duration


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


def load_normalized_spx():
    spx = pl.read_parquet("I:SPXday20150101_20250905.parquet")
    spx = spx.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")  # 从毫秒时间戳转换
        .dt.convert_time_zone("America/New_York")
        .dt.replace(hour=0, minute=0, second=0)
        .cast(pl.Datetime("ns", "America/New_York"))  # 转换为纳秒精度以匹配portfolio
        .alias("date")
    )
    # 计算SPX基准收益（归一化到起始点）
    spx = spx.with_columns(
        (
            pl.col("close")
            / pl.col("close").first()
            * portfolio["equity_curve"].first()
        ).alias("spx_normalized")
    )

    return spx


class BBIBOLL_Strategy:
    def __init__(self, ohlcv_data_df, tickers):
        self.ohlcv_data_df = ohlcv_data_df
        self.pre_select_tickers = pre_select_tickers

    def load_bbiboll(self):
        bbiboll = pl.read_parquet("bbiboll.parquet")
        return bbiboll

    def indicators_bbiboll(self, cached=False):
        if cached and os.path.exists("bbiboll.parquet"):
            print("Loading cached BBIBOLL indicator from bbiboll.parquet")
            return self.load_bbiboll()
        else:
            # ticker 上市日 & 退市日
            self.pre_select_tickers = self.pre_select_tickers.with_columns(
                [
                    pl.col("delisted_utc")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    .alias("delisted_date")
                ]
            )

            # indicator
            bbiboll = calculate_bbiboll(self.ohlcv_data_df).with_columns(
                (pl.col("volume") * pl.col("close")).alias("turnover")
            )

            bbiboll = bbiboll.join(
                self.pre_select_tickers.select(["ticker", "delisted_date"]),
                on="ticker",
                how="left",
            )

            bbiboll.write_parquet("bbiboll.parquet")

            return bbiboll

    def compute_signals(self, indicators):

        # 随机选择10个ticker
        # available_tickers = indicators.select("ticker").unique().to_series().to_list()
        # examined_tickers = random.sample(available_tickers, min(10, len(available_tickers)))
        examined_tickers = ["FDUS"]
        print(f"selected tickers: {examined_tickers}")

        # with pl.Config(tbl_cols=20, tbl_rows=300):
        #     print(indicators.filter(
        #             (pl.col("ticker").is_in(examined_tickers))
        #             & (pl.col('timestamps').dt.date() >= datetime.date(2024,1,1))
        #         ).with_row_index()
        #     )

        # signals generate
        signals = (
            (
                indicators.filter(
                    pl.col("bbi").is_not_null()
                    & (pl.col("dev_pct") <= 1)
                    & (pl.col("timestamps").dt.date() >= datetime.date(2023, 2, 13))
                    & (
                        pl.col("delisted_date").is_null()
                        | (pl.col("timestamps").dt.date() <= pl.col("delisted_date"))
                    )
                    & (pl.col("ticker").is_in(examined_tickers))
                )
            )
            .select(["timestamps", "ticker"])
            .with_columns(pl.lit(1).alias("signal"))
        )

        return signals

    def trade_rules_porfolio(self, signal_table: pl.DataFrame):
        """
        1) 根据 signal_table 生成 trades（修正 block_id，计算 buy_row_id / sell_row_id）
        2) 将每笔 trade 展开为多天持仓记录（每个持仓日用 open -> next_open 计算当日收益）
        3) 按日期聚合所有活跃持仓的等权日收益，计算 equity_curve

        Inputs:
        - self.ohlcv_data_df: pl.DataFrame with columns ['ticker','timestamps','open','close', ...]
        - signal_table: pl.DataFrame with columns ['ticker','timestamps','signal']

        Returns:
        - trades: 每笔交易记录（含 buy/sell row ids 与总收益）
        - portfolio_daily: 每交易日组合日收益与累计权益曲线
        """

        # 1. 准备 signal -> last_signals (同前)
        signal_table = signal_table.sort(["ticker", "timestamps"])
        signal_table = signal_table.with_columns(
            (pl.col("timestamps").diff().dt.total_days().fill_null(999) > 1)
            .cum_sum()
            .over("ticker")
            .alias("block_id")
        )

        last_signals = signal_table.group_by(["ticker", "block_id"]).agg(
            pl.col("timestamps").max().alias("signal_date")
        )

        # 2. 准备 prices，带 row_id（每 ticker 的连续行号）
        prices = self.ohlcv_data_df.select(
            ["ticker", "timestamps", "open", "close"]
        ).sort(["ticker", "timestamps"])
        prices = prices.with_columns(
            pl.arange(0, pl.len()).over("ticker").alias("row_id")
        )

        # 3. 把 signal_date 对应到 prices 的 row_id
        last_signals = last_signals.join(
            prices.rename({"timestamps": "signal_date"}),
            on=["ticker", "signal_date"],
            how="left",
        )

        # 4. 计算买卖行 id：buy = row_id + 1, sell = row_id + 4
        last_signals = last_signals.with_columns(
            (pl.col("row_id") + 1).alias("buy_row_id"),
            (pl.col("row_id") + 4).alias("sell_row_id"),
        )

        # 5. 从 prices 取 buy/sell 信息（并保留 row_id 以便后续展开）
        buys = last_signals.join(
            prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
                {"row_id": "buy_row_id", "timestamps": "buy_date", "open": "buy_open"}
            ),
            on=["ticker", "buy_row_id"],
            how="left",
        )

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

        # 6. 合并 buy/sell，保留 block_id/buy_row_id/sell_row_id 用于匹配
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

        # 7. 剔除不完整/非法的 trades
        trades = trades.filter(
            pl.col("buy_row_id").is_not_null()
            & pl.col("sell_row_id").is_not_null()
            & (pl.col("buy_row_id") < pl.col("sell_row_id"))
            & pl.col("buy_open").is_not_null()
            & pl.col("sell_open").is_not_null()
        )

        # 8. 添加每笔交易的总体收益（开到开）
        trades = trades.with_columns(
            ((pl.col("sell_open") / pl.col("buy_open")) - 1).alias("return")
        )

        # ---------------------------
        # 9. 展开每笔交易到“每个持仓日”
        #   对于一笔交易，持仓日的 row_id 范围是 [buy_row_id, sell_row_id - 1]
        #   每个持仓日的日收益以 open(day+1) / open(day) - 1 计算
        # ---------------------------

        # 9.1 为展开方便，把 prices 的 row_id,timestamps,open 取出并重命名
        prices_short = prices.select(["ticker", "row_id", "timestamps", "open"]).rename(
            {"row_id": "day_row_id", "timestamps": "date", "open": "open_day"}
        )

        # 9.2 cross-join trades 与 prices_short （按 ticker），再筛选 day_row_id 在区间内
        # 注意：Polars 没有显式 cross join API；而 join(on='ticker', how='left') 后 filter 也可
        expanded = trades.join(prices_short, on="ticker", how="left").filter(
            (pl.col("day_row_id") >= pl.col("buy_row_id"))
            & (pl.col("day_row_id") < pl.col("sell_row_id"))
        )

        # 9.3 需要 day 的 next open（open_next），即 row_id = day_row_id + 1
        expanded = expanded.with_columns(
            (pl.col("day_row_id") + 1).alias("next_row_id")
        )

        prices_next = prices.select(["ticker", "row_id", "open"]).rename(
            {"row_id": "next_row_id", "open": "open_next"}
        )

        expanded = expanded.join(prices_next, on=["ticker", "next_row_id"], how="left")

        # 9.4 计算每个持仓日的 daily_return = open_next / open_day - 1
        # 可能出现 open_next 为 null（最后一行），但我们已在 trades 过滤上保证 sell_row 存在，因此 open_next 应存在；
        # 仍然要防御性过滤掉缺失值。
        expanded = expanded.with_columns(
            ((pl.col("open_next") / pl.col("open_day")) - 1).alias("daily_return")
        ).filter(pl.col("daily_return").is_not_null())

        # 10. 每日组合收益：按 date 聚合所有活跃持仓（等权平均）
        portfolio_daily = (
            expanded.group_by("date")
            .agg(
                pl.col("daily_return").mean().alias("portfolio_return"),
                pl.count().alias("n_positions"),
            )
            .sort("date")
        )

        # 11. 计算累计权益曲线（从 1 开始）
        portfolio_daily = portfolio_daily.with_columns(
            (1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve")
        )

        # 12. 清理输出 trades（保留索引信息以便审计）
        trades_out = trades.select(
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

        return trades_out, portfolio_daily


if __name__ == "__main__":
    # ========Config===========
    # tickers = ['NVDA','TSLA','FIG']
    pre_select_tickers = only_common_stocks()
    timeframe = "1d"
    start_date = "2022-01-01"
    end_date = "2025-09-05"

    # load data
    data_df = (
        stock_load_process(
            tickers=pre_select_tickers.to_series().to_list(),
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
        )
        .drop(["split_date", "window_start", "split_ratio"])
        .collect()
    )
    print(f"Memory size of lf_result: {data_df.estimated_size('mb'):.2f} MB")

    strategy = BBIBOLL_Strategy(data_df, pre_select_tickers)
    indicators = strategy.indicators_bbiboll(cached=True)
    signals = strategy.compute_signals(indicators)
    trades, portfolio = strategy.trade_rules_porfolio(signals)

    spx = load_normalized_spx()

    with pl.Config(tbl_cols=20, tbl_rows=500):
        print(f"siganls:{signals}")
        print(f"portfolio:{portfolio.head()}")
        print(f"trades:{trades.sort(['ticker','buy_date']).head(200)}")
        # print(f"spx:{spx.head()}")

    aligned_data = portfolio.join(
        spx.select(["date", "close", "spx_normalized"]), on="date", how="inner"
    )

    # print(trades)
    plt.plot(
        aligned_data["date"],
        aligned_data["equity_curve"],
        label="Strategy Equity Curve",
        linewidth=2,
    )
    plt.plot(
        aligned_data["date"],
        aligned_data["spx_normalized"],
        label="SPX",
        linewidth=2,
        alpha=0.7,
    )
    plt.title("Equity Curve")
    plt.show()
