import polars as pl


# 计算每个ticker的signal持续天数
def calculate_signal_duration(signals_df):
    """
    计算每个ticker的signal持续交易日天数
    考虑周末跳过，阈值为2天不算断开
    """

    # 按ticker和时间排序
    signals_sorted = signals_df.sort(["ticker", "timestamps"])

    # 计算每个ticker的信号持续期
    signal_durations = (
        signals_sorted.with_columns(
            [
                # 计算与前一个信号的时间差（天数）
                (pl.col("timestamps").diff().dt.total_days().over("ticker")).alias(
                    "days_diff"
                ),
                # 创建组标识：如果时间差大于2天，则开始新组
                (
                    (pl.col("timestamps").diff().dt.total_days() > 2)
                    .fill_null(True)
                    .cum_sum()
                    .over("ticker")
                ).alias("signal_group"),
            ]
        )
        # 按ticker和信号组分组，计算每组的持续天数
        .group_by(["ticker", "signal_group"]).agg(
            [
                pl.col("timestamps").min().alias("start_date"),
                pl.col("timestamps").max().alias("end_date"),
                pl.len().alias("signal_count"),
                # 计算实际持续的交易日天数
                (pl.col("timestamps").max() - pl.col("timestamps").min())
                .dt.total_days()
                .alias("duration_days"),
            ]
        )
        # 处理只有1天信号的情况
        .with_columns(
            pl.when(pl.col("duration_days") == 0)
            .then(1)
            .otherwise(pl.col("duration_days") + 1)
            .alias("duration_days")
        )
    ).sort(["ticker", "start_date"])

    # 计算每个ticker的平均持续天数
    avg_durations = (
        signal_durations.group_by("ticker")
        .agg(
            [
                pl.col("duration_days").mean().alias("avg_duration_days"),
                pl.col("duration_days").count().alias("signal_periods_count"),
                pl.col("duration_days").sum().alias("total_signal_days"),
            ]
        )
        .sort("avg_duration_days", descending=True)
    )

    return signal_durations, avg_durations

    signal_durations, avg_durations = calculate_signal_duration(signals)

    with pl.Config(tbl_cols=20, tbl_rows=500):
        print("每个ticker的信号持续期详情:")
        print(signal_durations.head(20))
        print("\n每个ticker的平均持续天数:")
        print(avg_durations.head(20))

        # 也可以查看整体统计
        print(f"\n整体统计:")
        print(f"平均信号持续天数: {avg_durations['avg_duration_days'].mean():.2f} 天")
        print(
            f"中位数信号持续天数: {avg_durations['avg_duration_days'].median():.2f} 天"
        )
        print(f"最长平均持续天数: {avg_durations['avg_duration_days'].max():.2f} 天")
        print(f"最短平均持续天数: {avg_durations['avg_duration_days'].min():.2f} 天")


def prepare_trades_old(lf_result, signal_table):
    """
    根据信号生成交易，并计算收益和组合曲线
    - lf_result: 包含所有股票日线数据 [ticker, timestamps, open, close]
    - signal_table: 包含信号 [ticker, timestamps, signal]
    """

    # 先按 ticker+date 排序
    signal_table = signal_table.sort(["ticker", "timestamps"])

    # 标记连续段落 (信号连续时属于同一个 block)
    signal_table = signal_table.with_columns(
        (pl.col("timestamps").diff().dt.total_days().fill_null(999) > 1)
        .cum_sum()
        .over("ticker")
        .alias("block_id")
    )

    # 每个段落只保留最后一个信号
    last_signals = signal_table.group_by(["ticker", "block_id"]).agg(
        pl.col("timestamps").max().alias("signal_date")
    )

    # 股票价格数据，加上 row_id 方便 shift
    prices = lf_result.select(["ticker", "timestamps", "open", "close"]).sort(
        ["ticker", "timestamps"]
    )
    prices = prices.with_columns(
        pl.arange(0, pl.count()).over("ticker").alias("row_id")
    )

    # 将信号日期 merge 进价格表，得到 row_id
    last_signals = last_signals.join(
        prices.rename({"timestamps": "signal_date"}),
        on=["ticker", "signal_date"],
        how="left",
    )

    # 卖出日期 = 最后信号日 + 4 行
    last_signals = last_signals.with_columns(
        (pl.col("row_id") + 4).alias("sell_row_id")
    )

    sells = last_signals.join(
        prices.rename({"timestamps": "sell_date", "open": "sell_open"}).select(
            ["ticker", "row_id", "sell_date", "sell_open"]
        ),
        left_on=["ticker", "sell_row_id"],
        right_on=["ticker", "row_id"],
        how="left",
    )

    # 买入日期 = signal_date 的下一行 (+1)
    buys = last_signals.with_columns((pl.col("row_id") + 1).alias("buy_row_id"))
    buys = buys.join(
        prices.rename({"timestamps": "buy_date", "open": "buy_open"}).select(
            ["ticker", "row_id", "buy_date", "buy_open"]
        ),
        left_on=["ticker", "buy_row_id"],
        right_on=["ticker", "row_id"],
        how="left",
    )

    # 用 block_id 合并买卖，避免一买多卖
    trades = buys.join(
        sells.select(["ticker", "block_id", "sell_date", "sell_open"]),
        on=["ticker", "block_id"],
        how="left",
    ).select(["ticker", "buy_date", "buy_open", "sell_date", "sell_open"])

    # 收益
    trades = trades.with_columns(
        ((pl.col("sell_open") / pl.col("buy_open")) - 1).alias("return")
    )

    # === 组合收益曲线 ===
    # 按 buy_date 聚合（等权平均当天所有股票的收益）
    portfolio = (
        trades.group_by("buy_date")
        .agg(pl.col("return").mean().alias("daily_return"))
        .sort("buy_date")
    )

    # 累计权益曲线
    portfolio = portfolio.with_columns(
        (1 + pl.col("daily_return")).cum_prod().alias("equity_curve")
    )

    return trades, portfolio


def prepare_trades(lf_result: pl.DataFrame, signal_table: pl.DataFrame):
    """
    1) 根据 signal_table 生成 trades（修正 block_id，计算 buy_row_id / sell_row_id）
    2) 将每笔 trade 展开为多天持仓记录（每个持仓日用 open -> next_open 计算当日收益）
    3) 按日期聚合所有活跃持仓的等权日收益，计算 equity_curve

    Inputs:
      - lf_result: pl.DataFrame with columns ['ticker','timestamps','open','close', ...]
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
    prices = lf_result.select(["ticker", "timestamps", "open", "close"]).sort(
        ["ticker", "timestamps"]
    )
    prices = prices.with_columns(
        pl.arange(0, pl.count()).over("ticker").alias("row_id")
    )

    # 3. 把 signal_date 对应到 prices 的 row_id
    last_signals = last_signals.join(
        prices.rename({"timestamps": "signal_date"}),
        on=["ticker", "signal_date"],
        how="left",
    )

    # 4. 计算买卖行 id：buy = row_id + 1, sell = row_id + 3
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
            {"row_id": "sell_row_id", "timestamps": "sell_date", "open": "sell_open"}
        ),
        on=["ticker", "sell_row_id"],
        how="left",
    )

    # 6. 合并 buy/sell，保留 block_id/buy_row_id/sell_row_id 用于匹配
    trades = buys.join(
        sells.select(["ticker", "block_id", "sell_row_id", "sell_date", "sell_open"]),
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
    expanded = expanded.with_columns((pl.col("day_row_id") + 1).alias("next_row_id"))

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
