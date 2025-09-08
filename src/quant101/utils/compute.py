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
