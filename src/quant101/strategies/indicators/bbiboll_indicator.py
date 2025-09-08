import numpy as np
import polars as pl
import talib


def calculate_bbiboll(df: pl.DataFrame, boll_length: int = 11, boll_multiple: int = 6):

    # Schema([
    # ('ticker', String),
    # ('timestamps', Datetime(time_unit='ns',
    # time_zone='America/New_York')),
    # ('volume', Int64),
    # ('open', Float64),
    # ('close', Float64),
    # ('high', Float64),
    # ('low', Float64),
    # ('window_start', Int64),
    # ('transactions', UInt32),
    # ('split_date', Date),
    # ('split_ratio', Float64)
    # ])

    """
    计算BBIBOLL指标

    Args:
        df: 股票数据DataFrame (Polars)
        boll_length: 布林带长度
        boll_multiple: 布林带倍数

    Returns:
        包含BBIBOLL指标的DataFrame，新增bbi、dev、upr、dwn四列
    """
    print("Calculating BBIBOLL...")

    def calculate_group_bbiboll(
        group_df: pl.DataFrame, pct_window: int = 252
    ) -> pl.DataFrame:
        # 获取收盘价数组
        close_prices = group_df.select("close").to_numpy().flatten()

        # 计算BBI (多周期移动平均)
        ma3 = talib.SMA(close_prices, timeperiod=3)
        ma6 = talib.SMA(close_prices, timeperiod=6)
        ma12 = talib.SMA(close_prices, timeperiod=12)
        ma24 = talib.SMA(close_prices, timeperiod=24)

        # BBI = (MA3 + MA6 + MA12 + MA24) / 4
        bbi = (ma3 + ma6 + ma12 + ma24) / 4

        # 计算DEV (BBI的标准差 * 倍数)
        dev = talib.STDDEV(bbi, timeperiod=boll_length) * boll_multiple

        # 计算上轨和下轨
        upr = bbi + dev
        dwn = bbi - dev

        # 将结果添加到DataFrame
        result = group_df.with_columns(
            [
                pl.Series("bbi", bbi),
                pl.Series("dev", dev),
                pl.Series("upr", upr),
                pl.Series("dwn", dwn),
            ]
        )

        # pct_window = 2
        min_periods = min(50, pct_window // 3)

        # 创建expanding window的排名
        dev_ranks = []
        rank_starts = []
        rank_ends = []
        dev_values = result.select("dev").to_numpy().flatten()

        for i in range(len(dev_values)):
            start_idx = max(0, i - pct_window + 1) if i >= min_periods else 0
            window_values = dev_values[start_idx : i + 1]

            if len(window_values) >= min_periods:
                # 计算当前值在窗口中的排名 (转换为百分位)
                rank = np.sum(window_values <= dev_values[i])
                dev_ranks.append(rank)
                rank_starts.append(start_idx)
                rank_ends.append(i)
            else:
                dev_ranks.append(np.nan)
                rank_starts.append(start_idx)
                rank_ends.append(i)

        result = result.with_columns(
            [
                pl.Series("dev_pct", dev_ranks),
                pl.Series("rank_start", rank_starts),
                pl.Series("rank_end", rank_ends),
            ]
        )

        result = result.with_columns(
            [
                pl.col("dev")
                .rank("ordinal")
                .over("ticker")
                .alias("longterm_dev_pct_rank")
            ]
        )

        return result

    # 按ticker分组计算
    result = (
        df.sort(["ticker", "timestamps"])
        .group_by("ticker")
        .map_groups(calculate_group_bbiboll)
    )

    print("✅ BBIBOLL indicator calculation completed")
    return result
