import datetime
import os

import polars as pl

from cores.config import all_tickers_dir


def generate_backtest_date(
    start_date: str,
    reverse: bool,
    reverse_limit: str = None,
    period: str = "week",
    reverse_limit_count: int = 52,
):

    backtest_dates = []
    if not reverse:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        today = datetime.datetime.now()

        while current_date <= today:
            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date += datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 12:
                    current_date = current_date.replace(
                        year=current_date.year + 1, month=1
                    )
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            elif period == "day":
                current_date += datetime.timedelta(days=1)
    else:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        if reverse_limit:
            limit_date = datetime.datetime.strptime(reverse_limit, "%Y-%m-%d")
        else:
            limit_date = None

        count = 0
        while True:
            # 检查是否到达限制
            if reverse_limit and current_date < limit_date:
                break
            if not reverse_limit and count >= reverse_limit_count:
                break

            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date -= datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 1:
                    current_date = current_date.replace(
                        year=current_date.year - 1, month=12
                    )
                else:
                    current_date = current_date.replace(month=current_date.month - 1)
            elif period == "day":
                current_date -= datetime.timedelta(days=1)

            count += 1

    return backtest_dates


def load_irx_data(start, end):
    try:
        irx = pl.read_parquet("I:IRXday.parquet")
        irx = irx.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.replace_time_zone("America/New_York")
            .dt.replace(hour=0, minute=0, second=0)
            .cast(pl.Datetime("ns", "America/New_York"))
            .alias("date")
        )

        irx = irx.filter(
            (
                pl.col("date").dt.date()
                >= datetime.datetime.strptime(start, "%Y-%m-%d").date()
            )
            & (
                pl.col("date").dt.date()
                <= datetime.datetime.strptime(end, "%Y-%m-%d").date()
            )
        ).sort("date")

        irx = (
            irx.with_columns(
                (pl.col("close") / 25200).alias("irx_rate"),
            )
        ).select(["date", "irx_rate"])

        return irx
    except Exception as e:
        print(f"加载IRX数据失败: {e}")
        return None


def load_spx_benchmark(start, end):
    """加载SPX基准数据"""
    try:
        spx = pl.read_parquet("I:SPXday.parquet")
        spx = spx.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.convert_time_zone("America/New_York")
            .dt.replace(hour=0, minute=0, second=0)
            .cast(pl.Datetime("ns", "America/New_York"))
            .alias("date")
        )

        spx = spx.filter(
            (
                pl.col("date").dt.date()
                >= datetime.datetime.strptime(start, "%Y-%m-%d").date()
            )
            & (
                pl.col("date").dt.date()
                <= datetime.datetime.strptime(end, "%Y-%m-%d").date()
            )
        ).sort("date")

        # 计算基准收益曲线（归一化）
        spx = spx.with_columns(
            (pl.col("close") / pl.col("close").first()).alias("benchmark_return")
        ).select(["date", "close", "benchmark_return"])

        return spx

    except Exception as e:
        print(f"加载SPX基准数据失败: {e}")
        return None


def only_common_stocks(filter_date: str = "2015-01-01"):
    all_tickers_file = os.path.join(all_tickers_dir, f"all_stocks_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    tickers = all_tickers.filter(
        (pl.col("type").is_in(["CS", "ADRC"]))
        & (
            pl.col("delisted_utc").is_null()
            | (
                pl.col("delisted_utc")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                .dt.date()
                > datetime.datetime.strptime(filter_date, "%Y-%m-%d").date()
            )
        )
    ).select(pl.col(["ticker", "active", "composite_figi"]))

    # print(f"Using {all_tickers_file}, total {len(tickers)} active tickers")

    return tickers


if __name__ == "__main__":
    selected_tickers = only_common_stocks()
    with pl.Config(tbl_rows=10, tbl_cols=50):
        print(selected_tickers.shape)

        print(
            selected_tickers.filter(
                # (~pl.col("composite_figi").is_unique())
                # & (pl.col("composite_figi").is_not_null())
                # & ((pl.col('composite_figi').is_null()) | (pl.col('share_class_figi').is_null()) )
                # & (pl.col('composite_figi') == 'BBG000VLBCQ1')
                # & (pl.col('ticker').is_in(['META']))
                (pl.col("composite_figi") == "BBG00KLHTJY4")
                # (pl.col('ticker').is_in(['DVLT']))
            ).sort("composite_figi")
        )
