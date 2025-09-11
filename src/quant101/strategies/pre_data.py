import datetime
import os

import polars as pl

from quant101.core_2.config import all_tickers_dir


def load_spx_benchmark(start, end):
    """加载SPX基准数据"""
    try:
        spx = pl.read_parquet("I:SPXday20150101_20250905.parquet")
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


def pre_select_tickers():
    all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    tickers = all_tickers.filter((pl.col("type").is_in(["CS", "ADRC"])))

    return tickers


if __name__ == "__main__":
    selected_tickers = pre_select_tickers()
    with pl.Config(tbl_rows=10, tbl_cols=50):
        print(selected_tickers.shape)

        print(
            selected_tickers.filter(
                (~pl.col("composite_figi").is_unique())
                & (pl.col("composite_figi").is_not_null())
                # & ((pl.col('composite_figi').is_null()) | (pl.col('share_class_figi').is_null()) )
                # & (pl.col('cik') == '0001805521')
                # & (pl.col('ticker').is_in(['FFAI', 'FFIE']))
            ).sort("composite_figi")
        )
