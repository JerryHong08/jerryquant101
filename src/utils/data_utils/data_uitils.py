import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import exchange_calendars as xcals
import polars as pl

from cores.config import get_asset_overview_data


def get_mapped_tickers():
    # ======================
    # 1. Load the data
    # ======================
    df = get_asset_overview_data(asset="stocks").lazy()

    # ======================
    # 2. fill null FIGI
    # ======================
    df = (
        df.with_columns(
            pl.when(pl.col("composite_figi").is_null())
            .then(pl.concat_str([pl.lit("NULL_"), pl.arange(0, pl.len())]))
            .otherwise(pl.col("composite_figi"))
            .alias("composite_figi"),
            pl.when(pl.col("share_class_figi").is_null())
            .then(pl.concat_str([pl.lit("NULL_"), pl.arange(0, pl.len())]))
            .alias("share_class_figi"),
            pl.col("type").fill_null("UNKNOWN_TYPE"),
        )
        .group_by([pl.all().exclude("last_updated_utc")])
        .agg(pl.col("last_updated_utc").max())
        .sort("last_updated_utc")
        .unique(subset=["ticker", "cik"], keep="last")
    )

    # ======================
    # 3. create bipartite graph edges (ticker <-> figi + type constrain)
    # ======================
    edges = (
        df.select(["ticker", "type", "composite_figi", "share_class_figi"])
        .unpivot(index=["ticker", "type"], value_name="figi")
        .filter(~pl.col("figi").str.starts_with("NULL_"))  # filter out null FIGI
        .select(["ticker", "type", "figi"])
        .unique()
    )

    # ======================
    # 4. Find connected components and create mapping {ticker -> group_id}
    # ======================
    groups = df.select(["ticker", "type"]).with_columns(
        (pl.col("ticker") + "_" + pl.col("type")).hash().alias("group_id")
    )

    changed = True
    while changed:
        # ticker -> figi
        t2f = edges.join(groups, on=["ticker", "type"], how="left")

        # figi -> min(group_id)
        f2g = t2f.group_by(["figi", "type"]).agg(
            pl.col("group_id").min().alias("group_id")
        )

        # figi -> ticker
        new_groups = (
            edges.join(f2g, on=["figi", "type"], how="left")
            .select(["ticker", "type", "group_id"])
            .group_by(["ticker", "type"])
            .agg(pl.col("group_id").min())
        )

        updated = groups.join(
            new_groups, on=["ticker", "type"], how="left", suffix="_new"
        )
        updated = updated.with_columns(
            pl.min_horizontal("group_id", "group_id_new").alias("group_id")
        ).select(["ticker", "type", "group_id"])

        updated_df = updated.collect()
        changed = not updated_df.equals(groups.collect())
        groups = updated_df.lazy()

    # ======================
    # 5. join back to original dataframe
    # ======================
    df = df.join(groups, on=["ticker", "type"], how="left")

    # ======================
    # 6. groupby aggregation
    # ======================
    result = (
        df.group_by("group_id")
        .agg(
            [
                pl.col("ticker").sort_by("last_updated_utc").alias("all_tickers_names"),
                pl.col("type").sort_by("last_updated_utc").alias("all_types"),
                pl.col("ticker")
                .sort_by("last_updated_utc")
                .last()
                .alias("latest_ticker"),
                pl.col("delisted_utc")
                .sort_by("last_updated_utc")
                .alias("all_delisted_utc"),
                pl.col("last_updated_utc").sort().alias("all_last_updated_utc"),
            ]
        )
        .with_columns(pl.col("all_tickers_names").alias("ticker"))
        .explode(["ticker"])
        .rename(
            {
                "all_tickers_names": "tickers",
            }
        )
    ).collect()

    return result


def resolve_date_range(
    start_date: str, timedelta: int = 0, calendar_name: str = "XNYS"
) -> Tuple[str, str]:
    """
    Return exact start and end date based on
    given start_date, end_date, timedelta, calendar
    """
    cal = xcals.get_calendar(calendar_name)
    snys_schedule = cal.schedule

    df_schedule = snys_schedule.reset_index()
    start = datetime.fromisoformat(start_date)

    try:
        date_column = df_schedule.columns[0]

        # find the closest date before or equal to given start_date
        mask = df_schedule[date_column].dt.date <= start.date()
        matching_indices = df_schedule.index[mask].tolist()

        if not matching_indices or len(matching_indices) == len(df_schedule):
            raise ValueError(f"start_date is out of range")
        else:
            start_idx = matching_indices[-1]

        start_date = str(df_schedule.iloc[start_idx][date_column].date())
        target_idx = start_idx + timedelta

        # make sure end_date not out of the range
        if target_idx < 0 or target_idx >= len(df_schedule):
            raise IndexError(
                f"Target index {target_idx} is out of range [0, {len(df_schedule)-1}]"
            )

        end_row = df_schedule.iloc[target_idx]

        if end_row[date_column].date() < start.date():
            end_date = start_date
            start_date = str(end_row[date_column].date())
        else:
            end_date = str(end_row[date_column].date())

        return start_date, end_date

    except Exception as e:
        print(f"Error: {e}")
        return start_date, start_date


if __name__ == "__main__":
    resolve_date_range(start_date="2025-10-17", timedelta=-1)
    # result = get_mapped_tickers()

    # print(result.with_columns(
    #       pl.col('tickers').list.len()
    #     ).sort('tickers', descending=True)
    # )

    # result = result.with_columns(
    #     pl.col("tickers").list.join(", "),
    #     pl.col("all_types").list.join(", "),
    #     pl.col("all_delisted_utc").list.join(", "),
    #     pl.col("all_last_updated_utc").list.join(", "),
    # ).sort("group_id")

    # result.write_csv("tickers_name_alignment.csv")
