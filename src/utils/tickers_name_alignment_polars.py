import os

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
    )

    # ======================
    # 3. create bipartite graph edges (ticker <-> figi + type 限制)
    # ======================
    edges = (
        df.select(["ticker", "type", "composite_figi", "share_class_figi"])
        .unpivot(index=["ticker", "type"], value_name="figi")
        .filter(~pl.col("figi").str.starts_with("NULL_"))  # 关键
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

        # figi -> min(group_id) （按 type 分开，不同 type 不混）
        f2g = t2f.group_by(["figi", "type"]).agg(
            pl.col("group_id").min().alias("group_id")
        )

        # 回传 figi -> ticker
        new_groups = (
            edges.join(f2g, on=["figi", "type"], how="left")
            .select(["ticker", "type", "group_id"])
            .group_by(["ticker", "type"])
            .agg(pl.col("group_id").min())
        )

        # 合并
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


if __name__ == "__main__":
    result = get_mapped_tickers()

    # print(result.with_columns(
    #       pl.col('tickers').list.len()
    #     ).sort('tickers', descending=True)
    # )

    result = result.with_columns(
        pl.col("tickers").list.join(", "),
        pl.col("all_types").list.join(", "),
        pl.col("all_delisted_utc").list.join(", "),
        pl.col("all_last_updated_utc").list.join(", "),
    ).sort("group_id")

    result.write_csv("tickers_name_alignment_polars.csv")
