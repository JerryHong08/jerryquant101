import os

import polars as pl

from core_2.config import all_tickers_dir, get_asset_overview_data


def get_mapped_tickers():
    # ======================
    # 1. Load the data
    # ======================
    df = get_asset_overview_data(asset="stocks")

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
            .otherwise(pl.col("share_class_figi"))
            .alias("share_class_figi"),
        )
        .group_by([pl.all().exclude("last_updated_utc")])
        .agg(pl.col("last_updated_utc").max())
        .sort("last_updated_utc")
    )

    # ======================
    # 3. create bipartite graph edges (ticker <-> figi)
    # ======================
    edges = (
        df.select(["ticker", "composite_figi", "share_class_figi"])
        .unpivot(index="ticker", value_name="figi")
        .select(["ticker", "figi"])
        .unique()
    )

    # ======================
    # 4. Find connected components and create mapping {ticker -> group_id} 迭代传播
    # ======================
    groups = df.select(["ticker"]).with_columns(
        pl.col("ticker").hash().alias("group_id")
    )

    changed = True
    while changed:
        # ticker -> figi
        t2f = edges.join(groups, on="ticker", how="left")
        # figi -> min(group_id)
        f2g = t2f.group_by("figi").agg(pl.col("group_id").min().alias("group_id"))
        # 回传 figi -> ticker
        new_groups = edges.join(f2g, on="figi", how="left").select(
            ["ticker", "group_id"]
        )

        # 取最小 group_id
        new_groups = new_groups.group_by("ticker").agg(pl.col("group_id").min())

        # 合并
        updated = groups.join(new_groups, on="ticker", how="left", suffix="_new")
        updated = updated.with_columns(
            pl.min_horizontal("group_id", "group_id_new").alias("group_id")
        ).select(["ticker", "group_id"])

        changed = not updated.equals(groups)
        groups = updated

    # ======================
    # 5. join back to original dataframe
    # ======================
    df = df.join(groups, on="ticker", how="left")

    with pl.Config(tbl_cols=50, tbl_width_chars=1000):
        print(
            df.select(
                pl.all().exclude(
                    [
                        "cik",
                        "currency_name",
                        "currency_name",
                        "base_currency_name",
                        "base_currency_symbol",
                        "currency_symbol",
                        "locale",
                    ]
                )
            ).filter(
                pl.col("group_id")
                == df.filter(pl.col("ticker") == "META").select("group_id").item()
            )
        )

    # ======================
    # 6. groupby aggregation
    # ======================
    result = (
        df.group_by("group_id")
        .agg(
            [
                pl.col("ticker").sort_by("last_updated_utc").alias("all_tickers_names"),
                pl.col("ticker")
                .sort_by("last_updated_utc")
                .last()
                .alias("lasted_name"),
                pl.col("last_updated_utc").sort().alias("all_last_updated_utc"),
                pl.col("delisted_utc").sort().alias("all_delisted_utc"),
            ]
        )
        .with_columns(pl.col("all_tickers_names").alias("ticker"))
        .explode(["ticker"])
        .rename(
            {
                "all_tickers_names": "tickers",
                "all_last_updated_utc": "last_updated_utc",
                "lasted_name": "latest_ticker",
            }
        )
    )

    return result


if __name__ == "__main__":
    get_mapped_tickers()

# result = result.with_columns(
#     pl.col("tickers").list.join(", "),
#     pl.col("last_updated_utc").list.join(", "),
#     pl.col("all_delisted_utc").list.join(", "),
# ).sort('group_id')

# result.write_csv("tickers_name_alignment.csv")

# print(result.head())
