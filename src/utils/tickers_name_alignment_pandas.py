import os

import networkx as nx
import pandas as pd
import polars as pl

from core_2.config import all_tickers_dir

# ======================
# 1. 加载数据
# ======================
all_tickers_file = os.path.join(all_tickers_dir, f"all_stocks_*.parquet")
df = pl.read_parquet(all_tickers_file).to_pandas()

# ======================
# 2. 预处理 null FIGI
# ======================
df["composite_figi"] = df["composite_figi"].astype(object)
df.loc[df["composite_figi"].isna(), "composite_figi"] = [
    f"NULL_{i}" for i in df.index[df["composite_figi"].isna()]
]

df["share_class_figi"] = df["share_class_figi"].astype(object)
df.loc[df["share_class_figi"].isna(), "share_class_figi"] = [
    f"NULL_{i}" for i in df.index[df["share_class_figi"].isna()]
]

# ======================
# 3. 构建 bipartite graph (ticker <-> figi)
# ======================
edges = pd.melt(
    df[["ticker", "composite_figi", "share_class_figi"]],
    id_vars="ticker",
    value_name="figi",
)[["ticker", "figi"]]

G = nx.Graph()
G.add_edges_from(edges.itertuples(index=False, name=None))

# ======================
# 4. 找连通分量并生成映射 {ticker -> group_id}
# ======================
mapping = {}
for i, comp in enumerate(nx.connected_components(G)):
    for node in comp:
        if node in df["ticker"].values:  # 只保留 ticker 节点
            mapping[node] = i

df["group_id"] = df["ticker"].map(mapping)


# ======================
# 5. groupby 聚合
# ======================
def agg_group(grp: pd.DataFrame):
    # 按 last_updated_utc 排序
    grp_sorted = grp.sort_values("last_updated_utc")
    return pd.Series(
        {
            "all_tickers_names": list(grp_sorted["ticker"]),
            "lasted_name": grp_sorted["ticker"].iloc[-1],
            "all_last_updated_utc": list(grp_sorted["last_updated_utc"]),
            "all_delisted_utc": list(grp_sorted["delisted_utc"]),
        }
    )


result = df.groupby("group_id").apply(agg_group).reset_index()

result.to_csv("tickers_name_alignment.csv", index=False)
print(result.head())
