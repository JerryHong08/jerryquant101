import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

# 连接数据库（内存模式）
con = duckdb.connect()

# 加载 DuckDB 的 S3 插件
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# 配置 S3 连接参数
con.execute("SET s3_region='us-east-1';")
con.execute("SET s3_endpoint='files.polygon.io';")
# -- 重点：Polygon flat files 的 endpoint
con.execute(f"SET s3_access_key_id='{ACCESS_KEY_ID}';")
con.execute(f"SET s3_secret_access_key='{SECRET_ACCESS_KEY}';")
con.execute("SET s3_url_style='path';")
# -- 避免走 virtual-host 风格 URL

# 直接在远程 S3 上跑 SQL
query = """
SELECT *
FROM read_csv_auto('/mnt/blackdisk/quant_data/polygon_data/raw/us_stocks_sip/day_aggs_v1/202[2-5]/*/*.csv.gz')
ORDER BY volume DESC
LIMIT 1;
"""
# FROM read_csv_auto('s3://flatfiles/us_stocks_sip/day_aggs_v1/202[2-5]/*/*.csv.gz')
# FROM read_parquet('/mnt/blackdisk/quant_data/polygon_data/lake/us_stocks_sip/day_aggs_v1/202[2-5]/*/*.parquet')

# WHERE ticker = 'TSLA'
# ORDER BY sip_timestamp DESC


result = con.execute(query).fetchdf()
print(result)
