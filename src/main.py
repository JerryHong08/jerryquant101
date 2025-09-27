import matplotlib.pyplot as plt
import polars as pl

from backtesting.backtest_pre_data import load_irx_data

# irx = load_irx_data("2021-01-05", "2025-09-27")
irx = pl.read_parquet("I:IRXday.parquet")
irx = irx.with_columns(
    pl.from_epoch(pl.col("timestamp"), time_unit="ms")
    .dt.replace_time_zone("America/New_York")
    .cast(pl.Datetime("ns", "America/New_York"))
    .alias("date")
)

# print(irx.sort("timestamp").tail())
print(irx.sort("date").tail())

# plt.figure(figsize=(12, 6))
# plt.plot(irx['date'], irx['irx_rate'])
# plt.title('IRX Close Price Over Time')
# plt.xlabel('Date')
# plt.ylabel('Close Price')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# 组合每日表现记录数量: 1183
# 计算性能指标...
# 2021-01-05 00:00:00-05:00 2025-09-19 00:00:00-04:00
# 1182
# IRX date range: 2021-01-05 00:00:00-05:00 to 2025-09-18 00:00:00-04:00
# Overlapping dates: 926
# Valid data points after alignment: 926
# 926 926
# Calculating risk metrics with IRX data...
# 926 926
