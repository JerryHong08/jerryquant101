# FibPatternFactor(factor_base.Factor):

#     input: snapshot_df, historical 4-hour bar data
#     compute:
#         1) high & low
#         2) fib retracement levels = high - fib_ratio*(high-low)
#         3) key threshold: 0.618 or 0.718 + (volume spike OR velocity spike)
