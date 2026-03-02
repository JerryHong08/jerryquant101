"""
Risk & Portfolio Module — Risk metrics, distribution analysis, and position sizing.

Sub-modules:
    risk_metrics: VaR, CVaR, drawdown, skewness, kurtosis, tail ratio
    return_analysis: Normality tests, QQ-plot data, Gaussian comparison
    position_sizing: Equal-weight, inverse-vol, vol-target, signal-weighted

Convention:
    - Risk functions: operate on 1-D numpy arrays of returns
    - Position sizing: operate on Polars DataFrames with (date, ticker, value/weight)
    - VaR/CVaR returned as positive numbers (loss magnitude)
    - Weights normalized: sum(|weight|) = 1.0 per date

Reference: docs/quant_lab.tex — Part IV, Chapters 13–14
"""

from risk.position_sizing import (
    compute_realized_volatility,
    size_equal_weight,
    size_inverse_volatility,
    size_signal_weighted,
    size_volatility_target,
)
from risk.return_analysis import (
    distribution_summary,
    gaussian_comparison,
    normality_tests,
    qq_data,
    tail_analysis,
)
from risk.risk_metrics import (
    cvar_historical,
    cvar_parametric,
    drawdown_series,
    max_drawdown,
    return_kurtosis,
    return_skewness,
    risk_summary,
    tail_ratio,
    var_historical,
    var_parametric,
)

__all__ = [
    # risk_metrics
    "var_historical",
    "var_parametric",
    "cvar_historical",
    "cvar_parametric",
    "drawdown_series",
    "max_drawdown",
    "return_skewness",
    "return_kurtosis",
    "tail_ratio",
    "risk_summary",
    # return_analysis
    "normality_tests",
    "qq_data",
    "gaussian_comparison",
    "tail_analysis",
    "distribution_summary",
    # position_sizing
    "size_equal_weight",
    "size_inverse_volatility",
    "size_volatility_target",
    "size_signal_weighted",
    "compute_realized_volatility",
]
