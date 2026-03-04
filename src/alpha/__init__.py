"""
Alpha Research Module — Factor construction, evaluation, and combination.

Sub-modules:
    forward_returns: Compute N-day forward returns for the universe
    factor_analyzer: IC, IR, IC decay, turnover analysis
    preprocessing: Winsorize, z-score, rank-normalize, sector neutralize
    combination: Factor combination methods (equal-weight, IC-weight, MV)

Convention:
    - Factor DataFrame: (date, ticker, value) — long format, one row per stock-date
    - Returns DataFrame: (date, ticker, forward_return_1d, ..., forward_return_20d)
"""

from alpha.combination import combine_factors
from alpha.factor_analyzer import FactorAnalyzer
from alpha.forward_returns import compute_forward_returns
from alpha.preprocessing import preprocess_factor
from alpha.factor_backtest import run_factor_portfolio_backtest

__all__ = [
    "compute_forward_returns",
    "FactorAnalyzer",
    "preprocess_factor",
    "combine_factors",
    "run_factor_portfolio_backtest",
]

