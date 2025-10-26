"""
量化交易回测框架

提供标准化的策略开发、回测执行和结果分析功能。
"""

from .engine import BacktestEngine
from .performance_analyzer import PerformanceAnalyzer
from .strategy_base import StrategyBase
from .visualizer import BacktestVisualizer

__all__ = [
    "StrategyBase",
    "BacktestEngine",
    "PerformanceAnalyzer",
    "BacktestVisualizer",
]

__version__ = "1.0.0"
