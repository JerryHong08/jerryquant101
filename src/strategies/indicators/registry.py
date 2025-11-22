# indicators/registry.py
from typing import Callable, Dict

import polars as pl

from .base import apply_grouped

_INDICATORS: Dict[str, Callable] = {}


def register(name: str, grouped: bool = True):
    """
    register for indicators
    grouped=True group by ticker
    """

    def decorator(func: Callable):
        def wrapper(df: pl.DataFrame, **params) -> pl.DataFrame:
            if grouped:
                return apply_grouped(df, func, **params)
            else:
                return func(df, **params)

        _INDICATORS[name] = wrapper
        return wrapper

    return decorator


def get_indicator(name: str) -> Callable:
    return _INDICATORS[name]


def list_indicators():
    return list(_INDICATORS.keys())
