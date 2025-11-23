# tests/api/test_data_manager_debug.py
from unittest.mock import MagicMock, patch

import pytest

from live_monitor.market_mover_monitor.core.api.data_manager import DataManager


def test_debug_get_stock_detail():
    dm = DataManager()

    original_method = dm.get_stock_detail

    def debug_wrapper(ticker):
        print(f"Testing with ticker: {ticker}")
        result = original_method(ticker)
        print(f"Result type: {type(result)}")
        print(f"Result: {result}")
        return result

    dm.get_stock_detail = debug_wrapper

    # run
    try:
        result = dm.get_stock_detail("AAPL")
        print("Test completed successfully")
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
