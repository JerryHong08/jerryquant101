# tests/data/providers/test_fundamental.py
import pytest

from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)


def test_debug_get_stock_detail():

    def debug_wrapper(ticker):

        provider = FloatSharesProvider()

        print(f"Testing with ticker: {ticker}")
        result = provider.fetch_from_web(ticker)
        print(f"Result type:\n {type(result)}")
        print(f"Result:\n{result}")
        return result

    # run
    try:
        result = debug_wrapper("BURU")
        print("Test completed successfully")
        print(f"Final result:\n {result}")
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
