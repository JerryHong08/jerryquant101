# tests/data/providers/test_fundamental.py
import asyncio

import pytest

from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)


def test_debug_get_stock_detail():
    async def debug_wrapper_web(ticker):
        provider = FloatSharesProvider()
        print(f"Testing web fetch with ticker: {ticker}")
        result = await provider.fetch_from_web(ticker)
        print(f"Web result type: {type(result)}")
        print(f"Web result: {result}")
        return result

    async def debug_wrapper_local(ticker):
        provider = FloatSharesProvider()
        print(f"Testing local fetch with ticker: {ticker}")
        result = await provider.fetch_from_local(ticker)
        print(f"Local result type: {type(result)}")
        print(f"Local result: {result}")
        return result

    try:
        print("=== Testing Local Fetch ===")
        local_result = asyncio.run(debug_wrapper_local("BURU"))

        print("\n=== Testing Web Fetch ===")
        web_result = asyncio.run(debug_wrapper_web("BURU"))

        print("Test completed successfully")
        print(f"Local result: {local_result}")
        print(f"Web result: {web_result}")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise
