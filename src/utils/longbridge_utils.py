import json
import os
import time

import dotenv
import polars as pl
from longport.openapi import Config, Market, QuoteContext, SecuritiesUpdateMode

dotenv.load_dotenv()

# 临时禁用代理以确保 LongPort API 正常工作
old_http_proxy = os.environ.pop("http_proxy", None)
old_https_proxy = os.environ.pop("https_proxy", None)
old_all_proxy = os.environ.pop("all_proxy", None)


def update_watchlist(wachlist_name, tickers):
    # max 314 valid tickers
    tickers = [f"{ticker}.US" for ticker in tickers]
    try:
        config = Config(
            app_key=os.getenv("LONGPORT_APP_KEY"),
            app_secret=os.getenv("LONGPORT_APP_SECRET"),
            access_token=os.getenv("LONGPORT_ACCESS_TOKEN"),
        )
        ctx = QuoteContext(config)
        watchlist = ctx.watchlist()

        updated = False
        for group in watchlist:
            if group.name == wachlist_name:
                watchlist_id = group.id
                ctx.delete_watchlist_group(watchlist_id)
                watchlist_id = ctx.create_watchlist_group(
                    name=wachlist_name, securities=tickers
                )
                print(
                    f"Watchlist delete&create succeed: {wachlist_name}, ID: {watchlist_id}"
                )
                updated = True
        if not updated:
            watchlist_id = ctx.create_watchlist_group(
                name=wachlist_name, securities=tickers
            )
            print(f"Watchlist created succeed: {wachlist_name}, ID: {watchlist_id}")

        watchlist = ctx.watchlist()
        for group in watchlist:
            if group.id == watchlist_id:
                watchlist_symbols = [
                    s.symbol.replace(".US", "") for s in group.securities
                ]
                print(f"Watchlist '{group.name}' with {len(watchlist_symbols)} symbols")

        print("Watchlist update completed.")
    finally:
        # 恢复代理设置，以免影响其他需要代理的操作
        if old_http_proxy:
            os.environ["http_proxy"] = old_http_proxy
        if old_https_proxy:
            os.environ["https_proxy"] = old_https_proxy
        if old_all_proxy:
            os.environ["all_proxy"] = old_all_proxy
