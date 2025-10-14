import json
import os
import time

import dotenv
import polars as pl
from longport.openapi import Config, Market, QuoteContext, SecuritiesUpdateMode

dotenv.load_dotenv()

# temporarily disable proxy settings in environment variables to make longbridge work
old_http_proxy = os.environ.pop("http_proxy", None)
old_https_proxy = os.environ.pop("https_proxy", None)
old_all_proxy = os.environ.pop("all_proxy", None)


def delete_tickers_in_all(ctx, tickers_to_delete):
    """
    In Longbridge, the tickers added into the watchlist group you create will be also
    added into the 'all' watchlist automatically.
    But when you delete the watchlist group you created, the tickers in that group
    will NOT be deleted from the 'all' watchlist automatically.
    So you may need to delete the tickers from the 'all' watchlist again.

    But you can directly delete tickers from the 'all' watchlist.
    """
    watchlist = ctx.watchlist()
    for group in watchlist:
        if group.name == "all":
            ctx.update_watchlist_group(
                group.id, securities=tickers_to_delete, mode=SecuritiesUpdateMode.Remove
            )
    print("Watchlist previous tickers delete succeed")


def update_watchlist(watchlist_name, tickers):
    if len(watchlist_name) > 20:
        raise ValueError("Watchlist name should be 10 characters or less.")

    tickers = [f"{ticker}.US" for ticker in tickers]
    print(tickers)
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
            if group.name == watchlist_name:
                watchlist_id = group.id
                tickers_in_group = [s.symbol for s in group.securities]
                delete_tickers_in_all(ctx, tickers_in_group)
                ctx.delete_watchlist_group(group.id)
                time.sleep(1)
                watchlist_id = ctx.create_watchlist_group(
                    name=watchlist_name, securities=tickers
                )

                print(f"Watchlist create succeed: {watchlist_name}, ID: {watchlist_id}")
                updated = True

        if not updated:
            watchlist_id = ctx.create_watchlist_group(
                name=watchlist_name, securities=tickers
            )
            print(
                f"Watchlist first time created succeed: {watchlist_name}, ID: {watchlist_id}"
            )

        watchlist = ctx.watchlist()
        for group in watchlist:
            if group.id == watchlist_id:
                watchlist_symbols = [
                    s.symbol.replace(".US", "") for s in group.securities
                ]
                print(f"Watchlist '{group.name}' with {len(watchlist_symbols)} symbols")

        print("Watchlist update completed.")
    finally:
        # restore proxy settings
        if old_http_proxy:
            os.environ["http_proxy"] = old_http_proxy
        if old_https_proxy:
            os.environ["https_proxy"] = old_https_proxy
        if old_all_proxy:
            os.environ["all_proxy"] = old_all_proxy


if __name__ == "__main__":
    config = Config(
        app_key=os.getenv("LONGPORT_APP_KEY"),
        app_secret=os.getenv("LONGPORT_APP_SECRET"),
        access_token=os.getenv("LONGPORT_ACCESS_TOKEN"),
    )
    ctx = QuoteContext(config)
    delete_tickers_in_all(ctx, ["NVDA.US"])
