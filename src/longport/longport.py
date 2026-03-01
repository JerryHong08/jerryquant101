import logging
import os
import time
from datetime import date, datetime
from decimal import Decimal

import dotenv

from longport.openapi import (
    AdjustType,
    Config,
    Market,
    OrderSide,
    OrderStatus,
    OrderType,
    OutsideRTH,
    Period,
    QuoteContext,
    TimeInForceType,
    TradeContext,
    TradeSessions,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

dotenv.load_dotenv()


def run(account: str):
    try:
        config = Config(
            app_key=os.getenv(f"LONGBRIDGE_APP_KEY{account}"),
            app_secret=os.getenv(f"LONGBRIDGE_APP_SECRET{account}"),
            access_token=os.getenv(f"LONGBRIDGE_ACCESS_TOKEN{account}"),
            http_url="https://openapi.longportapp.com",
        )

        logger.info("trading loaded")

        Trade = TradeContext(config)
    except Exception as e:
        logger.error(f"Failed to create TradeContext: {e}")
        raise

    account_balance = Trade.account_balance()
    logger.info("Account Balance: %s\n", account_balance)

    holdings = Trade.stock_positions()
    logger.info("Holdings: %s\n", holdings)

    # trade2 = Trade.submit_order(
    #     "OPEN.US",
    #     OrderType.LO,
    #     OrderSide.Sell,
    #     Decimal(600),
    #     TimeInForceType.GoodTilCanceled,
    #     submitted_price=Decimal(2.47),
    #     outside_rth=OutsideRTH.AnyTime,
    #     # remark="Hello from Python SDK"
    # )
    # logger.info(trade2)

    resp = Trade.estimate_max_purchase_quantity(
        symbol="OPEN.US",
        order_type=OrderType.LO,
        price=Decimal(2.47),
        side=OrderSide.Buy,
    )
    logger.info("Estimate Max Purchase Quantity Response: %s", resp)
    time.sleep(2)

    orders = Trade.today_orders()
    logger.info("Orders: %s\n", orders)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Longbridge Test Script")
    parser.add_argument("--account", type=str, required=True, help="Account identifier")
    args = parser.parse_args()

    run(account=args.account)
