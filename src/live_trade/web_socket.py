import os
from typing import List

from dotenv import load_dotenv
from polygon import WebSocketClient
from polygon.websocket.models import Feed, Market, WebSocketMessage

load_dotenv()

polygon_api_key_live = os.getenv("POLYGON_API_KEY")

client = WebSocketClient(
    api_key=polygon_api_key_live, feed=Feed.RealTime, market=Market.Stocks
)

# aggregates (per second)
client.subscribe("A.TSLA")  # single ticker
# client.subscribe("A.*") # all tickers
# client.subscribe("A.AAPL") # single ticker
# client.subscribe("A.AAPL", "AM.MSFT") # multiple tickers


def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)


# print messages
client.run(handle_msg)
