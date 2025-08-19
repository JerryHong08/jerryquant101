import os

import dotenv
from polygon import RESTClient

dotenv.load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key)

quote = client.get_last_quote(
    "SORA",
)

print(quote)
