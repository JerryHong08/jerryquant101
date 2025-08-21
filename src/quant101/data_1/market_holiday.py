import os
from datetime import date

import dotenv
from longport.openapi import Config, Market, QuoteContext

dotenv.load_dotenv()

# 临时禁用代理以确保 LongPort API 正常工作
old_http_proxy = os.environ.pop("http_proxy", None)
old_https_proxy = os.environ.pop("https_proxy", None)
old_all_proxy = os.environ.pop("all_proxy", None)

try:
    # Load configuration
    config = Config(
        app_key=os.getenv("LONGPORT_APP_KEY"),
        app_secret=os.getenv("LONGPORT_APP_SECRET"),
        access_token=os.getenv("LONGPORT_ACCESS_TOKEN"),
    )

    # Create a context for quote APIs
    ctx = QuoteContext(config)

    resp = ctx.trading_days(Market.US, date(2025, 1, 1), date(2025, 2, 1))
    print(resp)

finally:
    # 恢复代理设置，以免影响其他需要代理的操作
    if old_http_proxy:
        os.environ["http_proxy"] = old_http_proxy
    if old_https_proxy:
        os.environ["https_proxy"] = old_https_proxy
    if old_all_proxy:
        os.environ["all_proxy"] = old_all_proxy
