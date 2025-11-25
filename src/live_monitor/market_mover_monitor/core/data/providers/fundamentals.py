import asyncio
import os
from typing import List, Optional

import httpx
import polars as pl
import requests
from bs4 import BeautifulSoup

from cores.config import float_shares_dir
from live_monitor.market_mover_monitor.core.data.schema import (
    FloatShares,
    FloatSourceData,
)
from live_monitor.market_mover_monitor.core.data.transforms import (
    _parse_number,
    _parse_percent,
)


class FloatSharesProvider:

    URL = "https://knowthefloat.com/ticker/{ticker}"
    RETRIES = 2
    TIMEOUT = 3

    @classmethod
    async def fetch_from_web(cls, ticker: str) -> Optional[FloatShares]:
        url = cls.URL.format(ticker=ticker)
        client = httpx.AsyncClient(timeout=cls.TIMEOUT)

        for attempt in range(cls.RETRIES + 1):
            try:
                resp = await client.get(url, timeout=5)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt < cls.RETRIES:
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    return None

        soup = BeautifulSoup(resp.content, "html.parser")
        cards = soup.find_all("div", class_="col-lg-3 col-md-6 col-sm-12")

        results: List[FloatSourceData] = []
        for card in cards:
            img = card.find("img")
            source = img["alt"] if img and "alt" in img.attrs else None
            if not source:
                continue

            float_val = _parse_number(
                card.find("div", class_="float-section").find("p").get_text(strip=True)
                if card.find("div", class_="float-section")
                else None
            )
            short_val = _parse_percent(
                card.find("div", class_="short-percent-section")
                .find("p")
                .get_text(strip=True)
                if card.find("div", class_="short-percent-section")
                else None
            )
            out_val = _parse_number(
                card.find("div", class_="outstanding-shares-section")
                .find("p")
                .get_text(strip=True)
                if card.find("div", class_="outstanding-shares-section")
                else None
            )

            results.append(
                FloatSourceData(
                    source=source,
                    float_shares=float_val,
                    short_percent=short_val,
                    outstanding_shares=out_val,
                )
            )

        print(f"Debug web results: {results}")

        return FloatShares(
            ticker=ticker.upper(),
            data=results,
        )

    def fetch_from_local(cls, ticker: str) -> Optional[FloatShares]:
        float_shares_file = os.path.join(
            float_shares_dir,
            max(
                [
                    f
                    for f in os.listdir(float_shares_dir)
                    if f.startswith(f"float_shares_") and f.endswith(".parquet")
                ]
            ),
        )

        if not float_shares_file:
            return None

        try:
            float_val = (
                pl.read_parquet(float_shares_file)
                .filter(pl.col("symbol") == ticker)
                .select("floatShares")
                .unique()
                .item()
            )
        except Exception:
            return None

        return FloatShares(
            ticker=ticker.upper(),
            data=[
                FloatSourceData(
                    source="local_fmp",
                    float_shares=float_val,
                    short_percent=None,
                    outstanding_shares=None,
                )
            ],
        )
