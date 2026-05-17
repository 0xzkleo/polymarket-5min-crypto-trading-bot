"""
data_sources.coinbase
======================
Coinbase Advanced Trade public API adapter — BTC-USD spot price and market data.
No API key required for price and book data.
"""
import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger


class CoinbaseDataSource:
    """
    Coinbase public API data source for BTC-USD.

    Provides real-time price, order book, recent trades, 24h stats, and OHLCV
    candles using only the public (unauthenticated) Coinbase endpoints.
    """

    def __init__(
        self,
        base_url: str = "https://api.coinbase.com",
        product_id: str = "BTC-USD",
    ):
        self.base_url = base_url
        self.product_id = product_id
        self.session: Optional[httpx.AsyncClient] = None
        self._last_price: Optional[Decimal] = None
        self._last_update: Optional[datetime] = None
        logger.info(f"Initialized Coinbase data source for {product_id}")

    async def connect(self) -> bool:
        try:
            self.session = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=30.0,
                headers={"User-Agent": "PolymarketBot/1.0", "Accept": "application/json"},
            )
            resp = await self.session.get("/v2/time")
            resp.raise_for_status()
            logger.info("Connected to Coinbase API")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Coinbase: {e}")
            return False

    async def disconnect(self) -> None:
        if self.session:
            await self.session.aclose()

    async def get_current_price(self) -> Optional[Decimal]:
        try:
            resp = await self.session.get(f"/v2/prices/{self.product_id}/spot")
            resp.raise_for_status()
            price = Decimal(str(resp.json()["data"]["amount"]))
            self._last_price = price
            self._last_update = datetime.now()
            logger.debug(f"Coinbase BTC price: ${price:,.2f}")
            return price
        except Exception as e:
            logger.error(f"Error fetching Coinbase price: {e}")
            return None

    async def get_order_book(self, level: int = 2) -> Optional[Dict[str, Any]]:
        try:
            resp = await self.session.get(
                f"/products/{self.product_id}/book", params={"level": level}
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "timestamp": datetime.now(),
                "bids": [{"price": Decimal(b[0]), "size": Decimal(b[1])} for b in data.get("bids", [])],
                "asks": [{"price": Decimal(a[0]), "size": Decimal(a[1])} for a in data.get("asks", [])],
            }
        except Exception as e:
            logger.error(f"Error fetching Coinbase order book: {e}")
            return None

    async def get_24h_stats(self) -> Optional[Dict[str, Any]]:
        try:
            resp = await self.session.get(f"/products/{self.product_id}/stats")
            resp.raise_for_status()
            data = resp.json()
            return {
                "timestamp": datetime.now(),
                "open": Decimal(str(data["open"])),
                "high": Decimal(str(data["high"])),
                "low": Decimal(str(data["low"])),
                "volume": Decimal(str(data["volume"])),
                "last": Decimal(str(data["last"])),
            }
        except Exception as e:
            logger.error(f"Error fetching Coinbase 24h stats: {e}")
            return None

    async def get_recent_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        try:
            resp = await self.session.get(
                f"/products/{self.product_id}/trades", params={"limit": limit}
            )
            resp.raise_for_status()
            return [
                {
                    "timestamp": datetime.fromisoformat(t["time"].replace("Z", "+00:00")),
                    "trade_id": t["trade_id"],
                    "price": Decimal(str(t["price"])),
                    "size": Decimal(str(t["size"])),
                    "side": t["side"],
                }
                for t in resp.json()[:limit]
            ]
        except Exception as e:
            logger.error(f"Error fetching Coinbase trades: {e}")
            return []

    @property
    def last_price(self) -> Optional[Decimal]:
        return self._last_price

    async def health_check(self) -> bool:
        try:
            return await self.get_current_price() is not None
        except Exception:
            return False


_coinbase_instance: Optional[CoinbaseDataSource] = None


def get_coinbase_source() -> CoinbaseDataSource:
    global _coinbase_instance
    if _coinbase_instance is None:
        _coinbase_instance = CoinbaseDataSource()
    return _coinbase_instance
