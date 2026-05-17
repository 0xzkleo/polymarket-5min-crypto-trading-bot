"""
data_sources.news_social
=========================
Fear & Greed Index and crypto news sentiment data source.

- Fear & Greed Index: https://api.alternative.me/fng/ (free, no API key)
- Crypto news: CryptoPanic (requires free API key for full access)
"""
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger


class NewsSocialDataSource:
    """
    Aggregated crypto sentiment data source.

    Primary signal: Fear & Greed Index (0=Extreme Fear, 100=Extreme Greed).
    Secondary signal: CryptoPanic news sentiment (requires free API key).
    """

    def __init__(self):
        self.session: Optional[httpx.AsyncClient] = None
        self.sentiment_api_url = "https://api.alternative.me/fng/"
        self.news_api_url = "https://cryptopanic.com/api/v1/posts/"
        self._last_sentiment: Optional[Dict[str, Any]] = None
        self._last_news: List[Dict[str, Any]] = []
        logger.info("Initialized News/Social data source")

    async def connect(self) -> bool:
        try:
            self.session = httpx.AsyncClient(
                timeout=30.0, headers={"User-Agent": "PolymarketBot/1.0"}
            )
            resp = await self.session.get(self.sentiment_api_url)
            resp.raise_for_status()
            logger.info("Connected to News/Social APIs")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to News APIs: {e}")
            return False

    async def disconnect(self) -> None:
        if self.session:
            await self.session.aclose()

    async def get_fear_greed_index(self) -> Optional[Dict[str, Any]]:
        try:
            resp = await self.session.get(self.sentiment_api_url)
            resp.raise_for_status()
            current = resp.json()["data"][0]
            sentiment = {
                "timestamp": datetime.fromtimestamp(int(current["timestamp"])),
                "value": int(current["value"]),
                "classification": current["value_classification"],
                "time_until_update": current.get("time_until_update"),
            }
            self._last_sentiment = sentiment
            logger.debug(
                f"Fear & Greed: {sentiment['value']} ({sentiment['classification']})"
            )
            return sentiment
        except Exception as e:
            logger.error(f"Error fetching Fear & Greed Index: {e}")
            return None

    async def get_crypto_news(
        self,
        filter_: str = "hot",
        currencies: str = "BTC",
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        try:
            params = {
                "auth_token": "YOUR_CRYPTOPANIC_API_KEY",
                "filter": filter_,
                "currencies": currencies,
                "public": "true",
            }
            resp = await self.session.get(self.news_api_url, params=params)
            if resp.status_code == 401:
                logger.warning("CryptoPanic API key not configured")
                return self._last_news
            resp.raise_for_status()
            news = [
                {
                    "timestamp": datetime.fromisoformat(
                        a["published_at"].replace("Z", "+00:00")
                    ),
                    "title": a["title"],
                    "url": a["url"],
                    "source": a["source"]["title"],
                    "votes": (
                        a.get("votes", {}).get("positive", 0)
                        - a.get("votes", {}).get("negative", 0)
                    ),
                    "sentiment": (
                        "positive"
                        if a.get("votes", {}).get("positive", 0)
                        > a.get("votes", {}).get("negative", 0)
                        else "negative"
                    ),
                }
                for a in resp.json().get("results", [])[:limit]
            ]
            self._last_news = news
            return news
        except Exception as e:
            logger.error(f"Error fetching crypto news: {e}")
            return self._last_news

    async def get_sentiment_score(self) -> Optional[float]:
        try:
            fg_data = await self.get_fear_greed_index()
            if not fg_data:
                return None
            fg_score = fg_data["value"]
            news = await self.get_crypto_news(limit=10)
            if news:
                positive_count = sum(1 for n in news if n.get("sentiment") == "positive")
                news_score = (positive_count / len(news)) * 100
                total_score = fg_score * 0.7 + news_score * 0.3
            else:
                total_score = float(fg_score)
            logger.info(f"Aggregate sentiment score: {total_score:.1f}")
            return total_score
        except Exception as e:
            logger.error(f"Error calculating sentiment score: {e}")
            return None

    @property
    def last_sentiment(self) -> Optional[Dict[str, Any]]:
        return self._last_sentiment

    async def health_check(self) -> bool:
        try:
            return await self.get_fear_greed_index() is not None
        except Exception:
            return False


_news_instance: Optional[NewsSocialDataSource] = None


def get_news_social_source() -> NewsSocialDataSource:
    global _news_instance
    if _news_instance is None:
        _news_instance = NewsSocialDataSource()
    return _news_instance
