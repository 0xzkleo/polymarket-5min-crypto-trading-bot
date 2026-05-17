"""data_sources — External market data adapters."""
from data_sources.coinbase import CoinbaseDataSource
from data_sources.news_social import NewsSocialDataSource

__all__ = ["CoinbaseDataSource", "NewsSocialDataSource"]
