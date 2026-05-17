"""
bot — Polymarket BTC 15-min trading bot package.

Entry points
------------
- ``bot.runner.run_integrated_bot``  primary runtime function
- ``bot.strategy.IntegratedBTCStrategy``  Nautilus strategy class
- ``bot.models.PaperTrade``  simulation trade dataclass
"""
from bot.runner import run_integrated_bot

__all__ = ["run_integrated_bot"]
