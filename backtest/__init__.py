"""
backtest
========
Historical-replay backtester for the Polymarket BTC 15-minute UP/DOWN strategy.

Because historical Polymarket order-book/tick data is not publicly available,
this harness replays *real* historical BTC price data (Binance 1-minute klines)
and measures the strategy's **directional edge** — i.e. how often the signal
pipeline correctly predicts whether BTC is higher at the 15-minute settlement.
P&L is then modelled with the binary Polymarket payoff at a configurable entry
price (default 0.50 = a coin-flip market), plus an optional fee/spread cost.

The headline metric is *directional accuracy*: if it is reliably > 50% the
strategy has a real edge; the P&L figure scales that edge against an assumed
entry price.
"""

from backtest.engine import BacktestConfig, BacktestEngine, BacktestResult

__all__ = ["BacktestConfig", "BacktestEngine", "BacktestResult"]
