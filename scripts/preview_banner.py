"""Quick visual preview of the order/trade banners."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from loguru import logger
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    format="<level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,
)

from bot.strategy import IntegratedBTCStrategy as S


class _Stub:
    _BANNER_WIDTH = S._BANNER_WIDTH
    _log_event_banner = S._log_event_banner


stub = _Stub()

print()
print("===  ORDER PLACED (warning level)  ===")
stub._log_event_banner(
    level="warning",
    tag="ORDER PLACED",
    title="BUY YES (UP)  $1.00 USD",
    lines=[
        ("Order ID",  "BTC-15M-0100-1779514750560"),
        ("Market",    "btc-updown-15m-1779514800"),
        ("Token",     "0xbde5fe9e...4ce2-5693958...3779.POLYMARKET"),
        ("", ""),
        ("Side",      "BUY  (YES (UP))"),
        ("Notional",  "$1.00 USD  (quote_quantity=True, p=6)"),
        ("TIF",       "IOC"),
        ("Ref price", "$0.7050"),
        ("Quote",     "bid=$0.7000  ask=$0.7100  mid=$0.7050"),
        ("Mkt close", "+4.2 min"),
        ("", ""),
        (
            "Signal",
            "BULLISH  score=68.5  conf=72.0%  n=4",
        ),
        (
            "Sources",
            "CVDOrderBook, OHLCVMomentum, OrderBookImbalance, TickVelocity",
        ),
        ("ML engine", "inactive"),
        ("Risk",      "SL=-18%  TP=+40%  exit_cutoff=30s"),
        ("", ""),
        (
            "Session",
            "trades=0  pnl=$+0.0000  open=0  pending=0",
        ),
    ],
)

print()
print("===  ORDER FILLED (success level)  ===")
stub._log_event_banner(
    level="success",
    tag="ORDER FILLED",
    title="BUY  qty=1.4184  @ $0.7050  = $1.0000",
    lines=[
        ("Order ID", "BTC-15M-0100-1779514750560"),
        ("Fill px",  "$0.7050"),
        ("Fill qty", "1.418440 tokens"),
        ("Notional", "$1.0000"),
        ("Slippage", "+0.0 bps  (ref=$0.7050)"),
        ("Latency",  "0.84s  (submit -> fill)"),
    ],
)

print()
print("===  TRADE CLOSED (WIN -> success level)  ===")
stub._log_event_banner(
    level="success",
    tag="TRADE CLOSED #1",
    title="WIN  (TAKE-PROFIT)  P&L $+0.4253  (+42.53%)",
    lines=[
        ("Trade ID",  "BTC-15M-0100-1779514750560"),
        ("Market",    "btc-updown-15m-1779514800"),
        ("Direction", "LONG"),
        ("Exit code", "TAKE-PROFIT"),
        ("", ""),
        ("Entry px",  "$0.7050"),
        ("Exit px",   "$1.0050"),
        ("Qty",       "1.418440 tokens"),
        ("Notional",  "$1.00"),
        ("Hold",      "8.4m"),
        ("", ""),
        ("P&L",       "$+0.4253  (+42.53%)"),
        (
            "Session",
            "1W/0L  (winrate 100.0%)  cum=$+0.4253  open=0",
        ),
        ("Capital",   "$1000.4253"),
    ],
)

print()
print("===  ORDER REJECTED (FAK -> error level)  ===")
stub._log_event_banner(
    level="error",
    tag="ORDER REJECTED",
    title="client_id=BTC-15M-0100-1779514750560  (FAK)",
    lines=[
        ("Order ID", "BTC-15M-0100-1779514750560"),
        ("Reason",   "no orders found at price 0.7050"),
        ("Action",   "no liquidity (FAK) - trade timer reset; will retry"),
    ],
)
