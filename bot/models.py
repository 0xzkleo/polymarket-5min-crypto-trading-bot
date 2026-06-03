"""
bot.models — Shared dataclasses, constants, and helper functions used by the
trading strategy and runner.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


# ── Trading-window constants ──────────────────────────────────────────────────
QUOTE_STABILITY_REQUIRED: int = 3       # valid ticks before market is considered stable
QUOTE_MIN_SPREAD: float = 0.001         # bid AND ask must be at least this
MARKET_INTERVAL_SECONDS: int = 900      # 15-minute markets


@dataclass
class PaperTrade:
    """Simulation trade with full context for post-session analytics."""

    # Core trade fields
    trade_id: str
    timestamp: datetime
    direction: str          # "LONG" | "SHORT"
    size_usd: float
    entry_price: float      # held-token price at entry (0-1)
    exit_price: float       # held-token settlement price (0 or 1; entry while PENDING)
    pnl_usd: float
    pnl_pct: float
    outcome: str            # "PENDING" | "WIN" | "LOSS" | "BREAKEVEN" | "UNRESOLVED"

    # Signal context
    signal_score: float
    signal_confidence: float
    num_signals: int = 0
    ml_p_up: float = 0.0    # ML model p(UP) at time of trade
    ml_edge: float = 0.0    # abs(ml_p_up - poly_price)

    # Market context
    market_slug: str = ""
    btc_spot_price: float = 0.0
    vol_regime: str = ""
    funding_rate: float = 0.0

    # Settlement context (mirrors LiveTrade so analytics treat paper/live alike)
    filled_qty: float = 0.0         # tokens held = size_usd / entry_price
    close_reason: str = ""          # "" while open; EXIT_TP | EXIT_STOP | TIME-EXIT | SETTLEMENT | ...
    ml_trade_id: Optional[int] = None

    # Session tracking
    session_trade_num: int = 0

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction,
            "size_usd": self.size_usd,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "pnl_usd": round(self.pnl_usd, 6),
            "pnl_pct": round(self.pnl_pct * 100, 4),
            "outcome": self.outcome,
            "signal_score": self.signal_score,
            "signal_confidence": self.signal_confidence,
            "num_signals": self.num_signals,
            "ml_p_up": round(self.ml_p_up, 4),
            "ml_edge": round(self.ml_edge, 4),
            "market_slug": self.market_slug,
            "btc_spot_price": self.btc_spot_price,
            "vol_regime": self.vol_regime,
            "funding_rate": round(self.funding_rate, 6),
            "filled_qty": round(self.filled_qty, 6),
            "close_reason": self.close_reason,
            "ml_trade_id": self.ml_trade_id,
            "session_trade_num": self.session_trade_num,
        }


@dataclass
class LiveTrade:
    """Realised live trade — recorded once a live position is closed.

    A live trade is closed in one of three ways:
      * ``EXIT_STOP``    — manual SELL fired by stop-loss
      * ``EXIT_TP``      — manual SELL fired by take-profit
      * ``SETTLEMENT``   — Polymarket auto-resolved at the 15-min boundary
                          (token paid 1.0 to the winner / 0.0 to the loser)
    """

    trade_id: str           # entry client_order_id
    ml_trade_id: Optional[int]
    timestamp: datetime
    closed_at: datetime
    direction: str          # "LONG" | "SHORT"
    label: str              # "YES (UP)" | "NO (DOWN)"
    market_slug: str

    size_usd: float         # USD notional of the BUY entry
    filled_qty: float       # Polymarket tokens held
    entry_price: float      # held-token price at entry (0-1)
    exit_price: float       # held-token price at exit (0-1)
    pnl_usd: float          # qty * (exit - entry)
    pnl_pct: float          # (exit - entry) / entry

    outcome: str            # "WIN" | "LOSS" | "BREAKEVEN" | "UNRESOLVED"
    close_reason: str       # "EXIT_STOP" | "EXIT_TP" | "SETTLEMENT" | ...

    # Identifiers from the venue
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None

    # Session tracking
    session_trade_num: int = 0

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "ml_trade_id": self.ml_trade_id,
            "timestamp": self.timestamp.isoformat(),
            "closed_at": self.closed_at.isoformat(),
            "direction": self.direction,
            "label": self.label,
            "market_slug": self.market_slug,
            "size_usd": round(self.size_usd, 6),
            "filled_qty": round(self.filled_qty, 6),
            "entry_price": round(self.entry_price, 6),
            "exit_price": round(self.exit_price, 6),
            "pnl_usd": round(self.pnl_usd, 6),
            "pnl_pct": round(self.pnl_pct * 100, 4),
            "outcome": self.outcome,
            "close_reason": self.close_reason,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "session_trade_num": self.session_trade_num,
        }


def _make_stub_signal(direction: str, ml_p_up: Optional[float] = None):
    """
    Minimal signal stub for paper-trade logging when the ML model fires but no
    individual signal processor produced a fused result.  Avoids crashes in
    ``_record_paper_trade`` which expects ``.direction``, ``.score``,
    ``.confidence``, and ``.num_signals``.
    """
    from core.strategy.processors.base import SignalDirection
    from dataclasses import dataclass as _dc

    @_dc
    class _Stub:
        direction: object
        score: float
        confidence: float
        num_signals: int = 0

    d = SignalDirection.BULLISH if direction == "long" else SignalDirection.BEARISH
    conf = ml_p_up if ml_p_up is not None else 0.60
    return _Stub(direction=d, score=conf * 100, confidence=conf)
