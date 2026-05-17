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
    entry_price: float      # Polymarket probability at entry
    exit_price: float       # Polymarket probability at exit
    pnl_usd: float
    pnl_pct: float
    outcome: str            # "WIN" | "LOSS" | "PENDING"

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
