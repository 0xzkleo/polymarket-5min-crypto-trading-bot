"""
core.strategy.processors.ohlcv_momentum
========================================
OHLCV momentum + volatility regime + time-of-day session filter.

Computes RSI, MACD, Bollinger Bands, and ATR from 100 Binance 1-min candles.
Classifies the volatility regime (LOW / NORMAL / HIGH) and adjusts confidence
according to the current UTC trading session.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx
from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)

BINANCE_REST = "https://api.binance.com"

DEAD_ZONE_HOURS = set(range(6, 10))    # 06:00–09:59 UTC
NY_OPEN_HOURS   = set(range(13, 16))   # 13:00–15:59 UTC
ASIA_OPEN_HOURS = {0, 1}               # 00:00–01:59 UTC


def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [d for d in deltas[-period:] if d > 0]
    losses = [-d for d in deltas[-period:] if d < 0]
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


def _macd(closes: List[float]) -> Tuple[float, float]:
    if len(closes) < 26:
        return 0.0, 0.0
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = [ema12[i] - ema26[i] for i in range(len(ema26))]
    signal_line = _ema(macd_line[-9:], 9)
    return macd_line[-1], signal_line[-1]


def _bollinger(closes: List[float], period: int = 20, std_dev: float = 2.0) -> float:
    if len(closes) < period:
        return 0.5
    window = closes[-period:]
    mid = sum(window) / period
    std = math.sqrt(sum((c - mid) ** 2 for c in window) / period)
    if std == 0:
        return 0.5
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    return (closes[-1] - lower) / (upper - lower) if (upper - lower) > 0 else 0.5


def _atr(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> float:
    if len(closes) < 2 or len(highs) < 2:
        return 0.0
    trs = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, min(period + 1, len(closes)))
    ]
    return sum(trs) / len(trs) if trs else 0.0


class OHLCVMomentumProcessor(BaseSignalProcessor):
    """
    RSI + MACD + Bollinger Bands + ATR volatility regime + session filter.
    """

    def __init__(
        self,
        rsi_overbought: float = 68.0,
        rsi_oversold: float = 32.0,
        bb_upper: float = 0.85,
        bb_lower: float = 0.15,
        cache_seconds: int = 120,
        min_confidence: float = 0.55,
    ):
        super().__init__("OHLCVMomentum")
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.bb_upper = bb_upper
        self.bb_lower = bb_lower
        self.cache_seconds = cache_seconds
        self.min_confidence = min_confidence
        self._cached_klines: Optional[Dict] = None
        self._cache_time: Optional[datetime] = None
        logger.info(
            f"Initialized OHLCV Momentum Processor: "
            f"RSI({rsi_oversold}/{rsi_overbought}), BB({bb_lower}/{bb_upper})"
        )

    def _fetch_klines(self) -> Optional[Dict]:
        now = datetime.now(timezone.utc)
        if (
            self._cached_klines is not None
            and self._cache_time is not None
            and (now - self._cache_time).total_seconds() < self.cache_seconds
        ):
            return self._cached_klines

        try:
            with httpx.Client(timeout=6.0) as client:
                resp = client.get(
                    f"{BINANCE_REST}/api/v3/klines",
                    params={"symbol": "BTCUSDT", "interval": "1m", "limit": 100},
                )
                resp.raise_for_status()
                raw = resp.json()

            highs  = [float(c[2]) for c in raw]
            lows   = [float(c[3]) for c in raw]
            closes = [float(c[4]) for c in raw]

            rsi = _rsi(closes)
            macd_line, macd_signal = _macd(closes)
            pct_b = _bollinger(closes)
            atr = _atr(highs, lows, closes)
            atr_pct = atr / closes[-1] if closes[-1] > 0 else 0.0

            vol_regime = (
                "LOW" if atr_pct < 0.002 else ("HIGH" if atr_pct > 0.005 else "NORMAL")
            )

            ret1  = (closes[-1] - closes[-2])  / closes[-2]  if len(closes) >= 2  else 0.0
            ret3  = (closes[-1] - closes[-4])  / closes[-4]  if len(closes) >= 4  else 0.0
            ret5  = (closes[-1] - closes[-6])  / closes[-6]  if len(closes) >= 6  else 0.0
            ret15 = (closes[-1] - closes[-16]) / closes[-16] if len(closes) >= 16 else 0.0

            result = {
                "rsi": rsi, "macd_line": macd_line, "macd_signal": macd_signal,
                "pct_b": pct_b, "atr": atr, "atr_pct": atr_pct,
                "vol_regime": vol_regime,
                "ret1": ret1, "ret3": ret3, "ret5": ret5, "ret15": ret15,
                "closes": closes,
            }
            self._cached_klines = result
            self._cache_time = now
            logger.info(
                f"OHLCV: RSI={rsi:.1f}, MACD={macd_line:+.1f}/{macd_signal:+.1f}, "
                f"%B={pct_b:.2f}, ATR={atr_pct:.4%} [{vol_regime}]"
            )
            return result

        except Exception as e:
            logger.warning(f"OHLCV klines fetch failed: {e}")
            return self._cached_klines

    def _session_multiplier(self) -> Tuple[float, str]:
        hour = datetime.now(timezone.utc).hour
        if hour in DEAD_ZONE_HOURS:
            return 0.80, "dead_zone"
        if hour in NY_OPEN_HOURS:
            return 1.10, "ny_open"
        if hour in ASIA_OPEN_HOURS:
            return 1.05, "asia_open"
        return 1.00, "normal"

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None

        data = self._fetch_klines()
        if data is None:
            return None

        rsi        = data["rsi"]
        macd_line  = data["macd_line"]
        macd_signal = data["macd_signal"]
        pct_b      = data["pct_b"]
        vol_regime = data["vol_regime"]
        ret15      = data["ret15"]

        if metadata is not None:
            metadata["vol_regime"] = vol_regime
            metadata["rsi"]  = rsi
            metadata["ret15"] = ret15

        session_mult, session_name = self._session_multiplier()

        bullish_votes = bearish_votes = 0

        if rsi < self.rsi_oversold:
            bullish_votes += 2
        elif rsi > self.rsi_overbought:
            bearish_votes += 2

        if macd_line > macd_signal and macd_line > 0:
            bullish_votes += 1
        elif macd_line < macd_signal and macd_line < 0:
            bearish_votes += 1

        if pct_b < self.bb_lower:
            bullish_votes += 1
        elif pct_b > self.bb_upper:
            bearish_votes += 1

        if ret15 > 0.003:
            bullish_votes += 1
        elif ret15 < -0.003:
            bearish_votes += 1

        total_votes = bullish_votes + bearish_votes
        if total_votes == 0:
            return None

        dominant = max(bullish_votes, bearish_votes)
        if dominant < 2:
            return None

        if bullish_votes >= bearish_votes:
            direction = SignalDirection.BULLISH
            vote_ratio = bullish_votes / total_votes
        else:
            direction = SignalDirection.BEARISH
            vote_ratio = bearish_votes / total_votes

        base_confidence = 0.55 + (vote_ratio - 0.5) * 0.50
        if vol_regime == "HIGH":
            base_confidence += 0.04
        elif vol_regime == "LOW":
            base_confidence -= 0.03

        confidence = min(0.85, base_confidence * session_mult)
        if confidence < self.min_confidence:
            return None

        strength = (
            SignalStrength.STRONG
            if dominant >= 4
            else (SignalStrength.MODERATE if dominant >= 3 else SignalStrength.WEAK)
        )

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.MOMENTUM,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={
                "rsi": round(rsi, 2),
                "macd_line": round(macd_line, 4),
                "macd_signal": round(macd_signal, 4),
                "pct_b": round(pct_b, 3),
                "vol_regime": vol_regime,
                "session": session_name,
                "bullish_votes": bullish_votes,
                "bearish_votes": bearish_votes,
                "ret15m": round(ret15, 5),
            },
        )
        self._record_signal(signal)
        logger.info(
            f"OHLCVMomentum {direction.value.upper()}: "
            f"votes={bullish_votes}B/{bearish_votes}Be, RSI={rsi:.1f}, "
            f"vol={vol_regime}, session={session_name}, conf={confidence:.2%}"
        )
        return signal
