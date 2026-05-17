"""core.strategy.processors.tick_velocity — Polymarket probability velocity processor."""
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)


class TickVelocityProcessor(BaseSignalProcessor):
    """
    Measures how fast the Polymarket UP probability is moving in the last 60 s.

    Fast moves in probability reflect real order flow — an actionable signal.
    """

    def __init__(
        self,
        velocity_threshold_60s: float = 0.015,
        velocity_threshold_30s: float = 0.010,
        min_ticks: int = 5,
        min_confidence: float = 0.55,
    ):
        super().__init__("TickVelocity")
        self.velocity_threshold_60s = velocity_threshold_60s
        self.velocity_threshold_30s = velocity_threshold_30s
        self.min_ticks = min_ticks
        self.min_confidence = min_confidence
        logger.info(
            f"Initialized Tick Velocity Processor: "
            f"60s={velocity_threshold_60s:.1%}, 30s={velocity_threshold_30s:.1%}"
        )

    def _get_price_at(
        self, tick_buffer: List[Dict], seconds_ago: float, now: datetime
    ) -> Optional[float]:
        target = now - timedelta(seconds=seconds_ago)
        best: Optional[float] = None
        best_diff = float("inf")
        for tick in tick_buffer:
            ts = tick["ts"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            diff = abs((ts - target).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best = float(tick["price"])
        return best if best_diff <= 15 else None

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled or not metadata:
            return None

        tick_buffer = metadata.get("tick_buffer")
        if not tick_buffer or len(tick_buffer) < self.min_ticks:
            return None

        now = datetime.now(timezone.utc)
        curr = float(current_price)

        price_60s = self._get_price_at(tick_buffer, 60, now)
        price_30s = self._get_price_at(tick_buffer, 30, now)

        if price_60s is None and price_30s is None:
            return None

        vel_60s = ((curr - price_60s) / price_60s) if price_60s else None
        vel_30s = ((curr - price_30s) / price_30s) if price_30s else None

        acceleration = 0.0
        if vel_60s is not None and vel_30s is not None:
            vel_first_30s = vel_60s - vel_30s
            acceleration = vel_30s - vel_first_30s

        primary_vel = vel_30s if vel_30s is not None else vel_60s
        threshold = (
            self.velocity_threshold_30s
            if vel_30s is not None
            else self.velocity_threshold_60s
        )

        if primary_vel is None or abs(primary_vel) < threshold:
            return None

        direction = SignalDirection.BULLISH if primary_vel > 0 else SignalDirection.BEARISH
        abs_vel = abs(primary_vel)

        if abs_vel >= 0.04:
            strength = SignalStrength.VERY_STRONG
        elif abs_vel >= 0.025:
            strength = SignalStrength.STRONG
        elif abs_vel >= 0.015:
            strength = SignalStrength.MODERATE
        else:
            strength = SignalStrength.WEAK

        confidence = min(0.82, 0.55 + (abs_vel / threshold - 1) * 0.12)

        accel_same_dir = (acceleration > 0 and primary_vel > 0) or (
            acceleration < 0 and primary_vel < 0
        )
        if accel_same_dir and abs(acceleration) > 0.005:
            confidence = min(0.88, confidence + 0.06)

        if vel_60s is not None and vel_30s is not None and (vel_60s > 0) != (vel_30s > 0):
            confidence *= 0.80

        if confidence < self.min_confidence:
            return None

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.MOMENTUM,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={
                "velocity_60s": round(vel_60s, 6) if vel_60s else None,
                "velocity_30s": round(vel_30s, 6) if vel_30s else None,
                "acceleration": round(acceleration, 6),
                "ticks_in_buffer": len(tick_buffer),
            },
        )
        self._record_signal(signal)
        logger.info(
            f"TickVelocity {direction.value.upper()}: "
            f"vel={primary_vel*100:+.3f}%, accel={acceleration*100:+.4f}%, "
            f"conf={confidence:.2%}"
        )
        return signal
