"""core.strategy.processors.spike — Spike detection signal processor."""
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)


class SpikeDetectionProcessor(BaseSignalProcessor):
    """
    Detects price spikes in the Polymarket UP probability.

    Two detection modes:
    1. MA DEVIATION: price deviates >5% from 20-period MA → mean reversion
    2. VELOCITY SPIKE: price moves >3% in last 3 ticks → momentum continuation
    """

    def __init__(
        self,
        spike_threshold: float = 0.05,
        lookback_periods: int = 20,
        min_confidence: float = 0.55,
        velocity_threshold: float = 0.03,
    ):
        super().__init__("SpikeDetection")
        self.spike_threshold = spike_threshold
        self.lookback_periods = lookback_periods
        self.min_confidence = min_confidence
        self.velocity_threshold = velocity_threshold
        logger.info(
            f"Initialized Spike Detector: "
            f"deviation={spike_threshold:.1%}, velocity={velocity_threshold:.1%}, "
            f"lookback={lookback_periods}"
        )

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None
        if len(historical_prices) < self.lookback_periods:
            return None

        recent = historical_prices[-self.lookback_periods:]
        ma = sum(float(p) for p in recent) / len(recent)
        curr = float(current_price)
        deviation = (curr - ma) / ma if ma > 0 else 0.0
        deviation_abs = abs(deviation)

        velocity = 0.0
        if len(historical_prices) >= 3:
            prev3 = float(historical_prices[-3])
            velocity = (curr - prev3) / prev3 if prev3 > 0 else 0.0

        logger.debug(
            f"SpikeDetector: price={curr:.4f}, MA={ma:.4f}, "
            f"deviation={deviation:+.3%}, velocity={velocity:+.3%}"
        )

        # Signal 1: MA deviation spike → mean reversion
        if deviation_abs >= self.spike_threshold:
            direction = SignalDirection.BEARISH if deviation > 0 else SignalDirection.BULLISH
            target = Decimal(str(ma))
            if deviation_abs >= 0.12:
                strength = SignalStrength.VERY_STRONG
            elif deviation_abs >= 0.08:
                strength = SignalStrength.STRONG
            elif deviation_abs >= 0.05:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            confidence = min(0.90, 0.50 + (deviation_abs - self.spike_threshold) * 3.0)
            if confidence < self.min_confidence:
                return None

            stop_distance = abs(Decimal(str(curr)) - Decimal(str(ma))) * Decimal("1.5")
            stop_loss = (
                Decimal(str(curr)) + stop_distance
                if direction == SignalDirection.BEARISH
                else Decimal(str(curr)) - stop_distance
            )

            signal = TradingSignal(
                timestamp=datetime.now(),
                source=self.name,
                signal_type=SignalType.SPIKE_DETECTED,
                direction=direction,
                strength=strength,
                confidence=confidence,
                current_price=current_price,
                target_price=target,
                stop_loss=stop_loss,
                metadata={
                    "detection_mode": "ma_deviation",
                    "deviation_pct": deviation,
                    "moving_average": ma,
                    "velocity": velocity,
                },
            )
            self._record_signal(signal)
            logger.info(
                f"SPIKE MA deviation {direction.value.upper()}: "
                f"deviation={deviation:+.3%}, conf={confidence:.2%}"
            )
            return signal

        # Signal 2: velocity spike → short-term momentum continuation
        if abs(velocity) >= self.velocity_threshold and deviation_abs < self.spike_threshold * 0.6:
            direction = SignalDirection.BULLISH if velocity > 0 else SignalDirection.BEARISH
            vel_strength = abs(velocity) / self.velocity_threshold
            if vel_strength >= 3:
                strength, confidence = SignalStrength.MODERATE, 0.65
            elif vel_strength >= 2:
                strength, confidence = SignalStrength.WEAK, 0.60
            else:
                strength, confidence = SignalStrength.WEAK, 0.57

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
                    "detection_mode": "velocity",
                    "velocity_pct": velocity,
                    "moving_average": ma,
                    "deviation_pct": deviation,
                },
            )
            self._record_signal(signal)
            logger.info(
                f"SPIKE velocity {direction.value.upper()}: "
                f"velocity={velocity:+.3%}, conf={confidence:.2%}"
            )
            return signal

        return None
