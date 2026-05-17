"""core.strategy.processors.divergence — Price divergence signal processor."""
from datetime import datetime
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


class PriceDivergenceProcessor(BaseSignalProcessor):
    """
    Detects mispricings between Polymarket UP probability and BTC spot momentum.

    Two sub-signals:
    1. EXTREME PROBABILITY FADE — poly_prob >68% or <32% with no confirming momentum
    2. MOMENTUM MISPRICING — strong spot momentum but poly_prob still near 50/50
    """

    def __init__(
        self,
        divergence_threshold: float = 0.05,
        min_confidence: float = 0.55,
        momentum_threshold: float = 0.003,
        extreme_prob_threshold: float = 0.68,
        low_prob_threshold: float = 0.32,
    ):
        super().__init__("PriceDivergence")
        self.min_confidence = min_confidence
        self.momentum_threshold = momentum_threshold
        self.extreme_prob_threshold = extreme_prob_threshold
        self.low_prob_threshold = low_prob_threshold
        self._spot_history: List[float] = []
        self._max_spot_history = 10
        logger.info(
            f"Initialized Price Divergence Processor: "
            f"momentum={momentum_threshold:.1%}, "
            f"extreme_fade={extreme_prob_threshold:.0%}/{low_prob_threshold:.0%}"
        )

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled or not metadata:
            return None

        poly_prob = float(current_price)
        spot_price = metadata.get("spot_price")
        poly_momentum = float(metadata.get("momentum", 0.0))

        if spot_price is not None:
            self._spot_history.append(float(spot_price))
            if len(self._spot_history) > self._max_spot_history:
                self._spot_history.pop(0)

        spot_momentum = 0.0
        if spot_price is not None and len(self._spot_history) >= 3:
            oldest = self._spot_history[-min(3, len(self._spot_history))]
            spot_momentum = (float(spot_price) - oldest) / oldest
        elif spot_price is None:
            spot_momentum = poly_momentum

        logger.info(
            f"PriceDivergence: poly={poly_prob:.3f}, "
            f"spot_mom={spot_momentum:+.4f}, "
            f"spot={'${:,.2f}'.format(spot_price) if spot_price else 'N/A'}"
        )

        # Signal 1: extreme probability fade
        if poly_prob >= self.extreme_prob_threshold:
            if spot_momentum <= 0.001:
                extremeness = (poly_prob - self.extreme_prob_threshold) / (
                    1.0 - self.extreme_prob_threshold
                )
                confidence = min(0.80, self.min_confidence + extremeness * 0.25)
                strength = (
                    SignalStrength.STRONG if extremeness > 0.5 else SignalStrength.MODERATE
                )
                signal = TradingSignal(
                    timestamp=datetime.now(),
                    source=self.name,
                    signal_type=SignalType.PRICE_DIVERGENCE,
                    direction=SignalDirection.BEARISH,
                    strength=strength,
                    confidence=confidence,
                    current_price=current_price,
                    metadata={
                        "signal_type": "extreme_prob_fade_down",
                        "poly_prob": poly_prob,
                        "spot_momentum": spot_momentum,
                    },
                )
                self._record_signal(signal)
                logger.info(
                    f"BEARISH fade: poly Up too high ({poly_prob:.0%}), conf={confidence:.2%}"
                )
                return signal

        elif poly_prob <= self.low_prob_threshold:
            if spot_momentum >= -0.001:
                extremeness = (self.low_prob_threshold - poly_prob) / self.low_prob_threshold
                confidence = min(0.80, self.min_confidence + extremeness * 0.25)
                strength = (
                    SignalStrength.STRONG if extremeness > 0.5 else SignalStrength.MODERATE
                )
                signal = TradingSignal(
                    timestamp=datetime.now(),
                    source=self.name,
                    signal_type=SignalType.PRICE_DIVERGENCE,
                    direction=SignalDirection.BULLISH,
                    strength=strength,
                    confidence=confidence,
                    current_price=current_price,
                    metadata={
                        "signal_type": "extreme_prob_fade_up",
                        "poly_prob": poly_prob,
                        "spot_momentum": spot_momentum,
                    },
                )
                self._record_signal(signal)
                logger.info(
                    f"BULLISH fade: poly Down too high ({1-poly_prob:.0%}), conf={confidence:.2%}"
                )
                return signal

        # Signal 2: momentum mispricing
        if 0.35 <= poly_prob <= 0.65 and abs(spot_momentum) >= self.momentum_threshold:
            momentum_strength = abs(spot_momentum) / self.momentum_threshold
            confidence = min(0.78, 0.55 + min(momentum_strength - 1, 2) * 0.08)

            if momentum_strength >= 3:
                strength = SignalStrength.STRONG
            elif momentum_strength >= 2:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            if confidence < self.min_confidence:
                return None

            direction = (
                SignalDirection.BULLISH if spot_momentum > 0 else SignalDirection.BEARISH
            )
            signal = TradingSignal(
                timestamp=datetime.now(),
                source=self.name,
                signal_type=SignalType.PRICE_DIVERGENCE,
                direction=direction,
                strength=strength,
                confidence=confidence,
                current_price=current_price,
                metadata={
                    "signal_type": "momentum_mispricing",
                    "poly_prob": poly_prob,
                    "spot_momentum": spot_momentum,
                },
            )
            self._record_signal(signal)
            logger.info(
                f"{direction.value.upper()} momentum mispricing: "
                f"spot={spot_momentum:+.3%}, poly={poly_prob:.0%}, conf={confidence:.2%}"
            )
            return signal

        return None
