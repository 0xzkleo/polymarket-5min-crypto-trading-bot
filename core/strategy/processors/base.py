"""core.strategy.processors.base — Abstract base classes and signal data types."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional


class SignalType(Enum):
    SPIKE_DETECTED = "spike_detected"
    MEAN_REVERSION = "mean_reversion"
    MOMENTUM = "momentum"
    SENTIMENT_SHIFT = "sentiment_shift"
    VOLUME_SURGE = "volume_surge"
    PRICE_DIVERGENCE = "price_divergence"
    ANOMALY = "anomaly"


class SignalStrength(Enum):
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    VERY_STRONG = 4


class SignalDirection(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass
class TradingSignal:
    """Trading signal produced by a signal processor."""

    timestamp: datetime
    source: str
    signal_type: SignalType
    direction: SignalDirection
    strength: SignalStrength
    confidence: float  # 0.0–1.0

    current_price: Decimal
    target_price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    @property
    def score(self) -> float:
        """Signal score 0–100 combining strength and confidence."""
        strength_weight = self.strength.value / 4.0
        return (strength_weight * 0.5 + self.confidence * 0.5) * 100


class BaseSignalProcessor(ABC):
    """Abstract base for all signal processors."""

    def __init__(self, name: str):
        self.name = name
        self._enabled = True
        self._signals_generated = 0
        self._last_signal: Optional[TradingSignal] = None

    @abstractmethod
    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        """Process market data and return a signal or *None*."""

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    @property
    def signals_generated(self) -> int:
        return self._signals_generated

    def _record_signal(self, signal: TradingSignal) -> None:
        self._signals_generated += 1
        self._last_signal = signal

    def get_stats(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "enabled": self._enabled,
            "signals_generated": self._signals_generated,
            "last_signal": (
                {
                    "timestamp": self._last_signal.timestamp.isoformat(),
                    "type": self._last_signal.signal_type.value,
                    "direction": self._last_signal.direction.value,
                    "score": self._last_signal.score,
                }
                if self._last_signal
                else None
            ),
        }
