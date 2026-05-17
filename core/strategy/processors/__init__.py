"""core.strategy.processors — Signal processor classes."""
from core.strategy.processors.base import (
    BaseSignalProcessor,
    TradingSignal,
    SignalType,
    SignalDirection,
    SignalStrength,
)

__all__ = [
    "BaseSignalProcessor",
    "TradingSignal",
    "SignalType",
    "SignalDirection",
    "SignalStrength",
]
