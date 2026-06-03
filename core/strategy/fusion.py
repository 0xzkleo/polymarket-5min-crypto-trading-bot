"""core.strategy.fusion — Multi-signal weighted fusion engine."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from loguru import logger

from core.strategy.processors.base import SignalDirection, SignalStrength, TradingSignal


@dataclass
class FusedSignal:
    timestamp: datetime
    direction: SignalDirection
    confidence: float
    score: float
    signals: List[TradingSignal]
    weights: Dict[str, float]
    metadata: Dict[str, Any]

    @property
    def num_signals(self) -> int:
        return len(self.signals)

    @property
    def is_strong(self) -> bool:
        return self.score >= 70.0

    @property
    def is_actionable(self) -> bool:
        return self.score >= 60.0 and self.confidence >= 0.6


class SignalFusionEngine:
    """Weighted voting across multiple signal processors."""

    def __init__(self):
        self.weights: Dict[str, float] = {
            "OrderBookImbalance": 0.30,
            "TickVelocity":       0.25,
            "PriceDivergence":    0.18,
            "SpikeDetection":     0.12,
            "DeribitPCR":         0.10,
            "SentimentAnalysis":  0.05,
            # Not in active fusion set — zero unless re-weighted
            "OHLCVMomentum":      0.0,
            "CVDOrderBook":       0.0,
            "Liquidations":       0.0,
            "FundingRateOI":      0.0,
            "default":            0.0,
        }
        self._signal_history: List[FusedSignal] = []
        self._max_history = 100
        self._fusions_performed = 0
        logger.info("Initialized Signal Fusion Engine")
        logger.info(f"Default weights: {self.weights}")

    def set_weight(self, processor_name: str, weight: float) -> None:
        if not 0.0 <= weight <= 1.0:
            raise ValueError("Weight must be between 0.0 and 1.0")
        self.weights[processor_name] = weight
        logger.info(f"Set weight for {processor_name}: {weight:.2f}")

    def fuse_signals(
        self,
        signals: List[TradingSignal],
        min_signals: int = 1,
        min_score: float = 50.0,
    ) -> Optional[FusedSignal]:
        if not signals or len(signals) < min_signals:
            return None

        current_time = datetime.now()
        recent_signals = [
            s for s in signals
            if (current_time - s.timestamp) < timedelta(minutes=5)
        ]
        if len(recent_signals) < min_signals:
            return None

        bullish_contrib = bearish_contrib = 0.0

        for signal in recent_signals:
            weight = self.weights.get(signal.source, self.weights["default"])
            strength_factor = (signal.strength.value if signal.strength else 2) / 4.0
            conf = min(1.0, max(0.0, signal.confidence))
            contribution = weight * conf * strength_factor

            direction_str = str(signal.direction).upper()
            if "BULLISH" in direction_str:
                bullish_contrib += contribution
            elif "BEARISH" in direction_str:
                bearish_contrib += contribution

        total_contrib = bullish_contrib + bearish_contrib
        if total_contrib < 0.0001:
            return None

        if bullish_contrib >= bearish_contrib:
            direction = SignalDirection.BULLISH
            dominant = bullish_contrib
        else:
            direction = SignalDirection.BEARISH
            dominant = bearish_contrib

        consensus_score = (dominant / total_contrib) * 100 if total_contrib > 0 else 0.0
        avg_conf = sum(s.confidence for s in recent_signals) / len(recent_signals)

        if consensus_score < min_score:
            return None

        fused = FusedSignal(
            timestamp=current_time,
            direction=direction,
            confidence=avg_conf,
            score=consensus_score,
            signals=recent_signals,
            weights=self.weights.copy(),
            metadata={
                "bullish_contrib": round(bullish_contrib, 4),
                "bearish_contrib": round(bearish_contrib, 4),
                "total_contrib": round(total_contrib, 4),
                "num_bullish": sum(
                    1 for s in recent_signals if "BULLISH" in str(s.direction).upper()
                ),
                "num_bearish": sum(
                    1 for s in recent_signals if "BEARISH" in str(s.direction).upper()
                ),
            },
        )

        self._fusions_performed += 1
        self._signal_history.append(fused)
        if len(self._signal_history) > self._max_history:
            self._signal_history.pop(0)

        logger.info(
            f"Fused {len(recent_signals)} signals → {direction.value} "
            f"(score={consensus_score:.1f}, conf={avg_conf:.1%})"
        )
        return fused

    def get_recent_fusions(self, limit: int = 10) -> List[FusedSignal]:
        return self._signal_history[-limit:]

    def get_statistics(self) -> Dict[str, Any]:
        recent = self._signal_history[-20:]
        return {
            "total_fusions": self._fusions_performed,
            "recent_fusions": len(recent),
            "avg_score": sum(f.score for f in recent) / len(recent) if recent else 0.0,
            "avg_confidence": sum(f.confidence for f in recent) / len(recent) if recent else 0.0,
            "weights": self.weights.copy(),
        }


_fusion_engine_instance: Optional[SignalFusionEngine] = None


def get_fusion_engine() -> SignalFusionEngine:
    global _fusion_engine_instance
    if _fusion_engine_instance is None:
        _fusion_engine_instance = SignalFusionEngine()
    return _fusion_engine_instance
