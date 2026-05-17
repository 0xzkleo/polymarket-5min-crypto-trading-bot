"""core.strategy.processors.funding_oi — Binance funding rate & OI processor."""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import httpx
from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)

BINANCE_FUTURES_BASE = "https://fapi.binance.com"


class FundingRateOIProcessor(BaseSignalProcessor):
    """
    Generates signals from BTC perpetual funding rate and open-interest trends.

    HIGH positive funding (+0.05%) → longs crowded → contrarian BEARISH
    HIGH negative funding (-0.03%) → shorts crowded → contrarian BULLISH
    OI rising in the same direction → confirms + boosts confidence
    """

    def __init__(
        self,
        bullish_funding_threshold: float = -0.0003,
        bearish_funding_threshold: float = 0.0005,
        oi_change_threshold: float = 0.02,
        cache_seconds: int = 300,
        min_confidence: float = 0.55,
    ):
        super().__init__("FundingRateOI")
        self.bullish_funding_threshold = bullish_funding_threshold
        self.bearish_funding_threshold = bearish_funding_threshold
        self.oi_change_threshold = oi_change_threshold
        self.cache_seconds = cache_seconds
        self.min_confidence = min_confidence
        self._cached: Optional[Dict] = None
        self._cache_time: Optional[datetime] = None
        logger.info(
            f"Initialized Funding+OI Processor: "
            f"bullish<{bullish_funding_threshold:.4%}, "
            f"bearish>{bearish_funding_threshold:.4%}"
        )

    def _fetch_data(self) -> Optional[Dict]:
        try:
            with httpx.Client(timeout=6.0) as client:
                fr_resp = client.get(
                    f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
                    params={"symbol": "BTCUSDT", "limit": 3},
                )
                fr_resp.raise_for_status()
                fr_data = fr_resp.json()
                latest_funding = float(fr_data[-1]["fundingRate"]) if fr_data else 0.0

                oi_resp = client.get(
                    f"{BINANCE_FUTURES_BASE}/futures/data/openInterestHist",
                    params={"symbol": "BTCUSDT", "period": "15m", "limit": 5},
                )
                oi_resp.raise_for_status()
                oi_data = oi_resp.json()

                oi_change = 0.0
                if len(oi_data) >= 2:
                    oi_latest = float(oi_data[-1]["sumOpenInterestValue"])
                    oi_oldest = float(oi_data[0]["sumOpenInterestValue"])
                    if oi_oldest > 0:
                        oi_change = (oi_latest - oi_oldest) / oi_oldest

                return {
                    "funding_rate": latest_funding,
                    "oi_change": oi_change,
                }

        except Exception as e:
            logger.warning(f"FundingRateOI fetch failed: {e}")
            return None

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None

        now = datetime.now(timezone.utc)
        cache_valid = (
            self._cached is not None
            and self._cache_time is not None
            and (now - self._cache_time).total_seconds() < self.cache_seconds
        )
        if cache_valid:
            data = self._cached
        else:
            data = self._fetch_data()
            if data is None:
                return None
            self._cached = data
            self._cache_time = now

        funding = data["funding_rate"]
        oi_change = data["oi_change"]

        funding_direction: Optional[SignalDirection] = None
        funding_confidence = 0.0

        if funding >= self.bearish_funding_threshold:
            funding_direction = SignalDirection.BEARISH
            extremeness = (funding - self.bearish_funding_threshold) / self.bearish_funding_threshold
            funding_confidence = min(0.80, 0.57 + extremeness * 0.15)
        elif funding <= self.bullish_funding_threshold:
            funding_direction = SignalDirection.BULLISH
            extremeness = (
                (self.bullish_funding_threshold - funding) / abs(self.bullish_funding_threshold)
            )
            funding_confidence = min(0.80, 0.57 + extremeness * 0.15)

        oi_confirms = (
            (funding_direction == SignalDirection.BEARISH and oi_change > self.oi_change_threshold)
            or (
                funding_direction == SignalDirection.BULLISH
                and oi_change < -self.oi_change_threshold
            )
        )
        if oi_confirms:
            funding_confidence = min(0.88, funding_confidence + 0.08)

        if funding_direction is None or funding_confidence < self.min_confidence:
            return None

        if funding_confidence >= 0.80:
            strength = SignalStrength.STRONG
        elif funding_confidence >= 0.70:
            strength = SignalStrength.MODERATE
        else:
            strength = SignalStrength.WEAK

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.SENTIMENT_SHIFT,
            direction=funding_direction,
            strength=strength,
            confidence=funding_confidence,
            current_price=current_price,
            metadata={
                "funding_rate": round(funding, 6),
                "oi_change": round(oi_change, 5),
                "oi_confirms": oi_confirms,
            },
        )
        self._record_signal(signal)
        logger.info(
            f"FundingRateOI {funding_direction.value.upper()}: "
            f"funding={funding:.5%}, OI_change={oi_change:+.3%}, conf={funding_confidence:.2%}"
        )
        return signal
