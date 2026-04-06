"""
Funding Rate & Open Interest Signal Processor
==============================================
Fetches BTC perpetual futures funding rate and open interest from Binance
Futures REST API (free, no auth required).

WHY FUNDING RATE MATTERS:
  Perpetual futures traders pay each other a funding fee every 8 hours.
  When funding is highly positive, longs are crowded and paying shorts.
  This means:
    - HIGH POSITIVE funding (>0.05%)  → longs overcrowded → mean-reversion BEARISH
    - HIGH NEGATIVE funding (<-0.03%) → shorts overcrowded → mean-reversion BULLISH
    - Near zero                        → balanced → no strong signal

WHY OPEN INTEREST MATTERS:
  Rising OI with rising price = new money entering longs = trend confirmation BULLISH
  Rising OI with falling price = new money entering shorts = trend confirmation BEARISH
  Falling OI = de-leveraging = trend exhaustion / reversal signal

COMBINED SIGNAL:
  We combine both signals with OI rate-of-change to get stronger conviction.

APIS (free, no key needed):
  Funding rate: GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1
  OI history:   GET https://fapi.binance.com/futures/data/openInterestHist
                    ?symbol=BTCUSDT&period=15m&limit=5

CACHE: 5 minutes (funding only changes every 8h; OI every 15m is fine)
"""

import httpx
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from loguru import logger

from core.strategy_brain.signal_processors.base_processor import (
    BaseSignalProcessor,
    TradingSignal,
    SignalType,
    SignalDirection,
    SignalStrength,
)

BINANCE_FUTURES_BASE = "https://fapi.binance.com"


class FundingRateOIProcessor(BaseSignalProcessor):
    """
    Generates signals from BTC perp funding rate and open interest trends.
    """

    def __init__(
        self,
        bullish_funding_threshold: float = -0.0003,   # -0.03%: shorts overcrowded
        bearish_funding_threshold: float = 0.0005,    # +0.05%: longs overcrowded
        oi_change_threshold: float = 0.02,            # 2% OI change per 15-min bar
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
            f"Initialized Funding Rate+OI Processor: "
            f"bullish_funding<{bullish_funding_threshold:.4%}, "
            f"bearish_funding>{bearish_funding_threshold:.4%}"
        )

    def _get_client(self) -> httpx.Client:
        return httpx.Client(timeout=6.0)

    def _fetch_data(self) -> Optional[Dict]:
        """Fetch funding rate and OI from Binance Futures REST."""
        try:
            with self._get_client() as client:
                # 1. Current funding rate
                fr_resp = client.get(
                    f"{BINANCE_FUTURES_BASE}/fapi/v1/fundingRate",
                    params={"symbol": "BTCUSDT", "limit": 3},
                )
                fr_resp.raise_for_status()
                fr_data = fr_resp.json()

                latest_funding = float(fr_data[-1]["fundingRate"]) if fr_data else 0.0

                # 2. OI history (last 5 bars of 15m)
                oi_resp = client.get(
                    f"{BINANCE_FUTURES_BASE}/futures/data/openInterestHist",
                    params={"symbol": "BTCUSDT", "period": "15m", "limit": 5},
                )
                oi_resp.raise_for_status()
                oi_data = oi_resp.json()

                # Compute OI rate of change (latest vs 4 bars ago)
                oi_change = 0.0
                oi_latest = None
                oi_oldest = None
                if len(oi_data) >= 2:
                    oi_latest = float(oi_data[-1]["sumOpenInterestValue"])
                    oi_oldest = float(oi_data[0]["sumOpenInterestValue"])
                    if oi_oldest > 0:
                        oi_change = (oi_latest - oi_oldest) / oi_oldest

                result = {
                    "funding_rate": latest_funding,
                    "oi_latest_usd": oi_latest,
                    "oi_oldest_usd": oi_oldest,
                    "oi_change": oi_change,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }

                logger.info(
                    f"Binance Futures: funding={latest_funding:.5%}, "
                    f"OI_change={oi_change:+.3%} over last 4x15m"
                )
                return result

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

        # ── Funding rate signal ──────────────────────────────────────────────
        funding_direction = None
        funding_confidence = 0.0

        if funding >= self.bearish_funding_threshold:
            # Longs crowded → fade → BEARISH
            funding_direction = SignalDirection.BEARISH
            extremeness = (funding - self.bearish_funding_threshold) / self.bearish_funding_threshold
            funding_confidence = min(0.80, 0.57 + extremeness * 0.15)

        elif funding <= self.bullish_funding_threshold:
            # Shorts crowded → fade → BULLISH
            funding_direction = SignalDirection.BULLISH
            extremeness = (self.bullish_funding_threshold - funding) / abs(self.bullish_funding_threshold)
            funding_confidence = min(0.80, 0.57 + extremeness * 0.15)

        # ── OI confirmation ──────────────────────────────────────────────────
        # If OI is rising in the SAME direction as funding signal → stronger conviction
        # If OI is falling → de-leveraging → weaker conviction
        oi_confirms = False
        if funding_direction == SignalDirection.BEARISH and oi_change > self.oi_change_threshold:
            # New longs piling in (OI rising) while funding is already high → more overextended
            oi_confirms = True
        elif funding_direction == SignalDirection.BULLISH and oi_change < -self.oi_change_threshold:
            # New shorts piling in (OI rising on short side) while funding is negative → more overextended
            oi_confirms = True

        if oi_confirms:
            funding_confidence = min(0.88, funding_confidence + 0.08)
            logger.info(f"FundingRateOI: OI change confirms signal → confidence boosted")

        if funding_direction is None or funding_confidence < self.min_confidence:
            logger.debug(f"FundingRateOI: no signal (funding={funding:.5%})")
            return None

        # Strength
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
                "interpretation": (
                    "longs_crowded_fade_bearish" if funding_direction == SignalDirection.BEARISH
                    else "shorts_crowded_fade_bullish"
                ),
            },
        )

        self._record_signal(signal)
        logger.info(
            f"Generated {funding_direction.value.upper()} signal (FundingRateOI): "
            f"funding={funding:.5%}, OI_change={oi_change:+.3%}, "
            f"confidence={funding_confidence:.2%}"
        )
        return signal