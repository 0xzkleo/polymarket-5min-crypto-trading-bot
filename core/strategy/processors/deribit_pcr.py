"""core.strategy.processors.deribit_pcr — Deribit put/call ratio signal processor."""
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

DERIBIT_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"


class DeribitPCRProcessor(BaseSignalProcessor):
    """
    Contrarian signals from the Deribit BTC options put/call ratio.

    PCR > 1.2  (more puts) → fear → contrarian BULLISH
    PCR < 0.7  (more calls) → greed → contrarian BEARISH
    """

    def __init__(
        self,
        bullish_pcr_threshold: float = 1.20,
        bearish_pcr_threshold: float = 0.70,
        max_days_to_expiry: int = 2,
        min_open_interest: float = 100.0,
        cache_seconds: int = 300,
        min_confidence: float = 0.55,
    ):
        super().__init__("DeribitPCR")
        self.bullish_pcr_threshold = bullish_pcr_threshold
        self.bearish_pcr_threshold = bearish_pcr_threshold
        self.max_days_to_expiry = max_days_to_expiry
        self.min_open_interest = min_open_interest
        self.cache_seconds = cache_seconds
        self.min_confidence = min_confidence
        self._cached_result: Optional[Dict] = None
        self._cache_time: Optional[datetime] = None
        logger.info(
            f"Initialized Deribit PCR Processor: "
            f"bullish>{bullish_pcr_threshold}, bearish<{bearish_pcr_threshold}, "
            f"max_dte={max_days_to_expiry}d"
        )

    def _parse_dte(self, instrument_name: str) -> Optional[int]:
        try:
            parts = instrument_name.split("-")
            if len(parts) < 3:
                return None
            expiry_dt = datetime.strptime(parts[1], "%d%b%y").replace(tzinfo=timezone.utc)
            return max(0, (expiry_dt - datetime.now(timezone.utc)).days)
        except Exception:
            return None

    def _fetch_pcr(self) -> Optional[Dict]:
        try:
            with httpx.Client(timeout=8.0) as client:
                resp = client.get(
                    DERIBIT_URL, params={"currency": "BTC", "kind": "option"}
                )
                resp.raise_for_status()
                summaries = resp.json().get("result", [])

            if not summaries:
                return None

            put_oi = call_oi = short_put_oi = short_call_oi = 0.0
            for item in summaries:
                name = item.get("instrument_name", "")
                oi = float(item.get("open_interest", 0))
                if oi < self.min_open_interest:
                    continue
                is_put, is_call = name.endswith("-P"), name.endswith("-C")
                if is_put:
                    put_oi += oi
                elif is_call:
                    call_oi += oi
                dte = self._parse_dte(name)
                if dte is not None and dte <= self.max_days_to_expiry:
                    if is_put:
                        short_put_oi += oi
                    elif is_call:
                        short_call_oi += oi

            overall_pcr = put_oi / call_oi if call_oi > 0 else 1.0
            short_pcr = (
                short_put_oi / short_call_oi if short_call_oi > 0 else overall_pcr
            )

            result = {
                "overall_pcr": round(overall_pcr, 4),
                "short_pcr": round(short_pcr, 4),
                "put_oi": round(put_oi, 2),
                "call_oi": round(call_oi, 2),
                "short_put_oi": round(short_put_oi, 2),
                "short_call_oi": round(short_call_oi, 2),
            }
            logger.info(
                f"Deribit: overall_PCR={overall_pcr:.3f}, short_PCR={short_pcr:.3f}"
            )
            return result

        except Exception as e:
            logger.warning(f"Deribit PCR fetch failed: {e}")
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
            self._cached_result is not None
            and self._cache_time is not None
            and (now - self._cache_time).total_seconds() < self.cache_seconds
        )

        if cache_valid:
            pcr_data = self._cached_result
        else:
            pcr_data = self._fetch_pcr()
            if pcr_data is None:
                return None
            self._cached_result = pcr_data
            self._cache_time = now

        return self._generate_signal(current_price, pcr_data)

    def _generate_signal(
        self, current_price: Decimal, pcr_data: Dict
    ) -> Optional[TradingSignal]:
        pcr = pcr_data.get("short_pcr") or pcr_data.get("overall_pcr", 1.0)

        if pcr >= self.bullish_pcr_threshold:
            direction = SignalDirection.BULLISH
            extremeness = (pcr - self.bullish_pcr_threshold) / self.bullish_pcr_threshold
            confidence = min(0.80, 0.57 + extremeness * 0.15)
            strength = (
                SignalStrength.VERY_STRONG
                if pcr >= 1.60
                else (SignalStrength.STRONG if pcr >= 1.40 else SignalStrength.MODERATE)
            )
            logger.info(f"DeribitPCR HIGH PCR={pcr:.3f} → contrarian BULLISH")

        elif pcr <= self.bearish_pcr_threshold:
            direction = SignalDirection.BEARISH
            extremeness = (self.bearish_pcr_threshold - pcr) / self.bearish_pcr_threshold
            confidence = min(0.80, 0.57 + extremeness * 0.15)
            strength = (
                SignalStrength.VERY_STRONG
                if pcr <= 0.45
                else (SignalStrength.STRONG if pcr <= 0.55 else SignalStrength.MODERATE)
            )
            logger.info(f"DeribitPCR LOW PCR={pcr:.3f} → contrarian BEARISH")

        else:
            return None

        if confidence < self.min_confidence:
            return None

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.SENTIMENT_SHIFT,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={"pcr": round(pcr, 4), **pcr_data},
        )
        self._record_signal(signal)
        logger.info(
            f"DeribitPCR {direction.value.upper()}: PCR={pcr:.3f}, conf={confidence:.2%}"
        )
        return signal
