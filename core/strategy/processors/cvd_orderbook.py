"""core.strategy.processors.cvd_orderbook — CVD + Binance spot order book processor."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
import websockets
from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)

BINANCE_SPOT_WS   = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
BINANCE_SPOT_REST = "https://api.binance.com"


class CVDTradeEvent:
    __slots__ = ("timestamp", "usd_value", "is_buy")

    def __init__(self, timestamp: datetime, usd_value: float, is_buy: bool):
        self.timestamp = timestamp
        self.usd_value = usd_value
        self.is_buy = is_buy


class CVDOrderBookProcessor(BaseSignalProcessor):
    """
    CVD (Cumulative Volume Delta) from Binance aggTrades + Binance spot order-book
    imbalance — two correlated signals emitted as one.
    """

    def __init__(
        self,
        cvd_window_seconds: int = 900,
        cvd_threshold_usd: float = 5_000_000,
        ob_imbalance_threshold: float = 0.30,
        ob_top_levels: int = 20,
        ob_cache_seconds: int = 10,
        min_confidence: float = 0.55,
    ):
        super().__init__("CVDOrderBook")
        self.cvd_window_seconds = cvd_window_seconds
        self.cvd_threshold_usd = cvd_threshold_usd
        self.ob_imbalance_threshold = ob_imbalance_threshold
        self.ob_top_levels = ob_top_levels
        self.ob_cache_seconds = ob_cache_seconds
        self.min_confidence = min_confidence
        self._trade_events: deque = deque(maxlen=100_000)
        self._stream_thread: Optional[threading.Thread] = None
        self._running = False
        self._ob_cache: Optional[Dict] = None
        self._ob_cache_time: Optional[datetime] = None
        logger.info(
            f"Initialized CVD+OrderBook Processor: "
            f"window={cvd_window_seconds}s, cvd_threshold=${cvd_threshold_usd/1e6:.0f}M"
        )

    def start_stream(self) -> None:
        if self._running:
            return
        self._running = True
        self._stream_thread = threading.Thread(
            target=self._run_stream_loop, daemon=True, name="CVDStream"
        )
        self._stream_thread.start()
        logger.info("CVD aggTrade stream thread started")

    def stop_stream(self) -> None:
        self._running = False

    def _run_stream_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self._running:
            try:
                loop.run_until_complete(self._stream())
            except Exception as e:
                logger.warning(f"CVD stream error: {e} — reconnecting in 5s")
                time.sleep(5)
        loop.close()

    async def _stream(self) -> None:
        async with websockets.connect(
            BINANCE_SPOT_WS, ping_interval=20, ping_timeout=10
        ) as ws:
            logger.info("Connected to Binance aggTrade stream")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    price = float(msg["p"])
                    qty = float(msg["q"])
                    ts_ms = int(msg["T"])
                    is_buy = not bool(msg["m"])
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    self._trade_events.append(CVDTradeEvent(ts, price * qty, is_buy))
                except Exception:
                    continue

    def _compute_cvd(self) -> Dict[str, float]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.cvd_window_seconds)
        buy_vol = sell_vol = 0.0
        for ev in self._trade_events:
            if ev.timestamp < cutoff:
                continue
            if ev.is_buy:
                buy_vol += ev.usd_value
            else:
                sell_vol += ev.usd_value
        return {
            "cvd_delta": buy_vol - sell_vol,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "total_vol": buy_vol + sell_vol,
        }

    def _fetch_order_book(self) -> Optional[Dict]:
        now = datetime.now(timezone.utc)
        if (
            self._ob_cache is not None
            and self._ob_cache_time is not None
            and (now - self._ob_cache_time).total_seconds() < self.ob_cache_seconds
        ):
            return self._ob_cache
        try:
            with httpx.Client(timeout=4.0) as client:
                resp = client.get(
                    f"{BINANCE_SPOT_REST}/api/v3/depth",
                    params={"symbol": "BTCUSDT", "limit": self.ob_top_levels},
                )
                resp.raise_for_status()
                data = resp.json()
            bid_vol = sum(float(b[1]) for b in data.get("bids", []))
            ask_vol = sum(float(a[1]) for a in data.get("asks", []))
            total = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0
            result = {"bid_vol": bid_vol, "ask_vol": ask_vol, "imbalance": imbalance}
            self._ob_cache = result
            self._ob_cache_time = now
            return result
        except Exception as e:
            logger.warning(f"Binance spot OB fetch failed: {e}")
            return self._ob_cache

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None

        cvd = self._compute_cvd()
        ob = self._fetch_order_book()
        cvd_delta = cvd["cvd_delta"]
        ob_imbalance = ob["imbalance"] if ob else 0.0

        logger.info(
            f"CVD: delta=${cvd_delta/1e6:+.2f}M | OB imbalance={ob_imbalance:+.3f}"
        )

        cvd_direction = cvd_confidence = None
        if abs(cvd_delta) >= self.cvd_threshold_usd:
            cvd_direction = SignalDirection.BULLISH if cvd_delta > 0 else SignalDirection.BEARISH
            magnitude = min(abs(cvd_delta) / self.cvd_threshold_usd, 5.0)
            cvd_confidence = min(0.80, 0.55 + (magnitude - 1) * 0.06)

        ob_direction = ob_confidence = None
        if ob and abs(ob_imbalance) >= self.ob_imbalance_threshold:
            ob_direction = SignalDirection.BULLISH if ob_imbalance > 0 else SignalDirection.BEARISH
            ob_confidence = min(0.78, 0.55 + abs(ob_imbalance) * 0.35)

        if cvd_direction is None and ob_direction is None:
            return None

        if cvd_direction is not None and ob_direction is not None:
            if cvd_direction == ob_direction:
                direction = cvd_direction
                confidence = min(0.90, (cvd_confidence + ob_confidence) / 2 + 0.08)
                signal_note = "cvd_and_ob_agree"
            else:
                logger.debug("CVD and OB conflict — no signal")
                return None
        elif cvd_direction is not None:
            direction, confidence, signal_note = cvd_direction, cvd_confidence, "cvd_only"
        else:
            direction, confidence, signal_note = ob_direction, ob_confidence, "ob_only"

        if confidence < self.min_confidence:
            return None

        strength = (
            SignalStrength.STRONG
            if confidence >= 0.80
            else (SignalStrength.MODERATE if confidence >= 0.68 else SignalStrength.WEAK)
        )

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.VOLUME_SURGE,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={
                "cvd_delta_usd": round(cvd_delta, 0),
                "ob_imbalance": round(ob_imbalance, 4) if ob else None,
                "signal_source": signal_note,
            },
        )
        self._record_signal(signal)
        logger.info(
            f"CVDOrderBook {direction.value.upper()}: "
            f"cvd=${cvd_delta/1e6:+.2f}M, ob={ob_imbalance:+.3f}, "
            f"conf={confidence:.2%}, source={signal_note}"
        )
        return signal
