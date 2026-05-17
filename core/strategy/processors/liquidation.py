"""core.strategy.processors.liquidation — Binance liquidation cascade processor."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import websockets
from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws/btcusdt@forceOrder"


class LiquidationEvent:
    __slots__ = ("timestamp", "side", "usd_value")

    def __init__(self, timestamp: datetime, side: str, usd_value: float):
        self.timestamp = timestamp
        self.side = side        # "SELL" = long liq (bearish) | "BUY" = short liq (bullish)
        self.usd_value = usd_value


class LiquidationProcessor(BaseSignalProcessor):
    """
    Streams Binance Futures forceOrders WebSocket and generates directional signals
    from liquidation cascade imbalances.

    Call ``start_stream()`` once at bot startup.
    """

    def __init__(
        self,
        window_seconds: int = 60,
        min_usd_threshold: float = 500_000,
        imbalance_threshold: float = 0.65,
        min_confidence: float = 0.55,
    ):
        super().__init__("Liquidations")
        self.window_seconds = window_seconds
        self.min_usd_threshold = min_usd_threshold
        self.imbalance_threshold = imbalance_threshold
        self.min_confidence = min_confidence
        self._events: deque = deque(maxlen=10_000)
        self._stream_thread: Optional[threading.Thread] = None
        self._running = False
        logger.info(
            f"Initialized Liquidation Processor: "
            f"window={window_seconds}s, min=${min_usd_threshold/1e6:.1f}M, "
            f"imbalance={imbalance_threshold:.0%}"
        )

    def start_stream(self) -> None:
        if self._running:
            return
        self._running = True
        self._stream_thread = threading.Thread(
            target=self._run_stream_loop, daemon=True, name="LiquidationStream"
        )
        self._stream_thread.start()
        logger.info("Liquidation stream thread started")

    def stop_stream(self) -> None:
        self._running = False

    def _run_stream_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self._running:
            try:
                loop.run_until_complete(self._stream())
            except Exception as e:
                logger.warning(f"Liquidation stream error: {e} — reconnecting in 5s")
                time.sleep(5)
        loop.close()

    async def _stream(self) -> None:
        async with websockets.connect(
            BINANCE_FUTURES_WS, ping_interval=20, ping_timeout=10
        ) as ws:
            logger.info("Connected to Binance forceOrders stream")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    order = msg.get("o", {})
                    side = order.get("S", "")
                    qty = float(order.get("q", 0))
                    price = float(order.get("p", 0))
                    ts_ms = int(order.get("T", 0))
                    if qty <= 0 or price <= 0:
                        continue
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    self._events.append(LiquidationEvent(ts, side, qty * price))
                except Exception:
                    continue

    def _get_window_snapshot(self) -> Dict[str, float]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        long_liq = short_liq = 0.0
        for ev in self._events:
            if ev.timestamp < cutoff:
                continue
            if ev.side == "SELL":
                long_liq += ev.usd_value
            elif ev.side == "BUY":
                short_liq += ev.usd_value
        return {"long_liq_usd": long_liq, "short_liq_usd": short_liq}

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None

        snap = self._get_window_snapshot()
        long_liq = snap["long_liq_usd"]
        short_liq = snap["short_liq_usd"]
        total = long_liq + short_liq

        logger.info(
            f"Liquidations (last {self.window_seconds}s): "
            f"long=${long_liq/1e6:.2f}M, short=${short_liq/1e6:.2f}M"
        )

        if total < self.min_usd_threshold:
            return None

        imbalance = (long_liq - short_liq) / total
        if abs(imbalance) < self.imbalance_threshold:
            return None

        direction = SignalDirection.BEARISH if imbalance > 0 else SignalDirection.BULLISH
        dominant_usd = long_liq if imbalance > 0 else short_liq

        if dominant_usd >= 5_000_000:
            strength = SignalStrength.VERY_STRONG
        elif dominant_usd >= 2_000_000:
            strength = SignalStrength.STRONG
        elif dominant_usd >= 1_000_000:
            strength = SignalStrength.MODERATE
        else:
            strength = SignalStrength.WEAK

        confidence = min(0.88, 0.55 + abs(imbalance) * 0.40)
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
                "long_liq_usd": round(long_liq, 0),
                "short_liq_usd": round(short_liq, 0),
                "total_liq_usd": round(total, 0),
                "imbalance": round(imbalance, 4),
            },
        )
        self._record_signal(signal)
        logger.info(
            f"Liquidations {direction.value.upper()}: "
            f"imbalance={imbalance:+.3f}, total=${total/1e6:.2f}M, conf={confidence:.2%}"
        )
        return signal
