"""
Liquidation Cascade Signal Processor
======================================
Streams real-time BTC liquidations from Binance Futures forceOrders WebSocket.

WHY THIS IS THE BIGGEST DRIVER:
  When leveraged traders get liquidated, the exchange is forced to market-sell
  (long liquidations) or market-buy (short liquidations) their positions. Large
  liquidations cause cascading price moves because:
    1. The forced market order moves price
    2. That price move liquidates the next layer of leveraged positions
    3. Repeat — cascade

  A $5M long liquidation burst in the last 60 seconds = strong bearish signal
  A $5M short liquidation burst in the last 60 seconds = strong bullish signal

HOW IT WORKS:
  WebSocket: wss://fstream.binance.com/ws/btcusdt@forceOrder
  Each message: { "o": { "S": "SELL"/"BUY", "q": qty, "p": price, "T": timestamp } }
    S=SELL = long position being liquidated (bearish pressure)
    S=BUY  = short position being liquidated (bullish pressure)

  We accumulate a rolling 60s window of liquidation USD volume, split by side.
  Signal fires when liq_imbalance > threshold.

INTEGRATION:
  1. Start the WebSocket listener in a background thread at bot startup
  2. Pass liquidation_data=self._liq_processor.get_snapshot() in metadata
  3. The processor reads from its own internal rolling buffer (no external feed needed)
"""

import asyncio
import json
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List
from loguru import logger

import websockets

from core.strategy_brain.signal_processors.base_processor import (
    BaseSignalProcessor,
    TradingSignal,
    SignalType,
    SignalDirection,
    SignalStrength,
)

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws/btcusdt@forceOrder"


class LiquidationEvent:
    __slots__ = ("timestamp", "side", "usd_value")

    def __init__(self, timestamp: datetime, side: str, usd_value: float):
        self.timestamp = timestamp
        self.side = side          # "SELL" = long liq (bearish) | "BUY" = short liq (bullish)
        self.usd_value = usd_value


class LiquidationProcessor(BaseSignalProcessor):
    """
    Streams Binance Futures liquidations and generates directional signals.

    Call start_stream() once at bot startup (runs in a daemon thread).
    The processor self-populates its rolling buffer.
    Just call .process() normally — no external data needed in metadata.
    """

    def __init__(
        self,
        window_seconds: int = 60,          # rolling window to accumulate liquidations
        min_usd_threshold: float = 500_000, # minimum $500k to generate signal
        imbalance_threshold: float = 0.65,  # 65% one-sided to signal
        min_confidence: float = 0.55,
    ):
        super().__init__("Liquidations")

        self.window_seconds = window_seconds
        self.min_usd_threshold = min_usd_threshold
        self.imbalance_threshold = imbalance_threshold
        self.min_confidence = min_confidence

        # Rolling buffer — thread-safe via GIL for simple deque appends
        self._events: deque = deque(maxlen=10_000)
        self._stream_thread: Optional[threading.Thread] = None
        self._running = False

        logger.info(
            f"Initialized Liquidation Processor: "
            f"window={window_seconds}s, "
            f"min_usd=${min_usd_threshold/1e6:.1f}M, "
            f"imbalance_threshold={imbalance_threshold:.0%}"
        )

    # ── WebSocket stream ─────────────────────────────────────────────────────

    def start_stream(self) -> None:
        """Start the liquidation WebSocket in a background daemon thread."""
        if self._running:
            return
        self._running = True
        self._stream_thread = threading.Thread(
            target=self._run_stream_loop,
            daemon=True,
            name="LiquidationStream",
        )
        self._stream_thread.start()
        logger.info("Liquidation stream thread started")

    def stop_stream(self) -> None:
        self._running = False

    def _run_stream_loop(self) -> None:
        """Thread entry point — runs its own event loop."""
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
            BINANCE_FUTURES_WS,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            logger.info("Connected to Binance forceOrders stream")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    order = msg.get("o", {})
                    side = order.get("S", "")          # SELL or BUY
                    qty = float(order.get("q", 0))
                    price = float(order.get("p", 0))
                    ts_ms = int(order.get("T", 0))

                    if qty <= 0 or price <= 0:
                        continue

                    usd_value = qty * price
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    self._events.append(LiquidationEvent(ts, side, usd_value))

                except Exception:
                    continue

    # ── Snapshot helper ──────────────────────────────────────────────────────

    def _get_window_snapshot(self) -> Dict[str, float]:
        """Return total USD liquidations by side in the rolling window."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        long_liq_usd = 0.0   # SELL = long liquidated
        short_liq_usd = 0.0  # BUY  = short liquidated
        for ev in self._events:
            if ev.timestamp < cutoff:
                continue
            if ev.side == "SELL":
                long_liq_usd += ev.usd_value
            elif ev.side == "BUY":
                short_liq_usd += ev.usd_value
        return {"long_liq_usd": long_liq_usd, "short_liq_usd": short_liq_usd}

    # ── Signal processor interface ───────────────────────────────────────────

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
            f"long_liq=${long_liq/1e6:.2f}M, "
            f"short_liq=${short_liq/1e6:.2f}M, "
            f"total=${total/1e6:.2f}M"
        )

        if total < self.min_usd_threshold:
            logger.debug(f"Liquidations below threshold (${total/1e6:.2f}M < ${self.min_usd_threshold/1e6:.1f}M)")
            return None

        imbalance = (long_liq - short_liq) / total  # +1 = all long liqs (bearish), -1 = all short liqs (bullish)

        if abs(imbalance) < self.imbalance_threshold:
            logger.debug(f"Liquidation imbalance too balanced: {imbalance:+.3f}")
            return None

        # Long liquidations = forced selling = bearish pressure
        # Short liquidations = forced buying = bullish pressure
        if imbalance > 0:
            direction = SignalDirection.BEARISH
            dominant_usd = long_liq
        else:
            direction = SignalDirection.BULLISH
            dominant_usd = short_liq

        abs_imb = abs(imbalance)

        if dominant_usd >= 5_000_000:
            strength = SignalStrength.VERY_STRONG
        elif dominant_usd >= 2_000_000:
            strength = SignalStrength.STRONG
        elif dominant_usd >= 1_000_000:
            strength = SignalStrength.MODERATE
        else:
            strength = SignalStrength.WEAK

        confidence = min(0.88, 0.55 + abs_imb * 0.40)

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
                "window_seconds": self.window_seconds,
            },
        )

        self._record_signal(signal)
        logger.info(
            f"Generated {direction.value.upper()} signal (Liquidations): "
            f"imbalance={imbalance:+.3f}, total=${total/1e6:.2f}M, "
            f"confidence={confidence:.2%}"
        )
        return signal