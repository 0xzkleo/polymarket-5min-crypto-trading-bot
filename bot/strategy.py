"""
bot.strategy — IntegratedBTCStrategy: the main Nautilus trading strategy.

Subscribes to Polymarket BTC 15-min UP/DOWN markets, runs a full 6-step ML
decision loop, and routes orders through paper-trading or live Nautilus
execution depending on the current simulation mode.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy

from bot.models import (
    MARKET_INTERVAL_SECONDS,
    QUOTE_MIN_SPREAD,
    QUOTE_STABILITY_REQUIRED,
    PaperTrade,
    _make_stub_signal,
)
from core.settlement import get_settlement_tracker
from core.strategy.fusion import get_fusion_engine
from core.strategy.ml_engine import get_ml_engine
from core.strategy.processors.cvd_orderbook import CVDOrderBookProcessor
from core.strategy.processors.deribit_pcr import DeribitPCRProcessor
from core.strategy.processors.divergence import PriceDivergenceProcessor
from core.strategy.processors.funding_oi import FundingRateOIProcessor
from core.strategy.processors.liquidation import LiquidationProcessor
from core.strategy.processors.ohlcv_momentum import OHLCVMomentumProcessor
from core.strategy.processors.orderbook import OrderBookImbalanceProcessor
from core.strategy.processors.sentiment import SentimentProcessor
from core.strategy.processors.spike import SpikeDetectionProcessor
from core.strategy.processors.tick_velocity import TickVelocityProcessor
from execution.risk_engine import get_risk_engine
from feedback.learning_engine import get_learning_engine
from monitoring.metrics_exporter import get_grafana_exporter
from monitoring.performance_tracker import get_performance_tracker


class IntegratedBTCStrategy(Strategy):
    """
    Integrated BTC 15-min trading strategy.

    Lifecycle
    ---------
    1. ``on_start`` — loads all BTC 15-min instruments, subscribes, starts
       background threads (WebSocket streams, settlement tracker, Grafana).
    2. ``on_quote_tick`` — buffers ticks; when inside the 13–14-min trade
       window fires ``_make_trading_decision_sync`` via executor.
    3. ``_make_trading_decision`` — the 6-step ML decision loop.
    4. ``on_stop`` — saves paper trades.
    """

    def __init__(
        self,
        redis_client=None,
        enable_grafana: bool = True,
        test_mode: bool = False,
    ):
        super().__init__()

        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = 90

        self.instrument_id: Optional[InstrumentId] = None
        self.redis_client = redis_client
        self.current_simulation_mode = False

        self.all_btc_instruments: List[Dict] = []
        self.current_instrument_index: int = -1
        self.next_switch_time: Optional[datetime] = None

        self._stable_tick_count = 0
        self._market_stable = False
        self._last_instrument_switch = None

        self.last_trade_time = -1
        self._waiting_for_market_open = False
        self._last_bid_ask = None

        # Async instrument loading state. Nautilus loads Polymarket instruments
        # in a background task; on_start() can run before the cache is populated.
        # Track retries so the timer loop can re-attempt without spamming logs.
        self._instruments_loaded: bool = False
        self._instrument_load_attempts: int = 0
        self._max_instrument_load_attempts: int = 60  # ~10 min at 10s ticks

        self._tick_buffer: deque = deque(maxlen=500)
        self._yes_token_id: Optional[str] = None

        # ── Signal processors ─────────────────────────────────────────────────
        self.spike_detector = SpikeDetectionProcessor(
            spike_threshold=0.05,
            lookback_periods=20,
        )
        self.sentiment_processor = SentimentProcessor(
            extreme_fear_threshold=25,
            extreme_greed_threshold=75,
        )
        self.divergence_processor = PriceDivergenceProcessor(
            divergence_threshold=0.05,
        )
        self.orderbook_processor = OrderBookImbalanceProcessor(
            imbalance_threshold=0.30,
            min_book_volume=50.0,
        )
        self.tick_velocity_processor = TickVelocityProcessor(
            velocity_threshold_60s=0.015,
            velocity_threshold_30s=0.010,
        )
        self.deribit_pcr_processor = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20,
            bearish_pcr_threshold=0.70,
            max_days_to_expiry=2,
            cache_seconds=300,
        )
        self.liquidation_processor = LiquidationProcessor(
            window_seconds=60,
            min_usd_threshold=500_000,
            imbalance_threshold=0.65,
        )
        self.funding_oi_processor = FundingRateOIProcessor(
            bullish_funding_threshold=-0.0003,
            bearish_funding_threshold=0.0005,
            oi_change_threshold=0.02,
            cache_seconds=300,
        )
        self.cvd_ob_processor = CVDOrderBookProcessor(
            cvd_window_seconds=900,
            cvd_threshold_usd=5_000_000,
            ob_imbalance_threshold=0.30,
        )
        self.ohlcv_momentum_processor = OHLCVMomentumProcessor(
            rsi_overbought=68.0,
            rsi_oversold=32.0,
            cache_seconds=120,
        )

        # ── Signal fusion weights ────────────────────────────────────────────
        self.fusion_engine = get_fusion_engine()
        self.fusion_engine.set_weight("OrderBookImbalance", 0.30)
        self.fusion_engine.set_weight("TickVelocity",       0.25)
        self.fusion_engine.set_weight("PriceDivergence",    0.18)
        self.fusion_engine.set_weight("SpikeDetection",     0.12)
        self.fusion_engine.set_weight("DeribitPCR",         0.10)
        self.fusion_engine.set_weight("SentimentAnalysis",  0.05)
        self.fusion_engine.set_weight("Liquidations",       0.20)
        self.fusion_engine.set_weight("CVDOrderBook",       0.18)
        self.fusion_engine.set_weight("FundingRateOI",      0.10)
        self.fusion_engine.set_weight("OHLCVMomentum",      0.10)

        # ── Supporting systems ────────────────────────────────────────────────
        self.risk_engine = get_risk_engine()
        self.performance_tracker = get_performance_tracker()
        self.learning_engine = get_learning_engine()
        self.ml_engine = get_ml_engine()
        self.settlement_tracker = get_settlement_tracker()

        self.grafana_exporter = get_grafana_exporter() if enable_grafana else None

        # ── Price history ─────────────────────────────────────────────────────
        self.price_history: list = []
        self.max_history = 100
        self.paper_trades: List[PaperTrade] = []

        self.test_mode = test_mode
        self._last_learning_optimization = datetime.now(timezone.utc)
        self._learning_interval_hours: float = (5 / 60) if test_mode else (7 * 24)

        if test_mode:
            logger.info("=" * 80)
            logger.info("  TEST MODE ACTIVE - Trading every minute!")
            logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY INITIALIZED")
        logger.info("  All signal processors ready")
        logger.info("  Risk engine ready")
        logger.info("  ML engine ready")
        logger.info("  $1 per trade maximum")
        logger.info("=" * 80)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _seconds_to_next_15min_boundary(self) -> float:
        now_ts = datetime.now(timezone.utc).timestamp()
        next_boundary = (math.floor(now_ts / MARKET_INTERVAL_SECONDS) + 1) * MARKET_INTERVAL_SECONDS
        return next_boundary - now_ts

    def _is_quote_valid(self, bid, ask) -> bool:
        if bid is None or ask is None:
            return False
        try:
            b, a = float(bid), float(ask)
        except (TypeError, ValueError):
            return False
        return QUOTE_MIN_SPREAD <= b < 1.0 and QUOTE_MIN_SPREAD <= a < 1.0

    def _reset_stability(self, reason: str = "") -> None:
        if self._market_stable:
            logger.warning(f"Market stability RESET{' – ' + reason if reason else ''}")
        self._market_stable = False
        self._stable_tick_count = 0

    # ── Redis ─────────────────────────────────────────────────────────────────

    async def check_simulation_mode(self) -> bool:
        if not self.redis_client:
            return self.current_simulation_mode
        try:
            sim_mode = self.redis_client.get("btc_trading:simulation_mode")
            if sim_mode is not None:
                redis_simulation = sim_mode == "1"
                if redis_simulation != self.current_simulation_mode:
                    self.current_simulation_mode = redis_simulation
                    mode_text = "SIMULATION" if redis_simulation else "LIVE TRADING"
                    logger.warning(f"Trading mode changed to: {mode_text}")
                    if not redis_simulation:
                        logger.warning("LIVE TRADING ACTIVE - Real money at risk!")
                return redis_simulation
        except Exception as e:
            logger.warning(f"Failed to check Redis simulation mode: {e}")
        return self.current_simulation_mode

    # ── Strategy lifecycle ────────────────────────────────────────────────────

    def on_start(self) -> None:
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY STARTED")
        logger.info("=" * 80)

        # First attempt at the instrument cache. The Polymarket adapter
        # populates the cache asynchronously, so an empty result here is
        # expected on a fresh start; the timer loop will keep retrying.
        if not self._load_all_btc_instruments():
            logger.warning(
                "Instrument cache empty at startup — timer loop will retry "
                "until BTC 15-min markets become available."
            )

        if self.instrument_id:
            try:
                quote = self.cache.quote_tick(self.instrument_id)
                if quote and quote.bid_price and quote.ask_price:
                    current_price = (quote.bid_price + quote.ask_price) / 2
                    self.price_history.append(current_price)
                    logger.info(f"Initial price: ${float(current_price):.4f}")
            except Exception as e:
                logger.debug(f"No initial price yet: {e}")

        if len(self.price_history) < 20:
            self._generate_synthetic_history(target_count=20, existing_count=len(self.price_history))

        self.run_in_executor(self._start_timer_loop)

        if self.grafana_exporter:
            import threading
            threading.Thread(target=self._start_grafana_sync, daemon=True).start()

        self.liquidation_processor.start_stream()
        self.cvd_ob_processor.start_stream()
        self.settlement_tracker.start_tracking()
        logger.info("Liquidation stream started")
        logger.info("CVD aggTrade stream started")
        logger.info("Settlement tracker started")
        logger.info(
            f"ML engine active: {self.ml_engine.is_active} "
            f"(samples={self.ml_engine._sample_count}/{self.ml_engine.min_samples})"
        )

        logger.info("=" * 80)
        logger.info("Strategy active — will trade in the 13–14-min window")
        logger.info(f"Price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("READY TO TRADE")
        else:
            logger.warning(f"Need more history ({len(self.price_history)}/20)")
        logger.info("=" * 80)

    def _generate_synthetic_history(self, target_count: int = 20, existing_count: int = 0) -> None:
        base_price = self.price_history[-1] if self.price_history else Decimal("0.5")
        needed = target_count - existing_count
        if needed <= 0:
            return
        for _ in range(needed):
            change = Decimal(str(random.uniform(-0.03, 0.03)))
            new_price = max(Decimal("0.01"), min(Decimal("0.99"), base_price * (Decimal("1") + change)))
            self.price_history.append(new_price)
            base_price = new_price

    # ── Instrument loading ────────────────────────────────────────────────────

    def _load_all_btc_instruments(self, *, quiet: bool = False) -> bool:
        """
        Scan the Nautilus instrument cache for BTC 15-min markets and bind the
        active one. Returns True if any markets were found, False otherwise.

        Nautilus populates the instrument cache asynchronously, so this must be
        callable repeatedly until it succeeds. ``quiet=True`` suppresses the
        per-attempt log lines used by the retry loop.
        """
        instruments = self.cache.instruments()
        if not quiet:
            logger.info(f"Loading BTC instruments from {len(instruments)} total...")

        now = datetime.now(timezone.utc)
        current_timestamp = int(now.timestamp())

        btc_instruments = []

        for instrument in instruments:
            try:
                info = getattr(instrument, "info", None) or {}
                if not info:
                    continue
                question = (info.get("question") or "").lower()
                slug = (info.get("market_slug") or "").lower()

                if not (("btc" in question or "btc" in slug) and "15m" in slug):
                    continue

                try:
                    market_timestamp = int(slug.split("-")[-1])
                except (ValueError, IndexError):
                    continue

                end_timestamp = market_timestamp + 900
                if end_timestamp <= current_timestamp:
                    continue

                raw_id = str(instrument.id)
                without_suffix = raw_id.split(".")[0] if "." in raw_id else raw_id
                token_id = (
                    without_suffix.split("-")[-1]
                    if "-" in without_suffix
                    else without_suffix
                )

                # Polymarket binary markets expose two CLOB tokens per slug,
                # one per outcome. Trust info["outcome"] when present; fall
                # back to insertion order only as a last resort.
                outcome = (info.get("outcome") or "").strip().lower()

                btc_instruments.append({
                    "instrument": instrument,
                    "slug": slug,
                    "start_time": datetime.fromtimestamp(market_timestamp, tz=timezone.utc),
                    "end_time": datetime.fromtimestamp(end_timestamp, tz=timezone.utc),
                    "market_timestamp": market_timestamp,
                    "end_timestamp": end_timestamp,
                    "time_diff_minutes": (market_timestamp - current_timestamp) / 60,
                    "token_id": token_id,
                    "outcome": outcome,
                })
            except Exception:
                continue

        if not btc_instruments:
            if not quiet:
                logger.warning("No BTC 15-min instruments in cache yet")
            return False

        # Group both tokens (YES/NO) under their shared slug.
        grouped: Dict[str, dict] = {}
        for inst in btc_instruments:
            slug = inst["slug"]
            entry = grouped.get(slug)
            if entry is None:
                entry = {
                    **inst,
                    "yes_instrument_id": None,
                    "no_instrument_id": None,
                    "yes_token_id": None,
                    "no_token_id": None,
                }
                grouped[slug] = entry

            outcome = inst["outcome"]
            inst_id = inst["instrument"].id
            tok_id = inst["token_id"]

            if outcome == "yes" or outcome == "up":
                entry["yes_instrument_id"] = inst_id
                entry["yes_token_id"] = tok_id
            elif outcome == "no" or outcome == "down":
                entry["no_instrument_id"] = inst_id
                entry["no_token_id"] = tok_id
            else:
                # Outcome metadata missing — fall back to first/second seen.
                if entry["yes_instrument_id"] is None:
                    entry["yes_instrument_id"] = inst_id
                    entry["yes_token_id"] = tok_id
                else:
                    entry["no_instrument_id"] = inst_id
                    entry["no_token_id"] = tok_id

        btc_instruments = sorted(grouped.values(), key=lambda x: x["market_timestamp"])

        logger.info("=" * 80)
        logger.info(f"FOUND {len(btc_instruments)} BTC 15-MIN MARKETS:")
        for i, inst in enumerate(btc_instruments):
            is_active = inst["time_diff_minutes"] <= 0 and inst["end_timestamp"] > current_timestamp
            status = "ACTIVE" if is_active else ("FUTURE" if inst["time_diff_minutes"] > 0 else "PAST")
            yes_marker = "Y" if inst.get("yes_instrument_id") else "-"
            no_marker = "N" if inst.get("no_instrument_id") else "-"
            logger.info(
                f"  [{i}] {inst['slug']}: {status} [{yes_marker}{no_marker}] "
                f"(starts {inst['start_time'].strftime('%H:%M:%S')}, "
                f"ends {inst['end_time'].strftime('%H:%M:%S')})"
            )
        logger.info("=" * 80)

        self.all_btc_instruments = btc_instruments

        active_idx = None
        for i, inst in enumerate(btc_instruments):
            if inst["time_diff_minutes"] <= 0 and inst["end_timestamp"] > current_timestamp:
                active_idx = i
                break

        if active_idx is not None:
            self._bind_market(active_idx, waiting=False)
        else:
            future_markets = [inst for inst in btc_instruments if inst["time_diff_minutes"] > 0]
            nearest = (
                min(future_markets, key=lambda x: x["time_diff_minutes"])
                if future_markets
                else btc_instruments[-1]
            )
            self._bind_market(btc_instruments.index(nearest), waiting=True)

        self._instruments_loaded = True
        return True

    def _bind_market(self, index: int, *, waiting: bool) -> None:
        """Bind the strategy to a market entry by index in ``all_btc_instruments``."""
        if not (0 <= index < len(self.all_btc_instruments)):
            return

        market = self.all_btc_instruments[index]
        self.current_instrument_index = index
        # Default subscription/instrument target is the YES token when present.
        self.instrument_id = market.get("yes_instrument_id") or market["instrument"].id
        self._yes_instrument_id = market.get("yes_instrument_id") or market["instrument"].id
        self._no_instrument_id = market.get("no_instrument_id")
        self._yes_token_id = market.get("yes_token_id") or market.get("token_id")

        if waiting:
            self.next_switch_time = market["start_time"]
            self._waiting_for_market_open = True
            logger.info(f"NO CURRENT MARKET — waiting for: {market['slug']}")
        else:
            self.next_switch_time = market["end_time"]
            self._waiting_for_market_open = False
            logger.info(f"CURRENT MARKET: {market['slug']} (index {index})")
            logger.info(f"  Next switch at: {self.next_switch_time.strftime('%H:%M:%S')}")

        try:
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"  Subscribed to: {self.instrument_id}")
        except Exception as e:
            logger.warning(f"Failed to subscribe quote ticks for {self.instrument_id}: {e}")

    def _switch_to_next_market(self) -> bool:
        if not self.all_btc_instruments:
            logger.error("No instruments loaded!")
            return False

        next_index = self.current_instrument_index + 1
        if next_index >= len(self.all_btc_instruments):
            logger.warning("No more markets — will restart bot")
            return False

        next_market = self.all_btc_instruments[next_index]
        now = datetime.now(timezone.utc)

        if now < next_market["start_time"]:
            logger.info(f"Waiting for next market at {next_market['start_time'].strftime('%H:%M:%S')}")
            return False

        logger.info("=" * 80)
        logger.info(f"SWITCHING TO NEXT MARKET: {next_market['slug']}")
        logger.info(f"  Current time: {now.strftime('%H:%M:%S')}")
        logger.info("=" * 80)

        self._bind_market(next_index, waiting=False)

        self._stable_tick_count = QUOTE_STABILITY_REQUIRED
        self._market_stable = True
        self.last_trade_time = -1
        logger.info("  Trade timer reset — will trade on next tick")
        return True

    # ── Timer loop ────────────────────────────────────────────────────────────

    def _start_timer_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._timer_loop())
        finally:
            loop.close()

    async def _timer_loop(self) -> None:
        while True:
            uptime_minutes = (
                (datetime.now(timezone.utc) - self.bot_start_time).total_seconds() / 60
            )
            if uptime_minutes >= self.restart_after_minutes:
                logger.warning("AUTO-RESTART — loading fresh market filters")
                import signal as _signal
                os.kill(os.getpid(), _signal.SIGTERM)
                return

            # Retry instrument loading until Nautilus has populated the cache.
            if not self._instruments_loaded:
                self._instrument_load_attempts += 1
                # Log the first few attempts then go quiet to avoid log spam.
                quiet = self._instrument_load_attempts > 3
                if self._load_all_btc_instruments(quiet=quiet):
                    logger.info(
                        f"Instruments loaded after {self._instrument_load_attempts} "
                        f"attempt(s) — strategy is ready to trade."
                    )
                elif self._instrument_load_attempts == self._max_instrument_load_attempts:
                    logger.error(
                        f"Failed to load BTC 15-min instruments after "
                        f"{self._max_instrument_load_attempts} attempts. "
                        f"Check Polymarket connectivity and Gamma API patches."
                    )
                elif self._instrument_load_attempts > self._max_instrument_load_attempts:
                    # Stop retrying but keep the loop alive for stop/restart logic.
                    pass

            now = datetime.now(timezone.utc)

            if self.next_switch_time and now >= self.next_switch_time:
                if self._waiting_for_market_open:
                    logger.info("=" * 80)
                    logger.info(f"WAITING MARKET NOW OPEN: {now.strftime('%H:%M:%S')} UTC")
                    logger.info("=" * 80)
                    if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                        current_market = self.all_btc_instruments[self.current_instrument_index]
                        self.next_switch_time = current_market["end_time"]
                        logger.info(f"  Market ends at {self.next_switch_time.strftime('%H:%M:%S')} UTC")
                    self._waiting_for_market_open = False
                    self._market_stable = True
                    self._stable_tick_count = QUOTE_STABILITY_REQUIRED
                    self.last_trade_time = -1
                    logger.info("  MARKET OPEN — ready to trade on next tick")
                else:
                    self._switch_to_next_market()

            # Scheduled learning-engine weight optimisation
            hours_since_learn = (
                (datetime.now(timezone.utc) - self._last_learning_optimization).total_seconds() / 3600
            )
            if hours_since_learn >= self._learning_interval_hours:
                logger.info("=" * 60)
                logger.info("SCHEDULED: Running learning engine weight optimisation")
                logger.info("=" * 60)
                try:
                    new_weights = await self.learning_engine.optimize_weights()
                    self._last_learning_optimization = datetime.now(timezone.utc)
                    logger.info(
                        f"Learning engine complete: {len(new_weights)} weights updated. "
                        f"Next run in {self._learning_interval_hours:.1f}h"
                    )
                except Exception as _le:
                    logger.warning(f"Learning engine scheduled run failed: {_le}")

            await asyncio.sleep(10)

    # ── Quote tick handler ────────────────────────────────────────────────────

    def on_quote_tick(self, tick: QuoteTick) -> None:
        try:
            if self.instrument_id is None or tick.instrument_id != self.instrument_id:
                return

            now = datetime.now(timezone.utc)
            bid, ask = tick.bid_price, tick.ask_price

            if bid is None or ask is None:
                return

            try:
                bid_decimal = bid.as_decimal()
                ask_decimal = ask.as_decimal()
            except Exception:
                return

            mid_price = (bid_decimal + ask_decimal) / 2
            self.price_history.append(mid_price)
            if len(self.price_history) > self.max_history:
                self.price_history.pop(0)

            self._last_bid_ask = (bid_decimal, ask_decimal)
            self._tick_buffer.append({"ts": now, "price": mid_price})

            if not self._market_stable:
                self._stable_tick_count += 1
                if self._stable_tick_count >= 1:
                    self._market_stable = True
                    logger.info("Market STABLE")
                else:
                    return

            if self._waiting_for_market_open:
                return

            if not (0 <= self.current_instrument_index < len(self.all_btc_instruments)):
                return

            current_market = self.all_btc_instruments[self.current_instrument_index]
            market_start_ts = current_market["market_timestamp"]

            elapsed_secs = now.timestamp() - market_start_ts
            if elapsed_secs < 0:
                return

            sub_interval = int(elapsed_secs // MARKET_INTERVAL_SECONDS)
            trade_key = (market_start_ts, sub_interval)

            # Trade window: minutes 13–14 of each 15-min market
            TRADE_WINDOW_START = 540
            TRADE_WINDOW_END   = 600
            seconds_into_sub = elapsed_secs % MARKET_INTERVAL_SECONDS

            if self.test_mode:
                # In test mode trade every minute instead of every 15 min
                TRADE_WINDOW_START = 0
                TRADE_WINDOW_END   = 900

            if TRADE_WINDOW_START <= seconds_into_sub < TRADE_WINDOW_END and trade_key != self.last_trade_time:
                self.last_trade_time = trade_key

                logger.info("=" * 80)
                logger.info(f"TRADE WINDOW: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                logger.info(f"  Market:   {current_market['slug']}")
                logger.info(
                    f"  Sub-interval #{sub_interval} "
                    f"({seconds_into_sub:.1f}s in = {seconds_into_sub/60:.1f} min)"
                )
                logger.info(
                    f"  Price: ${float(mid_price):,.4f} | "
                    f"Bid: ${float(bid_decimal):,.4f} | "
                    f"Ask: ${float(ask_decimal):,.4f}"
                )
                logger.info(
                    f"  Trend: {'STRONG' if float(mid_price) > 0.60 or float(mid_price) < 0.40 else 'WEAK'}"
                )
                logger.info("=" * 80)

                self.run_in_executor(lambda: self._make_trading_decision_sync(float(mid_price)))

        except Exception as e:
            logger.error(f"Error processing quote tick: {e}")

    # ── Trading decision ──────────────────────────────────────────────────────

    def _make_trading_decision_sync(self, current_price: float) -> None:
        """Synchronous wrapper — called from executor thread."""
        price_decimal = Decimal(str(current_price))
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._make_trading_decision(price_decimal))
        finally:
            loop.close()

    async def _fetch_market_context(self, current_price: Decimal) -> dict:
        """Fetch real external data to populate all signal-processor metadata."""
        current_price_float = float(current_price)

        recent_prices = [float(p) for p in self.price_history[-20:]]
        sma_20 = sum(recent_prices) / len(recent_prices)
        deviation = (current_price_float - sma_20) / sma_20
        momentum = (
            (current_price_float - float(self.price_history[-5])) / float(self.price_history[-5])
            if len(self.price_history) >= 5
            else 0.0
        )
        variance = sum((p - sma_20) ** 2 for p in recent_prices) / len(recent_prices)
        volatility = math.sqrt(variance)

        metadata: dict = {
            "deviation": deviation,
            "momentum": momentum,
            "volatility": volatility,
            "tick_buffer": list(self._tick_buffer),
            "yes_token_id": self._yes_token_id,
        }

        # Fear & Greed
        try:
            from data_sources.news_social import NewsSocialDataSource
            news_source = NewsSocialDataSource()
            await news_source.connect()
            fg = await news_source.get_fear_greed_index()
            await news_source.disconnect()
            if fg and "value" in fg:
                metadata["sentiment_score"] = float(fg["value"])
                metadata["sentiment_classification"] = fg.get("classification", "")
                logger.info(
                    f"Fear & Greed: {metadata['sentiment_score']:.0f} "
                    f"({metadata['sentiment_classification']})"
                )
            else:
                logger.warning("Fear & Greed fetch returned no data")
        except Exception as e:
            logger.warning(f"Could not fetch Fear & Greed: {e}")

        # Coinbase spot price
        try:
            from data_sources.coinbase import CoinbaseDataSource
            coinbase = CoinbaseDataSource()
            await coinbase.connect()
            spot = await coinbase.get_current_price()
            await coinbase.disconnect()
            if spot:
                metadata["spot_price"] = float(spot)
                logger.info(f"Coinbase spot: ${float(spot):,.2f}")
            else:
                logger.warning("Coinbase price fetch returned None")
        except Exception as e:
            logger.warning(f"Could not fetch Coinbase spot price: {e}")

        logger.info(
            f"Market context — deviation={deviation:.2%}, "
            f"momentum={momentum:.2%}, volatility={volatility:.4f}, "
            f"sentiment={'%.0f' % metadata['sentiment_score'] if 'sentiment_score' in metadata else 'N/A'}, "
            f"spot=${'%.2f' % metadata['spot_price'] if 'spot_price' in metadata else 'N/A'}"
        )

        # Liquidation snapshot
        liq_snap = self.liquidation_processor._get_window_snapshot()
        liq_total = liq_snap["long_liq_usd"] + liq_snap["short_liq_usd"]
        liq_imbalance = (
            (liq_snap["long_liq_usd"] - liq_snap["short_liq_usd"]) / liq_total
            if liq_total > 0
            else 0.0
        )
        metadata["liq_imbalance"] = liq_imbalance
        metadata["liq_total_usd"] = liq_total

        # CVD snapshot
        cvd_snap = self.cvd_ob_processor._compute_cvd()
        metadata["cvd_delta_usd"] = cvd_snap["cvd_delta"]

        # Binance spot order book
        ob_snap = self.cvd_ob_processor._fetch_order_book()
        if ob_snap:
            metadata["ob_imbalance"] = ob_snap["imbalance"]

        # Funding + OI
        try:
            fi_data = self.funding_oi_processor._fetch_data()
            if fi_data:
                metadata["funding_rate"] = fi_data["funding_rate"]
                metadata["oi_change"] = fi_data["oi_change"]
        except Exception:
            pass

        # OHLCV
        try:
            klines = self.ohlcv_momentum_processor._fetch_klines()
            if klines:
                for key in ("vol_regime", "rsi", "macd_line", "macd_signal",
                            "pct_b", "ret1", "ret3", "ret5", "ret15"):
                    metadata[key] = klines[key]
        except Exception:
            pass

        logger.info(
            f"Extended context — liq_imb={liq_imbalance:+.3f}, "
            f"cvd=${cvd_snap['cvd_delta']/1e6:+.1f}M, "
            f"funding={metadata.get('funding_rate', 0):.5%}, "
            f"vol_regime={metadata.get('vol_regime', 'N/A')}"
        )

        return metadata

    async def _make_trading_decision(self, current_price: Decimal) -> None:
        """
        6-step ML decision loop.

        Step 1  Collect all 8 features → feature vector
        Step 2  XGBoost model → p(BTC UP)
        Step 3  Compare vs Polymarket implied odds
        Step 4  Edge check → bet if mispriced
        Step 5  Register with settlement tracker
        Step 6  Weekly retrain triggered by settlement tracker
        """
        is_simulation = await self.check_simulation_mode()
        logger.info(f"Mode: {'SIMULATION' if is_simulation else 'LIVE TRADING'}")

        if len(self.price_history) < 20:
            logger.warning(f"Not enough price history ({len(self.price_history)}/20)")
            return

        poly_price = float(current_price)
        logger.info(f"Current Polymarket price: {poly_price:.4f}")

        metadata = await self._fetch_market_context(current_price)
        signals = self._process_signals(current_price, metadata)

        for sig in signals:
            if sig.source == "TickVelocity":
                metadata["velocity_60s"] = sig.metadata.get("velocity_60s") or 0.0
                metadata["velocity_30s"] = sig.metadata.get("velocity_30s") or 0.0
                break

        fused = self.fusion_engine.fuse_signals(signals, min_signals=1, min_score=40.0)

        if signals:
            n_bull = sum(1 for s in signals if "BULLISH" in str(s.direction).upper())
            n_bear = sum(1 for s in signals if "BEARISH" in str(s.direction).upper())
            logger.info(f"Signals: {len(signals)} fired — {n_bull} bullish / {n_bear} bearish")
        else:
            logger.info("No individual signals fired — proceeding to ML/trend filter")

        if fused:
            logger.info(
                f"Fusion: {fused.direction.value} score={fused.score:.1f} conf={fused.confidence:.2%}"
            )

        # STEP 2 — ML model
        flat_metadata = {
            k: float(v) if hasattr(v, "__float__") else v
            for k, v in metadata.items()
            if not isinstance(v, (list, dict))
        }
        feature_vector = self.ml_engine.build_feature_vector(
            metadata=flat_metadata,
            poly_price=poly_price,
        )

        ml_p_up: Optional[float] = None
        if self.ml_engine.is_active and feature_vector is not None:
            ml_p_up = self.ml_engine.predict(feature_vector)
            logger.info(
                f"STEP 2 — ML model active: p(UP)={ml_p_up:.3f} | "
                f"poly={poly_price:.3f} | samples={self.ml_engine._sample_count}"
            )
        else:
            logger.info(
                f"STEP 2 — ML warming up "
                f"({self.ml_engine._sample_count}/{self.ml_engine.min_samples} samples)"
            )

        # STEPS 3 + 4 — Edge check / fallback trend filter
        # The market-order patch reads MARKET_BUY_USD at submit time, so the
        # strategy must use the same value for risk validation and logging
        # otherwise the risk engine and the actual fill amount disagree.
        try:
            position_size_usd = max(0.01, float(os.getenv("MARKET_BUY_USD", "1.0")))
        except (TypeError, ValueError):
            position_size_usd = 1.0
        POSITION_SIZE_USD = Decimal(str(position_size_usd))
        direction: Optional[str] = None
        bet_edge: float = 0.0

        if ml_p_up is not None:
            should_bet, ml_direction, _edge = self.ml_engine.should_bet(
                p_up=ml_p_up,
                poly_price=poly_price,
            )
            if not should_bet:
                logger.info(
                    f"STEP 4 — No edge: model={ml_p_up:.3f} market={poly_price:.3f} "
                    f"gap={abs(ml_p_up - poly_price):.3f} < required={self.ml_engine.min_edge:.3f}"
                )
                if feature_vector is not None:
                    slug = (
                        self.all_btc_instruments[self.current_instrument_index]["slug"]
                        if self.current_instrument_index >= 0
                        else "unknown"
                    )
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                return
            direction = ml_direction
            bet_edge = _edge
            logger.info(
                f"STEP 4 — Edge found: model={ml_p_up:.3f} market={poly_price:.3f} "
                f"edge={bet_edge:.3f} → bet {direction.upper()}"
            )
        else:
            TREND_UP, TREND_DOWN = 0.60, 0.40
            if poly_price > TREND_UP:
                direction = "long"
                logger.info(f"STEP 4 (fallback) — trend UP {poly_price:.2%} → YES")
            elif poly_price < TREND_DOWN:
                direction = "short"
                logger.info(f"STEP 4 (fallback) — trend DOWN {poly_price:.2%} → NO")
            else:
                logger.info(
                    f"STEP 4 (fallback) — neutral {poly_price:.2%} — skip"
                )
                if feature_vector is not None:
                    slug = (
                        self.all_btc_instruments[self.current_instrument_index]["slug"]
                        if self.current_instrument_index >= 0
                        else "unknown"
                    )
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                return

        # Risk engine
        is_valid, error = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD,
            direction=direction,
            current_price=current_price,
        )
        if not is_valid:
            logger.warning(f"Risk engine blocked: {error}")
            return

        # Liquidity guard
        last_tick = getattr(self, "_last_bid_ask", None)
        if last_tick:
            last_bid, last_ask = last_tick
            MIN_LIQ = Decimal("0.02")
            if direction == "long" and last_ask <= MIN_LIQ:
                logger.warning(f"No ask liquidity ({float(last_ask):.4f}) — retry next tick")
                self.last_trade_time = -1
                return
            if direction == "short" and last_bid <= MIN_LIQ:
                logger.warning(f"No bid liquidity ({float(last_bid):.4f}) — retry next tick")
                self.last_trade_time = -1
                return

        # STEP 5 setup — save feature vector
        trade_id: Optional[int] = None
        if feature_vector is not None:
            slug = (
                self.all_btc_instruments[self.current_instrument_index]["slug"]
                if self.current_instrument_index >= 0
                else "unknown"
            )
            trade_id = self.ml_engine.record_trade(
                market_slug=slug,
                poly_price=poly_price,
                feature_vector=feature_vector,
            )
            logger.info(f"STEP 5 setup — feature vector saved, trade_id={trade_id}")

        signal_for_logging = fused if fused is not None else _make_stub_signal(direction, ml_p_up)

        if is_simulation:
            await self._record_paper_trade(
                signal_for_logging, POSITION_SIZE_USD, current_price, direction,
                ml_p_up=ml_p_up if ml_p_up is not None else 0.0,
                ml_edge=bet_edge if ml_p_up is not None else 0.0,
                metadata=flat_metadata,
            )
        else:
            await self._place_real_order(signal_for_logging, POSITION_SIZE_USD, current_price, direction)

        # STEP 5 — Register settlement
        if trade_id is not None and self.current_instrument_index >= 0:
            market_info = self.all_btc_instruments[self.current_instrument_index]
            self.settlement_tracker.register_trade(
                trade_id=trade_id,
                market_slug=market_info["slug"],
                market_start_ts=market_info["market_timestamp"],
                market_end_ts=market_info["end_timestamp"],
                direction=direction,
                poly_price=poly_price,
            )
            logger.info(
                f"STEP 5 — Settlement tracker registered trade_id={trade_id} "
                f"(closes {market_info['end_time'].strftime('%H:%M:%S')} UTC)"
            )

    # ── Paper trading ─────────────────────────────────────────────────────────

    async def _record_paper_trade(
        self,
        signal,
        position_size,
        current_price: Decimal,
        direction: str,
        ml_p_up: float = 0.0,
        ml_edge: float = 0.0,
        metadata: dict = None,
    ) -> None:
        if metadata is None:
            metadata = {}

        now = datetime.now(timezone.utc)
        exit_delta = timedelta(minutes=1) if self.test_mode else timedelta(minutes=15)
        exit_time = now + exit_delta

        if "BULLISH" in str(signal.direction):
            movement = random.uniform(-0.04, 0.10)
        else:
            movement = random.uniform(-0.10, 0.04)

        exit_price_raw = max(0.01, min(0.99, float(current_price) + movement))
        exit_price_dec = Decimal(str(exit_price_raw))

        if direction == "long":
            pnl = float(position_size) * (exit_price_raw - float(current_price))
        else:
            pnl = float(position_size) * (float(current_price) - exit_price_raw)

        pnl_pct = pnl / float(position_size) if float(position_size) > 0 else 0.0
        outcome = "WIN" if pnl > 0 else "LOSS"

        slug = ""
        if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
            slug = self.all_btc_instruments[self.current_instrument_index].get("slug", "")

        trade_id = f"paper_{int(now.timestamp() * 1000)}"
        session_num = len(self.paper_trades) + 1

        paper_trade = PaperTrade(
            trade_id=trade_id,
            timestamp=now,
            direction=direction.upper(),
            size_usd=float(position_size),
            entry_price=float(current_price),
            exit_price=exit_price_raw,
            pnl_usd=pnl,
            pnl_pct=pnl_pct,
            outcome=outcome,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            num_signals=signal.num_signals if hasattr(signal, "num_signals") else 1,
            ml_p_up=ml_p_up,
            ml_edge=ml_edge,
            market_slug=slug,
            btc_spot_price=float(metadata.get("spot_price", 0.0) or 0.0),
            vol_regime=str(metadata.get("vol_regime", "") or ""),
            funding_rate=float(metadata.get("funding_rate", 0.0) or 0.0),
            session_trade_num=session_num,
        )
        self.paper_trades.append(paper_trade)

        settled = [t for t in self.paper_trades if t.outcome in ("WIN", "LOSS")]
        wins = sum(1 for t in settled if t.outcome == "WIN")
        win_rate = wins / len(settled) if settled else 0.0
        total_pnl = sum(t.pnl_usd for t in self.paper_trades)

        self.performance_tracker.record_trade(
            trade_id=trade_id,
            direction=direction,
            entry_price=current_price,
            exit_price=exit_price_dec,
            size=position_size,
            entry_time=now,
            exit_time=exit_time,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            metadata={
                "simulated": True,
                "num_signals": signal.num_signals if hasattr(signal, "num_signals") else 1,
                "fusion_score": signal.score,
                "ml_p_up": ml_p_up,
                "ml_edge": ml_edge,
                "market_slug": slug,
                "signal_sources": (
                    [s.source for s in signal.contributing_signals]
                    if hasattr(signal, "contributing_signals") and signal.contributing_signals
                    else [str(signal.direction).replace("SignalDirection.", "")]
                ),
            },
        )

        if hasattr(self, "grafana_exporter") and self.grafana_exporter:
            self.grafana_exporter.increment_trade_counter(won=(pnl > 0))
            self.grafana_exporter.record_trade_duration(exit_delta.total_seconds())

        icon = "[WIN]" if outcome == "WIN" else "[LOSS]"
        logger.info("=" * 80)
        logger.info(f"[SIM] TRADE #{session_num}  {icon}  {direction.upper()}")
        logger.info(f"  Market:     {slug or 'unknown'}")
        logger.info(
            f"  Entry prob: {float(current_price):.4f}  -->  "
            f"Exit: {exit_price_raw:.4f}  (move: {movement:+.4f})"
        )
        logger.info(f"  BTC spot:   ${metadata.get('spot_price', 0):,.0f}")
        logger.info(f"  P&L:        ${pnl:+.4f}  ({pnl_pct*100:+.2f}%)")
        logger.info(f"  ML p(UP):   {ml_p_up:.3f}  edge={ml_edge:.3f}")
        logger.info(
            f"  Signal:     score={signal.score:.1f}  conf={signal.confidence:.2%}  "
            f"n={signal.num_signals if hasattr(signal,'num_signals') else 1}"
        )
        logger.info(
            f"  Vol regime: {metadata.get('vol_regime', 'N/A')}   "
            f"funding={metadata.get('funding_rate', 0):.5%}"
        )
        logger.info(
            f"  Session:    {wins}/{len(settled)} wins  ({win_rate:.1%})   "
            f"cumulative PnL=${total_pnl:+.4f}"
        )
        logger.info("=" * 80)

        self._save_paper_trades()

    def _save_paper_trades(self) -> None:
        try:
            trades_data = [t.to_dict() for t in self.paper_trades]
            with open("paper_trades.json", "w") as f:
                json.dump(trades_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    # ── Real order ────────────────────────────────────────────────────────────

    async def _place_real_order(self, signal, position_size, current_price: Decimal, direction: str) -> None:
        if not self.instrument_id:
            logger.error("No instrument available — instrument cache not yet loaded")
            return

        # Refuse to send a live BUY if the market-order patch is missing,
        # otherwise Nautilus would treat the dummy 5-token quantity as the
        # real order size and we'd massively over-spend.
        try:
            from patches.market_orders import _patch_applied as _mo_patch_applied
        except Exception:
            _mo_patch_applied = False
        if not _mo_patch_applied:
            logger.error(
                "Market-order patch is NOT applied — refusing to submit live BUY "
                "(would otherwise send a 5-token order instead of the configured "
                "USD amount)."
            )
            self._track_order_event("rejected")
            return

        try:
            logger.info("=" * 80)
            logger.info("LIVE MODE — PLACING REAL ORDER!")
            logger.info("=" * 80)

            side = OrderSide.BUY

            if direction == "long":
                trade_instrument_id = getattr(self, "_yes_instrument_id", None) or self.instrument_id
                trade_label = "YES (UP)"
            else:
                no_id = getattr(self, "_no_instrument_id", None)
                if no_id is None:
                    logger.warning("NO token instrument not found — cannot bet DOWN. Skipping.")
                    return
                trade_instrument_id = no_id
                trade_label = "NO (DOWN)"

            instrument = self.cache.instrument(trade_instrument_id)
            if not instrument:
                logger.error(f"Instrument not in cache: {trade_instrument_id}")
                return

            logger.info(f"Buying {trade_label} token: {trade_instrument_id}")

            # The patch reads MARKET_BUY_USD at submit time. Use it here too so
            # logs and the risk engine see the same number that will actually
            # be sent on-chain.
            try:
                max_usd_amount = max(0.01, float(os.getenv("MARKET_BUY_USD", str(float(position_size)))))
            except (TypeError, ValueError):
                max_usd_amount = float(position_size)

            precision = instrument.size_precision

            # The patch ignores this quantity for BUY orders and uses USD
            # instead, but Nautilus still validates against instrument.min_quantity
            # before the patch runs, so we pass a value just above the minimum.
            min_qty_val = float(getattr(instrument, "min_quantity", None) or 5.0)
            token_qty = round(max(min_qty_val, 5.0), precision)

            logger.info(
                f"BUY {trade_label}: ${max_usd_amount:.2f} USD "
                f"(Nautilus placeholder qty={token_qty:.6f})"
            )

            qty = Quantity(token_qty, precision=precision)
            timestamp_ms = int(time.time() * 1000)
            # Plain alphanumeric/dash id only — some venues reject special chars.
            usd_label = str(int(round(max_usd_amount * 100))).zfill(4)
            unique_id = f"BTC-15M-{usd_label}-{timestamp_ms}"

            order = self.order_factory.market(
                instrument_id=trade_instrument_id,
                order_side=side,
                quantity=qty,
                client_order_id=ClientOrderId(unique_id),
                quote_quantity=False,
                time_in_force=TimeInForce.IOC,
            )

            self.submit_order(order)

            logger.info("REAL ORDER SUBMITTED!")
            logger.info(f"  Order ID:  {unique_id}")
            logger.info(f"  Direction: {trade_label}")
            logger.info(f"  Notional:  ~${max_usd_amount:.2f}")
            logger.info(f"  Price:     ${float(current_price):.4f}")
            logger.info("=" * 80)

            # Stash metadata for on_order_filled so the risk engine can be
            # updated with the actual fill price/size when the trade lands.
            if not hasattr(self, "_pending_orders"):
                self._pending_orders: Dict[str, dict] = {}
            self._pending_orders[unique_id] = {
                "instrument_id": trade_instrument_id,
                "direction": direction,
                "size_usd": max_usd_amount,
                "expected_price": float(current_price),
                "label": trade_label,
            }

            self._track_order_event("placed")

        except Exception as e:
            logger.error(f"Error placing real order: {e}")
            import traceback
            traceback.print_exc()
            self._track_order_event("rejected")

    # ── Signal processing ─────────────────────────────────────────────────────

    def _process_signals(self, current_price: Decimal, metadata: dict = None) -> list:
        signals = []
        if metadata is None:
            metadata = {}

        proc_meta = {
            k: Decimal(str(v)) if isinstance(v, float) else v
            for k, v in metadata.items()
        }

        spike_signal = self.spike_detector.process(current_price, self.price_history, proc_meta)
        if spike_signal:
            signals.append(spike_signal)

        if "sentiment_score" in proc_meta:
            s = self.sentiment_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if "spot_price" in proc_meta:
            s = self.divergence_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if proc_meta.get("yes_token_id"):
            s = self.orderbook_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if proc_meta.get("tick_buffer"):
            s = self.tick_velocity_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        pcr_signal = self.deribit_pcr_processor.process(current_price, self.price_history, proc_meta)
        if pcr_signal:
            signals.append(pcr_signal)

        liq_signal = self.liquidation_processor.process(current_price, self.price_history, proc_meta)
        if liq_signal:
            signals.append(liq_signal)

        fi_signal = self.funding_oi_processor.process(current_price, self.price_history, proc_meta)
        if fi_signal:
            signals.append(fi_signal)

        cvd_signal = self.cvd_ob_processor.process(current_price, self.price_history, proc_meta)
        if cvd_signal:
            signals.append(cvd_signal)

        ohlcv_signal = self.ohlcv_momentum_processor.process(current_price, self.price_history, proc_meta)
        if ohlcv_signal:
            signals.append(ohlcv_signal)

        return signals

    # ── Order events ──────────────────────────────────────────────────────────

    def _track_order_event(self, event_type: str) -> None:
        try:
            pt = self.performance_tracker
            if hasattr(pt, "record_order_event"):
                pt.record_order_event(event_type)
            elif hasattr(pt, "increment_counter"):
                pt.increment_counter(event_type)
            elif hasattr(pt, "increment_order_counter"):
                pt.increment_order_counter(event_type)
            else:
                logger.debug(f"PerformanceTracker: no order-counter method for '{event_type}'")
        except Exception as e:
            logger.warning(f"Failed to track order event '{event_type}': {e}")

    def on_order_filled(self, event) -> None:
        logger.info("=" * 80)
        logger.info("ORDER FILLED!")
        logger.info(f"  Order:       {event.client_order_id}")
        logger.info(f"  Fill Price:  ${float(event.last_px):.4f}")
        logger.info(f"  Quantity:    {float(event.last_qty):.6f}")
        logger.info("=" * 80)
        self._track_order_event("filled")

        # Register the live fill with the risk engine so total exposure is
        # actually enforced (otherwise get_total_exposure() always sees 0).
        try:
            client_id = str(event.client_order_id)
            pending = getattr(self, "_pending_orders", {}).pop(client_id, None)
            if pending is not None:
                fill_price = Decimal(str(float(event.last_px)))
                self.risk_engine.add_position(
                    position_id=client_id,
                    size=Decimal(str(pending["size_usd"])),
                    entry_price=fill_price,
                    direction=pending["direction"],
                )
        except Exception as e:
            logger.warning(f"Failed to record live fill in risk engine: {e}")

    def on_order_denied(self, event) -> None:
        logger.error("=" * 80)
        logger.error("ORDER DENIED!")
        logger.error(f"  Order:  {event.client_order_id}")
        logger.error(f"  Reason: {event.reason}")
        logger.error("=" * 80)
        self._track_order_event("rejected")
        self._discard_pending_order(event)

    def on_order_rejected(self, event) -> None:
        reason = str(getattr(event, "reason", ""))
        if any(kw in reason.lower() for kw in ("no orders found", "fak", "no match")):
            logger.warning(
                f"FAK rejected (no liquidity) — resetting timer to retry\n  Reason: {reason}"
            )
            self.last_trade_time = -1
        else:
            logger.warning(f"Order rejected: {reason}")
        self._track_order_event("rejected")
        self._discard_pending_order(event)

    def _discard_pending_order(self, event) -> None:
        """Remove a no-longer-live order from the pending map (best effort)."""
        try:
            client_id = str(getattr(event, "client_order_id", ""))
            pending = getattr(self, "_pending_orders", None)
            if pending and client_id in pending:
                pending.pop(client_id, None)
        except Exception:
            pass

    # ── Grafana / stop ────────────────────────────────────────────────────────

    def _start_grafana_sync(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.grafana_exporter.start())
            logger.info("Grafana metrics started on port 8000")
        except Exception as e:
            logger.error(f"Failed to start Grafana: {e}")

    def on_stop(self) -> None:
        logger.info("Integrated BTC strategy stopped")
        logger.info(f"Total paper trades recorded: {len(self.paper_trades)}")
        if self.grafana_exporter:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(self.grafana_exporter.stop())
            except Exception:
                pass
