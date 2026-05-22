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
from pathlib import Path
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
    LiveTrade,
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

        # ── Live position / exit management ─────────────────────────────────
        # Pending BUY orders awaiting their first fill report.
        self._pending_orders: Dict[str, dict] = {}
        # Open positions, keyed by entry client_order_id, that the live
        # stop-loss / take-profit handler watches on every quote tick.
        self._open_positions: Dict[str, dict] = {}
        # Outstanding exit (SELL) orders, mapping exit client_id -> entry id.
        self._pending_exits: Dict[str, str] = {}

        try:
            self._stop_loss_pct = max(0.0, float(os.getenv("STOP_LOSS_PCT", "0.30")))
        except (TypeError, ValueError):
            self._stop_loss_pct = 0.30
        try:
            self._take_profit_pct = max(0.0, float(os.getenv("TAKE_PROFIT_PCT", "0.20")))
        except (TypeError, ValueError):
            self._take_profit_pct = 0.20
        # Don't try to exit in the last few seconds before settlement —
        # FAK rejects + settlement happen too fast to be useful.
        self._exit_cutoff_seconds = 30

        # ── Live realised-P&L tracking ──────────────────────────────────────
        # Closed live trades, mirror of `paper_trades.json` for the live path.
        self.live_trades: List[LiveTrade] = []
        self._live_session_num: int = 0
        # Hard cap (seconds after market end) before we give up waiting for a
        # definitive settlement price and resolve at the last seen bid.
        self._settle_grace_seconds: int = 600  # 10 min

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

            # Settle positions whose underlying market has already resolved.
            # Polymarket auto-resolves binary markets at end_timestamp; the
            # held token pays $1 to the winner / $0 to the loser. We compute
            # the realised P&L from a definitive settlement source (the
            # SettlementTracker's Chainlink outcome) and fall back to the
            # last observed bid for the held token if Chainlink is offline.
            self._settle_open_positions(now)

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
            bid, ask = tick.bid_price, tick.ask_price
            if bid is None or ask is None:
                return

            try:
                bid_decimal = bid.as_decimal()
                ask_decimal = ask.as_decimal()
            except Exception:
                return

            # Live exit check runs for every tick on any held token (YES or
            # NO), independent of which market is currently active for trading.
            try:
                self._check_position_exits(tick.instrument_id, bid_decimal, ask_decimal)
            except Exception as e:
                logger.warning(f"Exit check failed: {e}")

            # Everything below is signal/decision logic for the active market
            # only. Position exits already ran above for any held instrument.
            if self.instrument_id is None or tick.instrument_id != self.instrument_id:
                return

            now = datetime.now(timezone.utc)

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
            await self._place_real_order(
                signal_for_logging,
                POSITION_SIZE_USD,
                current_price,
                direction,
                ml_trade_id=trade_id,
            )

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

    async def _place_real_order(
        self,
        signal,
        position_size,
        current_price: Decimal,
        direction: str,
        ml_trade_id: Optional[int] = None,
    ) -> None:
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

            # Subscribe to ticks for the held token *before* the fill arrives so
            # the live stop-loss handler starts seeing prices immediately.
            if trade_instrument_id != self.instrument_id:
                try:
                    self.subscribe_quote_ticks(trade_instrument_id)
                    logger.info(f"  Subscribed to held-token ticks: {trade_instrument_id}")
                except Exception as e:
                    logger.warning(
                        f"Could not subscribe to {trade_instrument_id} for exit "
                        f"monitoring: {e}"
                    )

            logger.info("REAL ORDER SUBMITTED!")
            logger.info(f"  Order ID:  {unique_id}")
            logger.info(f"  Direction: {trade_label}")
            logger.info(f"  Notional:  ~${max_usd_amount:.2f}")
            logger.info(f"  Price:     ${float(current_price):.4f}")
            logger.info(
                f"  Stop/TP:   -{self._stop_loss_pct:.0%} / +{self._take_profit_pct:.0%}"
            )
            logger.info("=" * 80)

            market_end_ts = 0
            market_slug = ""
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                cur = self.all_btc_instruments[self.current_instrument_index]
                market_end_ts = int(cur.get("end_timestamp", 0))
                market_slug = str(cur.get("slug", ""))

            # Capture signal context for the LiveTrade record so live trades
            # carry the same diagnostic fields as paper trades.
            signal_score = float(getattr(signal, "score", 0.0) or 0.0)
            signal_conf = float(getattr(signal, "confidence", 0.0) or 0.0)

            # Stash metadata for on_order_filled so the risk engine can be
            # updated with the actual fill price/size when the trade lands.
            self._pending_orders[unique_id] = {
                "side": "BUY",
                "instrument_id": trade_instrument_id,
                "direction": direction,
                "size_usd": max_usd_amount,
                "expected_price": float(current_price),
                "label": trade_label,
                "market_end_ts": market_end_ts,
                "market_slug": market_slug,
                "ml_trade_id": ml_trade_id,
                "signal_score": signal_score,
                "signal_confidence": signal_conf,
                "stop_loss_pct": self._stop_loss_pct,
                "take_profit_pct": self._take_profit_pct,
                "submitted_at": datetime.now(timezone.utc),
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
        client_id = str(event.client_order_id)
        try:
            fill_price = Decimal(str(float(event.last_px)))
            fill_qty = Decimal(str(float(event.last_qty)))
        except Exception:
            fill_price = Decimal("0")
            fill_qty = Decimal("0")

        logger.info("=" * 80)
        logger.info("ORDER FILLED!")
        logger.info(f"  Order:       {client_id}")
        logger.info(f"  Fill Price:  ${float(fill_price):.4f}")
        logger.info(f"  Quantity:    {float(fill_qty):.6f}")
        logger.info("=" * 80)
        self._track_order_event("filled")

        # Branch on whether this fill closed an existing position (SELL) or
        # opened a new one (BUY).
        entry_id = self._pending_exits.pop(client_id, None)
        if entry_id is not None:
            self._handle_exit_fill(entry_id, client_id, fill_price)
            return

        pending = self._pending_orders.pop(client_id, None)
        if pending is not None:
            self._handle_entry_fill(client_id, pending, fill_price, fill_qty)

    def _handle_entry_fill(
        self,
        client_id: str,
        pending: dict,
        fill_price: Decimal,
        fill_qty: Decimal,
    ) -> None:
        """Track a freshly-opened position so the exit handler can monitor it."""
        try:
            self.risk_engine.add_position(
                position_id=client_id,
                size=Decimal(str(pending["size_usd"])),
                entry_price=fill_price,
                direction=pending["direction"],
            )
        except Exception as e:
            logger.warning(f"Failed to record live fill in risk engine: {e}")

        sl_pct = Decimal(str(pending.get("stop_loss_pct", self._stop_loss_pct)))
        tp_pct = Decimal(str(pending.get("take_profit_pct", self._take_profit_pct)))
        # Stop-loss / take-profit thresholds are relative to the held token's
        # price. For both LONG (YES bought) and SHORT (NO bought), a falling
        # token price means the position is losing.
        stop_loss = max(Decimal("0.01"), fill_price * (Decimal("1") - sl_pct))
        take_profit = min(Decimal("0.99"), fill_price * (Decimal("1") + tp_pct))

        position = {
            "instrument_id": pending["instrument_id"],
            "direction": pending["direction"],
            "label": pending.get("label", ""),
            "size_usd": float(pending["size_usd"]),
            "entry_price": fill_price,
            "filled_qty": fill_qty,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "market_end_ts": int(pending.get("market_end_ts", 0)),
            "market_slug": pending.get("market_slug", ""),
            "ml_trade_id": pending.get("ml_trade_id"),
            "signal_score": float(pending.get("signal_score", 0.0) or 0.0),
            "signal_confidence": float(pending.get("signal_confidence", 0.0) or 0.0),
            "exit_in_flight": False,
            "exit_order_id": None,
            "opened_at": datetime.now(timezone.utc),
            # Latest bid seen for the held token; updated on every quote tick
            # by ``_check_position_exits``. Used as a settlement-price source
            # when the market auto-resolves (no manual exit).
            "last_bid": None,
            "last_bid_ts": None,
            "last_bid_post_settle": None,
            "last_bid_post_settle_ts": None,
        }
        self._open_positions[client_id] = position

        logger.info(
            f"POSITION OPEN — {pending.get('label', '')} qty={float(fill_qty):.4f} "
            f"@ ${float(fill_price):.4f}  "
            f"stop=${float(stop_loss):.4f}  tp=${float(take_profit):.4f}"
        )

    def _handle_exit_fill(
        self,
        entry_id: str,
        exit_id: str,
        fill_price: Decimal,
    ) -> None:
        """An exit (SELL) order filled — close out the underlying position."""
        position = self._open_positions.pop(entry_id, None)
        if position is None:
            logger.warning(f"Exit fill {exit_id} had no matching open position {entry_id}")
            return

        # Decide whether this exit was driven by stop-loss, take-profit, or a
        # generic mid-market sell. ``fill_price`` may differ slightly from the
        # trigger threshold (slippage), so use small tolerances.
        try:
            sl = position["stop_loss"]
            tp = position["take_profit"]
            if fill_price >= tp:
                close_reason = "EXIT_TP"
            elif fill_price <= sl:
                close_reason = "EXIT_STOP"
            else:
                close_reason = "EXIT_MANUAL"
        except Exception:
            close_reason = "EXIT_MANUAL"

        self._close_live_position(
            entry_id=entry_id,
            position=position,
            exit_price=fill_price,
            exit_order_id=exit_id,
            close_reason=close_reason,
        )

    # ── Live realised-P&L recording ─────────────────────────────────────────

    def _close_live_position(
        self,
        entry_id: str,
        position: dict,
        exit_price: Decimal,
        exit_order_id: Optional[str],
        close_reason: str,
    ) -> None:
        """Record a closed live position's realised P&L in every consumer.

        - Removes the position from the risk engine
        - Appends a ``LiveTrade`` to ``self.live_trades`` and persists it
        - Forwards the trade to the global PerformanceTracker so cumulative
          metrics (win rate, ROI, Sharpe, drawdown) include live results
        """
        try:
            self.risk_engine.remove_position(entry_id, exit_price=exit_price)
        except Exception as e:
            logger.warning(f"Failed to remove position from risk engine: {e}")

        entry_price_dec = position["entry_price"] if isinstance(position["entry_price"], Decimal) \
            else Decimal(str(position["entry_price"]))
        qty_dec = position["filled_qty"] if isinstance(position["filled_qty"], Decimal) \
            else Decimal(str(position["filled_qty"]))

        entry_price_f = float(entry_price_dec)
        exit_price_f = float(exit_price)
        qty_f = float(qty_dec)

        realized = qty_f * (exit_price_f - entry_price_f)
        pnl_pct = (exit_price_f - entry_price_f) / entry_price_f if entry_price_f > 0 else 0.0

        if realized > 1e-6:
            outcome = "WIN"
        elif realized < -1e-6:
            outcome = "LOSS"
        else:
            outcome = "BREAKEVEN"
        if close_reason == "SETTLEMENT_UNRESOLVED":
            outcome = "UNRESOLVED"

        self._live_session_num += 1
        opened_at = position.get("opened_at") or datetime.now(timezone.utc)
        closed_at = datetime.now(timezone.utc)

        live_trade = LiveTrade(
            trade_id=entry_id,
            ml_trade_id=position.get("ml_trade_id"),
            timestamp=opened_at,
            closed_at=closed_at,
            direction=str(position.get("direction", "")).upper(),
            label=position.get("label", ""),
            market_slug=position.get("market_slug", ""),
            size_usd=float(position.get("size_usd", 0.0)),
            filled_qty=qty_f,
            entry_price=entry_price_f,
            exit_price=exit_price_f,
            pnl_usd=realized,
            pnl_pct=pnl_pct,
            outcome=outcome,
            close_reason=close_reason,
            entry_order_id=entry_id,
            exit_order_id=exit_order_id,
            session_trade_num=self._live_session_num,
        )
        self.live_trades.append(live_trade)

        # Mirror into the global performance tracker so Grafana, summaries,
        # and the supervisor dashboard reflect live performance.
        try:
            self.performance_tracker.record_trade(
                trade_id=entry_id,
                direction=str(position.get("direction", "long")),
                entry_price=entry_price_dec,
                exit_price=Decimal(str(exit_price_f)),
                size=Decimal(str(position.get("size_usd", 0.0))),
                entry_time=opened_at,
                exit_time=closed_at,
                signal_score=float(position.get("signal_score", 0.0) or 0.0),
                signal_confidence=float(position.get("signal_confidence", 0.0) or 0.0),
                metadata={
                    "simulated": False,
                    "close_reason": close_reason,
                    "market_slug": position.get("market_slug", ""),
                    "label": position.get("label", ""),
                    "filled_qty": qty_f,
                    "ml_trade_id": position.get("ml_trade_id"),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to record live trade in PerformanceTracker: {e}")

        # Live-session running totals (analogous to the simulation block).
        wins = sum(1 for t in self.live_trades if t.outcome == "WIN")
        losses = sum(1 for t in self.live_trades if t.outcome == "LOSS")
        total_pnl = sum(t.pnl_usd for t in self.live_trades)

        if hasattr(self, "grafana_exporter") and self.grafana_exporter and outcome in ("WIN", "LOSS"):
            try:
                self.grafana_exporter.increment_trade_counter(won=(outcome == "WIN"))
                self.grafana_exporter.record_trade_duration(
                    (closed_at - opened_at).total_seconds()
                )
            except Exception:
                pass

        marker = {
            "EXIT_TP": "[TAKE-PROFIT]",
            "EXIT_STOP": "[STOP-LOSS]",
            "EXIT_MANUAL": "[MANUAL EXIT]",
            "SETTLEMENT": "[SETTLED]",
            "SETTLEMENT_FALLBACK": "[SETTLED*]",
            "SETTLEMENT_UNRESOLVED": "[UNRESOLVED]",
        }.get(close_reason, f"[{close_reason}]")

        logger.info("=" * 80)
        logger.info(
            f"[LIVE] TRADE #{self._live_session_num}  {marker}  "
            f"{live_trade.direction}  {outcome}"
        )
        logger.info(f"  Market:   {live_trade.market_slug or 'unknown'}")
        logger.info(
            f"  Entry:    ${entry_price_f:.4f}  -->  Exit: ${exit_price_f:.4f}  "
            f"Qty: {qty_f:.4f}"
        )
        logger.info(
            f"  P&L:      ${realized:+.4f}  ({pnl_pct*100:+.2f}%)  "
            f"Notional=${live_trade.size_usd:.2f}"
        )
        logger.info(
            f"  Session:  {wins}W/{losses}L  cumulative=${total_pnl:+.4f}"
        )
        logger.info("=" * 80)

        self._save_live_trades()

    def _save_live_trades(self) -> None:
        """Persist closed live trades to ``live_trades.json`` in the project root."""
        try:
            path = Path("live_trades.json")
            with path.open("w") as f:
                json.dump([t.to_dict() for t in self.live_trades], f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save live trades: {e}")

    def _settle_open_positions(self, now: datetime) -> None:
        """Resolve realised P&L for positions whose market has already ended.

        Resolution sources, in order of preference:
          1. ``SettlementTracker.get_resolved_outcome(ml_trade_id)`` — definitive
             1.0 / 0.0 settlement based on Chainlink BTC/USD
          2. Last bid observed *after* market_end_ts (Polymarket's CLOB writes
             1.0 / 0.0 quotes after on-chain resolution)
          3. After ``_settle_grace_seconds`` (default 10 min), a fallback to
             the most recent bid even if it isn't at an extreme; the trade is
             marked ``UNRESOLVED`` so the user can review it.
        """
        if not self._open_positions:
            return

        now_ts = int(now.timestamp())

        for entry_id, position in list(self._open_positions.items()):
            end_ts = int(position.get("market_end_ts") or 0)
            if not end_ts or now_ts < end_ts:
                continue
            if position.get("exit_in_flight"):
                # Manual exit is still in flight — wait for its fill report
                # rather than double-counting at settlement.
                continue

            ml_trade_id = position.get("ml_trade_id")
            settle_price: Optional[Decimal] = None
            close_reason = "SETTLEMENT"

            # Source 1: Chainlink-backed outcome from the settlement tracker.
            if ml_trade_id is not None:
                try:
                    resolved = self.settlement_tracker.get_resolved_outcome(ml_trade_id)
                except Exception:
                    resolved = None
                if resolved is not None:
                    direction = str(position.get("direction", "long"))
                    won = (resolved["outcome"] == 1 and direction == "long") or \
                          (resolved["outcome"] == 0 and direction == "short")
                    settle_price = Decimal("1") if won else Decimal("0")

            # Source 2: last post-settlement bid clamped to {0, 1} when the
            # CLOB has clearly resolved.
            if settle_price is None:
                ps_bid = position.get("last_bid_post_settle")
                if ps_bid is not None:
                    if Decimal(str(ps_bid)) >= Decimal("0.95"):
                        settle_price = Decimal("1")
                    elif Decimal(str(ps_bid)) <= Decimal("0.05"):
                        settle_price = Decimal("0")

            # Source 3: grace-period fallback.
            if settle_price is None:
                if (now_ts - end_ts) >= self._settle_grace_seconds:
                    fallback_bid = (
                        position.get("last_bid_post_settle")
                        or position.get("last_bid")
                    )
                    if fallback_bid is not None:
                        settle_price = Decimal(str(fallback_bid))
                        close_reason = "SETTLEMENT_FALLBACK"
                    else:
                        # No price at all — close at entry, mark unresolved.
                        settle_price = position["entry_price"] if isinstance(
                            position["entry_price"], Decimal
                        ) else Decimal(str(position["entry_price"]))
                        close_reason = "SETTLEMENT_UNRESOLVED"
                else:
                    # Still within grace window — try again on the next tick.
                    continue

            self._open_positions.pop(entry_id, None)
            self._close_live_position(
                entry_id=entry_id,
                position=position,
                exit_price=settle_price,
                exit_order_id=None,
                close_reason=close_reason,
            )

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
        """Clean up state for an order that was denied/rejected before any fill.

        Handles both entry (BUY) and exit (SELL) orders so a failed exit lets
        the next quote tick try again instead of hanging forever.
        """
        try:
            client_id = str(getattr(event, "client_order_id", ""))
        except Exception:
            return

        # Failed entry: drop pending metadata.
        if client_id in self._pending_orders:
            self._pending_orders.pop(client_id, None)
            return

        # Failed exit: clear in-flight flag so the position retries on the
        # next eligible tick.
        entry_id = self._pending_exits.pop(client_id, None)
        if entry_id is not None and entry_id in self._open_positions:
            position = self._open_positions[entry_id]
            position["exit_in_flight"] = False
            position["exit_order_id"] = None
            logger.warning(
                f"Exit order {client_id} failed for position {entry_id} — "
                f"will retry on next tick"
            )

    # ── Live position exits ──────────────────────────────────────────────────

    def _check_position_exits(
        self,
        instrument_id,
        bid: Decimal,
        ask: Decimal,
    ) -> None:
        """Inspect every open position on this instrument; submit exits if hit."""
        if not self._open_positions:
            return

        now_ts = int(datetime.now(timezone.utc).timestamp())

        for entry_id, position in list(self._open_positions.items()):
            if position["instrument_id"] != instrument_id:
                continue

            # Always refresh the most recent bid for this position. The
            # post-settlement bid is what the settlement reaper uses to
            # determine realised P&L when no manual exit fired.
            position["last_bid"] = bid
            position["last_bid_ts"] = now_ts
            end_ts = position.get("market_end_ts", 0)
            if end_ts and now_ts >= end_ts:
                position["last_bid_post_settle"] = bid
                position["last_bid_post_settle_ts"] = now_ts

            if position["exit_in_flight"]:
                continue
            if position["filled_qty"] <= 0:
                continue

            # Skip exits in the final seconds before settlement: the FAK is
            # likely to fail and the binary outcome is about to pay out.
            if end_ts and (end_ts - now_ts) <= self._exit_cutoff_seconds:
                continue

            stop_loss = position["stop_loss"]
            take_profit = position["take_profit"]

            trigger: Optional[str] = None
            if bid <= stop_loss:
                trigger = "STOP-LOSS"
            elif bid >= take_profit:
                trigger = "TAKE-PROFIT"

            if trigger is None:
                continue

            logger.warning(
                f"{trigger} TRIGGERED for {position.get('label', '')} "
                f"(entry=${float(position['entry_price']):.4f} "
                f"bid=${float(bid):.4f} stop=${float(stop_loss):.4f} "
                f"tp=${float(take_profit):.4f})"
            )

            self._submit_exit_order(entry_id, position, trigger)

    def _submit_exit_order(self, entry_id: str, position: dict, reason: str) -> None:
        """Submit a market SELL for the held token quantity to close ``position``."""
        instrument_id = position["instrument_id"]
        instrument = self.cache.instrument(instrument_id)
        if not instrument:
            logger.error(f"Cannot exit {entry_id}: instrument {instrument_id} not in cache")
            return

        precision = instrument.size_precision
        # Polymarket market SELL is base-denominated (token quantity) per the
        # adapter patch, so send the exact filled quantity.
        try:
            qty_float = round(float(position["filled_qty"]), precision)
        except Exception:
            qty_float = float(position["filled_qty"])

        if qty_float <= 0:
            logger.error(f"Cannot exit {entry_id}: zero quantity")
            return

        try:
            qty = Quantity(qty_float, precision=precision)
        except Exception as e:
            logger.error(f"Failed to build Quantity for exit: {e}")
            return

        timestamp_ms = int(time.time() * 1000)
        # Keep a deterministic prefix so the exit can be correlated back to the
        # entry in audit logs.
        suffix = entry_id.split("-")[-1][-6:] if "-" in entry_id else "000000"
        exit_id = f"EXIT-{suffix}-{timestamp_ms}"

        try:
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.SELL,
                quantity=qty,
                client_order_id=ClientOrderId(exit_id),
                quote_quantity=False,
                time_in_force=TimeInForce.IOC,
            )
            self.submit_order(order)
        except Exception as e:
            logger.error(f"Failed to submit exit order: {e}")
            return

        position["exit_in_flight"] = True
        position["exit_order_id"] = exit_id
        self._pending_exits[exit_id] = entry_id

        logger.info("=" * 80)
        logger.info(f"EXIT ORDER SUBMITTED — {reason}")
        logger.info(f"  Entry ID: {entry_id}")
        logger.info(f"  Exit ID:  {exit_id}")
        logger.info(f"  SELL qty: {qty_float:.6f} of {instrument_id}")
        logger.info("=" * 80)
        self._track_order_event("placed")

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
        if self.live_trades:
            wins = sum(1 for t in self.live_trades if t.outcome == "WIN")
            losses = sum(1 for t in self.live_trades if t.outcome == "LOSS")
            total = sum(t.pnl_usd for t in self.live_trades)
            logger.info(
                f"Live trades recorded: {len(self.live_trades)} "
                f"({wins}W / {losses}L)  cumulative P&L=${total:+.4f}"
            )
            self._save_live_trades()
        if self.grafana_exporter:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(self.grafana_exporter.stop())
            except Exception:
                pass
