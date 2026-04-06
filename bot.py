import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import math
from decimal import Decimal
import time
from dataclasses import dataclass
from typing import List, Optional, Dict
import random

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


try:
    from patch_gamma_markets import apply_gamma_markets_patch, verify_patch
    patch_applied = apply_gamma_markets_patch()
    if patch_applied:
        verify_patch()
    else:
        print("ERROR: Failed to apply gamma_market patch")
        sys.exit(1)
except ImportError as e:
    print(f"ERROR: Could not import patch module: {e}")
    print("Make sure patch_gamma_markets.py is in the same directory")
    sys.exit(1)

# Now import Nautilus
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import (
    PolymarketDataClientConfig,
    PolymarketExecClientConfig,
)
from nautilus_trader.adapters.polymarket.factories import (
    PolymarketLiveDataClientFactory,
    PolymarketLiveExecClientFactory,
)
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, ClientOrderId
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data import QuoteTick

from dotenv import load_dotenv
from loguru import logger
import redis

# Import our phases
from core.strategy_brain.signal_processors.spike_detector import SpikeDetectionProcessor
from core.strategy_brain.signal_processors.sentiment_processor import SentimentProcessor
from core.strategy_brain.signal_processors.divergence_processor import PriceDivergenceProcessor
from core.strategy_brain.signal_processors.orderbook_processor import OrderBookImbalanceProcessor
from core.strategy_brain.signal_processors.tick_velocity_processor import TickVelocityProcessor
from core.strategy_brain.signal_processors.deribit_pcr_processor import DeribitPCRProcessor

from core.strategy_brain.signal_processors.liquidation_processor import LiquidationProcessor
from core.strategy_brain.signal_processors.funding_rate_oi_processor import FundingRateOIProcessor
from core.strategy_brain.signal_processors.cvd_orderbook_processor import CVDOrderBookProcessor
from core.strategy_brain.signal_processors.ohlcv_momentum_processor import OHLCVMomentumProcessor
from core.strategy_brain.ml_prediction_engine import get_ml_engine
from core.settlement_tracker import get_settlement_tracker
# ─────────────────────────────────────────────────────────────────────────────

from core.strategy_brain.fusion_engine.signal_fusion import get_fusion_engine
from execution.risk_engine import get_risk_engine
from monitoring.performance_tracker import get_performance_tracker
from monitoring.grafana_exporter import get_grafana_exporter
from feedback.learning_engine import get_learning_engine
load_dotenv()
from patch_market_orders import apply_market_order_patch
patch_applied = apply_market_order_patch()
if patch_applied:
    logger.info("Market order patch applied successfully")
else:
    logger.warning("Market order patch failed - orders may be rejected")


# =============================================================================
# CONSTANTS
# =============================================================================
QUOTE_STABILITY_REQUIRED = 3      # Need only 3 valid ticks to be stable (faster startup)
QUOTE_MIN_SPREAD = 0.001          # Both bid AND ask must be at least this
MARKET_INTERVAL_SECONDS = 900     # 15-minute markets


@dataclass
class PaperTrade:
    """Track paper/simulation trades with full context for analysis"""
    # Core trade fields
    trade_id: str
    timestamp: datetime
    direction: str          # "LONG" or "SHORT"
    size_usd: float
    entry_price: float      # Polymarket probability at entry
    exit_price: float       # Polymarket probability at exit
    pnl_usd: float          # simulated P&L in USD
    pnl_pct: float          # simulated P&L as fraction
    outcome: str            # "WIN" | "LOSS" | "PENDING"

    # Signal context
    signal_score: float
    signal_confidence: float
    num_signals: int = 0
    ml_p_up: float = 0.0    # ML model probability at time of trade
    ml_edge: float = 0.0    # abs(ml_p_up - poly_price)

    # Market context
    market_slug: str = ""
    btc_spot_price: float = 0.0  # BTC/USD from Coinbase at time of trade
    vol_regime: str = ""         # "low" | "medium" | "high"
    funding_rate: float = 0.0

    # Session tracking
    session_trade_num: int = 0   # trade # within this bot run

    def to_dict(self):
        return {
            'trade_id': self.trade_id,
            'timestamp': self.timestamp.isoformat(),
            'direction': self.direction,
            'size_usd': self.size_usd,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'pnl_usd': round(self.pnl_usd, 6),
            'pnl_pct': round(self.pnl_pct * 100, 4),
            'outcome': self.outcome,
            'signal_score': self.signal_score,
            'signal_confidence': self.signal_confidence,
            'num_signals': self.num_signals,
            'ml_p_up': round(self.ml_p_up, 4),
            'ml_edge': round(self.ml_edge, 4),
            'market_slug': self.market_slug,
            'btc_spot_price': self.btc_spot_price,
            'vol_regime': self.vol_regime,
            'funding_rate': round(self.funding_rate, 6),
            'session_trade_num': self.session_trade_num,
        }


def init_redis():
    """Initialize Redis connection for simulation mode control."""
    try:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 2)),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        redis_client.ping()
        logger.info("Redis connection established")
        return redis_client
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        logger.warning("Simulation mode will be static (from .env)")
        return None


def _make_stub_signal(direction: str, ml_p_up=None):
    """
    Minimal signal stub used for paper-trade logging when the ML model fires
    but no individual signal processor produced a fused signal.
    Avoids crashing _record_paper_trade which expects a signal with .direction,
    .score, .confidence, and .num_signals attributes.
    """
    from core.strategy_brain.signal_processors.base_processor import SignalDirection
    from dataclasses import dataclass
    from typing import List

    @dataclass
    class _Stub:
        direction: object
        score: float
        confidence: float
        num_signals: int = 0

    d = SignalDirection.BULLISH if direction == "long" else SignalDirection.BEARISH
    conf = ml_p_up if ml_p_up is not None else 0.60
    return _Stub(direction=d, score=conf * 100, confidence=conf)


class IntegratedBTCStrategy(Strategy):
    """
    Integrated BTC Strategy - FIXED VERSION
    - Subscribes immediately at startup
    - Forces stability for first trade
    - Correct timing for market switching
    """

    def __init__(self, redis_client=None, enable_grafana=True, test_mode=False):
        super().__init__()

        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = 90

        # Nautilus
        self.instrument_id = None
        self.redis_client = redis_client
        self.current_simulation_mode = False

        # Store ALL BTC instruments
        self.all_btc_instruments: List[Dict] = []
        self.current_instrument_index: int = -1
        self.next_switch_time: Optional[datetime] = None

        # Quote-stability tracking
        self._stable_tick_count = 0
        self._market_stable = False
        self._last_instrument_switch = None
        
        # =========================================================================
        # FIX 1: Force first trade by setting last_trade_time to -1
        # =========================================================================
        self.last_trade_time = -1  # Force first trade immediately!
        self._waiting_for_market_open = False  # True when waiting for a future market to open
        self._last_bid_ask = None  # (bid_decimal, ask_decimal) from last tick, for liquidity checks

        # Tick buffer: rolling 90s of ticks for TickVelocityProcessor
        from collections import deque
        self._tick_buffer: deque = deque(maxlen=500)  # ~500 ticks = well over 90s

        # YES token id for the current market (set in _load_all_btc_instruments)
        self._yes_token_id: Optional[str] = None

        # Phase 4: Signal Processors
        self.spike_detector = SpikeDetectionProcessor(
            spike_threshold=0.05,       # FIXED: was 0.15 (too high for probabilities)
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
            imbalance_threshold=0.30,   # 30% skew to signal
            min_book_volume=50.0,       # ignore illiquid books
        )
        self.tick_velocity_processor = TickVelocityProcessor(
            velocity_threshold_60s=0.015,  # 1.5% move in 60s
            velocity_threshold_30s=0.010,  # 1.0% move in 30s
        )
        self.deribit_pcr_processor = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20,
            bearish_pcr_threshold=0.70,
            max_days_to_expiry=2,
            cache_seconds=300,          # refresh every 5 min
        )

        # Phase 4: Signal Fusion — update weights for 6 processors
        self.fusion_engine = get_fusion_engine()
        # Rebalanced weights (must sum ≤ 1.0; higher = more influence)
        self.fusion_engine.set_weight("OrderBookImbalance", 0.30)  # best real-time signal
        self.fusion_engine.set_weight("TickVelocity",       0.25)  # fast poly momentum
        self.fusion_engine.set_weight("PriceDivergence",    0.18)  # spot momentum
        self.fusion_engine.set_weight("SpikeDetection",     0.12)  # mean reversion
        self.fusion_engine.set_weight("DeribitPCR",         0.10)  # institutional sentiment
        self.fusion_engine.set_weight("SentimentAnalysis",  0.05)  # daily F&G (weak)

        # Phase 5: Risk Management
        self.risk_engine = get_risk_engine()

        # Phase 6: Performance Tracking
        self.performance_tracker = get_performance_tracker()

        # Phase 7: Learning Engine
        self.learning_engine = get_learning_engine()

        # ── NEW: Missing 8 features ───────────────────────────────────────────
        # Feature 1: Liquidation cascade detector (Binance futures forceOrders stream)
        self.liquidation_processor = LiquidationProcessor(
            window_seconds=60,
            min_usd_threshold=500_000,
            imbalance_threshold=0.65,
        )

        # Feature 2: Funding rate + open interest (Binance futures REST)
        self.funding_oi_processor = FundingRateOIProcessor(
            bullish_funding_threshold=-0.0003,
            bearish_funding_threshold=0.0005,
            oi_change_threshold=0.02,
            cache_seconds=300,
        )

        # Features 3+4: CVD accumulator + Binance spot order book (WebSocket + REST)
        self.cvd_ob_processor = CVDOrderBookProcessor(
            cvd_window_seconds=900,
            cvd_threshold_usd=5_000_000,
            ob_imbalance_threshold=0.30,
        )

        # Features 5+6+7: OHLCV momentum + volatility regime + time-of-day
        self.ohlcv_momentum_processor = OHLCVMomentumProcessor(
            rsi_overbought=68.0,
            rsi_oversold=32.0,
            cache_seconds=120,
        )

        # Feature 8: Open interest already covered by funding_oi_processor above

        # Update fusion engine weights to include new processors
        self.fusion_engine.set_weight("Liquidations",    0.20)
        self.fusion_engine.set_weight("CVDOrderBook",    0.18)
        self.fusion_engine.set_weight("FundingRateOI",   0.10)
        self.fusion_engine.set_weight("OHLCVMomentum",   0.10)

        # ── NEW: ML prediction engine (Steps 2–4 of 6-step loop) ─────────────
        self.ml_engine = get_ml_engine()

        # ── NEW: Settlement tracker (Steps 5–6 of 6-step loop) ───────────────
        self.settlement_tracker = get_settlement_tracker()
        # ─────────────────────────────────────────────────────────────────────

        # Phase 6: Grafana (optional)
        if enable_grafana:
            self.grafana_exporter = get_grafana_exporter()
        else:
            self.grafana_exporter = None

        # Price history
        self.price_history = []
        self.max_history = 100

        # Paper trading tracker
        self.paper_trades: List[PaperTrade] = []

        self.test_mode = test_mode

        # Weekly learning engine scheduler
        # Tracks last time learning_engine.optimize_weights() ran mid-session
        self._last_learning_optimization: datetime = datetime.now(timezone.utc)
        # In test_mode run every 5 min so you can see it work; otherwise weekly
        self._learning_interval_hours: float = (5 / 60) if test_mode else (7 * 24)

        if test_mode:
            logger.info("=" * 80)
            logger.info("  TEST MODE ACTIVE - Trading every minute!")
            logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY INITIALIZED - FIXED VERSION")
        logger.info("  Phase 4: Signal processors ready")
        logger.info("  Phase 5: Risk engine ready")
        logger.info("  Phase 6: Performance tracking ready")
        logger.info("  Phase 7: Learning engine ready")
        logger.info("  $1 per trade maximum")
        logger.info("=" * 80)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _seconds_to_next_15min_boundary(self) -> float:
        """Return seconds until the next 15-minute UTC boundary."""
        now_ts = datetime.now(timezone.utc).timestamp()
        next_boundary = (math.floor(now_ts / MARKET_INTERVAL_SECONDS) + 1) * MARKET_INTERVAL_SECONDS
        return next_boundary - now_ts

    def _is_quote_valid(self, bid, ask) -> bool:
        """Return True only when BOTH bid and ask are present and make sense."""
        if bid is None or ask is None:
            return False
        try:
            b = float(bid)
            a = float(ask)
        except (TypeError, ValueError):
            return False
        if b < QUOTE_MIN_SPREAD or a < QUOTE_MIN_SPREAD:
            return False
        if b > 0.999 or a > 0.999:
            return False
        return True

    def _reset_stability(self, reason: str = ""):
        """Mark the market as unstable and reset the counter."""
        if self._market_stable:
            logger.warning(f"Market stability RESET{' – ' + reason if reason else ''}")
        self._market_stable = False
        self._stable_tick_count = 0

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------

    async def check_simulation_mode(self) -> bool:
        """Check Redis for current simulation mode."""
        if not self.redis_client:
            return self.current_simulation_mode
        try:
            sim_mode = self.redis_client.get('btc_trading:simulation_mode')
            if sim_mode is not None:
                redis_simulation = sim_mode == '1'
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

    # ------------------------------------------------------------------
    # Strategy lifecycle
    # ------------------------------------------------------------------

    def on_start(self):
        """Called when strategy starts - LOAD ALL MARKETS AND SUBSCRIBE IMMEDIATELY"""
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY STARTED - FIXED VERSION")
        logger.info("=" * 80)

        # =========================================================================
        # FIX 2: Load ALL BTC instruments at startup
        # =========================================================================
        self._load_all_btc_instruments()

        # =========================================================================
        # FIX 3: Force subscribe to current market IMMEDIATELY
        # =========================================================================
        if self.instrument_id:
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"✓ SUBSCRIBED to market: {self.instrument_id}")
            
            # Try to get current price from cache
            try:
                quote = self.cache.quote_tick(self.instrument_id)
                if quote and quote.bid_price and quote.ask_price:
                    current_price = (quote.bid_price + quote.ask_price) / 2
                    self.price_history.append(current_price)
                    logger.info(f"✓ Initial price: ${float(current_price):.4f}")
            except Exception as e:
                logger.debug(f"No initial price yet: {e}")

        # Generate synthetic history if needed
        if len(self.price_history) < 20:
            self._generate_synthetic_history(target_count=20, existing_count=len(self.price_history))

        # =========================================================================
        # FIX 4: Start the timer loop (but don't rely on it for trading)
        # =========================================================================
        self.run_in_executor(self._start_timer_loop)

        if self.grafana_exporter:
            import threading
            threading.Thread(target=self._start_grafana_sync, daemon=True).start()

        # ── NEW: Start background WebSocket streams ───────────────────────────
        self.liquidation_processor.start_stream()
        self.cvd_ob_processor.start_stream()
        self.settlement_tracker.start_tracking()
        logger.info("✓ Liquidation stream started")
        logger.info("✓ CVD aggTrade stream started")
        logger.info("✓ Settlement tracker started")
        logger.info(f"✓ ML engine active: {self.ml_engine.is_active} "
                    f"(samples={self.ml_engine._sample_count}/{self.ml_engine.min_samples})")
        # ─────────────────────────────────────────────────────────────────────

        logger.info("=" * 80)
        logger.info("Strategy active - will trade every 15 minutes")
        logger.info(f"Price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("✓ READY TO TRADE NOW!")
        else:
            logger.warning(f"⚠ Need more history ({len(self.price_history)}/20)")
        logger.info("=" * 80)

    def _generate_synthetic_history(self, target_count: int = 20, existing_count: int = 0):
        """Generate synthetic price history for testing"""
        if self.price_history:
            base_price = self.price_history[-1]
        else:
            base_price = Decimal("0.5")
        needed = target_count - existing_count
        if needed <= 0:
            return
        for _ in range(needed):
            change = Decimal(str(random.uniform(-0.03, 0.03)))
            new_price = base_price * (Decimal("1.0") + change)
            new_price = max(Decimal("0.01"), min(Decimal("0.99"), new_price))
            self.price_history.append(new_price)
            base_price = new_price

    # ------------------------------------------------------------------
    # Load all BTC instruments at once
    # ------------------------------------------------------------------

    def _load_all_btc_instruments(self):
        """Load ALL BTC instruments from cache and sort by start time"""
        instruments = self.cache.instruments()
        logger.info(f"Loading ALL BTC instruments from {len(instruments)} total...")
        
        now = datetime.now(timezone.utc)
        current_timestamp = int(now.timestamp())
        
        btc_instruments = []
        
        for instrument in instruments:
            try:
                if hasattr(instrument, 'info') and instrument.info:
                    question = instrument.info.get('question', '').lower()
                    slug = instrument.info.get('market_slug', '').lower()
                    
                    if ('btc' in question or 'btc' in slug) and '15m' in slug:
                        try:
                            timestamp_part = slug.split('-')[-1]
                            market_timestamp = int(timestamp_part)
                            
                            # The slug timestamp IS the market start time (Unix, no offset).
                            # end_date_iso is a DATE-only string (e.g. "2026-02-20"), NOT a datetime,
                            # so parsing it gives midnight UTC which is wrong for intraday markets.
                            # Always derive end_timestamp from the slug: start + 900s.
                            real_start_ts = market_timestamp
                            end_timestamp = market_timestamp + 900  # 15-min markets always
                            time_diff = real_start_ts - current_timestamp
                            
                            # Only include markets that haven't ended yet
                            if end_timestamp > current_timestamp:
                                # Extract YES token ID for CLOB order book API.
                                # Nautilus instrument ID format:
                                #   {condition_id}-{token_id}.POLYMARKET
                                # The CLOB /book endpoint only accepts the token_id
                                # (the part after the dash, before .POLYMARKET).
                                raw_id = str(instrument.id)
                                # Strip .POLYMARKET suffix first
                                without_suffix = raw_id.split('.')[0] if '.' in raw_id else raw_id
                                # Then take the token_id after the condition_id dash
                                yes_token_id = without_suffix.split('-')[-1] if '-' in without_suffix else without_suffix

                                btc_instruments.append({
                                    'instrument': instrument,
                                    'slug': slug,
                                    'start_time': datetime.fromtimestamp(real_start_ts, tz=timezone.utc),
                                    'end_time': datetime.fromtimestamp(end_timestamp, tz=timezone.utc),
                                    'market_timestamp': market_timestamp,
                                    'end_timestamp': end_timestamp,
                                    'time_diff_minutes': time_diff / 60,
                                    'yes_token_id': yes_token_id,
                                })
                        except (ValueError, IndexError):
                            continue
            except Exception:
                continue
        
        # Pair YES and NO tokens by slug.
        # Each Polymarket market has two tokens loaded as separate Nautilus instruments.
        # The first instrument found for a slug is stored as the primary (YES/UP).
        # The second instrument found for the same slug is the NO/DOWN token.
        seen_slugs = {}
        deduped = []
        for inst in btc_instruments:
            slug = inst['slug']
            if slug not in seen_slugs:
                # First token seen = YES (UP)
                inst['yes_instrument_id'] = inst['instrument'].id
                inst['no_instrument_id'] = None  # will be filled when second token found
                seen_slugs[slug] = inst
                deduped.append(inst)
            else:
                # Second token seen = NO (DOWN) — store it on the existing entry
                seen_slugs[slug]['no_instrument_id'] = inst['instrument'].id
        btc_instruments = deduped
        
        # Sort by start time (absolute timestamp, not time-of-day)
        btc_instruments.sort(key=lambda x: x['market_timestamp'])
        
        logger.info("=" * 80)
        logger.info(f"FOUND {len(btc_instruments)} BTC 15-MIN MARKETS:")
        for i, inst in enumerate(btc_instruments):
            # A market is ACTIVE if it has started AND not yet ended
            is_active = inst['time_diff_minutes'] <= 0 and inst['end_timestamp'] > current_timestamp
            status = "ACTIVE" if is_active else "FUTURE" if inst['time_diff_minutes'] > 0 else "PAST"
            logger.info(f"  [{i}] {inst['slug']}: {status} (starts at {inst['start_time'].strftime('%H:%M:%S')}, ends at {inst['end_time'].strftime('%H:%M:%S')})")
        logger.info("=" * 80)
        
        self.all_btc_instruments = btc_instruments
        
        # Find current market and SUBSCRIBE IMMEDIATELY
        # FIXED: A market is current if it has STARTED and not yet ENDED (use end_time, not a hardcoded 15-min window)
        for i, inst in enumerate(btc_instruments):
            is_active = inst['time_diff_minutes'] <= 0 and inst['end_timestamp'] > current_timestamp
            if is_active:
                self.current_instrument_index = i
                self.instrument_id = inst['instrument'].id
                self.next_switch_time = inst['end_time']
                self._yes_token_id = inst.get('yes_token_id')
                self._yes_instrument_id = inst.get('yes_instrument_id', inst['instrument'].id)
                self._no_instrument_id = inst.get('no_instrument_id')
                logger.info(f"✓ CURRENT MARKET: {inst['slug']} (index {i})")
                logger.info(f"  Next switch at: {self.next_switch_time.strftime('%H:%M:%S')}")
                logger.info(f"  YES token: {self._yes_token_id[:16]}…" if self._yes_token_id else "  YES token: unknown")
                
                # =========================================================================
                # CRITICAL FIX: Subscribe immediately!
                # =========================================================================
                self.subscribe_quote_ticks(self.instrument_id)
                logger.info(f"  ✓ SUBSCRIBED to current market")
                break
        
        if self.current_instrument_index == -1 and btc_instruments:
            # No currently-active market — find the NEAREST upcoming one
            # (smallest positive time_diff_minutes = starts soonest)
            future_markets = [inst for inst in btc_instruments if inst['time_diff_minutes'] > 0]
            if future_markets:
                nearest = min(future_markets, key=lambda x: x['time_diff_minutes'])
                nearest_idx = btc_instruments.index(nearest)
            else:
                # All markets are in the past — use the last one
                nearest = btc_instruments[-1]
                nearest_idx = len(btc_instruments) - 1

            self.current_instrument_index = nearest_idx
            inst = nearest
            self.instrument_id = inst['instrument'].id
            self._yes_token_id = inst.get('yes_token_id')
            self._yes_instrument_id = inst.get('yes_instrument_id', inst['instrument'].id)
            self._no_instrument_id = inst.get('no_instrument_id')
            self.next_switch_time = inst['start_time']  # switch_time = when it OPENS
            logger.info(f"⚠ NO CURRENT MARKET - WAITING FOR NEAREST FUTURE: {inst['slug']}")
            logger.info(f"  Starts in {inst['time_diff_minutes']:.1f} min at {self.next_switch_time.strftime('%H:%M:%S')} UTC")

            # Subscribe so we get ticks when it opens
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"  ✓ SUBSCRIBED to future market")
            # Block trading until the market actually opens (timer loop sets _market_open flag)
            self._waiting_for_market_open = True
            
    def _switch_to_next_market(self):
        """Switch to the next market in the pre-loaded list"""
        if not self.all_btc_instruments:
            logger.error("No instruments loaded!")
            return False
        
        next_index = self.current_instrument_index + 1
        if next_index >= len(self.all_btc_instruments):
            logger.warning("No more markets available - will restart bot")
            return False
        
        next_market = self.all_btc_instruments[next_index]
        now = datetime.now(timezone.utc)
        
        # Check if next market is ready
        if now < next_market['start_time']:
            logger.info(f"Waiting for next market at {next_market['start_time'].strftime('%H:%M:%S')}")
            return False
        
        # Switch to next market
        self.current_instrument_index = next_index
        self.instrument_id = next_market['instrument'].id
        self.next_switch_time = next_market['end_time']
        self._yes_token_id = next_market.get('yes_token_id')
        self._yes_instrument_id = next_market.get('yes_instrument_id', next_market['instrument'].id)
        self._no_instrument_id = next_market.get('no_instrument_id')
        
        logger.info("=" * 80)
        logger.info(f"SWITCHING TO NEXT MARKET: {next_market['slug']}")
        logger.info(f"  Current time: {now.strftime('%H:%M:%S')}")
        logger.info(f"  Market ends at: {self.next_switch_time.strftime('%H:%M:%S')}")
        logger.info("=" * 80)
        
        # =========================================================================
        # FIX 5: Force stability for new market and reset trade timer correctly
        # =========================================================================
        self._stable_tick_count = QUOTE_STABILITY_REQUIRED  # Force stable immediately
        self._market_stable = True
        self._waiting_for_market_open = False  # Market is now active
        
        # Reset trade timer so we trade at the NEXT quote we receive
        # Use -1 so any interval will trigger (same as startup)
        self.last_trade_time = -1
        logger.info(f"  Trade timer reset — will trade on next tick")
        
        self.subscribe_quote_ticks(self.instrument_id)
        return True

    # ------------------------------------------------------------------
    # Timer loop - SIMPLIFIED
    # ------------------------------------------------------------------

    def _start_timer_loop(self):
        """Start timer loop in executor"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._timer_loop())
        finally:
            loop.close()

    async def _timer_loop(self):
        """
        Timer loop: checks every 10 seconds if it's time to switch markets.
        Also handles the case where we're waiting for a future market to open.
        """
        while True:
            # --- auto-restart check ---
            uptime_minutes = (datetime.now(timezone.utc) - self.bot_start_time).total_seconds() / 60
            if uptime_minutes >= self.restart_after_minutes:
                logger.warning("AUTO-RESTART TIME - Loading fresh filters")
                import signal as _signal
                os.kill(os.getpid(), _signal.SIGTERM)
                return

            now = datetime.now(timezone.utc)

            if self.next_switch_time and now >= self.next_switch_time:
                if self._waiting_for_market_open:
                    # The future market we were waiting for has now opened
                    # Treat it like a market switch so trade timer resets
                    logger.info("=" * 80)
                    logger.info(f"⏰ WAITING MARKET NOW OPEN: {now.strftime('%H:%M:%S')} UTC")
                    logger.info("=" * 80)
                    # Update next_switch_time to the market's END time
                    if (self.current_instrument_index >= 0 and
                            self.current_instrument_index < len(self.all_btc_instruments)):
                        current_market = self.all_btc_instruments[self.current_instrument_index]
                        self.next_switch_time = current_market['end_time']
                        logger.info(f"  Market ends at {self.next_switch_time.strftime('%H:%M:%S')} UTC")
                    self._waiting_for_market_open = False
                    self._market_stable = True
                    self._stable_tick_count = QUOTE_STABILITY_REQUIRED
                    self.last_trade_time = -1  # Trade immediately on next tick
                    logger.info("  ✓ MARKET OPEN — ready to trade on next tick")
                else:
                    # Normal market switch
                    self._switch_to_next_market()

            # ── Weekly learning engine weight optimisation ──────────────────
            hours_since_learn = (
                (datetime.now(timezone.utc) - self._last_learning_optimization)
                .total_seconds() / 3600
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
            # ─────────────────────────────────────────────────────────────────

            await asyncio.sleep(10)

    # ------------------------------------------------------------------
    # Quote tick handler - SIMPLIFIED
    # ------------------------------------------------------------------

    def on_quote_tick(self, tick: QuoteTick):
        """Handle quote tick - TRADE when market opens and at each 15-min boundary"""
        try:
            # Only process ticks from current instrument
            if self.instrument_id is None or tick.instrument_id != self.instrument_id:
                return

            now = datetime.now(timezone.utc)
            bid = tick.bid_price
            ask = tick.ask_price

            if bid is None or ask is None:
                return
                
            try:
                bid_decimal = bid.as_decimal()
                ask_decimal = ask.as_decimal()
            except:
                return

            # Always store price history
            mid_price = (bid_decimal + ask_decimal) / 2
            self.price_history.append(mid_price)
            if len(self.price_history) > self.max_history:
                self.price_history.pop(0)
            
            # Store latest bid/ask for liquidity check before order placement
            self._last_bid_ask = (bid_decimal, ask_decimal)

            # Tick buffer for TickVelocityProcessor (rolling 90s window)
            self._tick_buffer.append({'ts': now, 'price': mid_price})

            # Stability gate
            if not self._market_stable:
                self._stable_tick_count += 1
                if self._stable_tick_count >= 1:
                    self._market_stable = True
                    logger.info(f"✓ Market STABLE immediately")
                else:
                    return

            # =========================================================================
            # FIXED TRADING LOGIC:
            # 
            # We trade once per 15-min market interval.
            # Instead of checking wall-clock 15-min boundaries (which caused the 2-hour
            # wait), we use a simple counter keyed to the Polymarket market's OWN
            # start time.
            #
            # The market's start_time is stored in all_btc_instruments[current_index].
            # Within each market, we compute a "sub-interval" index:
            #   sub_interval = elapsed_seconds_since_market_open // 900
            # Trade ID = (market_start_timestamp, sub_interval)
            # This fires once at market open AND once after every 15 min within
            # the same market if it's a multi-interval market.
            #
            # If _waiting_for_market_open is True (started before market opens),
            # we block trading until the timer loop calls _switch_to_next_market.
            # =========================================================================

            # Block trading if waiting for a future market to open
            if self._waiting_for_market_open:
                return

            # Get current market info
            if (self.current_instrument_index < 0 or
                    self.current_instrument_index >= len(self.all_btc_instruments)):
                return

            current_market = self.all_btc_instruments[self.current_instrument_index]
            market_start_ts = current_market['market_timestamp']  # Slug timestamp = market start (Unix)

            # How many 15-min intervals have elapsed since this market opened?
            elapsed_secs = now.timestamp() - market_start_ts
            if elapsed_secs < 0:
                # Market hasn't started yet — block
                return

            sub_interval = int(elapsed_secs // MARKET_INTERVAL_SECONDS)

            # Unique trade key: (market_start_timestamp, sub_interval)
            trade_key = (market_start_ts, sub_interval)

            # =========================================================================
            # TRADE WINDOW: minutes 13–14 of each 15-min market (780–840 seconds in)
            #
            # WHY LATE IN THE MARKET:
            #   At 13 minutes in, the UP/DOWN result is nearly decided. The price IS
            #   the trend — if YES is at $0.78, BTC went up during this interval.
            #   We're not predicting anymore, we're reading a nearly-resolved outcome.
            #
            # WHY NOT EARLIER (the old 30–90s window):
            #   At 30 seconds in, nobody knows which way BTC will move. The signals
            #   have no edge. This is why we were losing at prices near $0.50.
            #
            # TREND FILTER (applied in _make_trading_decision):
            #   Price > 0.60 → clear UP trend → buy YES
            #   Price < 0.40 → clear DOWN trend → buy NO
            #   Price 0.40–0.60 → coin flip → SKIP (don't trade)
            #
            # Share count intuition:
            #   1.4 shares = price $0.71 → strong trend, win rate ~71%
            #   1.9 shares = price $0.53 → weak trend, near coin flip
            #   2.0+ shares = price $0.50 → pure coin flip, SKIP
            # =========================================================================
            seconds_into_sub_interval = elapsed_secs % MARKET_INTERVAL_SECONDS
            TRADE_WINDOW_START = 540   # 13 minutes in
            TRADE_WINDOW_END   = 600   # 14 minutes in (60s window)

            if TRADE_WINDOW_START <= seconds_into_sub_interval < TRADE_WINDOW_END and trade_key != self.last_trade_time:
                self.last_trade_time = trade_key

                logger.info("=" * 80)
                logger.info(f" LATE-WINDOW TRADE: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                logger.info(f"   Market: {current_market['slug']}")
                logger.info(f"   Sub-interval #{sub_interval} ({seconds_into_sub_interval:.1f}s in = {seconds_into_sub_interval/60:.1f} min)")
                logger.info(f"   Price: ${float(mid_price):,.4f} | Bid: ${float(bid_decimal):,.4f} | Ask: ${float(ask_decimal):,.4f}")
                logger.info(f"   Trend strength: {'STRONG ✓' if float(mid_price) > 0.60 or float(mid_price) < 0.40 else 'WEAK — may skip'}")
                logger.info(f"   Price history: {len(self.price_history)} points")
                logger.info("=" * 80)

                self.run_in_executor(lambda: self._make_trading_decision_sync(float(mid_price)))

        except Exception as e:
            logger.error(f"Error processing quote tick: {e}")

    # ------------------------------------------------------------------
    # Trading decision (unchanged)
    # ------------------------------------------------------------------

    def _make_trading_decision_sync(self, current_price):
        from decimal import Decimal
        price_decimal = Decimal(str(current_price))
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._make_trading_decision(price_decimal))
        finally:
            loop.close()
    
    def _make_trading_decision_sync(self, current_price):
        """Synchronous wrapper for trading decision (called from executor)."""
        # Convert float back to Decimal for processing
        from decimal import Decimal
        price_decimal = Decimal(str(current_price))
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._make_trading_decision(price_decimal))
        finally:
            loop.close()
            
    async def _fetch_market_context(self, current_price: Decimal) -> dict:
        """
        Fetch REAL external data to populate signal processor metadata.

        Returns a dict with:
          - sentiment_score (float 0-100): live Fear & Greed index, or None
          - spot_price (float): live BTC-USD from Coinbase, or None
          - deviation (float): polymarket price vs SMA-20 (always computed)
          - momentum (float): 5-period rate of change (always computed)
          - volatility (float): price std-dev over last 20 ticks (always computed)
        """
        current_price_float = float(current_price)

        # --- Always-available stats from local price_history ---
        recent_prices = [float(p) for p in self.price_history[-20:]]
        sma_20 = sum(recent_prices) / len(recent_prices)
        deviation = (current_price_float - sma_20) / sma_20
        momentum = (
            (current_price_float - float(self.price_history[-5])) / float(self.price_history[-5])
            if len(self.price_history) >= 5 else 0.0
        )
        variance = sum((p - sma_20) ** 2 for p in recent_prices) / len(recent_prices)
        volatility = math.sqrt(variance)

        metadata = {
            "deviation": deviation,
            "momentum": momentum,
            "volatility": volatility,
            # Tick buffer for TickVelocityProcessor
            "tick_buffer": list(self._tick_buffer),
            # YES token id for OrderBookImbalanceProcessor
            "yes_token_id": self._yes_token_id,
        }

        # --- Real sentiment: Fear & Greed Index via NewsSocialDataSource ---
        try:
            from data_sources.news_social.adapter import NewsSocialDataSource
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
                logger.warning("Fear & Greed fetch returned no data — sentiment processor skipped")
        except Exception as e:
            logger.warning(f"Could not fetch Fear & Greed index: {e} — sentiment processor skipped")

        # --- Real spot price: Coinbase BTC-USD REST API ---
        try:
            from data_sources.coinbase.adapter import CoinbaseDataSource
            coinbase = CoinbaseDataSource()
            await coinbase.connect()
            spot = await coinbase.get_current_price()
            await coinbase.disconnect()
            if spot:
                metadata["spot_price"] = float(spot)
                logger.info(f"Coinbase spot price: ${float(spot):,.2f}")
            else:
                logger.warning("Coinbase price fetch returned None — divergence processor skipped")
        except Exception as e:
            logger.warning(f"Could not fetch Coinbase spot price: {e} — divergence processor skipped")

        logger.info(
            f"Market context — deviation={deviation:.2%}, "
            f"momentum={momentum:.2%}, volatility={volatility:.4f}, "
            f"sentiment={'%.0f' % metadata['sentiment_score'] if 'sentiment_score' in metadata else 'N/A'}, "
            f"spot=${'%.2f' % metadata['spot_price'] if 'spot_price' in metadata else 'N/A'}"
        )

        # ── NEW: Inject live data from new processors into metadata ──────────
        # Liquidation snapshot
        liq_snap = self.liquidation_processor._get_window_snapshot()
        liq_total = liq_snap["long_liq_usd"] + liq_snap["short_liq_usd"]
        liq_imbalance = (
            (liq_snap["long_liq_usd"] - liq_snap["short_liq_usd"]) / liq_total
            if liq_total > 0 else 0.0
        )
        metadata["liq_imbalance"] = liq_imbalance
        metadata["liq_total_usd"] = liq_total

        # CVD snapshot
        cvd_snap = self.cvd_ob_processor._compute_cvd()
        metadata["cvd_delta_usd"] = cvd_snap["cvd_delta"]

        # Binance spot OB snapshot
        ob_snap = self.cvd_ob_processor._fetch_order_book()
        if ob_snap:
            metadata["ob_imbalance"] = ob_snap["imbalance"]

        # Funding + OI (cached)
        try:
            fi_data = self.funding_oi_processor._fetch_data()
            if fi_data:
                metadata["funding_rate"] = fi_data["funding_rate"]
                metadata["oi_change"]    = fi_data["oi_change"]
        except Exception:
            pass

        # OHLCV (cached) — vol_regime + indicator values
        try:
            klines = self.ohlcv_momentum_processor._fetch_klines()
            if klines:
                metadata["vol_regime"]   = klines["vol_regime"]
                metadata["rsi"]          = klines["rsi"]
                metadata["macd_line"]    = klines["macd_line"]
                metadata["macd_signal"]  = klines["macd_signal"]
                metadata["pct_b"]        = klines["pct_b"]
                metadata["ret1"]         = klines["ret1"]
                metadata["ret3"]         = klines["ret3"]
                metadata["ret5"]         = klines["ret5"]
                metadata["ret15"]        = klines["ret15"]
        except Exception:
            pass

        logger.info(
            f"Extended context — liq_imb={liq_imbalance:+.3f}, "
            f"cvd=${cvd_snap['cvd_delta']/1e6:+.1f}M, "
            f"funding={metadata.get('funding_rate', 0):.5%}, "
            f"vol_regime={metadata.get('vol_regime', 'N/A')}"
        )
        # ─────────────────────────────────────────────────────────────────────

        return metadata

    async def _make_trading_decision(self, current_price: Decimal):
        """
        The full 6-step decision loop.

        STEP 1  Collect all 8 features → feature vector
        STEP 2  Run through XGBoost model → p(BTC goes UP)
        STEP 3  Compare model probability vs Polymarket implied odds
        STEP 4  If edge > MIN_EDGE → bet the mispriced side
        STEP 5  Register with settlement tracker → Chainlink records outcome
        STEP 6  Weekly retraining triggered automatically by settlement tracker

        Before the model has 200 labelled samples it falls back to:
          signal fusion score + trend filter (price >0.60 / <0.40)
        Every interval records the feature vector regardless, so the
        training DB grows even during the warm-up period.
        """
        # ── Mode check ────────────────────────────────────────────────────────
        is_simulation = await self.check_simulation_mode()
        logger.info(f"Mode: {'SIMULATION' if is_simulation else 'LIVE TRADING'}")

        if len(self.price_history) < 20:
            logger.warning(f"Not enough price history ({len(self.price_history)}/20)")
            return

        poly_price = float(current_price)
        logger.info(f"Current Polymarket price: {poly_price:.4f}")

        # ═══════════════════════════════════════════════════════════════════════
        # STEP 1 — Collect all 8 features
        # _fetch_market_context pulls from every data source and stores results
        # in metadata dict. _process_signals runs all processors and also writes
        # tick_vel_60s / tick_vel_30s back into metadata for the ML feature vector.
        # ═══════════════════════════════════════════════════════════════════════
        metadata = await self._fetch_market_context(current_price)

        # Run all processors — primarily to populate metadata with tick velocity
        # values and to give the fusion engine signal context.
        # NOTE: we do NOT gate on signals being non-empty — the ML model is the
        # primary decision maker and can fire even when individual processors
        # produce no signal.
        signals = self._process_signals(current_price, metadata)

        # Extract tick velocity from any TickVelocity signal that fired,
        # so the ML feature vector gets real values instead of zeros.
        for sig in signals:
            if sig.source == "TickVelocity":
                metadata["velocity_60s"] = sig.metadata.get("velocity_60s") or 0.0
                metadata["velocity_30s"] = sig.metadata.get("velocity_30s") or 0.0
                break

        # Signal fusion is kept as secondary context / fallback confidence gauge.
        fused = self.fusion_engine.fuse_signals(signals, min_signals=1, min_score=40.0)

        if signals:
            logger.info(f"Signals: {len(signals)} fired — "
                        f"{sum(1 for s in signals if 'BULLISH' in str(s.direction).upper())} bullish / "
                        f"{sum(1 for s in signals if 'BEARISH' in str(s.direction).upper())} bearish")
        else:
            logger.info("No individual signals fired — proceeding to ML/trend filter")

        if fused:
            logger.info(f"Fusion: {fused.direction.value} score={fused.score:.1f} conf={fused.confidence:.2%}")

        # ═══════════════════════════════════════════════════════════════════════
        # STEP 2 — Run feature vector through XGBoost model
        # ═══════════════════════════════════════════════════════════════════════
        # Build feature vector — strip list/dict values (not numeric features)
        flat_metadata = {
            k: float(v) if hasattr(v, "__float__") else v
            for k, v in metadata.items()
            if not isinstance(v, (list, dict))
        }
        feature_vector = self.ml_engine.build_feature_vector(
            metadata=flat_metadata,
            poly_price=poly_price,
        )

        ml_p_up: float | None = None
        if self.ml_engine.is_active and feature_vector is not None:
            ml_p_up = self.ml_engine.predict(feature_vector)
            logger.info(
                f"STEP 2 — ML model active: p(UP)={ml_p_up:.3f} | "
                f"poly={poly_price:.3f} | samples={self.ml_engine._sample_count}"
            )
        else:
            logger.info(
                f"STEP 2 — ML warming up "
                f"({self.ml_engine._sample_count}/{self.ml_engine.min_samples} samples) "
                f"— will use trend filter"
            )

        # ═══════════════════════════════════════════════════════════════════════
        # STEPS 3 + 4 — Compare vs market odds, decide direction
        # ═══════════════════════════════════════════════════════════════════════
        POSITION_SIZE_USD = Decimal("1.00")
        direction: str | None = None
        bet_edge: float = 0.0   # set by should_bet(); used for paper trade logging

        if ml_p_up is not None:
            # STEP 3: edge check
            should_bet, ml_direction, _edge = self.ml_engine.should_bet(
                p_up=ml_p_up,
                poly_price=poly_price,
            )
            # STEP 4: gate
            if not should_bet:
                logger.info(
                    f"STEP 4 — No edge: model={ml_p_up:.3f} market={poly_price:.3f} "
                    f"gap={abs(ml_p_up - poly_price):.3f} < required={self.ml_engine.min_edge:.3f} — skip"
                )
                # Still record the feature vector so the DB grows (outcome recorded at settlement)
                if feature_vector is not None:
                    slug = (self.all_btc_instruments[self.current_instrument_index]["slug"]
                            if self.current_instrument_index >= 0 else "unknown")
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
            # ── Fallback: trend filter (active until model has 200 samples) ──
            TREND_UP   = 0.60
            TREND_DOWN = 0.40
            if poly_price > TREND_UP:
                direction = "long"
                logger.info(f"STEP 4 (fallback) — trend UP {poly_price:.2%} → YES")
            elif poly_price < TREND_DOWN:
                direction = "short"
                logger.info(f"STEP 4 (fallback) — trend DOWN {poly_price:.2%} → NO")
            else:
                logger.info(
                    f"STEP 4 (fallback) — neutral {poly_price:.2%} "
                    f"(not >{TREND_UP:.0%} or <{TREND_DOWN:.0%}) — skip"
                )
                # Still record features for training even when we skip
                if feature_vector is not None:
                    slug = (self.all_btc_instruments[self.current_instrument_index]["slug"]
                            if self.current_instrument_index >= 0 else "unknown")
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                return

        # ── Risk engine ───────────────────────────────────────────────────────
        is_valid, error = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD,
            direction=direction,
            current_price=current_price,
        )
        if not is_valid:
            logger.warning(f"Risk engine blocked: {error}")
            return

        # ── Liquidity guard ───────────────────────────────────────────────────
        last_tick = getattr(self, "_last_bid_ask", None)
        if last_tick:
            last_bid, last_ask = last_tick
            MIN_LIQ = Decimal("0.02")
            if direction == "long" and last_ask <= MIN_LIQ:
                logger.warning(f"⚠ No ask liquidity ({float(last_ask):.4f}) — retry next tick")
                self.last_trade_time = -1
                return
            if direction == "short" and last_bid <= MIN_LIQ:
                logger.warning(f"⚠ No bid liquidity ({float(last_bid):.4f}) — retry next tick")
                self.last_trade_time = -1
                return

        # ═══════════════════════════════════════════════════════════════════════
        # STEP 5 setup — record feature vector in ML DB before placing bet
        # The settlement tracker will fill in the outcome after Chainlink settles.
        # ═══════════════════════════════════════════════════════════════════════
        trade_id: int | None = None
        if feature_vector is not None:
            slug = (self.all_btc_instruments[self.current_instrument_index]["slug"]
                    if self.current_instrument_index >= 0 else "unknown")
            trade_id = self.ml_engine.record_trade(
                market_slug=slug,
                poly_price=poly_price,
                feature_vector=feature_vector,
            )
            logger.info(f"STEP 5 setup — feature vector saved, trade_id={trade_id}")

        # ── Execute bet ───────────────────────────────────────────────────────
        # Pass fused signal for paper-trade logging; fall back to a minimal stub
        # if fusion produced nothing (ML-only path with no individual signals).
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

        # ═══════════════════════════════════════════════════════════════════════
        # STEP 5 — Register with settlement tracker
        # Background thread watches Chainlink; when market closes it writes
        # outcome back to the DB and calls ml_engine.maybe_retrain() (STEP 6).
        # ═══════════════════════════════════════════════════════════════════════
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
                f"(market closes at {market_info['end_time'].strftime('%H:%M:%S')} UTC)"
            )
            
    async def _record_paper_trade(self, signal, position_size, current_price, direction,
                                    ml_p_up: float = 0.0, ml_edge: float = 0.0,
                                    metadata: dict = None):
        """
        Record a simulation trade with full context.
        Outcome is simulated immediately using a realistic Polymarket movement model.
        All fields are persisted to paper_trades.json after every trade.
        """
        if metadata is None:
            metadata = {}

        now = datetime.now(timezone.utc)
        exit_delta = timedelta(minutes=1) if self.test_mode else timedelta(minutes=15)
        exit_time = now + exit_delta

        # Simulate realistic Polymarket probability movement
        if "BULLISH" in str(signal.direction):
            movement = random.uniform(-0.04, 0.10)   # buying YES: win if prob goes up
        else:
            movement = random.uniform(-0.10, 0.04)   # buying NO: win if prob goes down

        exit_price_raw = float(current_price) + movement
        exit_price_raw = max(0.01, min(0.99, exit_price_raw))
        exit_price_dec = Decimal(str(exit_price_raw))

        if direction == "long":
            pnl = float(position_size) * (exit_price_raw - float(current_price))
        else:
            pnl = float(position_size) * (float(current_price) - exit_price_raw)

        pnl_pct = pnl / float(position_size) if float(position_size) > 0 else 0.0
        outcome = "WIN" if pnl > 0 else "LOSS"

        slug = ""
        if (self.current_instrument_index >= 0 and
                self.current_instrument_index < len(self.all_btc_instruments)):
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
        total_settled = len(settled)
        win_rate = wins / total_settled if total_settled > 0 else 0.0
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
            }
        )

        if hasattr(self, "grafana_exporter") and self.grafana_exporter:
            self.grafana_exporter.increment_trade_counter(won=(pnl > 0))
            self.grafana_exporter.record_trade_duration(exit_delta.total_seconds())

        icon = "[WIN]" if outcome == "WIN" else "[LOSS]"
        logger.info("=" * 80)
        logger.info(f"[SIM] TRADE #{session_num}  {icon}  {direction.upper()}")
        logger.info(f"  Market:      {slug or 'unknown'}")
        logger.info(f"  Entry prob:  {float(current_price):.4f}  -->  Exit prob: {exit_price_raw:.4f}  (move: {movement:+.4f})")
        logger.info(f"  BTC spot:    ${metadata.get('spot_price', 0):,.0f}")
        logger.info(f"  P&L:         ${pnl:+.4f}  ({pnl_pct*100:+.2f}%)")
        logger.info(f"  ML p(UP):    {ml_p_up:.3f}  edge={ml_edge:.3f}")
        logger.info(f"  Signal:      score={signal.score:.1f}  conf={signal.confidence:.2%}  n={signal.num_signals if hasattr(signal,'num_signals') else 1}")
        logger.info(f"  Vol regime:  {metadata.get('vol_regime', 'N/A')}   funding={metadata.get('funding_rate', 0):.5%}")
        logger.info(f"  Session:     {wins}/{total_settled} wins  ({win_rate:.1%})   cumulative PnL=${total_pnl:+.4f}")
        logger.info("=" * 80)

        self._save_paper_trades()

    def _save_paper_trades(self):
        import json
        try:
            trades_data = [t.to_dict() for t in self.paper_trades]
            with open('paper_trades.json', 'w') as f:
                json.dump(trades_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    # ------------------------------------------------------------------
    # Real order (unchanged)
    # ------------------------------------------------------------------

    async def _place_real_order(self, signal, position_size, current_price, direction):
        if not self.instrument_id:
            logger.error("No instrument available")
            return

        try:
            # instrument is fetched below after determining YES vs NO token

            logger.info("=" * 80)
            logger.info("LIVE MODE - PLACING REAL ORDER!")
            logger.info("=" * 80)

            # On Polymarket, both UP and DOWN are BUY orders.
            # Bullish = buy YES token (self._yes_instrument_id)
            # Bearish = buy NO token  (self._no_instrument_id)
            # There is NO sell — you always buy whichever side you want.
            side = OrderSide.BUY

            if direction == "long":
                trade_instrument_id = getattr(self, '_yes_instrument_id', self.instrument_id)
                trade_label = "YES (UP)"
            else:
                no_id = getattr(self, '_no_instrument_id', None)
                if no_id is None:
                    logger.warning(
                        "NO token instrument not found for this market — "
                        "cannot bet DOWN. Skipping trade."
                    )
                    return
                trade_instrument_id = no_id
                trade_label = "NO (DOWN)"

            instrument = self.cache.instrument(trade_instrument_id)
            if not instrument:
                logger.error(f"Instrument not in cache: {trade_instrument_id}")
                return

            logger.info(f"Buying {trade_label} token: {trade_instrument_id}")

            trade_price = float(current_price)
            max_usd_amount = float(position_size)

            precision = instrument.size_precision

            # Always BUY — the market-order patch converts this to a USD amount.
            # Pass dummy qty=5 (minimum) so Nautilus risk engine doesn't deny it.
            min_qty_val = float(getattr(instrument, 'min_quantity', None) or 5.0)
            token_qty = max(min_qty_val, 5.0)
            token_qty = round(token_qty, precision)
            logger.info(
                f"BUY {trade_label}: dummy qty={token_qty:.6f} "
                f"(patch converts to ${max_usd_amount:.2f} USD)"
            )

            qty = Quantity(token_qty, precision=precision)
            timestamp_ms = int(time.time() * 1000)
            unique_id = f"BTC-15MIN-${max_usd_amount:.0f}-{timestamp_ms}"

            order = self.order_factory.market(
                instrument_id=trade_instrument_id,
                order_side=side,
                quantity=qty,
                client_order_id=ClientOrderId(unique_id),
                quote_quantity=False,
                time_in_force=TimeInForce.IOC,
            )

            self.submit_order(order)

            logger.info(f"REAL ORDER SUBMITTED!")
            logger.info(f"  Order ID: {unique_id}")
            logger.info(f"  Direction: {trade_label}")
            logger.info(f"  Side: BUY")
            logger.info(f"  Token Quantity: {token_qty:.6f}")
            logger.info(f"  Estimated Cost: ~${max_usd_amount:.2f}")
            logger.info(f"  Price: ${trade_price:.4f}")
            logger.info("=" * 80)

            self._track_order_event("placed")

        except Exception as e:
            logger.error(f"Error placing real order: {e}")
            import traceback
            traceback.print_exc()
            self._track_order_event("rejected")

    # ------------------------------------------------------------------
    # Signal processing
    # ------------------------------------------------------------------

    def _process_signals(self, current_price, metadata=None):
        signals = []
        if metadata is None:
            metadata = {}

        processed_metadata = {}
        for key, value in metadata.items():
            if isinstance(value, float):
                processed_metadata[key] = Decimal(str(value))
            else:
                processed_metadata[key] = value

        spike_signal = self.spike_detector.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if spike_signal:
            signals.append(spike_signal)

        if 'sentiment_score' in processed_metadata:
            sentiment_signal = self.sentiment_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if sentiment_signal:
                signals.append(sentiment_signal)

        if 'spot_price' in processed_metadata:
            divergence_signal = self.divergence_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if divergence_signal:
                signals.append(divergence_signal)

        # --- Order Book Imbalance (real-time Polymarket CLOB depth) ---
        if processed_metadata.get('yes_token_id'):
            ob_signal = self.orderbook_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if ob_signal:
                signals.append(ob_signal)

        # --- Tick Velocity (last 60s of Polymarket probability movement) ---
        if processed_metadata.get('tick_buffer'):
            tv_signal = self.tick_velocity_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if tv_signal:
                signals.append(tv_signal)

        # --- Deribit Put/Call Ratio (institutional options sentiment) ---
        pcr_signal = self.deribit_pcr_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if pcr_signal:
            signals.append(pcr_signal)

        # ── NEW processors ────────────────────────────────────────────────────

        # Liquidation cascades (Binance futures forceOrders stream)
        liq_signal = self.liquidation_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if liq_signal:
            signals.append(liq_signal)

        # Funding rate + open interest
        fi_signal = self.funding_oi_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if fi_signal:
            signals.append(fi_signal)

        # CVD + Binance spot order book imbalance
        cvd_signal = self.cvd_ob_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if cvd_signal:
            signals.append(cvd_signal)

        # OHLCV momentum + volatility regime + time-of-day
        ohlcv_signal = self.ohlcv_momentum_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if ohlcv_signal:
            signals.append(ohlcv_signal)

        return signals

    # ------------------------------------------------------------------
    # Order events
    # ------------------------------------------------------------------

    def _track_order_event(self, event_type: str) -> None:
        """
        Safely track an order event on the performance tracker.

        PerformanceTracker does not expose `increment_order_counter`, so we
        use whichever method is actually available, or fall back to a no-op.
        Supported event_type values: "placed", "filled", "rejected".
        """
        try:
            pt = self.performance_tracker
            # Try the method that actually exists first
            if hasattr(pt, 'record_order_event'):
                pt.record_order_event(event_type)
            elif hasattr(pt, 'increment_counter'):
                pt.increment_counter(event_type)
            elif hasattr(pt, 'increment_order_counter'):
                pt.increment_order_counter(event_type)
            else:
                # No suitable method found – log and carry on
                logger.debug(
                    f"PerformanceTracker has no order-counter method; "
                    f"ignoring event '{event_type}'"
                )
        except Exception as e:
            logger.warning(f"Failed to track order event '{event_type}': {e}")

    def on_order_filled(self, event):
        logger.info("=" * 80)
        logger.info(f"ORDER FILLED!")
        logger.info(f"  Order: {event.client_order_id}")
        logger.info(f"  Fill Price: ${float(event.last_px):.4f}")
        logger.info(f"  Quantity: {float(event.last_qty):.6f}")
        logger.info("=" * 80)
        self._track_order_event("filled")

    def on_order_denied(self, event):
        logger.error("=" * 80)
        logger.error(f"ORDER DENIED!")
        logger.error(f"  Order: {event.client_order_id}")
        logger.error(f"  Reason: {event.reason}")
        logger.error("=" * 80)
        self._track_order_event("rejected")

    def on_order_rejected(self, event):
        """Handle order rejection — reset trade timer so we can retry next tick."""
        reason = str(getattr(event, 'reason', ''))
        reason_lower = reason.lower()
        if 'no orders found' in reason_lower or 'fak' in reason_lower or 'no match' in reason_lower:
            logger.warning(
                f"⚠ FAK rejected (no liquidity) — resetting timer to retry next tick\n"
                f"  Reason: {reason}"
            )
            self.last_trade_time = -1  # Allow retry on next quote tick
        else:
            logger.warning(f"Order rejected: {reason}")

    # ------------------------------------------------------------------
    # Grafana / stop
    # ------------------------------------------------------------------

    def _start_grafana_sync(self):
        import asyncio
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.grafana_exporter.start())
            logger.info("Grafana metrics started on port 8000")
        except Exception as e:
            logger.error(f"Failed to start Grafana: {e}")

    def on_stop(self):
        logger.info("Integrated BTC strategy stopped")
        logger.info(f"Total paper trades recorded: {len(self.paper_trades)}")
        if self.grafana_exporter:
            import asyncio
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self.grafana_exporter.stop())
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_integrated_bot(simulation: bool = False, enable_grafana: bool = True, test_mode: bool = False):
    """Run the integrated BTC 15-min trading bot - LOADS ALL BTC MARKETS FOR THE DAY"""
    
    print("=" * 80)
    print("INTEGRATED POLYMARKET BTC 15-MIN TRADING BOT")
    print("Nautilus + 7-Phase System + Redis Control")
    print("=" * 80)

    redis_client = init_redis()

    if redis_client:
        try:
            # ALWAYS overwrite Redis with the current session mode.
            # This prevents a stale value from a previous --live run
            # silently overriding --test-mode or --simulation runs.
            mode_value = '1' if simulation else '0'
            redis_client.set('btc_trading:simulation_mode', mode_value)
            mode_label = 'SIMULATION' if simulation else 'LIVE'
            logger.info(f"Redis simulation_mode forced to: {mode_label} ({mode_value})")
        except Exception as e:
            logger.warning(f"Could not set Redis simulation mode: {e}")

    print(f"\nConfiguration:")
    print(f"  Initial Mode: {'SIMULATION' if simulation else 'LIVE TRADING'}")
    print(f"  Redis Control: {'Enabled' if redis_client else 'Disabled'}")
    print(f"  Grafana: {'Enabled' if enable_grafana else 'Disabled'}")
    print(f"  Max Trade Size: ${os.getenv('MARKET_BUY_USD', '1.00')}")
    print(f"  Quote stability gate: {QUOTE_STABILITY_REQUIRED} valid ticks")
    print()

    now = datetime.now(timezone.utc)
    
    # =========================================================================
    # Slug timestamps ARE standard Unix timestamps (no offset) aligned to
    # 15-min boundaries. Generate slugs for current + next 24 hours.
    # =========================================================================
    now = datetime.now(timezone.utc)
    unix_interval_start = (int(now.timestamp()) // 900) * 900  # current 15-min boundary

    btc_slugs = []
    for i in range(-1, 97):  # include 1 prior interval (in case we're just after boundary)
        timestamp = unix_interval_start + (i * 900)
        btc_slugs.append(f"btc-updown-15m-{timestamp}")

    filters = {
        "active": True,
        "closed": False,
        "archived": False,
        "slug": tuple(btc_slugs),
        "limit": 100,
    }

    logger.info("=" * 80)
    logger.info("LOADING BTC 15-MIN MARKETS BY SLUG")
    logger.info(f"  Interval start: {unix_interval_start} | Count: {len(btc_slugs)}")
    logger.info(f"  First: {btc_slugs[0]}  Last: {btc_slugs[-1]}")
    logger.info("=" * 80)

    instrument_cfg = InstrumentProviderConfig(
        load_all=True,
        filters=filters,
        use_gamma_markets=True,
    )

    poly_data_cfg = PolymarketDataClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        signature_type=1,
        instrument_provider=instrument_cfg,
    )

    poly_exec_cfg = PolymarketExecClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        signature_type=1,
        instrument_provider=instrument_cfg,
    )

    config = TradingNodeConfig(
        environment="live",
        trader_id="BTC-15MIN-INTEGRATED-001",
        logging=LoggingConfig(
            log_level="INFO",
            log_directory="./logs/nautilus",
        ),
        data_engine=LiveDataEngineConfig(qsize=6000),
        exec_engine=LiveExecEngineConfig(qsize=6000),
        risk_engine=LiveRiskEngineConfig(bypass=simulation),
        data_clients={POLYMARKET: poly_data_cfg},
        exec_clients={POLYMARKET: poly_exec_cfg},
    )

    strategy = IntegratedBTCStrategy(
        redis_client=redis_client,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
    )

    print("\nBuilding Nautilus node...")
    node = TradingNode(config=config)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
    node.trader.add_strategy(strategy)
    node.build()
    logger.info("Nautilus node built successfully")

    print()
    print("=" * 80)
    print("BOT STARTING")
    print("=" * 80)

    try:
        node.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        node.dispose()
        logger.info("Bot stopped")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Integrated BTC 15-Min Trading Bot")
    parser.add_argument("--live", action="store_true",
                        help="Run in LIVE mode (real money at risk!). Default is simulation.")
    parser.add_argument("--no-grafana", action="store_true", help="Disable Grafana metrics")
    parser.add_argument("--test-mode", action="store_true",
                        help="Run in TEST MODE (trade every minute for faster testing)")

    args = parser.parse_args()
    enable_grafana = not args.no_grafana
    test_mode = args.test_mode

    # --test-mode ALWAYS forces simulation even if --live is also passed
    if args.test_mode:
        simulation = True
    else:
        simulation = not args.live

    if not simulation:
        logger.warning("=" * 80)
        logger.warning("LIVE TRADING MODE — REAL MONEY AT RISK!")
        logger.warning("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(f"SIMULATION MODE — {'TEST MODE (fast clock)' if test_mode else 'paper trading only'}")
        logger.info("No real orders will be placed.")
        logger.info("=" * 80)

    run_integrated_bot(simulation=simulation, enable_grafana=enable_grafana, test_mode=test_mode)


if __name__ == "__main__":
    main()