"""
core.settlement.tracker
========================
Background daemon that monitors Polymarket's Chainlink BTC/USD TWAP to:
  1. Stream the live TWAP ("BTC Price" on Polymarket)
  2. Freeze Price-to-Beat at each 5-min market open
  3. Record the TWAP at market END (settlement)
  4. Determine the TRUE outcome (YES if exit > entry)
  5. Update the ML training database with the outcome
  6. Trigger weekly model retraining

Primary source (post 2026-08-07): Polymarket RTDS ``crypto_prices_twap_thirty``
which relays Chainlink's signed 30s TWAP — the same feed Polymarket's UI and
5-min Up/Down resolution use.

Fallbacks (display / ML only when RTDS is down):
  - On-chain Chainlink BTC/USD aggregator (ETH_RPC_URL)
  - Coinbase / Binance REST spot
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from core.settlement.twap_feed import PolymarketChainlinkTwapFeed, get_twap_feed

try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    logger.warning(
        "web3 not installed — on-chain Chainlink fallback disabled. "
        "Run: pip install web3"
    )

CHAINLINK_BTC_USD = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"

CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",         "type": "uint80"},
            {"name": "answer",          "type": "int256"},
            {"name": "startedAt",       "type": "uint256"},
            {"name": "updatedAt",       "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass
class PendingSettlement:
    trade_id: int
    market_slug: str
    market_start_ts: float
    market_end_ts: float
    direction: str          # "long" or "short"
    poly_price: float
    chainlink_entry: Optional[float] = None
    chainlink_exit: Optional[float] = None
    settled: bool = False


class SettlementTracker:
    """
    Tracks open trades and records Chainlink TWAP settlement outcomes.
    """

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        twap_feed: Optional[PolymarketChainlinkTwapFeed] = None,
        twap_window_seconds: int = 30,
    ):
        self.rpc_url = rpc_url or os.getenv("ETH_RPC_URL", "")
        self._w3: Optional[object] = None
        self._feed: Optional[object] = None
        self._decimals: Optional[int] = None

        self._pending: List[PendingSettlement] = []
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._ml_engine = None
        self._rest_warned = False
        self._price_source: str = "none"
        self._active_market_slug: Optional[str] = None
        self._active_market_start_ts: Optional[float] = None

        # Polymarket-authoritative TWAP (5-min → 30s window).
        self.twap_window_seconds = twap_window_seconds
        self.twap = twap_feed or get_twap_feed(window_seconds=twap_window_seconds)

        if WEB3_AVAILABLE and self.rpc_url:
            self._connect()

        # Start RTDS early so we buffer TWAP ticks before the first market bind.
        try:
            self.twap.start()
        except Exception as e:
            logger.warning(f"SettlementTracker: TWAP feed failed to start: {e}")

        logger.info(
            "SettlementTracker: primary price source = "
            f"Polymarket RTDS Chainlink TWAP {twap_window_seconds}s "
            f"(on-chain fallback={'yes' if self._feed else 'no'})"
        )

    def _connect(self) -> bool:
        try:
            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url))
            if not self._w3.is_connected():
                logger.warning("SettlementTracker: could not connect to RPC")
                return False
            self._feed = self._w3.eth.contract(
                address=Web3.to_checksum_address(CHAINLINK_BTC_USD),
                abi=CHAINLINK_ABI,
            )
            self._decimals = self._feed.functions.decimals().call()
            logger.info(
                f"SettlementTracker: on-chain Chainlink fallback ready "
                f"(decimals={self._decimals})"
            )
            return True
        except Exception as e:
            logger.warning(f"SettlementTracker: RPC connection failed: {e}")
            return False

    def _get_btc_price(self) -> Optional[float]:
        """Return the current BTC/USD price used for display + settlement.

        Priority:
          1. Polymarket RTDS Chainlink TWAP (matches UI / resolution)
          2. On-chain Chainlink aggregator
          3. Coinbase / Binance REST spot
        """
        twap = self.twap.get_latest()
        if twap is not None:
            self._price_source = f"rtds_twap_{self.twap_window_seconds}s"
            return twap

        if self._feed is not None:
            try:
                data = self._feed.functions.latestRoundData().call()
                self._price_source = "chainlink_onchain"
                return float(data[1]) / (10 ** self._decimals)
            except Exception as e:
                logger.warning(
                    f"Chainlink price fetch failed: {e} — trying REST fallback"
                )

        spot = self._get_btc_price_rest()
        if spot is not None:
            self._price_source = "rest_spot"
        return spot

    def _get_btc_price_rest(self) -> Optional[float]:
        """Fetch BTC/USD from public REST APIs (no auth, synchronous)."""
        endpoints = (
            ("Coinbase", "https://api.coinbase.com/v2/prices/BTC-USD/spot",
             lambda j: float(j["data"]["amount"])),
            ("Binance", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
             lambda j: float(j["price"])),
        )
        try:
            import httpx
        except ImportError:
            httpx = None

        for name, url, parse in endpoints:
            try:
                if httpx is not None:
                    with httpx.Client(timeout=6.0) as client:
                        resp = client.get(url)
                        resp.raise_for_status()
                        return parse(resp.json())
                else:
                    import json as _json
                    import urllib.request
                    with urllib.request.urlopen(url, timeout=6.0) as r:
                        return parse(_json.loads(r.read().decode()))
            except Exception as e:
                if not self._rest_warned:
                    logger.warning(f"REST BTC price ({name}) failed: {e}")
        self._rest_warned = True
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_current_btc_price(self) -> Optional[float]:
        """Live BTC price — Polymarket Chainlink TWAP when the RTDS feed is up."""
        return self._get_btc_price()

    def ensure_price_to_beat(
        self,
        market_slug: str,
        market_start_ts: float,
    ) -> Optional[float]:
        """
        Freeze (or return) the Price-to-Beat for a 5-min market.

        Priority:
          1. Gamma ``eventMetadata`` (authoritative once published)
          2. RTDS TWAP observation at the open boundary (±2s only)
          3. Already-cached open-boundary value

        Never freezes a live TWAP print — that is what drifted from Polymarket.
        """
        market_start_ts = float(market_start_ts)
        self._active_market_slug = market_slug
        self._active_market_start_ts = market_start_ts

        # Not open yet — never invent PTB from the current live price.
        if time.time() < market_start_ts:
            return None

        # Prefer Gamma when available (matches Polymarket forensic PTB).
        gamma_ptb = self._gamma_price_to_beat(market_slug, market_start_ts)
        if gamma_ptb is not None:
            return self.twap.set_price_to_beat(
                market_slug,
                gamma_ptb,
                source="gamma",
                reason="Gamma eventMetadata",
            )

        locked = self.twap.capture_price_to_beat(market_slug, market_start_ts)
        if locked is not None:
            return locked

        return self.twap.get_price_to_beat(market_slug)

    def _gamma_price_to_beat(
        self, market_slug: str, market_start_ts: float
    ) -> Optional[float]:
        """
        Recover PTB from Gamma when the RTDS open sample was missed.

        1. This event's ``eventMetadata.priceToBeat`` (often post-close)
        2. Previous event's ``eventMetadata.finalPrice``
           (invariant: finalPrice[N] == priceToBeat[N+1])
        """
        try:
            import httpx

            def _meta(slug: str) -> dict:
                with httpx.Client(timeout=8.0) as client:
                    resp = client.get(
                        "https://gamma-api.polymarket.com/events",
                        params={"slug": slug},
                    )
                    resp.raise_for_status()
                    data = resp.json()
                ev = data[0] if isinstance(data, list) and data else data
                if not isinstance(ev, dict):
                    return {}
                return ev.get("eventMetadata") or {}

            own = _meta(market_slug)
            if own.get("priceToBeat") is not None:
                return float(own["priceToBeat"])

            prev_slug = f"btc-updown-5m-{int(market_start_ts) - 300}"
            prev = _meta(prev_slug)
            if prev.get("finalPrice") is not None:
                return float(prev["finalPrice"])
            return None
        except Exception as e:
            logger.debug(f"Gamma PTB lookup failed: {e}")
            return None

    def get_price_to_beat(self, market_slug: str) -> Optional[float]:
        return self.twap.get_price_to_beat(market_slug)

    def register_active_market(
        self, market_slug: str, market_start_ts: float
    ) -> Optional[float]:
        """Track the live market and resolve PTB (including mid-cycle starts)."""
        return self.ensure_price_to_beat(market_slug, market_start_ts)

    def start_tracking(self) -> None:
        if self._running:
            return
        self._running = True
        # Start RTDS TWAP stream first so UI prices match Polymarket.
        try:
            self.twap.start()
        except Exception as e:
            logger.warning(f"Failed to start TWAP feed: {e}")
        self._thread = threading.Thread(
            target=self._settlement_loop,
            daemon=True,
            name="SettlementTracker",
        )
        self._thread.start()
        logger.info("Settlement tracker thread started")

    def stop_tracking(self) -> None:
        self._running = False
        try:
            self.twap.stop()
        except Exception:
            pass

    def register_trade(
        self,
        trade_id: int,
        market_slug: str,
        market_start_ts: float,
        market_end_ts: float,
        direction: str,
        poly_price: float,
    ) -> None:
        """Register a new trade for settlement monitoring."""
        entry_price = self.ensure_price_to_beat(market_slug, market_start_ts)
        if entry_price is None:
            entry_price = self._get_btc_price()

        ps = PendingSettlement(
            trade_id=trade_id,
            market_slug=market_slug,
            market_start_ts=market_start_ts,
            market_end_ts=market_end_ts,
            direction=direction,
            poly_price=poly_price,
            chainlink_entry=entry_price,
        )

        with self._lock:
            self._pending.append(ps)

        entry_str = f"${entry_price:.2f}" if entry_price is not None else "N/A"
        logger.info(
            f"SettlementTracker: registered trade {trade_id} "
            f"({market_slug}) — price-to-beat={entry_str}"
        )

    # ── Settlement loop ───────────────────────────────────────────────────────

    def _settlement_loop(self) -> None:
        while self._running:
            try:
                self._resolve_active_price_to_beat()
                self._check_settlements()
            except Exception as e:
                logger.warning(f"Settlement check error: {e}")
            time.sleep(5)

    def _resolve_active_price_to_beat(self) -> None:
        """Retry PTB capture for the live market (mid-cycle start recovery)."""
        slug = self._active_market_slug
        start_ts = self._active_market_start_ts
        if not slug or start_ts is None:
            return
        # Always re-run ensure so Gamma can upgrade a weaker TWAP lock once
        # forensic priceToBeat / previous finalPrice is published.
        before = self.twap.get_price_to_beat(slug)
        value = self.ensure_price_to_beat(slug, start_ts)
        if value is not None and (before is None or abs(value - before) > 1e-6):
            logger.info(f"PTB resolver filled {slug}: ${value:,.4f}")

    def _check_settlements(self) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()

        with self._lock:
            pending = list(self._pending)

        for ps in pending:
            if ps.settled:
                continue

            # Wait for the TWAP window after market end to fully elapse.
            if now_ts < ps.market_end_ts + self.twap_window_seconds:
                continue

            # Prefer TWAP at the end boundary; fall back to latest.
            exit_price = self.twap.get_price_near(ps.market_end_ts)
            if exit_price is None:
                exit_price = self._get_btc_price()
            if exit_price is None:
                logger.warning(
                    f"Could not fetch settlement price for trade {ps.trade_id} — retrying"
                )
                continue

            entry = (
                self.get_price_to_beat(ps.market_slug)
                or ps.chainlink_entry
                or exit_price
            )
            outcome = 1 if exit_price > entry else 0

            logger.info(
                f"Settlement: trade {ps.trade_id} ({ps.market_slug}) | "
                f"entry=${entry:.2f}, exit=${exit_price:.2f} | "
                f"outcome={'UP' if outcome == 1 else 'DOWN'} | "
                f"our_bet={ps.direction} | source={self._price_source}"
            )

            if self._ml_engine is None:
                from core.strategy.ml_engine import get_ml_engine
                self._ml_engine = get_ml_engine()

            self._ml_engine.record_outcome(
                trade_id=ps.trade_id,
                chainlink_entry=entry,
                chainlink_exit=exit_price,
                outcome=outcome,
            )

            ps.chainlink_entry = entry
            ps.chainlink_exit = exit_price
            ps.settled = True
            self._ml_engine.maybe_retrain()

        cutoff = datetime.now(timezone.utc).timestamp() - 3600
        with self._lock:
            self._pending = [
                p for p in self._pending
                if not p.settled or p.market_end_ts > cutoff
            ]

    def get_pending_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._pending if not p.settled)

    def get_resolved_outcome(self, trade_id: int) -> Optional[Dict]:
        """Return the resolved outcome for ``trade_id`` if TWAP has settled it."""
        with self._lock:
            for p in self._pending:
                if p.trade_id != trade_id:
                    continue
                if not p.settled or p.chainlink_exit is None:
                    return None
                entry = p.chainlink_entry if p.chainlink_entry is not None else p.chainlink_exit
                return {
                    "trade_id": p.trade_id,
                    "market_slug": p.market_slug,
                    "direction": p.direction,
                    "chainlink_entry": entry,
                    "chainlink_exit": p.chainlink_exit,
                    "outcome": 1 if p.chainlink_exit > entry else 0,
                }
        return None

    def get_stats(self) -> Dict:
        with self._lock:
            pending = [p for p in self._pending if not p.settled]
            settled = [p for p in self._pending if p.settled]
        thread_alive = bool(self._thread and self._thread.is_alive())
        twap_stats = self.twap.get_stats()
        twap_ok = bool(twap_stats.get("connected"))
        return {
            "chainlink_connected": twap_ok or self._feed is not None,
            "twap_connected": twap_ok,
            "price_source": (
                f"rtds_twap_{self.twap_window_seconds}s"
                if twap_ok
                else self._price_source
            ),
            "pending_settlements": len(pending),
            "settled_today": len(settled),
            "rpc_configured": bool(self.rpc_url) or twap_ok,
            "running": self._running and thread_alive,
            "twap": twap_stats,
        }


_tracker_instance: Optional[SettlementTracker] = None


def get_settlement_tracker() -> SettlementTracker:
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = SettlementTracker()
    return _tracker_instance
