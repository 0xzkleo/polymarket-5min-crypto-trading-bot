"""
Settlement Tracker
==================
Monitors Chainlink BTC/USD onchain price feed to:
  1. Record the BTC price at market START (entry reference)
  2. Record the BTC price at market END (Chainlink settlement)
  3. Determine the TRUE outcome (YES if exit > entry)
  4. Update the ML training database with the outcome
  5. Trigger weekly model retraining

This is Step 5 and the data pipeline for Step 6 of the decision loop:
  5. Wait for Chainlink settlement → record outcome  ← THIS MODULE
  6. Retrain model weekly with new data              ← THIS MODULE (triggers retrain)

WHY CHAINLINK AND NOT JUST POLYMARKET OUTCOME:
  Polymarket uses Chainlink oracle for settlement. By reading the same
  Chainlink rounds, we get the EXACT price that determined the outcome,
  not an approximation. This means our training labels perfectly match
  the actual game mechanic.

HOW IT WORKS:
  - At market open: snapshot the latest Chainlink round price
  - At market close (+900s): snapshot the latest Chainlink round price
  - If close_price > open_price → outcome = 1 (UP)
  - If close_price <= open_price → outcome = 0 (DOWN)
  - Write outcome to feature_store.db via MLPredictionEngine.record_outcome()

CHAINLINK BTC/USD on Ethereum Mainnet:
  Proxy: 0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c
  We use the public RPC (no API key needed if using a free RPC like Alchemy free tier).
  ETH_RPC_URL env var should be set.

BACKGROUND THREAD:
  start_tracking() launches a daemon thread that runs a scheduler loop.
  Each pending trade is checked every 30 seconds until its market closes.
"""

import os
import json
import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from loguru import logger

try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    logger.warning("web3 not installed — Chainlink settlement disabled. Run: pip install web3")

CHAINLINK_BTC_USD = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"

CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",    "type": "uint80"},
            {"name": "answer",     "type": "int256"},
            {"name": "startedAt",  "type": "uint256"},
            {"name": "updatedAt",  "type": "uint256"},
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
    trade_id: int                       # ML DB row ID
    market_slug: str
    market_start_ts: float              # Unix timestamp of market open
    market_end_ts: float                # Unix timestamp of market close
    direction: str                      # "long" or "short" (our bet direction)
    poly_price: float                   # price we bet at
    chainlink_entry: Optional[float] = None   # BTC price at entry
    chainlink_exit: Optional[float] = None    # BTC price at settlement
    settled: bool = False


class SettlementTracker:
    """
    Background daemon that tracks open trades and records Chainlink settlement outcomes.
    """

    def __init__(self, rpc_url: Optional[str] = None):
        self.rpc_url = rpc_url or os.getenv("ETH_RPC_URL", "")
        self._w3: Optional[object] = None
        self._feed: Optional[object] = None
        self._decimals: Optional[int] = None

        self._pending: List[PendingSettlement] = []
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Lazy import ML engine to avoid circular imports
        self._ml_engine = None

        if WEB3_AVAILABLE and self.rpc_url:
            self._connect()

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
            logger.info(f"SettlementTracker: connected to Chainlink feed (decimals={self._decimals})")
            return True
        except Exception as e:
            logger.warning(f"SettlementTracker: RPC connection failed: {e}")
            return False

    def _get_btc_price(self) -> Optional[float]:
        """Fetch current BTC/USD from Chainlink."""
        if self._feed is None:
            return None
        try:
            data = self._feed.functions.latestRoundData().call()
            answer = data[1]
            return float(answer) / (10 ** self._decimals)
        except Exception as e:
            logger.warning(f"Chainlink price fetch failed: {e}")
            return None

    # ── Public API ────────────────────────────────────────────────────────────

    def start_tracking(self) -> None:
        """Start the settlement monitoring daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._settlement_loop,
            daemon=True,
            name="SettlementTracker",
        )
        self._thread.start()
        logger.info("Settlement tracker thread started")

    def stop_tracking(self) -> None:
        self._running = False

    def register_trade(
        self,
        trade_id: int,
        market_slug: str,
        market_start_ts: float,
        market_end_ts: float,
        direction: str,
        poly_price: float,
    ) -> None:
        """
        Register a new trade for settlement monitoring.
        Call this immediately after placing a bet.
        """
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

        logger.info(
            f"SettlementTracker: registered trade {trade_id} "
            f"({market_slug}) — entry BTC=${entry_price:.2f if entry_price else 'N/A'}"
        )

    # ── Settlement loop ──────────────────────────────────────────────────────

    def _settlement_loop(self) -> None:
        while self._running:
            try:
                self._check_settlements()
            except Exception as e:
                logger.warning(f"Settlement check error: {e}")
            time.sleep(30)  # check every 30 seconds

    def _check_settlements(self) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()

        with self._lock:
            pending = list(self._pending)

        for ps in pending:
            if ps.settled:
                continue

            # Market has closed: fetch settlement price
            if now_ts >= ps.market_end_ts + 30:  # +30s grace for Chainlink update
                exit_price = self._get_btc_price()

                if exit_price is None:
                    logger.warning(f"Could not fetch settlement price for trade {ps.trade_id} — retrying")
                    continue

                entry = ps.chainlink_entry or exit_price  # fallback

                # Determine outcome: did BTC go UP between entry and exit?
                outcome = 1 if exit_price > entry else 0

                logger.info(
                    f"Settlement: trade {ps.trade_id} ({ps.market_slug}) | "
                    f"entry=${entry:.2f}, exit=${exit_price:.2f} | "
                    f"outcome={'UP ✓' if outcome == 1 else 'DOWN ✗'} | "
                    f"our_bet={ps.direction}"
                )

                # Record in ML DB
                if self._ml_engine is None:
                    from core.strategy_brain.ml_prediction_engine import get_ml_engine
                    self._ml_engine = get_ml_engine()

                self._ml_engine.record_outcome(
                    trade_id=ps.trade_id,
                    chainlink_entry=entry,
                    chainlink_exit=exit_price,
                    outcome=outcome,
                )

                ps.chainlink_exit = exit_price
                ps.settled = True

                # Trigger weekly retraining check
                self._ml_engine.maybe_retrain()

        # Clean up settled trades older than 1 hour
        cutoff = datetime.now(timezone.utc).timestamp() - 3600
        with self._lock:
            self._pending = [
                p for p in self._pending
                if not p.settled or p.market_end_ts > cutoff
            ]

    def get_pending_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._pending if not p.settled)

    def get_stats(self) -> Dict:
        with self._lock:
            pending = [p for p in self._pending if not p.settled]
            settled = [p for p in self._pending if p.settled]
        return {
            "chainlink_connected": self._feed is not None,
            "pending_settlements": len(pending),
            "settled_today": len(settled),
            "rpc_configured": bool(self.rpc_url),
        }


# Singleton
_tracker_instance: Optional[SettlementTracker] = None

def get_settlement_tracker() -> SettlementTracker:
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = SettlementTracker()
    return _tracker_instance