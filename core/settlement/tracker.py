"""
core.settlement.tracker
========================
Background daemon that monitors Chainlink BTC/USD on-chain price feed to:
  1. Record the BTC price at market START (entry reference)
  2. Record the BTC price at market END  (Chainlink settlement)
  3. Determine the TRUE outcome (YES if exit > entry)
  4. Update the ML training database with the outcome
  5. Trigger weekly model retraining

Chainlink BTC/USD — Ethereum Mainnet
  Proxy: 0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c
  Set ETH_RPC_URL env var to a free RPC endpoint (e.g. Alchemy free tier).
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    logger.warning(
        "web3 not installed — Chainlink settlement disabled. Run: pip install web3"
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
    Background daemon that tracks open trades and records Chainlink settlement
    outcomes.  Lazy-imports the ML engine to avoid circular imports.
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
            logger.info(
                f"SettlementTracker: connected to Chainlink feed "
                f"(decimals={self._decimals})"
            )
            return True
        except Exception as e:
            logger.warning(f"SettlementTracker: RPC connection failed: {e}")
            return False

    def _get_btc_price(self) -> Optional[float]:
        if self._feed is None:
            return None
        try:
            data = self._feed.functions.latestRoundData().call()
            return float(data[1]) / (10 ** self._decimals)
        except Exception as e:
            logger.warning(f"Chainlink price fetch failed: {e}")
            return None

    # ── Public API ────────────────────────────────────────────────────────────

    def start_tracking(self) -> None:
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
        """Register a new trade for settlement monitoring."""
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
            f"({market_slug}) — entry BTC={entry_str}"
        )

    # ── Settlement loop ───────────────────────────────────────────────────────

    def _settlement_loop(self) -> None:
        while self._running:
            try:
                self._check_settlements()
            except Exception as e:
                logger.warning(f"Settlement check error: {e}")
            time.sleep(30)

    def _check_settlements(self) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()

        with self._lock:
            pending = list(self._pending)

        for ps in pending:
            if ps.settled:
                continue

            if now_ts < ps.market_end_ts + 30:
                continue

            exit_price = self._get_btc_price()
            if exit_price is None:
                logger.warning(
                    f"Could not fetch settlement price for trade {ps.trade_id} — retrying"
                )
                continue

            entry = ps.chainlink_entry or exit_price
            outcome = 1 if exit_price > entry else 0

            logger.info(
                f"Settlement: trade {ps.trade_id} ({ps.market_slug}) | "
                f"entry=${entry:.2f}, exit=${exit_price:.2f} | "
                f"outcome={'UP' if outcome == 1 else 'DOWN'} | "
                f"our_bet={ps.direction}"
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

    def get_stats(self) -> Dict:
        with self._lock:
            pending  = [p for p in self._pending if not p.settled]
            settled  = [p for p in self._pending if p.settled]
        return {
            "chainlink_connected": self._feed is not None,
            "pending_settlements": len(pending),
            "settled_today": len(settled),
            "rpc_configured": bool(self.rpc_url),
        }


_tracker_instance: Optional[SettlementTracker] = None


def get_settlement_tracker() -> SettlementTracker:
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = SettlementTracker()
    return _tracker_instance
