"""
core.recording.signal_recorder
===============================
Persist a full snapshot of every live decision cycle so the *fused* multi-signal
strategy can be backtested over time — something the kline-only
``backtest.engine`` cannot do because orderbook / liquidation / funding /
sentiment signals are live-only and not reconstructable from historical klines.

What gets recorded, once per decision cycle
--------------------------------------------
* timestamp + market slug / start / end timestamps
* the Polymarket price (the actual price the bot would have paid)
* the BTC spot reference at decision time
* every individual processor :class:`TradingSignal` that fired (source,
  direction, strength, confidence, score, metadata)
* the fused signal (direction / score / confidence) if one was produced
* the ML model's ``p(UP)`` if the model was active
* the flattened feature metadata used by the ML engine

How the outcome is filled in
----------------------------
Each row is written immediately with ``outcome = NULL``. A background thread
later (after ``market_end_ts``) fetches the BTC price from the **same** source
the bot settles against (Chainlink via ``SettlementTracker``, REST fallback
otherwise) and writes:

    outcome = 1  if  btc_exit > btc_entry(decision)   else  0

i.e. "did BTC move up between the decision point and settlement?" — exactly the
directional question a fused-signal backtest needs to score each cycle.

Storage
-------
SQLite at ``SIGNAL_RECORDING_DB`` (default ``signal_recordings.db``). One row per
cycle in the ``cycles`` table; per-signal and metadata payloads are stored as
JSON text columns. Replay with ``python -m backtest.replay_recordings``.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from loguru import logger

DB_PATH = os.getenv("SIGNAL_RECORDING_DB", "signal_recordings.db")
_SETTLE_BUFFER_SEC = 30          # wait this long past market end before settling
_LOOP_INTERVAL_SEC = 30
_KEEP_UNSETTLED_HOURS = 24       # stop retrying rows older than this


def _direction_str(direction: Any) -> str:
    """Normalise a SignalDirection enum / string to 'bullish'|'bearish'|'neutral'."""
    s = str(getattr(direction, "value", direction)).lower()
    if "bull" in s:
        return "bullish"
    if "bear" in s:
        return "bearish"
    return "neutral"


def _jsonable(value: Any) -> Any:
    """Best-effort conversion of signal/metadata values to JSON-safe types."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    # Decimal, Enum, datetime, numpy scalars, etc.
    if hasattr(value, "value") and not isinstance(value, type):
        try:
            return _jsonable(value.value)
        except Exception:
            pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


def serialize_signal(sig: Any) -> Dict[str, Any]:
    """Turn a TradingSignal into a JSON-safe dict (loss-tolerant)."""
    try:
        return {
            "source": getattr(sig, "source", None),
            "signal_type": _jsonable(getattr(sig, "signal_type", None)),
            "direction": _direction_str(getattr(sig, "direction", None)),
            "strength": int(getattr(getattr(sig, "strength", None), "value", 2) or 2),
            "confidence": float(getattr(sig, "confidence", 0.0) or 0.0),
            "score": float(getattr(sig, "score", 0.0) or 0.0),
            "current_price": _jsonable(getattr(sig, "current_price", None)),
            "metadata": _jsonable(getattr(sig, "metadata", {}) or {}),
        }
    except Exception as e:  # never let recording break the trade loop
        logger.debug(f"serialize_signal failed: {e}")
        return {"source": getattr(sig, "source", "unknown"), "error": str(e)}


class SignalRecorder:
    """Records each decision cycle and resolves real BTC outcomes in the background."""

    def __init__(
        self,
        db_path: str = DB_PATH,
        price_fn: Optional[Callable[[], Optional[float]]] = None,
    ):
        self.db_path = db_path
        self._price_fn = price_fn
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._cycles_recorded = 0
        self._init_db()
        logger.info(f"SignalRecorder initialised (db={self.db_path})")

    # ── Database ───────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cycles (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp        TEXT NOT NULL,
                    market_slug      TEXT,
                    market_start_ts  REAL,
                    market_end_ts    REAL,
                    poly_price       REAL,
                    btc_entry        REAL,
                    ml_p_up          REAL,
                    fused_direction  TEXT,
                    fused_score      REAL,
                    fused_confidence REAL,
                    num_signals      INTEGER,
                    signals_json     TEXT,
                    metadata_json    TEXT,
                    btc_exit         REAL,
                    outcome          INTEGER,
                    settled          INTEGER DEFAULT 0,
                    created_at       TEXT DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cycles_unsettled "
                "ON cycles (settled, market_end_ts)"
            )
            conn.commit()

    # ── Recording ──────────────────────────────────────────────────────────────

    def record_cycle(
        self,
        *,
        market_slug: str,
        market_start_ts: float,
        market_end_ts: float,
        poly_price: float,
        btc_spot: Optional[float],
        signals: List[Any],
        fused: Any = None,
        ml_p_up: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Persist one decision-cycle snapshot. Returns the row id (or None)."""
        try:
            sig_rows = [serialize_signal(s) for s in (signals or [])]
            fused_direction = _direction_str(getattr(fused, "direction", None)) if fused else None
            fused_score = float(getattr(fused, "score", 0.0)) if fused else None
            fused_conf = float(getattr(fused, "confidence", 0.0)) if fused else None

            row = (
                datetime.now(timezone.utc).isoformat(),
                market_slug,
                float(market_start_ts) if market_start_ts is not None else None,
                float(market_end_ts) if market_end_ts is not None else None,
                float(poly_price) if poly_price is not None else None,
                float(btc_spot) if btc_spot is not None else None,
                float(ml_p_up) if ml_p_up is not None else None,
                fused_direction,
                fused_score,
                fused_conf,
                len(sig_rows),
                json.dumps(sig_rows),
                json.dumps(_jsonable(metadata or {})),
            )

            with self._lock, sqlite3.connect(self.db_path) as conn:
                cur = conn.execute(
                    """
                    INSERT INTO cycles (
                        timestamp, market_slug, market_start_ts, market_end_ts,
                        poly_price, btc_entry, ml_p_up, fused_direction,
                        fused_score, fused_confidence, num_signals,
                        signals_json, metadata_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    row,
                )
                conn.commit()
                self._cycles_recorded += 1
                return cur.lastrowid
        except Exception as e:
            logger.warning(f"SignalRecorder.record_cycle failed: {e}")
            return None

    # ── Background outcome resolution ────────────────────────────────────────────

    def _get_price(self) -> Optional[float]:
        if self._price_fn is not None:
            try:
                return self._price_fn()
            except Exception as e:
                logger.debug(f"recorder price_fn failed: {e}")
        # Lazy fallback to the settlement tracker's price source.
        try:
            from core.settlement import get_settlement_tracker
            return get_settlement_tracker().get_current_btc_price()
        except Exception as e:
            logger.debug(f"recorder settlement-price fallback failed: {e}")
            return None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._resolve_loop, daemon=True, name="SignalRecorder"
        )
        self._thread.start()
        logger.info("SignalRecorder outcome-resolution thread started")

    def stop(self) -> None:
        self._running = False

    def _resolve_loop(self) -> None:
        while self._running:
            try:
                self._resolve_pending()
            except Exception as e:
                logger.warning(f"SignalRecorder resolve error: {e}")
            time.sleep(_LOOP_INTERVAL_SEC)

    def _resolve_pending(self) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        cutoff = now_ts - _KEEP_UNSETTLED_HOURS * 3600

        with self._lock, sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, btc_entry, market_end_ts FROM cycles
                WHERE settled = 0 AND market_end_ts IS NOT NULL
                  AND market_end_ts + ? <= ?
                  AND market_end_ts >= ?
                ORDER BY market_end_ts
                LIMIT 50
                """,
                (_SETTLE_BUFFER_SEC, now_ts, cutoff),
            ).fetchall()

        if not rows:
            return

        exit_price = self._get_price()
        if exit_price is None:
            logger.debug("SignalRecorder: no BTC price available — retrying next loop")
            return

        with self._lock, sqlite3.connect(self.db_path) as conn:
            for row_id, btc_entry, _end in rows:
                if btc_entry is None:
                    # No entry reference — settle as undetermined but mark done.
                    conn.execute(
                        "UPDATE cycles SET btc_exit=?, settled=1 WHERE id=?",
                        (exit_price, row_id),
                    )
                    continue
                outcome = 1 if exit_price > btc_entry else 0
                conn.execute(
                    "UPDATE cycles SET btc_exit=?, outcome=?, settled=1 WHERE id=?",
                    (exit_price, outcome, row_id),
                )
            conn.commit()
        logger.debug(f"SignalRecorder: resolved {len(rows)} cycle outcome(s)")

    # ── Stats ───────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        try:
            with self._lock, sqlite3.connect(self.db_path) as conn:
                total = conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
                settled = conn.execute(
                    "SELECT COUNT(*) FROM cycles WHERE settled=1 AND outcome IS NOT NULL"
                ).fetchone()[0]
            return {
                "db_path": self.db_path,
                "cycles_recorded_session": self._cycles_recorded,
                "total_cycles": total,
                "settled_cycles": settled,
                "pending_cycles": total - settled,
            }
        except Exception as e:
            return {"error": str(e)}


_recorder_instance: Optional[SignalRecorder] = None


def get_signal_recorder(
    price_fn: Optional[Callable[[], Optional[float]]] = None,
) -> SignalRecorder:
    global _recorder_instance
    if _recorder_instance is None:
        _recorder_instance = SignalRecorder(price_fn=price_fn)
    elif price_fn is not None and _recorder_instance._price_fn is None:
        _recorder_instance._price_fn = price_fn
    return _recorder_instance
