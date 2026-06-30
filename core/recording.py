"""
core.recording
==============
Persists every strategy decision cycle (processor signals, fused vote, ML p_up)
for offline fused-signal backtesting. A background thread resolves the real
BTC outcome once each market's end timestamp passes.
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

from core.strategy.fusion import FusedSignal
from core.strategy.processors.base import TradingSignal

RECORDINGS_DB = os.getenv("SIGNAL_RECORDINGS_DB", "signal_recordings.db")
RESOLVE_INTERVAL_SEC = float(os.getenv("SIGNAL_RECORDING_RESOLVE_SEC", "15"))


def _serialize_signal(sig: TradingSignal) -> Dict[str, Any]:
    return {
        "source": sig.source,
        "direction": getattr(sig.direction, "value", str(sig.direction)),
        "confidence": float(sig.confidence),
        "score": float(sig.score),
        "signal_type": getattr(sig.signal_type, "value", str(sig.signal_type)),
        "strength": int(getattr(sig.strength, "value", sig.strength)),
        "metadata": {
            k: float(v) if hasattr(v, "__float__") else v
            for k, v in (sig.metadata or {}).items()
        },
        "timestamp": sig.timestamp.isoformat() if sig.timestamp else None,
    }


def _serialize_fused(fused: Optional[FusedSignal]) -> Optional[Dict[str, Any]]:
    if fused is None:
        return None
    return {
        "direction": getattr(fused.direction, "value", str(fused.direction)),
        "confidence": float(fused.confidence),
        "score": float(fused.score),
        "num_signals": int(fused.num_signals),
        "weights": fused.weights,
        "metadata": fused.metadata or {},
        "timestamp": fused.timestamp.isoformat() if fused.timestamp else None,
    }


class SignalRecorder:
    """Records decision cycles and resolves BTC market outcomes in the background."""

    def __init__(
        self,
        price_fn: Optional[Callable[[], Optional[float]]] = None,
        db_path: str = RECORDINGS_DB,
    ):
        self._price_fn = price_fn
        self.db_path = db_path
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_cycles (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at     TEXT NOT NULL,
                    market_slug     TEXT NOT NULL,
                    market_start_ts REAL,
                    market_end_ts   REAL,
                    poly_price      REAL,
                    btc_spot        REAL,
                    ml_p_up         REAL,
                    signals_json    TEXT NOT NULL,
                    fused_json      TEXT,
                    metadata_json   TEXT,
                    btc_entry       REAL,
                    btc_exit        REAL,
                    outcome         INTEGER,
                    resolved_at     TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_signal_cycles_pending
                ON signal_cycles (market_end_ts)
                WHERE outcome IS NULL
                """
            )
            conn.commit()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._resolve_loop,
            name="signal-recorder",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"SignalRecorder started (db={self.db_path})")

    def stop(self) -> None:
        self._running = False

    def record_cycle(
        self,
        *,
        market_slug: str,
        market_start_ts: Optional[float],
        market_end_ts: Optional[float],
        poly_price: float,
        btc_spot: Optional[float],
        signals: List[TradingSignal],
        fused: Optional[FusedSignal],
        ml_p_up: Optional[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "market_slug": market_slug,
            "market_start_ts": market_start_ts,
            "market_end_ts": market_end_ts,
            "poly_price": float(poly_price),
            "btc_spot": float(btc_spot) if btc_spot is not None else None,
            "ml_p_up": float(ml_p_up) if ml_p_up is not None else None,
            "signals_json": json.dumps([_serialize_signal(s) for s in signals]),
            "fused_json": json.dumps(_serialize_fused(fused)),
            "metadata_json": json.dumps(metadata or {}),
        }
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO signal_cycles (
                        recorded_at, market_slug, market_start_ts, market_end_ts,
                        poly_price, btc_spot, ml_p_up,
                        signals_json, fused_json, metadata_json
                    ) VALUES (
                        :recorded_at, :market_slug, :market_start_ts, :market_end_ts,
                        :poly_price, :btc_spot, :ml_p_up,
                        :signals_json, :fused_json, :metadata_json
                    )
                    """,
                    payload,
                )
                conn.commit()

    def _resolve_loop(self) -> None:
        while self._running:
            try:
                self._resolve_pending()
            except Exception as exc:
                logger.debug(f"SignalRecorder resolve loop error: {exc}")
            time.sleep(RESOLVE_INTERVAL_SEC)

    def _resolve_pending(self) -> None:
        if self._price_fn is None:
            return

        now = time.time()
        exit_price = self._price_fn()
        if exit_price is None:
            return

        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT market_slug, market_start_ts, market_end_ts
                    FROM signal_cycles
                    WHERE outcome IS NULL
                      AND market_end_ts IS NOT NULL
                      AND market_end_ts <= ?
                    GROUP BY market_slug, market_start_ts, market_end_ts
                    """,
                    (now,),
                ).fetchall()

                for row in rows:
                    entry_row = conn.execute(
                        """
                        SELECT btc_spot
                        FROM signal_cycles
                        WHERE market_slug = ?
                          AND market_start_ts IS ?
                          AND market_end_ts IS ?
                          AND btc_spot IS NOT NULL
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (row["market_slug"], row["market_start_ts"], row["market_end_ts"]),
                    ).fetchone()

                    entry_price = (
                        float(entry_row["btc_spot"])
                        if entry_row and entry_row["btc_spot"] is not None
                        else exit_price
                    )
                    outcome = 1 if exit_price > entry_price else 0
                    resolved_at = datetime.now(timezone.utc).isoformat()

                    conn.execute(
                        """
                        UPDATE signal_cycles
                        SET btc_entry = ?,
                            btc_exit = ?,
                            outcome = ?,
                            resolved_at = ?
                        WHERE market_slug = ?
                          AND market_start_ts IS ?
                          AND market_end_ts IS ?
                          AND outcome IS NULL
                        """,
                        (
                            entry_price,
                            exit_price,
                            outcome,
                            resolved_at,
                            row["market_slug"],
                            row["market_start_ts"],
                            row["market_end_ts"],
                        ),
                    )

                if rows:
                    conn.commit()
                    logger.info(
                        f"SignalRecorder resolved {len(rows)} market(s) "
                        f"(exit={exit_price:.2f})"
                    )

    def get_stats(self) -> Dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM signal_cycles").fetchone()[0]
            pending = conn.execute(
                "SELECT COUNT(*) FROM signal_cycles WHERE outcome IS NULL"
            ).fetchone()[0]
            resolved = conn.execute(
                "SELECT COUNT(*) FROM signal_cycles WHERE outcome IS NOT NULL"
            ).fetchone()[0]
        return {
            "db_path": self.db_path,
            "total_cycles": total,
            "pending_resolution": pending,
            "resolved_cycles": resolved,
            "running": self._running,
        }


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
