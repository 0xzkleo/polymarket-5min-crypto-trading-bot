"""
core.settlement.twap_feed
=========================
Polymarket RTDS relay of Chainlink-computed BTC/USD TWAP.

As of 2026-08-07, Polymarket BTC 5-min Up/Down markets resolve against the
Chainlink **30-second TWAP** (not a single spot tick). The same feed supplies:

  * live "BTC Price" on the Polymarket UI
  * "Price to Beat" — TWAP observed at the market open boundary
  * settlement close — TWAP observed at the market end boundary

RTDS: wss://ws-live-data.polymarket.com
Topic (5-min markets): crypto_prices_twap_thirty
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, List, Optional

from loguru import logger

try:
    import websockets
    from websockets.exceptions import ConnectionClosed

    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    ConnectionClosed = Exception  # type: ignore[misc, assignment]
    logger.warning("websockets not installed — Polymarket TWAP feed disabled")


RTDS_URL = "wss://ws-live-data.polymarket.com"
TWAP_30_TOPIC = "crypto_prices_twap_thirty"
TWAP_60_TOPIC = "crypto_prices_twap_sixty"
BTC_FILTER = '{"symbol":"btc/usd"}'

# Survive mid-cycle restarts: keep PTB + recent TWAP ticks on disk.
_CACHE_PATH = Path("data/twap_ptb_cache.json")
# Polymarket locks PTB from the TWAP observation at the open boundary.
# Only accept ticks this close to market_start — never "latest live" prints.
_OPEN_TICK_MAX_LAG_SEC = 3.0
# Source priority for overwrites (higher wins).
_PTB_SOURCE_RANK = {
    "gamma": 100,
    "twap_open": 80,
    "twap_near": 50,
    "legacy": 10,
}


@dataclass
class TwapTick:
    """One Chainlink TWAP observation relayed by Polymarket RTDS."""

    value: float
    obs_ts_ms: int          # Chainlink observation time (ms)
    window_s: int           # 30 or 60
    recv_ts: float          # local monotonic receipt time


class PolymarketChainlinkTwapFeed:
    """
    Background RTDS client for Chainlink BTC/USD TWAP.

    Keeps a short history so callers can freeze Price-to-Beat at a market
    open boundary and read the close TWAP at settlement.
    """

    def __init__(
        self,
        window_seconds: int = 30,
        symbol: str = "btc/usd",
        history_seconds: int = 900,
    ):
        if window_seconds not in (30, 60):
            raise ValueError("window_seconds must be 30 or 60")
        self.window_seconds = window_seconds
        self.symbol = symbol.lower()
        self.history_seconds = history_seconds
        self.topic = TWAP_30_TOPIC if window_seconds == 30 else TWAP_60_TOPIC

        self._lock = threading.RLock()
        self._history: Deque[TwapTick] = deque()
        self._latest: Optional[TwapTick] = None
        self._price_to_beat: Dict[str, float] = {}
        self._price_to_beat_source: Dict[str, str] = {}
        self._connected = False
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_error: Optional[str] = None
        self._last_cache_save: float = 0.0
        self._load_cache()

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("TWAP feed: websockets missing — not starting")
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name=f"TwapFeed-{self.window_seconds}s",
        )
        self._thread.start()
        logger.info(
            f"TWAP feed starting — topic={self.topic} symbol={self.symbol} "
            f"(cached_ptb={len(self._price_to_beat)}, hist={len(self._history)})"
        )

    def stop(self) -> None:
        self._running = False

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._connected and self._latest is not None

    def get_stats(self) -> Dict:
        with self._lock:
            age = None
            if self._latest is not None:
                age = max(0.0, time.time() - self._latest.recv_ts)
            return {
                "connected": self._connected and self._latest is not None,
                "window_seconds": self.window_seconds,
                "latest": self._latest.value if self._latest else None,
                "age_sec": age,
                "history_len": len(self._history),
                "last_error": self._last_error,
                "source": f"rtds:{self.topic}",
            }

    # ── price accessors ─────────────────────────────────────────────────────

    def get_latest(self) -> Optional[float]:
        with self._lock:
            return self._latest.value if self._latest else None

    def get_latest_tick(self) -> Optional[TwapTick]:
        with self._lock:
            return self._latest

    def get_open_boundary_tick(
        self,
        unix_ts: float,
        *,
        max_lag_sec: float = _OPEN_TICK_MAX_LAG_SEC,
    ) -> Optional[TwapTick]:
        """
        TWAP tick Polymarket uses for Price-to-Beat: observation at/just after
        the market open boundary (not a later live print).
        """
        target_ms = int(float(unix_ts) * 1000)
        max_lag_ms = int(max_lag_sec * 1000)
        with self._lock:
            exact: Optional[TwapTick] = None
            first_after: Optional[TwapTick] = None
            best_before: Optional[TwapTick] = None
            for tick in self._history:
                delta = tick.obs_ts_ms - target_ms
                if delta == 0:
                    exact = tick
                elif delta > 0:
                    if first_after is None or tick.obs_ts_ms < first_after.obs_ts_ms:
                        first_after = tick
                elif delta >= -max_lag_ms:
                    if best_before is None or tick.obs_ts_ms > best_before.obs_ts_ms:
                        best_before = tick

            if exact is not None:
                return exact
            if first_after is not None and (first_after.obs_ts_ms - target_ms) <= max_lag_ms:
                return first_after
            # Prefer a tick slightly before open only if nothing at/after exists
            # within the lag window (clock skew); still must be very close.
            if best_before is not None and first_after is None:
                return best_before
            return None

    def get_price_near(
        self,
        unix_ts: float,
        *,
        max_delta_sec: float = _OPEN_TICK_MAX_LAG_SEC,
        allow_early_join_sec: float = _OPEN_TICK_MAX_LAG_SEC,
    ) -> Optional[float]:
        """Return open-boundary TWAP only (strict)."""
        tick = self.get_open_boundary_tick(
            unix_ts, max_lag_sec=max(max_delta_sec, allow_early_join_sec)
        )
        return tick.value if tick else None

    def capture_price_to_beat(
        self,
        market_slug: str,
        market_start_ts: float,
        *,
        force: bool = False,
    ) -> Optional[float]:
        """
        Freeze Price-to-Beat from the TWAP observation at market open.

        Never locks a random live TWAP — that diverges from Polymarket's PTB.
        """
        if time.time() < float(market_start_ts):
            # Market not open yet — do not invent a PTB.
            return self.get_price_to_beat(market_slug) if not force else None

        tick = self.get_open_boundary_tick(market_start_ts)
        if tick is None:
            if force:
                return None
            return self.get_price_to_beat(market_slug)

        lag = abs(tick.obs_ts_ms / 1000.0 - float(market_start_ts))
        source = "twap_open" if lag <= 1.0 else "twap_near"
        return self.set_price_to_beat(
            market_slug,
            float(tick.value),
            source=source,
            reason=f"{source} lag={lag:.2f}s",
            force=force,
        )

    def set_price_to_beat(
        self,
        market_slug: str,
        value: float,
        *,
        source: str = "legacy",
        reason: str = "",
        force: bool = False,
    ) -> float:
        """
        Lock PTB. Higher-ranked sources overwrite weaker ones (e.g. Gamma
        replaces an early twap_near guess; never the reverse).
        """
        new_rank = _PTB_SOURCE_RANK.get(source, 0)
        with self._lock:
            old = self._price_to_beat.get(market_slug)
            old_src = self._price_to_beat_source.get(market_slug, "legacy")
            old_rank = _PTB_SOURCE_RANK.get(old_src, 0)
            if old is not None and not force and new_rank < old_rank:
                return old
            if old is not None and not force and new_rank == old_rank and abs(old - float(value)) < 1e-9:
                return old

            self._price_to_beat[market_slug] = float(value)
            self._price_to_beat_source[market_slug] = source
            if len(self._price_to_beat) > 200:
                for key in list(self._price_to_beat.keys())[:50]:
                    self._price_to_beat.pop(key, None)
                    self._price_to_beat_source.pop(key, None)
            changed = old is None or abs(old - float(value)) > 1e-6
            if changed:
                logger.info(
                    f"TWAP Price-to-Beat locked for {market_slug}: "
                    f"${float(value):,.4f}"
                    + (f" ({reason or source})" if (reason or source) else "")
                    + (f" [was ${old:,.4f}]" if old is not None else "")
                )
                self._save_cache(force=True)
            return self._price_to_beat[market_slug]

    def get_price_to_beat(self, market_slug: str) -> Optional[float]:
        with self._lock:
            return self._price_to_beat.get(market_slug)

    def clear_price_to_beat(self, market_slug: str) -> None:
        with self._lock:
            self._price_to_beat.pop(market_slug, None)
            self._price_to_beat_source.pop(market_slug, None)
        self._save_cache(force=True)

    # ── disk cache (mid-cycle restart) ──────────────────────────────────────

    def _load_cache(self) -> None:
        try:
            if not _CACHE_PATH.exists():
                return
            raw = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
            ptb = raw.get("price_to_beat") or {}
            sources = raw.get("price_to_beat_source") or {}
            hist = raw.get("history") or []
            with self._lock:
                for slug, val in ptb.items():
                    try:
                        self._price_to_beat[str(slug)] = float(val)
                        # Drop weak legacy freezes from older builds — they
                        # were often live TWAP prints, not open-boundary PTB.
                        src = str(sources.get(slug, "legacy"))
                        if src in ("legacy", "early-join", "early_join"):
                            continue
                        self._price_to_beat_source[str(slug)] = src
                    except (TypeError, ValueError):
                        continue
                # Re-filter: only keep PTBs that have a ranked source.
                self._price_to_beat = {
                    k: v
                    for k, v in self._price_to_beat.items()
                    if k in self._price_to_beat_source
                }
                cutoff = time.time() - self.history_seconds
                for item in hist:
                    try:
                        tick = TwapTick(
                            value=float(item["value"]),
                            obs_ts_ms=int(item["obs_ts_ms"]),
                            window_s=int(item.get("window_s", self.window_seconds)),
                            recv_ts=float(item.get("recv_ts", time.time())),
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
                    if tick.obs_ts_ms / 1000.0 >= cutoff:
                        self._history.append(tick)
                if self._history:
                    self._latest = self._history[-1]
            logger.info(
                f"TWAP cache loaded: ptb={len(self._price_to_beat)} "
                f"hist={len(self._history)} from {_CACHE_PATH}"
            )
        except Exception as e:
            logger.debug(f"TWAP cache load failed: {e}")

    def _save_cache(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_cache_save) < 5.0:
            return
        self._last_cache_save = now
        try:
            _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                hist: List[dict] = [
                    {
                        "value": t.value,
                        "obs_ts_ms": t.obs_ts_ms,
                        "window_s": t.window_s,
                        "recv_ts": t.recv_ts,
                    }
                    for t in list(self._history)[-2000:]
                ]
                payload = {
                    "price_to_beat": dict(self._price_to_beat),
                    "price_to_beat_source": dict(self._price_to_beat_source),
                    "history": hist,
                    "saved_at": now,
                }
            tmp = _CACHE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(_CACHE_PATH)
        except Exception as e:
            logger.debug(f"TWAP cache save failed: {e}")

    # ── stream loop ─────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while self._running:
            try:
                loop.run_until_complete(self._stream_once())
            except Exception as e:
                self._last_error = str(e)
                with self._lock:
                    self._connected = False
                logger.warning(f"TWAP feed error: {e} — reconnecting in 3s")
                time.sleep(3)
        with self._lock:
            self._connected = False
        loop.close()

    async def _stream_once(self) -> None:
        assert WEBSOCKETS_AVAILABLE
        async with websockets.connect(
            RTDS_URL,
            ping_interval=None,  # RTDS wants app-level PING text frames
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        ) as ws:
            sub = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": self.topic,
                        "type": "update",
                        "filters": BTC_FILTER,
                    }
                ],
            }
            await ws.send(json.dumps(sub))
            logger.info(f"TWAP feed subscribed: {self.topic} {BTC_FILTER}")

            with self._lock:
                self._connected = True
                self._last_error = None

            ping_task = asyncio.create_task(self._ping_loop(ws))
            try:
                async for raw in ws:
                    if not self._running:
                        break
                    if raw is None:
                        continue
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")
                    text = str(raw).strip()
                    if not text or text.upper() == "PONG":
                        continue
                    try:
                        msg = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    self._handle_message(msg)
            finally:
                ping_task.cancel()
                with self._lock:
                    self._connected = False

    async def _ping_loop(self, ws) -> None:
        """RTDS requires a text PING every ~5s."""
        try:
            while self._running:
                await asyncio.sleep(5)
                await ws.send("PING")
        except (asyncio.CancelledError, ConnectionClosed):
            return
        except Exception:
            return

    def _handle_message(self, msg: dict) -> None:
        topic = msg.get("topic")
        if topic and topic != self.topic:
            return
        # Some RTDS frames wrap batches under payload.data
        payload = msg.get("payload") or {}
        if isinstance(payload.get("data"), list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    self._ingest_tick(item, default_window=self.window_seconds)
            return
        self._ingest_tick(payload, default_window=self.window_seconds)

    def _ingest_tick(self, payload: dict, *, default_window: int) -> None:
        if not payload:
            return
        symbol = str(payload.get("symbol") or self.symbol).lower()
        if symbol and symbol != self.symbol:
            return
        try:
            value = float(payload.get("value"))
        except (TypeError, ValueError):
            # Prefer exact E18 string when present.
            raw = payload.get("full_accuracy_value")
            if raw is None:
                return
            try:
                value = float(raw) / 1e18
            except (TypeError, ValueError):
                return
        try:
            obs_ts_ms = int(payload.get("timestamp"))
        except (TypeError, ValueError):
            return
        # Guard against second vs millisecond timestamps.
        if obs_ts_ms < 10_000_000_000:
            obs_ts_ms *= 1000
        window_s = int(payload.get("window_s") or payload.get("windowSeconds") or default_window)

        tick = TwapTick(
            value=value,
            obs_ts_ms=obs_ts_ms,
            window_s=window_s,
            recv_ts=time.time(),
        )
        cutoff_ms = obs_ts_ms - self.history_seconds * 1000
        with self._lock:
            self._latest = tick
            self._history.append(tick)
            while self._history and self._history[0].obs_ts_ms < cutoff_ms:
                self._history.popleft()
        self._save_cache(force=False)


_twap_feed: Optional[PolymarketChainlinkTwapFeed] = None
_twap_lock = threading.Lock()


def get_twap_feed(window_seconds: int = 30) -> PolymarketChainlinkTwapFeed:
    """Process-wide singleton for the BTC TWAP feed used by 5-min markets."""
    global _twap_feed
    with _twap_lock:
        if _twap_feed is None:
            _twap_feed = PolymarketChainlinkTwapFeed(window_seconds=window_seconds)
        return _twap_feed
