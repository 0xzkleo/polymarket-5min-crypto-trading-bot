"""
backtest.data
=============
Fetch historical 1-minute BTC klines from Binance's public REST API (no auth).

Klines are returned as a list of ``Candle`` records sorted by open time. The
fetch is paginated (Binance caps each call at 1000 candles) and lightly cached
to disk so repeated backtests don't re-download the same range.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import httpx
from loguru import logger

BINANCE_REST = "https://api.binance.com"
_INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000}
_CACHE_DIR = Path("backtest/_cache")


@dataclass
class Candle:
    open_time: int      # ms since epoch (UTC)
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def open_dt(self) -> datetime:
        return datetime.fromtimestamp(self.open_time / 1000, tz=timezone.utc)


def _cache_path(symbol: str, interval: str, start_ms: int, end_ms: int) -> Path:
    return _CACHE_DIR / f"{symbol}_{interval}_{start_ms}_{end_ms}.json"


def fetch_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    days: int = 30,
    *,
    end_time: Optional[datetime] = None,
    use_cache: bool = True,
) -> List[Candle]:
    """Fetch ``days`` worth of klines ending at ``end_time`` (default: now)."""
    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported interval {interval!r}")

    end_dt = end_time or datetime.now(timezone.utc)
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = end_ms - days * 24 * 60 * 60 * 1000
    step = _INTERVAL_MS[interval]

    if use_cache:
        cpath = _cache_path(symbol, interval, start_ms, end_ms)
        if cpath.exists():
            try:
                raw = json.loads(cpath.read_text())
                logger.info(f"Loaded {len(raw)} cached candles from {cpath}")
                return [Candle(*c) for c in raw]
            except Exception:
                pass

    candles: List[Candle] = []
    cursor = start_ms
    logger.info(
        f"Downloading {symbol} {interval} klines for {days}d "
        f"({end_dt.strftime('%Y-%m-%d %H:%M')} UTC backwards)..."
    )
    with httpx.Client(timeout=15.0) as client:
        while cursor < end_ms:
            try:
                resp = client.get(
                    f"{BINANCE_REST}/api/v3/klines",
                    params={
                        "symbol": symbol,
                        "interval": interval,
                        "startTime": cursor,
                        "endTime": end_ms,
                        "limit": 1000,
                    },
                )
                resp.raise_for_status()
                batch = resp.json()
            except Exception as e:
                logger.error(f"Kline fetch failed at cursor={cursor}: {e}")
                break

            if not batch:
                break

            for c in batch:
                candles.append(
                    Candle(
                        open_time=int(c[0]),
                        open=float(c[1]),
                        high=float(c[2]),
                        low=float(c[3]),
                        close=float(c[4]),
                        volume=float(c[5]),
                    )
                )
            last_open = int(batch[-1][0])
            cursor = last_open + step
            if len(batch) < 1000:
                break
            time.sleep(0.15)  # be polite to the public endpoint

    # De-dup + sort by open time.
    seen = set()
    unique: List[Candle] = []
    for c in sorted(candles, key=lambda x: x.open_time):
        if c.open_time in seen:
            continue
        seen.add(c.open_time)
        unique.append(c)

    logger.info(f"Fetched {len(unique)} candles ({symbol} {interval})")

    if use_cache and unique:
        try:
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cpath = _cache_path(symbol, interval, start_ms, end_ms)
            cpath.write_text(
                json.dumps(
                    [[c.open_time, c.open, c.high, c.low, c.close, c.volume] for c in unique]
                )
            )
        except Exception as e:
            logger.warning(f"Could not write kline cache: {e}")

    return unique
