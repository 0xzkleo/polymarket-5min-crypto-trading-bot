"""
backtest.engine
===============
Replay the OHLCV-momentum signal over historical BTC 15-minute windows and
settle each on the real BTC up/down outcome.

The signal logic mirrors ``core.strategy.processors.ohlcv_momentum`` exactly
(same RSI/MACD/Bollinger/return votes, thresholds, confidence and session
multiplier) but is driven from historical candles instead of a live fetch, so
the backtest reflects what the bot's primary kline-derived signal would have
done.

Scope note: orderbook / liquidation / funding / sentiment processors require
live venue data that is not in historical klines, so they are not part of this
replay. This harness therefore measures the *OHLCV-momentum directional edge*,
which is the strategy's main kline-reconstructable signal.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from backtest.data import Candle
from core.strategy.processors.ohlcv_momentum import (
    ASIA_OPEN_HOURS,
    DEAD_ZONE_HOURS,
    NY_OPEN_HOURS,
    _atr,
    _bollinger,
    _macd,
    _rsi,
)

MARKET_SECONDS = 900  # 15-minute markets


@dataclass
class BacktestConfig:
    symbol: str = "BTCUSDT"
    days: int = 30
    decision_minute: float = 9.0       # minutes into the 15-min window to decide
    entry_price: float = 0.50          # assumed Polymarket price of the bet side
    fee_pct: float = 0.0               # round-trip cost as fraction of stake
    stake: float = 1.0                 # USD per trade
    min_confidence: float = 0.55
    # Signal thresholds (kept in sync with OHLCVMomentumProcessor defaults).
    rsi_overbought: float = 68.0
    rsi_oversold: float = 32.0
    bb_upper: float = 0.85
    bb_lower: float = 0.15
    lookback: int = 100                # trailing candles fed to the indicators


@dataclass
class BacktestTrade:
    decision_time: str
    direction: str            # "long" | "short"
    confidence: float
    entry_btc: float
    exit_btc: float
    btc_up: bool
    correct: bool
    pnl: float
    vol_regime: str
    hour: int


@dataclass
class BacktestResult:
    config: dict
    windows_evaluated: int = 0
    trades: int = 0
    no_signal: int = 0
    wins: int = 0
    losses: int = 0
    directional_accuracy: float = 0.0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    roi_pct: float = 0.0
    long_trades: int = 0
    long_accuracy: float = 0.0
    short_trades: int = 0
    short_accuracy: float = 0.0
    by_regime: dict = field(default_factory=dict)
    by_hour: dict = field(default_factory=dict)
    equity_curve: List[float] = field(default_factory=list)
    trade_log: List[dict] = field(default_factory=list)


def _session_multiplier(hour: int):
    if hour in DEAD_ZONE_HOURS:
        return 0.80, "dead_zone"
    if hour in NY_OPEN_HOURS:
        return 1.10, "ny_open"
    if hour in ASIA_OPEN_HOURS:
        return 1.05, "asia_open"
    return 1.00, "normal"


class BacktestEngine:
    """Replays the OHLCV momentum signal over historical 15-min windows."""

    def __init__(self, config: BacktestConfig):
        self.cfg = config

    def _signal(
        self, closes: List[float], highs: List[float], lows: List[float], hour: int
    ) -> Optional[tuple]:
        """Return (direction, confidence, vol_regime) or None — mirrors the
        OHLCVMomentumProcessor voting exactly."""
        cfg = self.cfg
        if len(closes) < 26:
            return None

        rsi = _rsi(closes)
        macd_line, macd_signal = _macd(closes)
        pct_b = _bollinger(closes)
        atr = _atr(highs, lows, closes)
        atr_pct = atr / closes[-1] if closes[-1] > 0 else 0.0
        vol_regime = "LOW" if atr_pct < 0.002 else ("HIGH" if atr_pct > 0.005 else "NORMAL")

        ret15 = (closes[-1] - closes[-16]) / closes[-16] if len(closes) >= 16 else 0.0

        bullish = bearish = 0
        if rsi < cfg.rsi_oversold:
            bullish += 2
        elif rsi > cfg.rsi_overbought:
            bearish += 2
        if macd_line > macd_signal and macd_line > 0:
            bullish += 1
        elif macd_line < macd_signal and macd_line < 0:
            bearish += 1
        if pct_b < cfg.bb_lower:
            bullish += 1
        elif pct_b > cfg.bb_upper:
            bearish += 1
        if ret15 > 0.003:
            bullish += 1
        elif ret15 < -0.003:
            bearish += 1

        total = bullish + bearish
        if total == 0:
            return None
        dominant = max(bullish, bearish)
        if dominant < 2:
            return None

        if bullish >= bearish:
            direction = "long"
            vote_ratio = bullish / total
        else:
            direction = "short"
            vote_ratio = bearish / total

        base_conf = 0.55 + (vote_ratio - 0.5) * 0.50
        if vol_regime == "HIGH":
            base_conf += 0.04
        elif vol_regime == "LOW":
            base_conf -= 0.03

        session_mult, _ = _session_multiplier(hour)
        confidence = min(0.85, base_conf * session_mult)
        if confidence < cfg.min_confidence:
            return None
        return direction, confidence, vol_regime

    def run(self, candles: List[Candle]) -> BacktestResult:
        cfg = self.cfg
        result = BacktestResult(config=cfg.__dict__.copy())

        if len(candles) < cfg.lookback + 20:
            logger.error("Not enough candles to backtest")
            return result

        by_time: Dict[int, int] = {c.open_time: i for i, c in enumerate(candles)}
        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        decision_offset_ms = int(cfg.decision_minute * 60 * 1000)
        end_offset_ms = MARKET_SECONDS * 1000

        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        gross_win = 0.0
        gross_loss = 0.0

        # Iterate over wall-clock 15-min market boundaries.
        first_ms = candles[0].open_time
        last_ms = candles[-1].open_time
        market_start = first_ms - (first_ms % (MARKET_SECONDS * 1000))

        ts = market_start
        while ts + end_offset_ms <= last_ms:
            decision_ms = ts + decision_offset_ms
            exit_ms = ts + end_offset_ms

            di = by_time.get(decision_ms)
            xi = by_time.get(exit_ms)
            if di is None or xi is None or di < cfg.lookback:
                ts += MARKET_SECONDS * 1000
                continue

            result.windows_evaluated += 1

            window_closes = closes[di - cfg.lookback + 1 : di + 1]
            window_highs = highs[di - cfg.lookback + 1 : di + 1]
            window_lows = lows[di - cfg.lookback + 1 : di + 1]

            decision_dt = datetime.fromtimestamp(decision_ms / 1000, tz=timezone.utc)
            sig = self._signal(window_closes, window_highs, window_lows, decision_dt.hour)
            if sig is None:
                result.no_signal += 1
                ts += MARKET_SECONDS * 1000
                continue

            direction, confidence, vol_regime = sig
            entry_btc = closes[di]
            exit_btc = closes[xi]
            btc_up = exit_btc > entry_btc
            correct = (direction == "long" and btc_up) or (direction == "short" and not btc_up)

            # Binary payoff at the assumed entry price.
            p = max(0.01, min(0.99, cfg.entry_price))
            qty = cfg.stake / p
            if correct:
                pnl = qty * (1.0 - p)
            else:
                pnl = -cfg.stake
            pnl -= cfg.fee_pct * cfg.stake

            equity += pnl
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
            if pnl > 0:
                gross_win += pnl
                result.wins += 1
            else:
                gross_loss += -pnl
                result.losses += 1

            result.trades += 1
            if direction == "long":
                result.long_trades += 1
            else:
                result.short_trades += 1

            reg = result.by_regime.setdefault(vol_regime, {"trades": 0, "correct": 0})
            reg["trades"] += 1
            reg["correct"] += int(correct)

            hb = result.by_hour.setdefault(decision_dt.hour, {"trades": 0, "correct": 0})
            hb["trades"] += 1
            hb["correct"] += int(correct)

            result.equity_curve.append(round(equity, 4))
            result.trade_log.append(
                BacktestTrade(
                    decision_time=decision_dt.isoformat(),
                    direction=direction,
                    confidence=round(confidence, 4),
                    entry_btc=round(entry_btc, 2),
                    exit_btc=round(exit_btc, 2),
                    btc_up=btc_up,
                    correct=correct,
                    pnl=round(pnl, 4),
                    vol_regime=vol_regime,
                    hour=decision_dt.hour,
                ).__dict__
            )

            ts += MARKET_SECONDS * 1000

        # Aggregate metrics.
        n = result.trades
        correct_total = sum(1 for t in result.trade_log if t["correct"])
        result.directional_accuracy = correct_total / n if n else 0.0
        result.win_rate = result.wins / n if n else 0.0
        result.total_pnl = round(equity, 4)
        result.avg_win = round(gross_win / result.wins, 4) if result.wins else 0.0
        result.avg_loss = round(-gross_loss / result.losses, 4) if result.losses else 0.0
        result.profit_factor = round(gross_win / gross_loss, 4) if gross_loss > 0 else float("inf")
        result.max_drawdown = round(max_dd, 4)
        invested = n * cfg.stake
        result.roi_pct = round((equity / invested) * 100, 4) if invested else 0.0

        lc = sum(1 for t in result.trade_log if t["direction"] == "long" and t["correct"])
        sc = sum(1 for t in result.trade_log if t["direction"] == "short" and t["correct"])
        result.long_accuracy = lc / result.long_trades if result.long_trades else 0.0
        result.short_accuracy = sc / result.short_trades if result.short_trades else 0.0

        for d in result.by_regime.values():
            d["accuracy"] = round(d["correct"] / d["trades"], 4) if d["trades"] else 0.0
        for d in result.by_hour.values():
            d["accuracy"] = round(d["correct"] / d["trades"], 4) if d["trades"] else 0.0

        return result
