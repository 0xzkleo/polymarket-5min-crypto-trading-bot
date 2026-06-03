"""
backtest.replay_recordings
==========================
Backtest the **full fused multi-signal strategy** over data captured live by
``core.recording.signal_recorder``.

Unlike ``backtest.engine`` (which can only replay the kline-derived OHLCV
momentum signal because orderbook / liquidation / funding / sentiment data is
not in historical klines), this harness replays the *actual* signals every
processor produced live, re-runs the weighted fusion, and settles each cycle on
the **real** BTC up/down outcome that the recorder resolved from Chainlink/REST.

This lets you:
  * measure the directional edge of the fused signal as it really fired,
  * sweep fusion weights / thresholds without re-running the live bot,
  * compare fusion vs. the live ML model's p(UP) on the same cycles.

Examples
--------
    # Replay everything with the live default fusion weights
    python -m backtest.replay_recordings

    # Only count cycles where >=2 signals agreed and consensus >=55
    python -m backtest.replay_recordings --min-signals 2 --min-score 55

    # Override a processor weight and write a per-cycle CSV
    python -m backtest.replay_recordings --weight SpikeDetection=0.5 --csv

    # Assume a flat 0.50 entry price instead of the real recorded poly_price
    python -m backtest.replay_recordings --entry-price 0.50

Notes
-----
* ``outcome`` is "did BTC move up between the decision point and settlement",
  matching how the recorder labels each cycle.
* By default the bet's entry price is the **real** Polymarket price recorded at
  decision time, so P&L reflects the actual odds the bot would have paid.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

DEFAULT_DB = "signal_recordings.db"

# Mirrors core.strategy.fusion.SignalFusionEngine default weights.
DEFAULT_WEIGHTS: Dict[str, float] = {
    "SpikeDetection":    0.40,
    "PriceDivergence":   0.30,
    "SentimentAnalysis": 0.20,
    "default":           0.10,
}


@dataclass
class ReplayResult:
    config: dict
    cycles_total: int = 0
    cycles_settled: int = 0
    no_fusion: int = 0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    directional_accuracy: float = 0.0
    total_pnl: float = 0.0
    roi_pct: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    long_trades: int = 0
    long_accuracy: float = 0.0
    short_trades: int = 0
    short_accuracy: float = 0.0
    ml_agree_trades: int = 0
    ml_accuracy: float = 0.0
    by_source_firing: dict = field(default_factory=dict)
    trade_log: List[dict] = field(default_factory=list)


def _fuse(
    signals: List[dict],
    weights: Dict[str, float],
    min_signals: int,
    min_score: float,
) -> Optional[dict]:
    """Replicate SignalFusionEngine.fuse_signals scoring (no recency filter)."""
    if not signals or len(signals) < min_signals:
        return None

    bullish = bearish = 0.0
    for s in signals:
        w = weights.get(s.get("source"), weights["default"])
        strength = (s.get("strength") or 2) / 4.0
        conf = min(1.0, max(0.0, float(s.get("confidence") or 0.0)))
        contrib = w * conf * strength
        if s.get("direction") == "bullish":
            bullish += contrib
        elif s.get("direction") == "bearish":
            bearish += contrib

    total = bullish + bearish
    if total < 0.0001:
        return None

    if bullish >= bearish:
        direction, dominant = "long", bullish
    else:
        direction, dominant = "short", bearish

    score = (dominant / total) * 100 if total > 0 else 0.0
    if score < min_score:
        return None
    return {"direction": direction, "score": score}


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay recorded fused signals vs real BTC outcomes")
    p.add_argument("--db", default=DEFAULT_DB, help="recorder SQLite DB path")
    p.add_argument("--min-signals", type=int, default=1, help="min processors firing to fuse")
    p.add_argument("--min-score", type=float, default=50.0, help="min consensus score (0-100)")
    p.add_argument("--min-confidence", type=float, default=0.0,
                   help="skip fused signals below this avg signal confidence")
    p.add_argument("--entry-price", type=float, default=None,
                   help="override bet entry price (default: real recorded poly_price)")
    p.add_argument("--fee", type=float, default=0.0, help="round-trip cost as fraction of stake")
    p.add_argument("--stake", type=float, default=1.0, help="USD per trade")
    p.add_argument("--weight", action="append", default=[],
                   help="override a processor weight, e.g. --weight SpikeDetection=0.5")
    p.add_argument("--csv", action="store_true", help="write replay_trades.csv")
    p.add_argument("--out", default="replay_results.json")
    return p.parse_args(argv)


def _load_weights(overrides: List[str]) -> Dict[str, float]:
    weights = dict(DEFAULT_WEIGHTS)
    for item in overrides:
        if "=" not in item:
            logger.warning(f"Ignoring malformed --weight {item!r} (expected name=value)")
            continue
        name, _, val = item.partition("=")
        try:
            weights[name.strip()] = float(val)
        except ValueError:
            logger.warning(f"Ignoring --weight {item!r}: {val!r} is not a number")
    return weights


def _count_cycles(db_path: str) -> int:
    if not Path(db_path).exists():
        return 0
    try:
        with sqlite3.connect(db_path) as conn:
            return conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def _load_rows(db_path: str) -> List[dict]:
    if not Path(db_path).exists():
        logger.error(
            f"Recording DB not found: {db_path}. Run the bot (live or sim) first "
            f"so core.recording.signal_recorder can capture cycles."
        )
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT * FROM cycles
                WHERE settled = 1 AND outcome IS NOT NULL
                ORDER BY market_end_ts
                """
            ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError as e:
        logger.error(f"Could not read recordings from {db_path}: {e}")
        return []


def run_replay(args: argparse.Namespace) -> ReplayResult:
    weights = _load_weights(args.weight)
    rows = _load_rows(args.db)

    result = ReplayResult(config={
        "db": args.db,
        "min_signals": args.min_signals,
        "min_score": args.min_score,
        "min_confidence": args.min_confidence,
        "entry_price": args.entry_price,
        "fee_pct": args.fee,
        "stake": args.stake,
        "weights": weights,
    })

    result.cycles_total = _count_cycles(args.db)
    result.cycles_settled = len(rows)
    if not rows:
        return result

    equity = peak = max_dd = 0.0
    gross_win = gross_loss = 0.0
    ml_correct = 0

    for r in rows:
        try:
            signals = json.loads(r.get("signals_json") or "[]")
        except Exception:
            signals = []

        for s in signals:
            src = s.get("source", "unknown")
            result.by_source_firing[src] = result.by_source_firing.get(src, 0) + 1

        fused = _fuse(signals, weights, args.min_signals, args.min_score)
        if fused is None:
            result.no_fusion += 1
            continue

        # Optional avg-confidence gate (mirrors fusion's avg_conf).
        if args.min_confidence > 0.0:
            confs = [float(s.get("confidence") or 0.0) for s in signals]
            if confs and (sum(confs) / len(confs)) < args.min_confidence:
                result.no_fusion += 1
                continue

        outcome = int(r["outcome"])              # 1 = BTC up, 0 = down
        btc_up = outcome == 1
        direction = fused["direction"]
        correct = (direction == "long" and btc_up) or (direction == "short" and not btc_up)

        p = args.entry_price if args.entry_price is not None else float(r.get("poly_price") or 0.5)
        p = max(0.01, min(0.99, p))
        qty = args.stake / p
        pnl = qty * (1.0 - p) if correct else -args.stake
        pnl -= args.fee * args.stake

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
            result.long_accuracy += int(correct)
        else:
            result.short_trades += 1
            result.short_accuracy += int(correct)

        # ML comparison: did the live model's p(UP) point the right way?
        ml_p_up = r.get("ml_p_up")
        if ml_p_up is not None:
            result.ml_agree_trades += 1
            ml_dir_up = float(ml_p_up) >= 0.5
            if ml_dir_up == btc_up:
                ml_correct += 1

        result.trade_log.append({
            "timestamp": r.get("timestamp"),
            "market_slug": r.get("market_slug"),
            "direction": direction,
            "fused_score": round(fused["score"], 2),
            "num_signals": r.get("num_signals"),
            "poly_price": r.get("poly_price"),
            "btc_entry": r.get("btc_entry"),
            "btc_exit": r.get("btc_exit"),
            "btc_up": btc_up,
            "correct": correct,
            "ml_p_up": ml_p_up,
            "pnl": round(pnl, 4),
        })

    n = result.trades
    correct_total = sum(1 for t in result.trade_log if t["correct"])
    result.directional_accuracy = correct_total / n if n else 0.0
    result.total_pnl = round(equity, 4)
    result.profit_factor = round(gross_win / gross_loss, 4) if gross_loss > 0 else float("inf")
    result.max_drawdown = round(max_dd, 4)
    invested = n * args.stake
    result.roi_pct = round((equity / invested) * 100, 4) if invested else 0.0
    result.long_accuracy = round(result.long_accuracy / result.long_trades, 4) if result.long_trades else 0.0
    result.short_accuracy = round(result.short_accuracy / result.short_trades, 4) if result.short_trades else 0.0
    result.ml_accuracy = round(ml_correct / result.ml_agree_trades, 4) if result.ml_agree_trades else 0.0
    return result


def _print_report(r: ReplayResult) -> None:
    bar = "=" * 70
    print("\n" + bar)
    print("  FUSED-SIGNAL REPLAY — recorded live signals vs real BTC outcomes")
    print(bar)
    print(f"  Recording DB     : {r.config['db']}")
    print(f"  Fusion gate      : min_signals={r.config['min_signals']}  "
          f"min_score={r.config['min_score']}  min_conf={r.config['min_confidence']}")
    ep = r.config["entry_price"]
    print(f"  Entry price      : {'real recorded poly_price' if ep is None else f'{ep:.2f} (override)'}"
          f"   fee/stake={r.config['fee_pct']:.2%}  stake=${r.config['stake']:.2f}")
    print("-" * 70)
    print(f"  Cycles recorded  : {r.cycles_total}   (settled & usable: {r.cycles_settled})")
    print(f"  Fused trades     : {r.trades}   (no-fusion cycles: {r.no_fusion})")
    print(bar)
    print("  DIRECTIONAL EDGE  (the metric that matters)")
    flag = ">50% EDGE" if r.directional_accuracy > 0.5 else "NO EDGE (<=50%)"
    print(f"  Directional acc  : {r.directional_accuracy:.2%}   [{flag}]")
    print(f"    long           : {r.long_accuracy:.2%}  ({r.long_trades} trades)")
    print(f"    short          : {r.short_accuracy:.2%}  ({r.short_trades} trades)")
    if r.ml_agree_trades:
        print(f"  ML p(UP) acc     : {r.ml_accuracy:.2%}  ({r.ml_agree_trades} cycles had a model)")
    print("-" * 70)
    print(f"  Win rate         : {(r.wins / r.trades if r.trades else 0):.2%}  ({r.wins}W / {r.losses}L)")
    print(f"  Total P&L        : ${r.total_pnl:+.2f}   ROI/turnover: {r.roi_pct:+.2f}%")
    pf = "inf" if r.profit_factor == float("inf") else f"{r.profit_factor:.2f}"
    print(f"  Profit factor    : {pf}")
    print(f"  Max drawdown     : ${r.max_drawdown:.2f}")
    print("-" * 70)
    print("  Processor firing counts (across settled cycles):")
    for src, cnt in sorted(r.by_source_firing.items(), key=lambda x: -x[1]):
        print(f"    {src:<22}: {cnt}")
    print(bar + "\n")


def main(argv=None) -> int:
    args = _parse_args(argv)
    result = run_replay(args)

    if result.cycles_settled == 0:
        logger.warning(
            "No settled cycles to replay yet. Let the bot run so the recorder "
            "captures cycles AND the background resolver labels outcomes "
            "(outcomes settle ~30s after each 15-min market closes)."
        )

    _print_report(result)

    payload = result.__dict__.copy()
    trade_log = payload.pop("trade_log")
    Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
    logger.info(f"Wrote summary to {args.out}")

    if args.csv and trade_log:
        csv_path = Path("replay_trades.csv")
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(trade_log[0].keys()))
            w.writeheader()
            w.writerows(trade_log)
        logger.info(f"Wrote {len(trade_log)} trades to {csv_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
