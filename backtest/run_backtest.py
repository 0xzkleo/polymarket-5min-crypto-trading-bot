"""
backtest.run_backtest
=====================
CLI entry point for the historical-replay backtester.

Examples
--------
    python -m backtest.run_backtest --days 30
    python -m backtest.run_backtest --days 60 --decision-min 9 --entry-price 0.5
    python -m backtest.run_backtest --days 30 --entry-price 0.55 --fee 0.01 --csv

Notes
-----
* Downloads free Binance 1-minute klines (cached under ``backtest/_cache``).
* ``--entry-price`` is the assumed Polymarket price of the side you bet. At 0.50
  the result is purely win-rate driven (>50% accuracy = profitable). Set it
  higher to model paying up for an edge.
* Results are written to ``backtest_results.json`` (+ ``--csv`` for a per-trade
  CSV at ``backtest_trades.csv``).
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from loguru import logger

from backtest.data import fetch_klines
from backtest.engine import BacktestConfig, BacktestEngine, BacktestResult


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polymarket BTC 15m strategy backtester")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--days", type=int, default=30, help="days of history to replay")
    p.add_argument("--decision-min", type=float, default=9.0,
                   help="minutes into the 15-min window to make the decision")
    p.add_argument("--entry-price", type=float, default=0.50,
                   help="assumed Polymarket price of the bet side (0-1)")
    p.add_argument("--fee", type=float, default=0.0,
                   help="round-trip cost as fraction of stake (e.g. 0.01 = 1%%)")
    p.add_argument("--stake", type=float, default=1.0, help="USD per trade")
    p.add_argument("--min-confidence", type=float, default=0.55)
    p.add_argument("--no-cache", action="store_true", help="ignore the kline disk cache")
    p.add_argument("--csv", action="store_true", help="also write backtest_trades.csv")
    p.add_argument("--out", default="backtest_results.json")
    return p.parse_args(argv)


def _print_report(r: BacktestResult) -> None:
    c = r.config
    bar = "=" * 70
    print("\n" + bar)
    print("  BACKTEST RESULTS — Polymarket BTC 15m (OHLCV-momentum signal)")
    print(bar)
    print(f"  Symbol/Range     : {c['symbol']}  |  {c['days']} days")
    print(f"  Decision point   : minute {c['decision_minute']:.1f} of each 15-min market")
    print(f"  Assumed entry px : {c['entry_price']:.2f}   fee/stake: {c['fee_pct']:.2%}   "
          f"stake: ${c['stake']:.2f}")
    print("-" * 70)
    print(f"  Windows evaluated: {r.windows_evaluated}")
    print(f"  Trades taken     : {r.trades}   (no-signal windows: {r.no_signal})")
    print(bar)
    print("  DIRECTIONAL EDGE  (the metric that matters)")
    acc_flag = ">50% EDGE" if r.directional_accuracy > 0.5 else "NO EDGE (<=50%)"
    print(f"  Directional acc  : {r.directional_accuracy:.2%}   [{acc_flag}]")
    print(f"    long           : {r.long_accuracy:.2%}  ({r.long_trades} trades)")
    print(f"    short          : {r.short_accuracy:.2%}  ({r.short_trades} trades)")
    print("-" * 70)
    print("  P&L  (binary payoff at assumed entry price)")
    print(f"  Win rate         : {r.win_rate:.2%}  ({r.wins}W / {r.losses}L)")
    print(f"  Total P&L        : ${r.total_pnl:+.2f}   ROI/turnover: {r.roi_pct:+.2f}%")
    print(f"  Avg win / loss   : ${r.avg_win:+.4f} / ${r.avg_loss:+.4f}")
    pf = "inf" if r.profit_factor == float("inf") else f"{r.profit_factor:.2f}"
    print(f"  Profit factor    : {pf}")
    print(f"  Max drawdown     : ${r.max_drawdown:.2f}")
    print("-" * 70)
    print("  Accuracy by volatility regime:")
    for reg, d in sorted(r.by_regime.items()):
        print(f"    {reg:<7}: {d.get('accuracy', 0):.2%}  ({d['trades']} trades)")
    print("  Best/worst hours (UTC, by accuracy, min 5 trades):")
    hours = [(h, d) for h, d in r.by_hour.items() if d["trades"] >= 5]
    hours.sort(key=lambda x: x[1].get("accuracy", 0), reverse=True)
    for h, d in hours[:3]:
        print(f"    {h:02d}:00  {d.get('accuracy', 0):.2%}  ({d['trades']} trades)")
    for h, d in hours[-3:][::-1]:
        print(f"    {h:02d}:00  {d.get('accuracy', 0):.2%}  ({d['trades']} trades)")
    print(bar + "\n")


def main(argv=None) -> int:
    args = _parse_args(argv)
    cfg = BacktestConfig(
        symbol=args.symbol,
        days=args.days,
        decision_minute=args.decision_min,
        entry_price=args.entry_price,
        fee_pct=args.fee,
        stake=args.stake,
        min_confidence=args.min_confidence,
    )

    candles = fetch_klines(
        symbol=cfg.symbol, interval="1m", days=cfg.days, use_cache=not args.no_cache
    )
    if not candles:
        logger.error("No candle data — aborting backtest")
        return 1

    result = BacktestEngine(cfg).run(candles)
    if result.trades == 0:
        logger.warning("No trades generated — try a longer range or lower --min-confidence")

    _print_report(result)

    out_path = Path(args.out)
    payload = result.__dict__.copy()
    # Keep the JSON light: drop the full trade_log into a separate CSV if asked.
    trade_log = payload.pop("trade_log")
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    logger.info(f"Wrote summary to {out_path}")

    if args.csv and trade_log:
        csv_path = Path("backtest_trades.csv")
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(trade_log[0].keys()))
            w.writeheader()
            w.writerows(trade_log)
        logger.info(f"Wrote {len(trade_log)} trades to {csv_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
