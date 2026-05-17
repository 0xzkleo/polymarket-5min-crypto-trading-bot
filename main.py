"""
main.py — Unified CLI entry point for the Polymarket BTC 15-min trading bot.

Usage
-----
    python main.py --test-mode        # simulation, trade every minute, full tracking
    python main.py --simulation       # simulation, normal 15-min clock
    python main.py --live             # REAL MONEY — requires .env keys

Test mode features
------------------
- Fires a simulated trade every ~1 minute instead of every 15 minutes
- Learning engine weight optimisation runs every 5 min instead of weekly
- All trades saved to paper_trades.json with full context
- Live console dashboard printed after every trade
- Run: python scripts/view_trades.py  at any time to see results
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from loguru import logger


def setup_logging(test_mode: bool, verbose: bool) -> None:
    logger.remove()
    fmt = (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level:<7}</level> | "
        "<cyan>{name}</cyan> | "
        "{message}"
    )
    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, format=fmt, level=level, colorize=True)

    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_label = "test" if test_mode else "sim"
    logger.add(
        log_dir / f"bot_{mode_label}_{ts}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {name} | {message}",
        level="DEBUG",
        rotation="50 MB",
        retention="14 days",
    )
    logger.info(f"Logging to: logs/bot_{mode_label}_{ts}.log")


def preflight(simulation: bool) -> bool:
    """Check required env vars and optional dependencies before starting."""
    ok = True
    print()
    print("=" * 70)
    print("PRE-FLIGHT CHECKS")
    print("=" * 70)

    required_live = [
        "POLYMARKET_PK",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_PASSPHRASE",
    ]
    required_all = ["ETH_RPC_URL"]

    if not simulation:
        for key in required_live:
            val = os.getenv(key)
            status = "OK" if val else "MISSING"
            icon = "[OK]" if val else "[!!]"
            print(f"  {icon}  {key}: {status}")
            if not val:
                ok = False

    for key in required_all:
        val = os.getenv(key)
        status = "OK" if val else "MISSING (settlement tracking disabled)"
        icon = "[OK]" if val else "[--]"
        print(f"  {icon}  {key}: {status}")

    optional = [
        ("xgboost",    "ML model"),
        ("sklearn",    "ML calibration"),
        ("web3",       "Chainlink settlement"),
        ("websockets", "Binance streams"),
        ("redis",      "simulation mode control"),
    ]
    for pkg, purpose in optional:
        try:
            __import__(pkg)
            print(f"  [OK]  {pkg}: installed ({purpose})")
        except ImportError:
            print(f"  [--]  {pkg}: not installed ({purpose} disabled)")

    print("=" * 70)
    return ok


def print_live_dashboard(paper_trades_path: str = "paper_trades.json") -> None:
    """Print a compact summary of current session results."""
    try:
        with open(paper_trades_path) as f:
            trades = json.load(f)
    except Exception:
        return

    if not trades:
        return

    settled = [t for t in trades if t.get("outcome") in ("WIN", "LOSS")]
    wins = sum(1 for t in settled if t["outcome"] == "WIN")
    losses = len(settled) - wins
    win_rate = wins / len(settled) * 100 if settled else 0
    total_pnl = sum(t.get("pnl_usd", 0) for t in trades)

    streak = 0
    streak_type = ""
    for t in reversed(settled):
        if not streak_type:
            streak_type = t["outcome"]
        if t["outcome"] == streak_type:
            streak += 1
        else:
            break

    print()
    print("+" + "-" * 50 + "+")
    print(f"|  SIMULATION DASHBOARD  (as of {datetime.now().strftime('%H:%M:%S')})  |")
    print("+" + "-" * 50 + "+")
    print(f"|  Trades: {len(trades):<5}  Settled: {len(settled):<5}              |")
    print(f"|  Wins:   {wins:<5}  Losses: {losses:<5}  Win rate: {win_rate:.1f}%    |")
    print(f"|  Cumulative PnL: ${total_pnl:+.4f}                    |")
    if streak_type:
        print(f"|  Current streak: {streak}x {streak_type}                      |")
    print("+" + "-" * 50 + "+")
    print(f"|  Run:  python scripts/view_trades.py  for full detail  |")
    print("+" + "-" * 50 + "+")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket BTC 15-min trading bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --test-mode            # fast simulation (1-min intervals)
  python main.py --simulation           # normal simulation (15-min intervals)
  python main.py --live                 # REAL MONEY
  python main.py --test-mode --verbose  # with DEBUG logs
  python main.py --test-mode --no-grafana
        """,
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--test-mode", action="store_true",
        help="Simulation with 1-min trade clock and 5-min learning cycle (fastest way to test)",
    )
    mode_group.add_argument(
        "--simulation", action="store_true",
        help="Simulation with normal 15-min clock",
    )
    mode_group.add_argument(
        "--live", action="store_true",
        help="LIVE mode — REAL MONEY. Requires all API keys in .env",
    )

    parser.add_argument("--no-grafana",  action="store_true", help="Disable Grafana metrics export")
    parser.add_argument("--verbose",     action="store_true", help="Enable DEBUG level logging")
    parser.add_argument("--skip-checks", action="store_true", help="Skip pre-flight env checks")

    args = parser.parse_args()

    if args.live:
        simulation = False
        test_mode = False
    elif args.test_mode:
        simulation = True
        test_mode = True
    else:
        simulation = True
        test_mode = False

    setup_logging(test_mode, args.verbose)
    enable_grafana = not args.no_grafana

    print()
    print("=" * 70)
    print("  POLYMARKET BTC 15-MIN TRADING BOT")
    print("=" * 70)
    if test_mode:
        print("  MODE:  TEST SIMULATION (1-min trade clock)")
        print("         Trades saved to paper_trades.json")
        print("         View results:  python scripts/view_trades.py")
        print("         Learning engine optimises every 5 minutes")
    elif simulation:
        print("  MODE:  SIMULATION (15-min clock, no real orders)")
        print("         Trades saved to paper_trades.json")
    else:
        print("  MODE:  *** LIVE TRADING — REAL MONEY AT RISK ***")
    print("=" * 70)

    if not args.skip_checks:
        ok = preflight(simulation)
        if not ok and not simulation:
            print()
            print("ERROR: Missing required API keys for live trading. Aborting.")
            print("       Add keys to your .env file and retry.")
            sys.exit(1)

    if not simulation:
        print()
        print("=" * 70)
        print("  WARNING: LIVE TRADING MODE")
        print("  Real money will be used. Press Ctrl+C to abort.")
        print("=" * 70)
        try:
            input("  Press ENTER to continue, or Ctrl+C to abort: ")
        except KeyboardInterrupt:
            print("\nAborted.")
            sys.exit(0)

    try:
        from bot.runner import run_integrated_bot
    except ImportError as e:
        logger.error(f"Could not import bot.runner: {e}")
        logger.error("Make sure you are running from the project root directory.")
        sys.exit(1)

    logger.info(f"Starting bot: simulation={simulation} test_mode={test_mode} grafana={enable_grafana}")

    try:
        run_integrated_bot(
            simulation=simulation,
            enable_grafana=enable_grafana,
            test_mode=test_mode,
        )
    except KeyboardInterrupt:
        print("\nShutting down...")
        print("Trades saved to paper_trades.json")
        print("Run:  python scripts/view_trades.py  to see results")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
