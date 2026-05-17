"""
supervisor.py — Auto-restart wrapper for the Polymarket BTC trading bot.

Runs ``main.py`` in a subprocess loop, restarting automatically after
normal exits (e.g. scheduled 90-min refresh) or errors.

Usage
-----
    python supervisor.py [args...]

All arguments are forwarded verbatim to main.py:

    python supervisor.py --test-mode
    python supervisor.py --live
    python supervisor.py --simulation --no-grafana
"""
from __future__ import annotations

import faulthandler
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

faulthandler.enable()

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

BOT_SCRIPT = "main.py"


def run_supervisor() -> None:
    """Launch main.py in a subprocess loop, forwarding all CLI arguments."""
    python_cmd = sys.executable
    bot_args = sys.argv[1:] if len(sys.argv) > 1 else []

    print("=" * 80)
    print("BTC 15-MIN TRADING BOT — AUTO-RESTART SUPERVISOR")
    print("=" * 80)
    print(f"Platform:    {sys.platform}")
    print(f"Python:      {python_cmd}")
    print(f"Bot script:  {BOT_SCRIPT}")
    print(f"Bot args:    {bot_args}")
    print(f"Virtual env: {sys.prefix}")
    print("=" * 80)
    print()

    if not os.path.exists(BOT_SCRIPT):
        print(f"ERROR: Bot script '{BOT_SCRIPT}' not found in {os.getcwd()}")
        print("Available .py files:")
        for f in os.listdir("."):
            if f.endswith(".py"):
                print(f"  - {f}")
        sys.exit(1)

    restart_count = 0

    while True:
        restart_count += 1

        print("=" * 80)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"Starting bot (restart #{restart_count})...")
        cmd = [python_cmd, BOT_SCRIPT, *bot_args]
        print(f"Command: {' '.join(cmd)}")
        print("=" * 80)
        print()

        try:
            result = subprocess.run(cmd, check=False)
            exit_code = result.returncode

            print()
            print("=" * 80)
            print(f"Bot stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Exit code: {exit_code}")
            print("=" * 80)

            # Normal termination codes → short restart delay
            if exit_code in (0, 143, 15, -15):
                print("Normal auto-restart — loading fresh market filters...")
                wait_time = 2
            else:
                print(f"Error detected (code {exit_code}) — waiting before retry...")
                wait_time = 10

            print(f"Restarting in {wait_time} seconds...")
            print()
            time.sleep(wait_time)

        except KeyboardInterrupt:
            print()
            print("=" * 80)
            print("Keyboard interrupt — stopping supervisor")
            print("=" * 80)
            break

        except Exception as e:
            print()
            print("=" * 80)
            print(f"ERROR running bot: {e}")
            print("=" * 80)
            print("Waiting 10 seconds before retry...")
            print()
            time.sleep(10)


if __name__ == "__main__":
    try:
        run_supervisor()
    except KeyboardInterrupt:
        print("\nStopped by user")
        sys.exit(0)
