"""
main.py — Unified CLI entry point for the Polymarket BTC 15-min trading bot.

Usage
-----
    python main.py --test-mode        # simulation, 1-min trade clock, full tracking
    python main.py --simulation       # simulation, normal 15-min clock
    python main.py --live             # REAL MONEY — requires .env keys
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
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.columns import Columns
from rich.rule import Rule
from rich import box

console = Console()


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(test_mode: bool, verbose: bool, enable_tui: bool = True) -> None:
    logger.remove()
    level = "DEBUG" if verbose else "INFO"

    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_label = "test" if test_mode else "sim"

    if enable_tui:
        from monitoring.terminal_ui import install_log_sink
        install_log_sink(level=level)
    else:
        fmt = (
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level:<7}</level> | "
            "<cyan>{name}</cyan> | "
            "{message}"
        )
        logger.add(sys.stderr, format=fmt, level=level, colorize=True)

    logger.add(
        log_dir / f"bot_{mode_label}_{ts}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {name} | {message}",
        level="DEBUG",
        rotation="50 MB",
        retention="14 days",
    )
    if not enable_tui:
        logger.info(f"Logging to: logs/bot_{mode_label}_{ts}.log")


# ── Startup banner ────────────────────────────────────────────────────────────

def print_promo() -> None:
    """Branded promo panel shown at every startup."""
    from rich.align import Align

    body = Text(justify="center")
    body.append("⭐  Want more profitable trading bots?  ⭐\n\n", style="bold yellow")
    body.append("  Gamma Trade Lab  ", style="bold white on dark_orange")
    body.append("  builds high-performance\n", style="white")
    body.append("  automated trading systems for crypto prediction markets.\n\n", style="dim white")

    body.append("  🌐  GitHub    ", style="dim")
    body.append("https://github.com/gamma-trade-lab\n", style="bold cyan underline")

    body.append("  📧  Gmail     ", style="dim")
    body.append("gammatradeorg@gmail.com\n", style="bold cyan")

    body.append("  ✈️  Telegram  ", style="dim")
    body.append("https://t.me/RetroValix", style="bold cyan underline")

    console.print(Panel(
        Align.center(body),
        title="[bold yellow]✦  GAMMA TRADE LAB  ✦[/bold yellow]",
        border_style="dark_orange",
        padding=(1, 4),
        subtitle="[dim yellow]Bots that actually work[/dim yellow]",
    ))
    console.print()


def print_banner(simulation: bool, test_mode: bool) -> None:
    if test_mode:
        mode_text  = Text("TEST SIMULATION", style="bold yellow")
        mode_desc  = "1-min trade clock  ·  5-min learning cycle"
        mode_style = "yellow"
    elif simulation:
        mode_text  = Text("SIMULATION", style="bold cyan")
        mode_desc  = "15-min clock  ·  paper trades only  ·  no real orders"
        mode_style = "cyan"
    else:
        mode_text  = Text("⚡ LIVE TRADING  —  REAL MONEY AT RISK", style="bold red blink")
        mode_desc  = "Real orders will be placed on Polymarket"
        mode_style = "red"

    title = Text()
    title.append("POLYMARKET ", style="bold white")
    title.append("BTC", style="bold yellow")
    title.append(" 15-MIN BOT", style="bold white")

    body = Text(justify="center")
    body.append("Mode:  ", style="dim")
    body.append(mode_text)
    body.append(f"\n{mode_desc}", style="dim")

    if test_mode:
        body.append("\n\nTrades → ", style="dim")
        body.append("paper_trades.json", style="cyan")
        body.append("   View → ", style="dim")
        body.append("python scripts/view_trades.py", style="cyan")

    console.print()
    console.print(Panel(body, title=title, border_style=mode_style, padding=(1, 4)))


# ── Pre-flight checks ─────────────────────────────────────────────────────────

def preflight(simulation: bool, silent: bool = False) -> bool:
    if not silent:
        console.print()
        console.print(Rule("[bold white]PRE-FLIGHT CHECKS[/bold white]", style="white"))

    ok = True

    table = Table(
        box=box.SIMPLE,
        show_header=False,
        pad_edge=False,
        padding=(0, 1),
    )
    table.add_column("icon",  style="bold", width=4)
    table.add_column("key",   style="cyan", min_width=32)
    table.add_column("status")

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
            if val:
                table.add_row("✓", key, Text("OK", style="green"))
            else:
                table.add_row("[red]✗[/red]", key, Text("MISSING", style="bold red"))
                ok = False

    for key in required_all:
        val = os.getenv(key)
        if val:
            table.add_row("✓", key, Text("OK", style="green"))
        else:
            table.add_row("[yellow]–[/yellow]", key,
                          Text("not set — settlement tracking disabled", style="yellow"))

    optional = [
        ("xgboost",    "ML model"),
        ("sklearn",    "ML calibration"),
        ("web3",       "Chainlink settlement"),
        ("websockets", "Binance streams"),
        ("redis",      "live mode switching"),
    ]
    for pkg, purpose in optional:
        try:
            __import__(pkg)
            table.add_row("✓", f"{pkg}", Text(f"installed  ({purpose})", style="green"))
        except ImportError:
            table.add_row("[dim]–[/dim]", f"{pkg}",
                          Text(f"not installed  ({purpose} disabled)", style="dim"))

    if not silent:
        console.print(table)
        console.print(Rule(style="white"))
    return ok


# ── Live session dashboard ────────────────────────────────────────────────────

def print_live_dashboard(paper_trades_path: str = "paper_trades.json") -> None:
    try:
        with open(paper_trades_path) as f:
            trades = json.load(f)
    except Exception:
        return

    if not trades:
        return

    settled    = [t for t in trades if t.get("outcome") in ("WIN", "LOSS")]
    wins       = sum(1 for t in settled if t["outcome"] == "WIN")
    losses     = len(settled) - wins
    win_rate   = wins / len(settled) * 100 if settled else 0
    total_pnl  = sum(t.get("pnl_usd", 0) for t in trades)

    streak = 0
    streak_type = ""
    for t in reversed(settled):
        if not streak_type:
            streak_type = t["outcome"]
        if t["outcome"] == streak_type:
            streak += 1
        else:
            break

    pnl_style    = "bold green" if total_pnl >= 0 else "bold red"
    wr_style     = "green" if win_rate >= 55 else ("yellow" if win_rate >= 45 else "red")
    streak_style = "green" if streak_type == "WIN" else "red"

    g = Table.grid(padding=(0, 2))
    g.add_column(style="dim", justify="right")
    g.add_column()

    g.add_row("Trades",   f"{len(trades)}  settled: {len(settled)}")
    g.add_row("Win rate", Text(f"{win_rate:.1f}%", style=wr_style))
    g.add_row("Wins / Losses", f"{wins} / {losses}")
    g.add_row("Cum. PnL", Text(f"${total_pnl:+.4f}", style=pnl_style))
    if streak_type:
        g.add_row("Streak", Text(f"{streak}× {streak_type}", style=streak_style))

    now = datetime.now().strftime("%H:%M:%S")
    console.print(Panel(
        g,
        title=f"[bold cyan]SESSION DASHBOARD[/bold cyan]  [dim]{now}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print(
        "  [dim]Full report →[/dim]  [cyan]python scripts/view_trades.py[/cyan]"
    )
    console.print()


# ── Main ──────────────────────────────────────────────────────────────────────

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
        help="Simulation with 1-min trade clock and 5-min learning cycle",
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
    parser.add_argument("--no-tui",      action="store_true", help="Disable live terminal dashboard (use stderr logs)")
    parser.add_argument("--verbose",     action="store_true", help="Enable DEBUG level logging")
    parser.add_argument("--skip-checks", action="store_true", help="Skip pre-flight env checks")

    args = parser.parse_args()

    if args.live:
        simulation = False
        test_mode  = False
    elif args.test_mode:
        simulation = True
        test_mode  = True
    else:
        simulation = True
        test_mode  = False

    enable_tui = not args.no_tui

    if not enable_tui:
        print_promo()
        print_banner(simulation, test_mode)

    setup_logging(test_mode, args.verbose, enable_tui=enable_tui)
    enable_grafana = not args.no_grafana

    if not args.skip_checks:
        ok = preflight(simulation, silent=enable_tui)
        if not ok and not simulation:
            console.print()
            console.print(Panel(
                "[bold red]Missing required API keys for live trading.[/bold red]\n"
                "Add them to your [cyan].env[/cyan] file and retry.",
                border_style="red",
                title="[red]ABORTED[/red]",
            ))
            sys.exit(1)

    if not simulation and not enable_tui:
        console.print()
        console.print(Panel(
            "[bold red]LIVE TRADING MODE[/bold red]\n\n"
            "Real money will be placed on Polymarket.\n"
            "Press [bold]ENTER[/bold] to continue or [bold]Ctrl+C[/bold] to abort.",
            border_style="red",
            padding=(1, 4),
        ))
        try:
            input("  → ")
        except KeyboardInterrupt:
            console.print("\n[yellow]Aborted.[/yellow]")
            sys.exit(0)

    try:
        from bot.runner import run_integrated_bot
    except ImportError as e:
        if enable_tui:
            console.print(f"[red]Could not import bot.runner: {e}[/red]")
        else:
            logger.error(f"Could not import bot.runner: {e}")
            logger.error("Make sure you are running from the project root directory.")
        sys.exit(1)

    try:
        run_integrated_bot(
            simulation=simulation,
            enable_grafana=enable_grafana,
            test_mode=test_mode,
            enable_tui=enable_tui,
        )
    except KeyboardInterrupt:
        if not enable_tui:
            console.print()
            console.print(Rule("[yellow]Shutting down[/yellow]", style="yellow"))
            console.print("  Trades saved to [cyan]paper_trades.json[/cyan]")
            console.print("  View results:  [cyan]python scripts/view_trades.py[/cyan]")
            console.print()
    except Exception as e:
        if enable_tui:
            console.print(f"[red]Bot crashed: {e}[/red]")
        else:
            logger.error(f"Bot crashed: {e}")
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
