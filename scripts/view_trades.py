"""
scripts/view_trades.py — Trade analytics dashboard (paper + live).

Usage
-----
    python scripts/view_trades.py                # paper trades (default)
    python scripts/view_trades.py --live         # live trades
    python scripts/view_trades.py --both         # paper + live combined
    python scripts/view_trades.py --last 20      # last N trades
    python scripts/view_trades.py --summary      # stats only
    python scripts/view_trades.py --csv          # export to CSV
    python scripts/view_trades.py --watch        # auto-refresh every 10 s
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.rule import Rule
from rich.columns import Columns
from rich import box

console = Console()

TRADES_FILE      = Path(__file__).parent.parent / "paper_trades.json"
LIVE_TRADES_FILE = Path(__file__).parent.parent / "live_trades.json"


# ── Data loading ──────────────────────────────────────────────────────────────

def load_trades(path=TRADES_FILE) -> List[Dict[str, Any]]:
    if not Path(path).exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception as e:
        console.print(f"[red]Error loading trades:[/red] {e}")
        return []


def compute_stats(trades: List[Dict]) -> Dict:
    if not trades:
        return {}
    settled = [t for t in trades if t.get("outcome") in ("WIN", "LOSS")]
    wins    = [t for t in settled if t["outcome"] == "WIN"]
    losses  = [t for t in settled if t["outcome"] == "LOSS"]
    pending = [t for t in trades  if t.get("outcome") == "PENDING"]
    n, ns, nw, nl = len(trades), len(settled), len(wins), len(losses)
    wr = nw / ns * 100 if ns else 0
    pnls = [t.get("pnl_usd", 0) for t in trades]
    total_pnl = sum(pnls)
    avg_w = sum(t.get("pnl_usd", 0) for t in wins)   / nw if nw else 0
    avg_l = sum(t.get("pnl_usd", 0) for t in losses) / nl if nl else 0
    pf = abs(avg_w * nw / (avg_l * nl)) if nl and avg_l else float("inf")

    best = worst = cur = 0
    ctype = None
    for t in settled:
        o = t["outcome"]
        if o == ctype:
            cur += 1
        else:
            ctype, cur = o, 1
        if ctype == "WIN":
            best = max(best, cur)
        else:
            worst = max(worst, cur)

    lstreak, ltype = 0, None
    for t in reversed(settled):
        o = t["outcome"]
        if ltype is None:
            ltype = o
        if o == ltype:
            lstreak += 1
        else:
            break

    eq = pk = dd = 0.0
    for p in pnls:
        eq += p
        pk = max(pk, eq)
        dd = max(dd, pk - eq)

    longs  = [t for t in settled if t.get("direction") == "LONG"]
    shorts = [t for t in settled if t.get("direction") == "SHORT"]
    lwr = sum(1 for t in longs  if t["outcome"] == "WIN") / len(longs)  * 100 if longs  else 0
    swr = sum(1 for t in shorts if t["outcome"] == "WIN") / len(shorts) * 100 if shorts else 0

    mle   = [t for t in trades if t.get("ml_edge", 0) > 0]
    aedge = sum(t["ml_edge"] for t in mle) / len(mle) if mle else 0
    apup  = sum(t.get("ml_p_up", 0) for t in trades) / n if n else 0

    rs: Dict = defaultdict(lambda: {"total": 0, "wins": 0})
    for t in settled:
        r = t.get("vol_regime") or "unknown"
        rs[r]["total"] += 1
        if t["outcome"] == "WIN":
            rs[r]["wins"] += 1

    tss = []
    for t in trades:
        try:
            tss.append(datetime.fromisoformat(t["timestamp"]))
        except Exception:
            pass
    dur = ""
    if len(tss) >= 2:
        dt = max(tss) - min(tss)
        h, rem = divmod(int(dt.total_seconds()), 3600)
        m = rem // 60
        dur = f"{h}h {m}m"

    return dict(
        total=n, settled=ns, wins=nw, losses=nl, pending=len(pending),
        win_rate=wr, total_pnl=total_pnl, avg_win=avg_w, avg_loss=avg_l,
        profit_factor=pf, best_streak=best, worst_streak=worst,
        latest_streak=lstreak, latest_type=ltype, max_drawdown=dd,
        long_count=len(longs), short_count=len(shorts), long_wr=lwr, short_wr=swr,
        avg_ml_edge=aedge, avg_ml_pup=apup,
        avg_score=sum(t.get("signal_score", 0) for t in trades) / n if n else 0,
        avg_conf=sum(t.get("signal_confidence", 0) for t in trades) / n if n else 0,
        regime_stats=dict(rs), session_duration=dur,
    )


# ── Summary panel ─────────────────────────────────────────────────────────────

def _pnl_text(v: float) -> Text:
    style = "bold green" if v >= 0 else "bold red"
    return Text(f"${v:+.4f}", style=style)

def _wr_text(v: float) -> Text:
    style = "green" if v >= 55 else ("yellow" if v >= 45 else "red")
    return Text(f"{v:.1f}%", style=style)

def _pf_text(v: float) -> Text:
    if v == float("inf"):
        return Text("∞", style="green")
    style = "green" if v > 1.5 else ("yellow" if v > 1.0 else "red")
    return Text(f"{v:.2f}", style=style)


def print_summary(s: Dict) -> None:
    if not s:
        console.print()
        console.print(Panel(
            "[dim]No trades yet.[/dim]\n\n"
            "Start the bot:  [cyan]python main.py --test-mode[/cyan]",
            border_style="dim",
        ))
        return

    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")

    # ── Overview table ────────────────────────────────────────────────────────
    overview = Table(box=None, show_header=False, pad_edge=False, padding=(0, 2))
    overview.add_column(style="dim", justify="right", min_width=18)
    overview.add_column()

    overview.add_row("Total trades",   str(s["total"]))
    overview.add_row(
        "Settled",
        f"{s['settled']}  "
        f"([green]{s['wins']} W[/green] / [red]{s['losses']} L[/red]  "
        f"pending: {s['pending']})"
    )
    overview.add_row("Win rate",       _wr_text(s["win_rate"]))
    overview.add_row("Session",        s["session_duration"] or "N/A")

    # ── P&L table ─────────────────────────────────────────────────────────────
    pnl_tbl = Table(box=None, show_header=False, pad_edge=False, padding=(0, 2))
    pnl_tbl.add_column(style="dim", justify="right", min_width=18)
    pnl_tbl.add_column()

    pnl_tbl.add_row("Cumulative PnL",  _pnl_text(s["total_pnl"]))
    pnl_tbl.add_row("Avg win",         Text(f"${s['avg_win']:+.4f}", style="green"))
    pnl_tbl.add_row("Avg loss",        Text(f"${s['avg_loss']:+.4f}", style="red"))
    pnl_tbl.add_row("Profit factor",   _pf_text(s["profit_factor"]))
    pnl_tbl.add_row(
        "Max drawdown",
        Text(
            f"${s['max_drawdown']:.4f}",
            style="green" if s["max_drawdown"] < 0.05
                  else ("yellow" if s["max_drawdown"] < 0.10 else "red"),
        ),
    )

    # ── Streaks ───────────────────────────────────────────────────────────────
    streak_tbl = Table(box=None, show_header=False, pad_edge=False, padding=(0, 2))
    streak_tbl.add_column(style="dim", justify="right", min_width=18)
    streak_tbl.add_column()

    lt, ls = s["latest_type"] or "", s["latest_streak"]
    streak_style = "green" if lt == "WIN" else "red"
    streak_tbl.add_row(
        "Current streak",
        Text(f"{ls}× {lt}", style=f"bold {streak_style}") if lt else Text("N/A", style="dim"),
    )
    streak_tbl.add_row("Best win",  Text(f"{s['best_streak']}×",  style="green"))
    streak_tbl.add_row("Worst loss",Text(f"{s['worst_streak']}×", style="red"))

    # ── Direction ─────────────────────────────────────────────────────────────
    dir_tbl = Table(box=None, show_header=False, pad_edge=False, padding=(0, 2))
    dir_tbl.add_column(style="dim", justify="right", min_width=18)
    dir_tbl.add_column()

    dir_tbl.add_row("LONG  trades",
                    f"{s['long_count']}   win rate: "
                    + _wr_text(s["long_wr"]).__str__())
    dir_tbl.add_row("SHORT trades",
                    f"{s['short_count']}   win rate: "
                    + _wr_text(s["short_wr"]).__str__())

    # ── Signal quality ────────────────────────────────────────────────────────
    sig_tbl = Table(box=None, show_header=False, pad_edge=False, padding=(0, 2))
    sig_tbl.add_column(style="dim", justify="right", min_width=18)
    sig_tbl.add_column()

    sig_tbl.add_row("Avg signal score", f"{s['avg_score']:.1f}")
    sig_tbl.add_row("Avg confidence",   f"{s['avg_conf']:.1%}")
    sig_tbl.add_row("Avg ML edge",      f"{s['avg_ml_edge']:.4f}")
    sig_tbl.add_row("Avg ML p(UP)",     f"{s['avg_ml_pup']:.3f}")

    console.print()
    console.print(Panel(overview, title="[bold white]OVERVIEW[/bold white]",       border_style="white",  padding=(0, 1)))
    console.print(Panel(pnl_tbl, title="[bold green]PROFITABILITY[/bold green]",   border_style="green",  padding=(0, 1)))
    console.print(Panel(streak_tbl, title="[bold cyan]STREAKS[/bold cyan]",        border_style="cyan",   padding=(0, 1)))
    console.print(Panel(sig_tbl, title="[bold blue]SIGNAL QUALITY[/bold blue]",    border_style="blue",   padding=(0, 1)))

    # ── Volatility regime breakdown ───────────────────────────────────────────
    if s["regime_stats"]:
        reg_tbl = Table(
            box=box.SIMPLE,
            show_header=True,
            header_style="bold dim",
            pad_edge=False,
        )
        reg_tbl.add_column("Regime",   style="cyan",  min_width=14)
        reg_tbl.add_column("Trades",   justify="right")
        reg_tbl.add_column("Win rate", justify="right")
        reg_tbl.add_column("Bar", min_width=20)

        for reg, d in sorted(s["regime_stats"].items()):
            n2  = d["total"]
            wr2 = d["wins"] / n2 * 100 if n2 else 0
            bar = Text()
            bar.append("█" * d["wins"],      style="green")
            bar.append("░" * (n2 - d["wins"]), style="red")
            reg_tbl.add_row(reg, str(n2), _wr_text(wr2), bar)

        console.print(Panel(
            reg_tbl,
            title="[bold yellow]VOLATILITY REGIME[/bold yellow]",
            border_style="yellow",
            padding=(0, 1),
        ))

    console.print(
        Panel(
            "[dim]Simulation only — no real money involved.[/dim]",
            border_style="dim",
            padding=(0, 2),
        )
    )
    console.print(f"  [dim]as of[/dim] [white]{now}[/white]")
    console.print()

    # ── Promo footer ──────────────────────────────────────────────────────────
    console.print(Panel(
        "[bold yellow]⭐  More profitable bots at Gamma Trade Lab  ⭐[/bold yellow]\n"
        "[cyan underline]github.com/gamma-trade-lab[/cyan underline]"
        "   [cyan]gammatradeorg@gmail.com[/cyan]"
        "   [cyan underline]t.me/RetroValix[/cyan underline]",
        border_style="dark_orange",
        padding=(0, 2),
    ))


# ── Trade log table ───────────────────────────────────────────────────────────

def print_trade_table(trades: List[Dict], limit: Optional[int] = None) -> None:
    disp = trades[-limit:] if limit else trades
    if not disp:
        return

    tbl = Table(
        title=f"TRADE LOG  ({len(disp)} of {len(trades)} total)",
        box=box.SIMPLE_HEAVY,
        show_header=True,
        header_style="bold dim",
        title_style="bold white",
        pad_edge=True,
    )
    tbl.add_column("#",        justify="right",  style="dim",    width=5)
    tbl.add_column("Time",     justify="left",   width=17)
    tbl.add_column("Dir",      justify="center", width=6)
    tbl.add_column("Entry",    justify="right",  width=7)
    tbl.add_column("Exit",     justify="right",  width=7)
    tbl.add_column("PnL",      justify="right",  width=10)
    tbl.add_column("ML p↑",    justify="right",  width=7)
    tbl.add_column("Edge",     justify="right",  width=6)
    tbl.add_column("Score",    justify="right",  width=6)
    tbl.add_column("Regime",   justify="left",   width=10)
    tbl.add_column("Outcome",  justify="center", width=9)

    for t in disp:
        ts  = datetime.fromisoformat(t["timestamp"]).strftime("%m-%d %H:%M:%S")
        num = str(t.get("session_trade_num", "?"))
        d   = t.get("direction", "?")
        en  = t.get("entry_price", 0.0)
        ex  = t.get("exit_price", 0.0)
        p   = t.get("pnl_usd", 0.0)
        mpu = t.get("ml_p_up", 0.0)
        edg = t.get("ml_edge", 0.0)
        sc  = t.get("signal_score", 0.0)
        reg = (t.get("vol_regime") or "?")[:10]
        out = t.get("outcome", "PENDING")

        pnl_txt = Text(f"${p:+.4f}", style="green" if p >= 0 else "red")
        out_txt = (
            Text("WIN",     style="bold green")  if out == "WIN"  else
            Text("LOSS",    style="bold red")     if out == "LOSS" else
            Text("PENDING", style="yellow")
        )
        dir_txt = Text(d, style="cyan" if d == "LONG" else "magenta")

        tbl.add_row(
            num, ts, dir_txt,
            f"{en:.4f}", f"{ex:.4f}",
            pnl_txt,
            f"{mpu:.3f}", f"{edg:.4f}", f"{sc:.1f}",
            reg, out_txt,
        )

    console.print()
    console.print(tbl)
    console.print()


# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(trades: List[Dict], path: str = "trades_export.csv") -> None:
    if not trades:
        console.print("[yellow]No trades to export.[/yellow]")
        return
    keys = [
        "trade_id", "timestamp", "direction", "size_usd", "entry_price",
        "exit_price", "pnl_usd", "pnl_pct", "outcome", "signal_score",
        "signal_confidence", "num_signals", "ml_p_up", "ml_edge",
        "market_slug", "btc_spot_price", "vol_regime", "funding_rate",
        "session_trade_num",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(trades)
    console.print(f"[green]Exported[/green] {len(trades)} trades → [cyan]{path}[/cyan]")


# ── Live-trade extra columns ───────────────────────────────────────────────────

def print_live_trade_table(trades: List[Dict], limit: Optional[int] = None) -> None:
    """Render a table tuned for live-trade fields (close_reason, label, filled_qty)."""
    if not trades:
        console.print("[dim]No live trades.[/dim]")
        return
    recent = trades[-limit:] if limit else trades

    tbl = Table(
        title="[bold cyan]LIVE TRADES[/bold cyan]",
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold white",
        show_lines=False,
        pad_edge=False,
        padding=(0, 1),
    )
    tbl.add_column("#",         width=4,  justify="right")
    tbl.add_column("Time",      width=8)
    tbl.add_column("Dir",       width=6)
    tbl.add_column("Label",     width=10)
    tbl.add_column("Entry",     width=7,  justify="right")
    tbl.add_column("Exit",      width=7,  justify="right")
    tbl.add_column("Qty",       width=8,  justify="right")
    tbl.add_column("P&L",       width=10, justify="right")
    tbl.add_column("Reason",    width=18)
    tbl.add_column("Outcome",   width=11)

    for t in recent:
        ts  = str(t.get("closed_at", t.get("timestamp", "?")))[:19].replace("T", " ")[-8:]
        num = str(t.get("session_trade_num", "?"))
        d   = str(t.get("direction", "?"))
        lbl = str(t.get("label", ""))[:10]
        en  = t.get("entry_price", 0.0)
        ex  = t.get("exit_price", 0.0)
        qty = t.get("filled_qty", 0.0)
        p   = t.get("pnl_usd", 0.0)
        reason = str(t.get("close_reason", "")).replace("_", " ")
        out = t.get("outcome", "UNRESOLVED")

        pnl_txt = Text(f"${p:+.4f}", style="green" if p >= 0 else "red")
        out_style = {"WIN": "bold green", "LOSS": "bold red", "BREAKEVEN": "yellow"}.get(out, "dim")
        out_txt = Text(out, style=out_style)
        dir_txt = Text(d, style="cyan" if "LONG" in d else "magenta")

        tbl.add_row(
            num, ts, dir_txt, lbl,
            f"{en:.4f}", f"{ex:.4f}", f"{qty:.4f}",
            pnl_txt, reason, out_txt,
        )

    console.print()
    console.print(tbl)
    console.print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="View paper and live trade results")
    parser.add_argument("--last",    type=int,  metavar="N",  help="Show only last N trades")
    parser.add_argument("--summary", action="store_true",     help="Summary stats only")
    parser.add_argument("--csv",     action="store_true",     help="Export to CSV")
    parser.add_argument("--watch",   action="store_true",     help="Auto-refresh every 10 seconds")
    parser.add_argument("--file",    type=str,  default=str(TRADES_FILE))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--live",  action="store_true", help="Show live trades (live_trades.json)")
    mode.add_argument("--both",  action="store_true", help="Show paper AND live trades")
    args = parser.parse_args()

    if args.live:
        paper_path = None
        live_path  = LIVE_TRADES_FILE
    elif args.both:
        paper_path = Path(args.file)
        live_path  = LIVE_TRADES_FILE
    else:
        paper_path = Path(args.file)
        live_path  = None

    def render() -> None:
        if args.watch:
            console.clear()

        if paper_path is not None:
            paper = load_trades(paper_path)
            if paper or not args.live:
                console.print(Rule("[bold white]PAPER / SIMULATION TRADES[/bold white]", style="white"))
                s = compute_stats(paper)
                print_summary(s)
                if not args.summary:
                    print_trade_table(paper, limit=args.last)
                if args.csv:
                    export_csv(paper, path=str(paper_path).replace(".json", ".csv"))

        if live_path is not None:
            live = load_trades(live_path)
            if live:
                console.print(Rule("[bold green]LIVE TRADES[/bold green]", style="green"))
                s = compute_stats(live)
                print_summary(s)
                if not args.summary:
                    print_live_trade_table(live, limit=args.last)
                if args.csv:
                    export_csv(live, path=str(live_path).replace(".json", ".csv"))
            else:
                console.print("[dim]No live trades found in live_trades.json[/dim]")

        if args.watch:
            console.print(
                Rule("[dim]Auto-refresh every 10 s  ·  Ctrl+C to stop[/dim]", style="dim")
            )

    if args.watch:
        try:
            while True:
                render()
                time.sleep(10)
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped.[/yellow]")
    else:
        render()


if __name__ == "__main__":
    main()
