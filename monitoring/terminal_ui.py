"""
monitoring.terminal_ui
======================
Live terminal dashboard for the Polymarket BTC 15-min trading bot.

Captures loguru output into an event feed and polls strategy state for
status, health, performance, tasks, and order activity panels.
"""
from __future__ import annotations

import re
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from loguru import logger
from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ── Shared hub ────────────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"\[([A-Za-z_ -]+)\]")
_BRACKET_TAG_RE = re.compile(r"^\[([A-Z]+)\]\s*(.+)")
_BANNER_RE = re.compile(r">>>\s*([^:]+):\s*(.+)")
_STEP_HEADER_RE = re.compile(r"STEP\s+(\d+)", re.IGNORECASE)
_BOX_LINE_RE = re.compile(r"^[│#]\s*(.+)")
_FUSED_LINE_RE = re.compile(
    r"Fused result\s+(▲|▼)?\s*(BULLISH|BEARISH|NEUTRAL).*score=([\d.]+).*conf=([\d.]+%)",
    re.IGNORECASE,
)
_ML_LINE_RE = re.compile(r"ML p\(UP\)\s+([\d.]+)", re.IGNORECASE)
_DIRECTION_LINE_RE = re.compile(r"Direction\s+(▲|▼)?\s*LONG|SHORT", re.IGNORECASE)
_NAUTILUS_STDERR_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*\[(INFO|WARN|ERROR|DEBUG)\]"
)
_SKIP_FEED_RE = re.compile(
    r"loop\.is_(running|closed)|DISPOSED|SHUTDOWN|CANCELLED",
    re.IGNORECASE,
)
_NOISE_RE = re.compile(
    r"(?i)"
    r"(\[gamma\]|Applying Polymarket|patches applied|verify:|"
    r"INTEGRATED BTC|STRATEGY INITIALIZED|STRATEGY STARTED|"
    r"DECISION CYCLE #|CYCLE #\d+ END|"
    r"stream started|tracker started|SignalRecorder|"
    r"FOUND \d+ BTC|Loading BTC|Redis connection|"
    r"Polymarket wallet|Nautilus node built|"
    r"Price history:|READY TO TRADE|Subscribed to:|"
    r"Trade timer reset|Bound market:|Waiting for next market|"
    r"ML edge gate:|Risk engine ready|All signal processors)"
)

_TAG_STYLE: Dict[str, Tuple[str, str]] = {
    "MARKET": ("✓", "cyan"),
    "FUSION": ("·", "white"),
    "ML": ("·", "magenta"),
    "DECISION": ("✓", "green"),
    "ORDER": ("✓", "cyan"),
    "REJECT": ("⚠", "yellow"),
    "WARN": ("⚠", "yellow"),
    "ERROR": ("⚠", "red"),
    "SYSTEM": ("✓", "green"),
}

_STEP_SLUG = {
    1: "S1",
    2: "S2",
    3: "S2",
    4: "S4",
    5: "S5",
    6: "S5",
}


@dataclass
class FeedEvent:
    time: str
    slug: str
    tag: str
    body: str
    level: str = "INFO"
    icon: str = "·"
    style: str = "white"


@dataclass
class ActivityEvent:
    time: str
    icon: str
    text: str
    style: str = "white"


@dataclass
class TerminalUIHub:
    """Thread-safe singleton holding dashboard state."""

    enabled: bool = False
    simulation: bool = True
    test_mode: bool = False
    redis_ok: bool = False
    strategy: Any = None
    events: Deque[FeedEvent] = field(default_factory=lambda: deque(maxlen=80))
    activities: Deque[ActivityEvent] = field(default_factory=lambda: deque(maxlen=12))
    status_line: str = "Starting..."
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _current_step: int = 0

    def add_event(self, level: str, message: str) -> None:
        msg = message.strip()
        if msg.startswith("┌"):
            step_match = _STEP_HEADER_RE.search(msg)
            if step_match:
                self._current_step = int(step_match.group(1))
            return

        parsed = _parse_log_event(level, message, self._current_step)
        if parsed is None:
            return
        if parsed.slug.startswith("S") and len(parsed.slug) == 2 and parsed.slug[1].isdigit():
            self._current_step = int(parsed.slug[1])

        with self._lock:
            self.events.append(parsed)
            if parsed.tag in ("REJECT", "ORDER") or parsed.level in ("ERROR",):
                self._add_activity(parsed)

    def add_tui_event(
        self,
        tag: str,
        body: str,
        *,
        slug: str = "S0",
        level: str = "INFO",
        activity: bool = False,
    ) -> None:
        ev = _make_event(tag, body, slug=slug, level=level)
        with self._lock:
            self.events.append(ev)
            if activity or tag in ("REJECT", "ORDER"):
                self._add_activity(ev)

    def _add_activity(self, ev: FeedEvent) -> None:
        icon = "✗" if ev.tag == "REJECT" or ev.level == "ERROR" else "✓"
        style = "red" if ev.tag == "REJECT" or ev.level == "ERROR" else "green"
        if ev.tag == "ORDER":
            icon = "▲"
            style = "cyan"
        self.activities.append(ActivityEvent(ev.time, icon, ev.body, style))

    def set_status(self, text: str) -> None:
        with self._lock:
            self.status_line = text


_hub = TerminalUIHub()


def get_hub() -> TerminalUIHub:
    return _hub


def tui_event(
    tag: str,
    body: str,
    *,
    slug: str = "S0",
    level: str = "INFO",
    activity: bool = False,
) -> None:
    """Emit a structured, brief dashboard event."""
    get_hub().add_tui_event(tag, body, slug=slug, level=level, activity=activity)


def _make_event(tag: str, body: str, *, slug: str, level: str) -> FeedEvent:
    icon, style = _TAG_STYLE.get(tag.upper(), ("·", "white"))
    if level.upper() in ("WARNING", "WARN"):
        icon, style = "⚠", "yellow"
    elif level.upper() == "ERROR":
        icon, style = "⚠", "red"
    elif level.upper() == "SUCCESS":
        icon, style = "✓", "green"
    return FeedEvent(
        time=datetime.now().strftime("%H:%M:%S"),
        slug=slug,
        tag=tag.upper(),
        body=body.strip(),
        level=level.upper(),
        icon=icon,
        style=style,
    )


def _parse_log_event(level: str, message: str, current_step: int) -> Optional[FeedEvent]:
    msg = message.strip()
    if not msg:
        return None
    if msg.startswith("[heartbeat]"):
        return None
    if len(msg) > 10 and len(set(msg)) == 1 and msg[0] in "=#─━":
        return None
    if _SKIP_FEED_RE.search(msg):
        return None
    if _NOISE_RE.search(msg):
        return None
    if msg.startswith(("┌", "└", "│  ▸")) or msg.startswith("│") and "──" in msg:
        step_match = _STEP_HEADER_RE.search(msg)
        if step_match:
            return None
        return None

    bracket = _BRACKET_TAG_RE.match(msg)
    if bracket:
        tag, body = bracket.groups()
        slug = _STEP_SLUG.get(current_step, "S0") if tag not in ("MARKET", "REJECT", "ORDER") else {
            "MARKET": "S0",
            "REJECT": "S5",
            "ORDER": "S5",
        }.get(tag, "S0")
        if tag == "MARKET":
            slug = "S0"
        return _make_event(tag, body, slug=slug, level=level)

    banner = _BANNER_RE.search(msg)
    if banner:
        tag_raw, title = banner.groups()
        tag = tag_raw.strip().upper()
        if "REJECT" in tag or "DENIED" in tag:
            return _make_event("REJECT", title.strip()[:80], slug="S5", level="ERROR")
        if "ORDER" in tag:
            return _make_event("ORDER", title.strip()[:80], slug="S5", level=level)

    box = _BOX_LINE_RE.match(msg)
    if box:
        inner = box.group(1).strip()
        fused = _FUSED_LINE_RE.search(inner)
        if fused:
            _arrow, direction, score, conf = fused.groups()
            return _make_event(
                "FUSION",
                f"signals → {direction.upper()} score={score} conf={conf}",
                slug="S1",
                level=level,
            )

    lower = msg.lower()
    if "failed" in lower or "error 429" in lower or "too many requests" in lower:
        source = msg.split(":")[0].strip() if ":" in msg else "System"
        body = msg.split(":", 1)[-1].strip() if ":" in msg else msg
        if len(body) > 90:
            body = body[:87] + "…"
        return _make_event("WARN", f"{source}: {body}", slug="S0", level="WARNING")

    if lower in ("bot ready",) or "bot ready" in lower:
        return _make_event("SYSTEM", "Bot ready", slug="S0", level="SUCCESS")

    if "built successfully" in lower:
        return _make_event("SYSTEM", "Nautilus node built", slug="S0", level="SUCCESS")

    if level.upper() == "ERROR":
        return _make_event("ERROR", msg[:90], slug="S0", level="ERROR")

    return None


def clear_screen() -> None:
    """Clear the terminal after startup banners, before the live dashboard."""
    Console().clear()


def register_strategy(
    strategy: Any,
    *,
    simulation: bool,
    test_mode: bool,
    redis_ok: bool,
) -> None:
    _hub.strategy = strategy
    _hub.simulation = simulation
    _hub.test_mode = test_mode
    _hub.redis_ok = redis_ok


def _loguru_sink(message) -> None:
    record = message.record
    level = record["level"].name
    text = record["message"]
    _hub.add_event(level, text)


class _FilteredStderr:
    """Swallow Nautilus native logs on stderr; route important lines to the feed."""

    def __init__(self, original: Any):
        self._original = original

    def write(self, text: str) -> None:
        if not text or not _hub.enabled:
            self._original.write(text)
            return

        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if _NAUTILUS_STDERR_RE.match(line) or "TradingNode:" in line:
                if not _SKIP_FEED_RE.search(line):
                    _hub.add_event("INFO", line.split(":", 1)[-1].strip()[:120])
                continue
            # Suppress other stderr noise while the TUI is active.
            if _hub.enabled:
                continue
            self._original.write(raw + "\n")

    def flush(self) -> None:
        self._original.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


def install_log_sink(level: str = "INFO") -> None:
    """Route loguru output to the TUI event feed (no stderr)."""
    _hub.enabled = True
    logger.add(_loguru_sink, level=level, format="{message}")
    if not isinstance(sys.stderr, _FilteredStderr):
        sys.stderr = _FilteredStderr(sys.stderr)


# ── Layout builders ───────────────────────────────────────────────────────────

def _fmt_money(value: Optional[float], prefix: str = "$") -> str:
    if value is None:
        return "—"
    return f"{prefix}{value:,.2f}"


def _fmt_pct(value: float, decimals: int = 1) -> str:
    return f"{value * 100:.{decimals}f}%"


def _header_panel(simulation: bool, test_mode: bool) -> Panel:
    now = datetime.now().strftime("%H:%M:%S %Z").strip()
    if not now.split()[-1] or now.endswith(":"):
        now = datetime.now().strftime("%H:%M:%S")

    title = Text()
    title.append("⬡ ", style="cyan")
    title.append("POLYMARKET BTC 15-MIN TRADING BOT", style="bold cyan")

    if test_mode:
        title.append("  [TEST]", style="bold yellow")
    elif simulation:
        title.append("  [SIM]", style="bold cyan")
    else:
        title.append("  [LIVE]", style="bold red")

    title.append(f"  {now}", style="dim")

    return Panel(Align.center(title), box=box.HEAVY, style="cyan", padding=(0, 1))


def _kv_table(rows: List[tuple], label_style: str = "dim") -> Table:
    table = Table.grid(padding=(0, 1))
    table.add_column(style=label_style, width=16)
    table.add_column()
    for label, value in rows:
        if isinstance(value, Text):
            table.add_row(label, value)
        else:
            table.add_row(label, str(value))
    return table


def _bot_status_panel(snapshot: Dict[str, Any], status_line: str) -> Panel:
    rows: List[tuple] = [
        ("Status", Text(status_line, style="bold cyan")),
        ("Market ID", snapshot.get("market_slug", "—")),
        ("BTC Price", Text(_fmt_money(snapshot.get("btc_price")), style="bold yellow")),
        ("Price To Beat", _fmt_money(snapshot.get("price_to_beat"))),
    ]

    up = snapshot.get("up_price")
    down = snapshot.get("down_price")
    rows.append(("UP Price", Text(f"${up:.4f}" if up is not None else "—", style="green")))
    rows.append(("DOWN Price", Text(f"${down:.4f}" if down is not None else "—", style="red")))

    pos = snapshot.get("position_summary", "None")
    rows.append(("Position", pos))
    rows.append(("Signal", snapshot.get("signal", "—")))
    rows.append(("Confidence", snapshot.get("confidence", "—")))
    rows.append(
        (
            "Next Window",
            Text(snapshot.get("next_window", "—"), style="green"),
        )
    )

    return Panel(
        _kv_table(rows),
        title="[bold cyan]Bot Status[/bold cyan]",
        border_style="cyan",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _system_health_panel(snapshot: Dict[str, Any]) -> Panel:
    def status_line(ok: bool, ok_text: str, fail_text: str = "Offline") -> Text:
        if ok:
            return Text(f"● {ok_text}", style="green")
        return Text(f"● {fail_text}", style="red")

    rpc_label = snapshot.get("rpc_label", "Chainlink RTDS")
    rpc = status_line(snapshot.get("rpc_ok", False), "Connected")
    rpc.append(f"  {rpc_label}", style="dim")

    rows = [
        ("RPC", rpc),
        ("WebSocket", status_line(snapshot.get("websocket_ok", False), "Connected")),
        (
            "ML Engine",
            Text(
                f"● Active ({snapshot.get('ml_samples', 0)} samples)"
                if snapshot.get("ml_active")
                else f"● Warming ({snapshot.get('ml_samples', 0)}/{snapshot.get('ml_min_samples', 0)})",
                style="green" if snapshot.get("ml_active") else "yellow",
            ),
        ),
        (
            "Settlement",
            status_line(snapshot.get("settlement_running", False), "Running", "Stopped"),
        ),
    ]
    if _hub.redis_ok:
        rows.append(("Redis", Text("● Connected", style="green")))

    return Panel(
        _kv_table(rows, label_style="bold white"),
        title="[bold green]System Health[/bold green]",
        border_style="green",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _performance_panel(snapshot: Dict[str, Any]) -> Panel:
    pnl = snapshot.get("total_pnl", 0.0)
    pnl_style = "bold green" if pnl >= 0 else "bold red"
    wr = snapshot.get("win_rate", 0.0)
    wr_style = "green" if wr >= 55 else ("yellow" if wr >= 45 else "red")

    rows = [
        ("Completed", str(snapshot.get("completed_trades", 0))),
        ("Wins", Text(str(snapshot.get("wins", 0)), style="green")),
        ("Win Rate", Text(f"{wr:.1f}%", style=wr_style)),
        ("PnL", Text(f"${pnl:+.4f}", style=pnl_style)),
        ("Drawdown", Text(f"{snapshot.get('drawdown_pct', 0):.2f}%", style="green")),
        ("Wallet", _fmt_money(snapshot.get("wallet_balance"))),
        ("Bot Start", snapshot.get("bot_start", "—")),
    ]

    return Panel(
        _kv_table(rows),
        title="[bold yellow]Performance[/bold yellow]",
        border_style="yellow",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _active_tasks_panel(snapshot: Dict[str, Any]) -> Panel:
    ml = snapshot.get("ml_samples", 0)
    lines = Group(
        Text("⏳ Settlement Tracker", style="cyan"),
        Text("   Monitoring open trades", style="dim"),
        Text(""),
        Text("⏱  Market Timer", style="cyan"),
        Text("   Watching 15-min boundaries", style="dim"),
        Text(""),
        Text("🧠 ML Engine", style="cyan"),
        Text(f"   Model active — {ml} samples", style="dim"),
        Text(""),
        Text("📡 Data Streams", style="cyan"),
        Text(
            "   CVD + Liquidation feeds "
            + ("active" if snapshot.get("streams_ok") else "starting…"),
            style="dim",
        ),
    )
    return Panel(
        lines,
        title="[bold cyan]Active Tasks[/bold cyan] [dim](background…)[/dim]",
        border_style="cyan",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _activities_panel(activities: List[ActivityEvent]) -> Panel:
    if not activities:
        body = Text("No order activity yet…", style="dim italic")
    else:
        lines = []
        for act in reversed(activities[-8:]):
            line = Text()
            line.append(f"{act.time}  ", style="dim")
            line.append(f"{act.icon}  ", style=act.style)
            line.append(act.text, style=act.style)
            lines.append(line)
        body = Group(*lines)

    return Panel(
        body,
        title="[bold magenta]Activities[/bold magenta]",
        border_style="magenta",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _tag_style(tag: str) -> str:
    return {
        "MARKET": "cyan",
        "FUSION": "orange1",
        "ML": "magenta",
        "DECISION": "green",
        "ORDER": "cyan",
        "REJECT": "yellow",
        "WARN": "yellow",
        "ERROR": "red",
        "SYSTEM": "green",
    }.get(tag, "white")


def _format_feed_line(ev: FeedEvent) -> Text:
    line = Text()
    line.append(f"{ev.time} ", style="dim")
    line.append(f"{ev.icon} ", style=ev.style)
    line.append(f"{ev.slug} ", style="dim")
    line.append(f"[{ev.tag}] ", style=_tag_style(ev.tag))
    line.append(ev.body, style=ev.style)
    return line


def _event_feed_panel(events: List[FeedEvent]) -> Panel:
    if not events:
        body = Text("Waiting for events…", style="dim italic")
    else:
        lines = [_format_feed_line(ev) for ev in list(events)[-16:]]
        body = Group(*lines)

    return Panel(
        body,
        title="[bold white]Event Feed[/bold white] [dim](slug · step · tag)[/dim]",
        border_style="bright_black",
        box=box.ROUNDED,
        padding=(0, 1),
    )


def _build_snapshot(strategy: Any) -> Dict[str, Any]:
    if strategy is None:
        return {}
    if hasattr(strategy, "get_dashboard_snapshot"):
        return strategy.get_dashboard_snapshot()
    return {}


def build_layout(hub: TerminalUIHub) -> Layout:
    snapshot = _build_snapshot(hub.strategy)

    with hub._lock:
        events = list(hub.events)
        activities = list(hub.activities)
        status_line = hub.status_line

    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top", size=12),
        Layout(name="bottom"),
    )
    layout["top"].split_row(
        Layout(name="status", ratio=2),
        Layout(name="health", ratio=2),
        Layout(name="performance", ratio=2),
    )
    layout["bottom"].split_row(
        Layout(name="tasks", ratio=2),
        Layout(name="activities", ratio=2),
        Layout(name="feed", ratio=3),
    )

    layout["header"].update(_header_panel(hub.simulation, hub.test_mode))
    layout["status"].update(_bot_status_panel(snapshot, status_line))
    layout["health"].update(_system_health_panel(snapshot))
    layout["performance"].update(_performance_panel(snapshot))
    layout["tasks"].update(_active_tasks_panel(snapshot))
    layout["activities"].update(_activities_panel(activities))
    layout["feed"].update(_event_feed_panel(events))

    return layout


# ── Runtime ───────────────────────────────────────────────────────────────────

def _refresh_status(hub: TerminalUIHub, strategy: Any) -> None:
    snapshot = _build_snapshot(strategy)
    if snapshot.get("open_positions"):
        hub.set_status("Monitoring positions")
    elif snapshot.get("trade_window_open"):
        hub.set_status("Trade window open")
    elif snapshot.get("waiting_for_market"):
        hub.set_status("Waiting for market open")
    elif snapshot.get("instruments_loaded"):
        hub.set_status("Watching market")
    else:
        hub.set_status("Loading instruments…")


def run_bot_session(
    boot_fn: Callable[[], Tuple[Any, Any, bool]],
    *,
    simulation: bool,
    test_mode: bool,
    refresh_hz: float = 2.0,
) -> None:
    """
    Show the live dashboard immediately, build the bot in the background,
    then run the Nautilus node — all logs go to the event feed only.
    """
    clear_screen()

    hub = get_hub()
    hub.simulation = simulation
    hub.test_mode = test_mode
    hub.set_status("Initializing…")

    build_result: List[Optional[Tuple[Any, Any, bool]]] = [None]
    build_error: List[BaseException] = []

    def _build() -> None:
        try:
            build_result[0] = boot_fn()
        except BaseException as exc:
            build_error.append(exc)

    build_thread = threading.Thread(target=_build, name="bot-build", daemon=True)
    build_thread.start()

    console = Console()
    interval = 1.0 / max(refresh_hz, 0.5)
    node: Any = None
    strategy: Any = None
    node_thread: Optional[threading.Thread] = None
    node_error: List[BaseException] = []

    try:
        with Live(
            build_layout(hub),
            console=console,
            refresh_per_second=refresh_hz,
            screen=True,
            transient=False,
        ) as live:
            while build_thread.is_alive():
                hub.set_status("Building Nautilus node…")
                live.update(build_layout(hub))
                time.sleep(interval)

            if build_error:
                hub.set_status("Startup failed")
                hub.add_event("ERROR", str(build_error[0]))
                live.update(build_layout(hub))
                time.sleep(2.0)
                raise build_error[0]

            node, strategy, redis_ok = build_result[0]  # type: ignore[misc]
            register_strategy(
                strategy,
                simulation=simulation,
                test_mode=test_mode,
                redis_ok=redis_ok,
            )
            hub.add_tui_event("SYSTEM", "Bot ready", slug="S0", level="SUCCESS")
            hub.set_status("Starting bot…")

            def _run_node() -> None:
                try:
                    node.run()
                except BaseException as exc:
                    node_error.append(exc)

            node_thread = threading.Thread(target=_run_node, name="nautilus-node", daemon=True)
            node_thread.start()

            while node_thread.is_alive():
                _refresh_status(hub, strategy)
                live.update(build_layout(hub))
                time.sleep(interval)

            live.update(build_layout(hub))
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        if node is not None:
            try:
                node.dispose()
            except Exception:
                pass

    if node_error:
        raise node_error[0]


def run_node_with_dashboard(
    node: Any,
    strategy: Any,
    *,
    simulation: bool,
    test_mode: bool,
    redis_ok: bool,
    refresh_hz: float = 2.0,
) -> None:
    """Run the Nautilus node in a background thread behind the live TUI."""
    register_strategy(strategy, simulation=simulation, test_mode=test_mode, redis_ok=redis_ok)
    hub = get_hub()
    hub.set_status("Monitoring positions")

    node_error: List[BaseException] = []

    def _run_node() -> None:
        try:
            node.run()
        except BaseException as exc:
            node_error.append(exc)

    thread = threading.Thread(target=_run_node, name="nautilus-node", daemon=True)
    thread.start()

    console = Console()
    interval = 1.0 / max(refresh_hz, 0.5)

    try:
        with Live(
            build_layout(hub),
            console=console,
            refresh_per_second=refresh_hz,
            screen=True,
            transient=False,
        ) as live:
            while thread.is_alive():
                snapshot = _build_snapshot(strategy)
                if snapshot.get("open_positions"):
                    hub.set_status("Monitoring positions")
                elif snapshot.get("trade_window_open"):
                    hub.set_status("Trade window open")
                elif snapshot.get("waiting_for_market"):
                    hub.set_status("Waiting for market open")
                elif snapshot.get("instruments_loaded"):
                    hub.set_status("Watching market")
                else:
                    hub.set_status("Loading instruments…")

                live.update(build_layout(hub))
                time.sleep(interval)

            # Node exited — show final frame briefly
            live.update(build_layout(hub))
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.dispose()
        except Exception:
            pass

    if node_error:
        raise node_error[0]
