"""scripts/redis_control.py — Toggle simulation/live mode via Redis without restarting the bot."""
import redis
import sys
import os
from dotenv import load_dotenv

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()
load_dotenv()


def get_redis_client():
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 2)),
            decode_responses=True,
            socket_connect_timeout=5,
        )
        client.ping()
        return client
    except Exception as e:
        console.print(f"[red]Redis connection failed:[/red] {e}")
        console.print("[dim]Make sure Redis is running:  redis-server[/dim]")
        return None


def get_current_mode(client) -> bool | None:
    try:
        mode = client.get("btc_trading:simulation_mode")
        return None if mode is None else (mode == "1")
    except Exception as e:
        print(f"Error reading mode: {e}")
        return None


def set_simulation_mode(client, simulation: bool) -> bool:
    try:
        client.set("btc_trading:simulation_mode", "1" if simulation else "0")
        label = "SIMULATION" if simulation else "LIVE TRADING"
        style = "cyan" if simulation else "bold red"
        console.print(f"  Mode set to: [{style}]{label}[/{style}]")
        return True
    except Exception as e:
        console.print(f"[red]Error setting mode:[/red] {e}")
        return False


def display_status(client) -> None:
    mode = get_current_mode(client)
    console.print()
    if mode is None:
        body = Text("Not set — using default from .env", style="dim")
        border = "dim"
    elif mode:
        body = Text("SIMULATION MODE\nNo real trades will be placed", style="cyan")
        border = "cyan"
    else:
        body = Text("LIVE TRADING MODE\nREAL MONEY AT RISK!", style="bold red")
        border = "red"
    console.print(Panel(body, title="[bold white]BTC BOT STATUS[/bold white]",
                        border_style=border, padding=(0, 2)))
    console.print()


def main() -> None:
    console.print()
    console.print(Panel(
        "[bold white]BTC BOT — SIMULATION MODE CONTROL[/bold white]",
        border_style="white", padding=(0, 2),
    ))

    client = get_redis_client()
    if not client:
        return

    display_status(client)

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command in ("sim", "simulation", "on"):
            set_simulation_mode(client, True)
            display_status(client)
        elif command in ("live", "off"):
            console.print("[bold red]WARNING:[/bold red] Switching to LIVE TRADING mode!")
            confirm = input("  Type 'yes' to confirm: ")
            if confirm.lower() == "yes":
                set_simulation_mode(client, False)
                display_status(client)
            else:
                console.print("[yellow]Cancelled.[/yellow]")
        elif command in ("status", "check"):
            pass
        else:
            console.print(f"[red]Unknown command:[/red] {command}")
            console.print("\n[dim]Usage:[/dim]")
            console.print("  python scripts/redis_control.py [cyan]sim[/cyan]    — Enable simulation mode")
            console.print("  python scripts/redis_control.py [red]live[/red]   — Enable live trading")
            console.print("  python scripts/redis_control.py status — Show current status")
    else:
        console.print("[dim]Commands:[/dim]  1) Simulation  2) Live trading  3) Status  4) Exit")
        while True:
            try:
                choice = input("\n  Choice (1-4): ").strip()
                if choice == "1":
                    set_simulation_mode(client, True)
                    display_status(client)
                elif choice == "2":
                    console.print("[bold red]WARNING:[/bold red] This will enable LIVE TRADING!")
                    confirm = input("  Type 'yes' to confirm: ")
                    if confirm.lower() == "yes":
                        set_simulation_mode(client, False)
                        display_status(client)
                    else:
                        console.print("[yellow]Cancelled.[/yellow]")
                elif choice == "3":
                    display_status(client)
                elif choice == "4":
                    console.print("[dim]Goodbye![/dim]")
                    break
                else:
                    console.print("[red]Invalid choice.[/red]")
            except KeyboardInterrupt:
                console.print("\n[dim]Goodbye![/dim]")
                break


if __name__ == "__main__":
    main()
