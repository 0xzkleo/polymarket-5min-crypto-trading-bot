"""
scripts/check_wallet.py
=======================
Verify your Polymarket wallet wiring before live trading.

Prints:
  • EOA address derived from POLYMARKET_PK   (the *signer*)
  • POLYMARKET_FUNDER from .env              (the *USDC holder* / proxy)
  • Working signature_type                   (probes 2 → 1 → 0)
  • Polymarket cash balance                  (authoritative — from CLOB API)
  • On-chain USDC + MATIC balance            (informational, Polygon mainnet)

Run:
    python scripts/check_wallet.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

load_dotenv(ROOT / ".env")
console = Console()

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

# Public Polygon RPCs (publicnode works without API key as of 2026).
# Override via POLYGON_RPC_URL in .env if you have a private one.
POLYGON_RPCS = [
    os.getenv("POLYGON_RPC_URL"),
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://polygon.gateway.tenderly.co",
]
POLYGON_RPCS = [r for r in POLYGON_RPCS if r]

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"     # Bridged USDC (Polymarket settles in this)
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # Circle native USDC

SIG_LABEL = {0: "EOA", 1: "POLY_PROXY", 2: "POLY_GNOSIS_SAFE"}

ERC20_ABI = [{
    "constant": True,
    "inputs": [{"name": "_owner", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "balance", "type": "uint256"}],
    "type": "function",
}]


def derive_eoa(pk: str) -> str:
    from eth_account import Account
    return Account.from_key(pk).address


def get_w3():
    """Return (w3, rpc_url) for the first responsive Polygon RPC, or (None, None)."""
    try:
        from web3 import Web3
    except ImportError:
        console.print("[yellow]web3 not installed — pip install web3[/yellow]")
        return None, None

    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 6}))
            cid = w3.eth.chain_id
            if cid == CHAIN_ID:
                return w3, rpc
        except Exception:
            continue
    return None, None


def onchain_balances(w3, address: str) -> dict:
    """Read raw on-chain balances. Useful sanity check, not authoritative for Polymarket cash."""
    from web3 import Web3
    out = {"USDC.e": None, "USDC": None, "MATIC": None}
    addr = Web3.to_checksum_address(address)
    for label, ca in [("USDC.e", USDC_E), ("USDC", USDC_NATIVE)]:
        try:
            c = w3.eth.contract(address=Web3.to_checksum_address(ca), abi=ERC20_ABI)
            out[label] = c.functions.balanceOf(addr).call() / 1e6
        except Exception as exc:
            out[label] = f"err: {type(exc).__name__}"
    try:
        out["MATIC"] = w3.eth.get_balance(addr) / 1e18
    except Exception as exc:
        out["MATIC"] = f"err: {type(exc).__name__}"
    return out


def probe_clob(pk: str, funder: str | None):
    """Try sig_types 2/1/0; on success return (working_sig_type, ClobClient)."""
    from py_clob_client.client import ClobClient
    candidates = [2, 1, 0] if funder else [0, 2, 1]
    for st in candidates:
        try:
            client = ClobClient(
                host=CLOB_HOST, key=pk, chain_id=CHAIN_ID,
                signature_type=st, funder=(None if st == 0 else funder),
            )
            client.derive_api_key()  # auth probe
            return st, client
        except Exception:
            continue
    return None, None


def polymarket_cash(client) -> float | None:
    """Authoritative balance: USDC available to the trader inside Polymarket."""
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        client.set_api_creds(client.create_or_derive_api_creds())
        ba = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        raw = int(ba.get("balance", 0))
        return raw / 1e6
    except Exception as exc:
        console.print(f"[yellow]CLOB balance lookup failed:[/yellow] {exc}")
        return None


def fmt_amount(v) -> str:
    if v is None:
        return "[dim]?[/dim]"
    if isinstance(v, str):
        return f"[dim]{v}[/dim]"
    return f"{v:.4f}"


def main() -> int:
    pk = (os.getenv("POLYMARKET_PK") or "").strip()
    funder = (os.getenv("POLYMARKET_FUNDER") or "").strip() or None
    declared_sig = os.getenv("POLYMARKET_SIG_TYPE")

    if not pk:
        console.print(Panel("[red]POLYMARKET_PK not set in .env[/red]", border_style="red"))
        return 2

    pk_clean = pk[2:] if pk.startswith("0x") else pk
    try:
        eoa = derive_eoa(pk_clean)
    except Exception as exc:
        console.print(Panel(f"[red]Bad POLYMARKET_PK:[/red] {exc}", border_style="red"))
        return 2

    cfg = Table(box=box.SIMPLE_HEAVY, show_header=False)
    cfg.add_column("key", style="bold cyan")
    cfg.add_column("value")
    cfg.add_row("EOA (signer)", eoa)
    cfg.add_row("Funder (USDC holder)", funder or "[dim](none — direct EOA)[/dim]")
    cfg.add_row("Declared SIG_TYPE", declared_sig or "[dim](unset → defaults to 2)[/dim]")
    console.print(Panel(cfg, title="[bold]Polymarket wallet config[/bold]", border_style="cyan"))

    console.print("\n[bold]Probing CLOB signature_type...[/bold]")
    working, client = probe_clob(pk_clean, funder)
    if working is None:
        console.print(Panel(
            "[red]No signature_type worked.[/red]\n\n"
            "Likely causes:\n"
            "  • The wallet has never traded on polymarket.com (do one trade in the UI to register it)\n"
            "  • Wrong private key for this funder\n"
            "  • CLOB outage — try again in a minute",
            border_style="red", title="[red]FAIL[/red]",
        ))
        return 1
    console.print(f"  [green]Working signature_type = {working} ({SIG_LABEL[working]})[/green]")

    if declared_sig and int(declared_sig) != working:
        console.print(Panel(
            f"[yellow].env has POLYMARKET_SIG_TYPE={declared_sig} but {working} is what works.[/yellow]\n"
            f"Update .env →  POLYMARKET_SIG_TYPE={working}",
            border_style="yellow",
        ))
    elif not declared_sig:
        console.print(f"[dim]Tip: add POLYMARKET_SIG_TYPE={working} to .env to lock it in.[/dim]")

    cash = polymarket_cash(client)
    bal_addr = funder if working in (1, 2) and funder else eoa
    cash_table = Table(box=box.SIMPLE_HEAVY)
    cash_table.add_column("Source", style="bold cyan")
    cash_table.add_column("Address", style="dim")
    cash_table.add_column("Amount", justify="right")
    cash_table.add_row(
        "Polymarket cash (CLOB)",
        bal_addr,
        f"[bold green]${cash:.2f}[/bold green]" if cash is not None else "[red]error[/red]",
    )
    console.print(Panel(cash_table, title="[bold]Authoritative Polymarket balance[/bold]", border_style="green"))

    console.print("\n[bold]On-chain balances (Polygon mainnet — informational):[/bold]")
    w3, rpc_used = get_w3()
    if w3 is None:
        console.print("[yellow]  Could not reach any Polygon RPC. Set POLYGON_RPC_URL in .env if needed.[/yellow]")
    else:
        console.print(f"  [dim]via {rpc_used}[/dim]")
        on_table = Table(box=box.SIMPLE)
        on_table.add_column("Address", style="cyan")
        on_table.add_column("USDC.e", justify="right")
        on_table.add_column("USDC (native)", justify="right")
        on_table.add_column("MATIC", justify="right")
        for label, addr in [("EOA", eoa)] + ([("Funder", funder)] if funder else []):
            bals = onchain_balances(w3, addr)
            on_table.add_row(
                f"{label}\n[dim]{addr}[/dim]",
                fmt_amount(bals["USDC.e"]),
                fmt_amount(bals["USDC"]),
                fmt_amount(bals["MATIC"]),
            )
        console.print(on_table)
        console.print(
            "[dim]Note: Polymarket holds your cash inside its Exchange/CTF contracts, "
            "so the on-chain USDC balance can legitimately be 0 even when you have funds. "
            "The CLOB number above is the source of truth.[/dim]"
        )

    if cash is None:
        console.print(Panel("[red]Could not read Polymarket balance. Check API keys.[/red]", border_style="red"))
        return 1
    if cash < 1:
        console.print(Panel(
            f"[yellow]Polymarket cash is only ${cash:.2f}.[/yellow]\n"
            "Deposit USDC (Polymarket → Deposit) before going live.",
            border_style="yellow",
        ))
    else:
        console.print(Panel(
            f"[bold green]Wallet looks ready — ${cash:.2f} available on Polymarket.[/bold green]",
            border_style="green",
        ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
