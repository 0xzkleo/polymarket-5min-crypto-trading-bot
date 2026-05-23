"""
scripts/regen_polymarket_keys.py
================================
Regenerate (or derive) Polymarket CLOB API credentials directly from
``POLYMARKET_PK`` so they are guaranteed to match the wallet the bot uses.

Polymarket has THREE wallet types and the correct ``signature_type`` must be
matched for any CLOB call to succeed:

    0 = EOA              (MetaMask / direct wallet)
    1 = POLY_PROXY       (legacy Polymarket proxy — older email accounts)
    2 = POLY_GNOSIS_SAFE (Gnosis Safe proxy — newer email/Magic accounts)

Most accounts created from 2023+ via email login use ``signature_type=2``.
If the bot is configured for the wrong type the CLOB returns 401/400 even
when the private key, funder, and API keys are otherwise correct.

By default this script auto-tries types 2 → 1 → 0 with ``derive_api_key``
first, then ``create_api_key``, and prints the combination that worked.

Usage
-----
    python scripts/regen_polymarket_keys.py                # auto-detect (recommended)
    python scripts/regen_polymarket_keys.py --sig-type 2   # force Gnosis Safe
    python scripts/regen_polymarket_keys.py --create-only  # skip derive
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Force UTF-8 stdout on Windows so arrows/etc. don't crash with cp1252.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from dotenv import load_dotenv
from py_clob_client.client import ClobClient

load_dotenv(ROOT / ".env")


CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

SIG_TYPE_NAMES = {
    0: "EOA (MetaMask / direct)",
    1: "POLY_PROXY (legacy email)",
    2: "POLY_GNOSIS_SAFE (modern email/Magic)",
}


def _extract_creds(creds) -> Tuple[str, str, str]:
    """Pull api_key/secret/passphrase out of whatever shape the SDK returns."""
    api_key = (
        getattr(creds, "api_key", None)
        or getattr(creds, "apiKey", None)
        or (creds.get("apiKey") if isinstance(creds, dict) else None)
    )
    api_secret = (
        getattr(creds, "api_secret", None)
        or getattr(creds, "secret", None)
        or (creds.get("secret") if isinstance(creds, dict) else None)
    )
    passphrase = (
        getattr(creds, "api_passphrase", None)
        or getattr(creds, "passphrase", None)
        or (creds.get("passphrase") if isinstance(creds, dict) else None)
    )
    if not (api_key and api_secret and passphrase):
        raise RuntimeError(f"Unexpected creds shape: {creds!r}")
    return api_key, api_secret, passphrase


def _try_one(
    pk: str,
    funder: Optional[str],
    sig_type: int,
    create_only: bool,
) -> Optional[Tuple[str, str, str]]:
    """Try derive (then create) for one (sig_type, funder) combo. Returns creds or None."""
    label = SIG_TYPE_NAMES[sig_type]
    print(f"\n→ Trying signature_type={sig_type} ({label}) funder={funder or 'none'}")
    try:
        client = ClobClient(
            host=CLOB_HOST,
            key=pk,
            chain_id=CHAIN_ID,
            signature_type=sig_type,
            funder=funder,
        )
    except Exception as e:
        print(f"  ClobClient init failed: {e}")
        return None

    if not create_only:
        try:
            print("  derive_api_key() ...", end=" ")
            creds = client.derive_api_key()
            api_key, secret, passphrase = _extract_creds(creds)
            print("OK (recovered existing keys)")
            return api_key, secret, passphrase
        except Exception as e:
            err = str(e)
            # 401/404 here usually means "no keys exist yet for this wallet" —
            # not necessarily a wrong signature type. Fall through to create.
            print(f"failed ({err[:120]})")

    try:
        print("  create_api_key() ...", end=" ")
        creds = client.create_api_key()
        api_key, secret, passphrase = _extract_creds(creds)
        print("OK (created new keys)")
        return api_key, secret, passphrase
    except Exception as e:
        err = str(e)
        print(f"failed ({err[:120]})")
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sig-type",
        type=int,
        choices=[0, 1, 2],
        default=None,
        help="Force a specific signature_type instead of auto-detecting.",
    )
    parser.add_argument(
        "--create-only",
        action="store_true",
        help="Skip derive_api_key and only call create_api_key.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Patch .env in place with the working keys + sig_type + funder.",
    )
    args = parser.parse_args()

    pk = os.getenv("POLYMARKET_PK", "").strip()
    funder_env = os.getenv("POLYMARKET_FUNDER", "").strip() or None

    if not pk:
        print("ERROR: POLYMARKET_PK not set in .env", file=sys.stderr)
        return 2

    print("=" * 70)
    print("Polymarket CLOB credential helper (auto-detect)")
    print("=" * 70)
    print(f"  Chain ID:       {CHAIN_ID} (Polygon mainnet)")
    print(f"  Funder (.env):  {funder_env or '(none)'}")
    print("=" * 70)

    # Build attempt list. When a funder is set we ALSO try sig_type=0 with
    # funder=None as a sanity check (covers the case where the user copied a
    # FUNDER value but actually trades from an EOA wallet).
    if args.sig_type is not None:
        attempts = [(args.sig_type, funder_env)]
    else:
        attempts = []
        if funder_env:
            # Modern email/Magic → Gnosis Safe most likely
            attempts.append((2, funder_env))
            # Legacy email → Polymarket proxy
            attempts.append((1, funder_env))
        # Direct EOA — works whether or not funder is set
        attempts.append((0, None))

    found = None
    used_sig_type = None
    used_funder = None
    for sig_type, funder in attempts:
        result = _try_one(pk, funder, sig_type, args.create_only)
        if result is not None:
            found = result
            used_sig_type = sig_type
            used_funder = funder
            break

    if found is None:
        print("\n" + "=" * 70)
        print("ALL ATTEMPTS FAILED.")
        print("=" * 70)
        print(
            "Likely causes:\n"
            "  • POLYMARKET_PK is not the private key of any wallet that has\n"
            "    ever been onboarded to Polymarket. Trade once from the wallet\n"
            "    on polymarket.com to register it server-side, then retry.\n"
            "  • You copied the WRONG private key. For email/Magic accounts the\n"
            "    correct PK is shown in polymarket.com → Profile → Settings →\n"
            "    'Show private key'.\n"
            "  • Network / CLOB outage. Try again in a minute.",
            file=sys.stderr,
        )
        return 1

    api_key, api_secret, passphrase = found

    print("\n" + "=" * 70)
    print("SUCCESS")
    print("=" * 70)
    print(f"  Working signature_type: {used_sig_type}  ({SIG_TYPE_NAMES[used_sig_type]})")
    print(f"  Working funder:         {used_funder or '(none — direct EOA)'}")
    print()
    print("Paste these THREE lines into your .env (replace existing):\n")
    print("─" * 70)
    print(f"POLYMARKET_API_KEY={api_key}")
    print(f"POLYMARKET_API_SECRET={api_secret}")
    print(f"POLYMARKET_PASSPHRASE={passphrase}")
    print("─" * 70)

    # Tell the user exactly what to put in .env so the bot uses the right wallet.
    print()
    print("─" * 70)
    print("Add (or update) these lines in .env so the bot signs correctly:")
    print(f"  POLYMARKET_SIG_TYPE={used_sig_type}")
    if used_funder:
        print(f"  POLYMARKET_FUNDER={used_funder}")
    elif funder_env:
        print(f"  # POLYMARKET_FUNDER={funder_env}   # not needed for EOA mode")
    print("─" * 70)
    if not used_funder and funder_env:
        print()
        print("NOTE: This wallet authenticates as a direct EOA (no funder).")
        print("      You can REMOVE POLYMARKET_FUNDER from .env, OR keep it")
        print("      if you only use the funder address for deposits.")

    if args.write:
        env_path = ROOT / ".env"
        updates = {
            "POLYMARKET_API_KEY": api_key,
            "POLYMARKET_API_SECRET": api_secret,
            "POLYMARKET_PASSPHRASE": passphrase,
            "POLYMARKET_SIG_TYPE": str(used_sig_type),
        }
        if used_funder:
            updates["POLYMARKET_FUNDER"] = used_funder

        text = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
        for k, v in updates.items():
            pattern = re.compile(rf"^{re.escape(k)}=.*$", re.MULTILINE)
            if pattern.search(text):
                text = pattern.sub(f"{k}={v}", text)
            else:
                text += ("\n" if text and not text.endswith("\n") else "") + f"{k}={v}\n"
        env_path.write_text(text, encoding="utf-8")
        print(f"\n.env updated in place: {env_path}")

    print("\nThen re-run:  python main.py --live")
    return 0


if __name__ == "__main__":
    sys.exit(main())
