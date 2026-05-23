"""
End-to-end test of the gamma_markets patch + instrument provider load.

Bypasses Nautilus's data/exec clients — directly invokes
PolymarketInstrumentProvider.load_all_async with the same filters runner.py uses.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="DEBUG", format="<level>{level: <8}</level> | {name} | {message}")

print("Step 1: applying gamma patch")
from patches.gamma_markets import apply_gamma_markets_patch, verify_patch
ok = apply_gamma_markets_patch()
print(f"  patch applied: {ok}")
verify_patch()

print("\nStep 2: building provider instance")
from nautilus_trader.adapters.polymarket.providers import (
    PolymarketInstrumentProvider,
    PolymarketInstrumentProviderConfig,
)
from nautilus_trader.common.component import LiveClock
from py_clob_client_v2.client import ClobClient


now = datetime.now(timezone.utc)
ts = (int(now.timestamp()) // 900) * 900
slugs = [f"btc-updown-15m-{ts + i*900}" for i in range(-1, 97)]

filters = {
    "active": True,
    "closed": False,
    "archived": False,
    "slug": tuple(slugs),
    "limit": 100,
}

cfg = PolymarketInstrumentProviderConfig(
    load_all=True,
    filters=filters,
    use_gamma_markets=True,
)

clock = LiveClock()

clob_client = ClobClient(
    host="https://clob.polymarket.com",
    key=os.getenv("POLYMARKET_PK"),
    chain_id=137,
    signature_type=int(os.getenv("POLYMARKET_SIG_TYPE", "2")),
    funder=(os.getenv("POLYMARKET_FUNDER") or "").strip() or None,
)

print(f"  config: load_all={cfg.load_all} use_gamma_markets={cfg.use_gamma_markets}")
print(f"  filters keys: {list(filters.keys())}")
print(f"  slugs count: {len(slugs)}")

provider = PolymarketInstrumentProvider(client=clob_client, clock=clock, config=cfg)

print(f"  provider._http_client: {type(provider._http_client).__name__}")

print("\nStep 3: invoking load_all_async (this is what Nautilus does at startup)")

async def run():
    await provider.load_all_async(filters)


try:
    asyncio.run(run())
    print(f"\nStep 4: results")
    instruments = list(provider.get_all().values())
    print(f"  Loaded {len(instruments)} instruments")
    for inst in instruments[:5]:
        print(f"    {inst.id}  (info.outcome={getattr(inst, 'info', {}).get('outcome')})")
except Exception as exc:
    import traceback
    print("\n!!! load_all_async RAISED:")
    traceback.print_exc()
