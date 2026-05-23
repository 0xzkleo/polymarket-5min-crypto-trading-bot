"""
Debug Gamma API instrument loading.

Tests multiple slug strategies to find why the bot loads zero instruments.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/")


async def fetch(client: httpx.AsyncClient, **params) -> list:
    r = await client.get(f"{BASE}/markets", params=params)
    if r.status_code != 200:
        return [{"_error": f"HTTP {r.status_code}: {r.text[:300]}"}]
    data = r.json()
    if isinstance(data, dict):
        return data.get("data") or []
    return data or []


async def main():
    now = datetime.now(timezone.utc)
    ts = (int(now.timestamp()) // 900) * 900
    print(f"Now (UTC): {now.isoformat()}")
    print(f"Current 15-min interval start: {ts}")
    print(f"  current slug guess: btc-updown-15m-{ts}")
    print(f"  next slug guess:    btc-updown-15m-{ts + 900}")
    print("=" * 80)

    async with httpx.AsyncClient(timeout=30) as c:
        # 1. Try the exact current slug
        print("\n[1] Query single current slug...")
        rows = await fetch(c, slug=f"btc-updown-15m-{ts}", limit=10)
        print(f"    result: {len(rows)} markets")
        for r in rows[:3]:
            if "_error" in r:
                print(f"    ERROR: {r['_error']}")
            else:
                print(f"    found: slug={r.get('slug')} active={r.get('active')} closed={r.get('closed')}")

        # 2. Search by BTC string for active markets
        print("\n[2] Search active BTC markets (any title)...")
        rows = await fetch(
            c,
            active="true",
            closed="false",
            archived="false",
            limit=200,
            order="endDate",
            ascending="true",
        )
        btc = [r for r in rows if "btc" in (r.get("slug") or "").lower()]
        print(f"    total active markets: {len(rows)} | BTC matches: {len(btc)}")
        for r in btc[:10]:
            print(f"      slug={r.get('slug')}  end={r.get('endDate')}")

        # 3. Search by tag (BTC tag id is typically 21 on Polymarket)
        print("\n[3] Try BTC 15m specifically via tag filter and time range...")
        end_min = now.isoformat().replace("+00:00", "Z")
        rows = await fetch(
            c,
            active="true",
            closed="false",
            archived="false",
            end_date_min=end_min,
            limit=500,
        )
        btc_15m = [r for r in rows if "btc-updown-15m" in (r.get("slug") or "").lower()]
        print(f"    total future-ending active: {len(rows)} | 'btc-updown-15m' slugs: {len(btc_15m)}")
        for r in btc_15m[:10]:
            print(f"      slug={r.get('slug')}  active={r.get('active')}  end={r.get('endDate')}")

        # 4. Try batch slug array (like runner.py)
        print("\n[4] Batch 98 slugs (like runner.py)...")
        slugs = [f"btc-updown-15m-{ts + i*900}" for i in range(-1, 97)]
        rows = await fetch(
            c,
            slug=slugs,
            active="true",
            closed="false",
            archived="false",
            limit=100,
        )
        print(f"    result: {len(rows)} markets")
        if rows and "_error" in rows[0]:
            print(f"    ERROR: {rows[0]['_error']}")
        else:
            for r in rows[:5]:
                print(f"      slug={r.get('slug')}  active={r.get('active')}")

        # 5. Try 10-slug batch
        print("\n[5] Batch 10 slugs...")
        rows = await fetch(
            c,
            slug=slugs[:10],
            active="true",
            closed="false",
            archived="false",
            limit=100,
        )
        print(f"    result: {len(rows)} markets")
        if rows and "_error" in rows[0]:
            print(f"    ERROR: {rows[0]['_error']}")
        else:
            for r in rows[:5]:
                print(f"      slug={r.get('slug')}")


if __name__ == "__main__":
    asyncio.run(main())
