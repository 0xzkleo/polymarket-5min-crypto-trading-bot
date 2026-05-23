"""Find event slugs for active BTC UpDown 15-min markets."""
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


async def main():
    now = datetime.now(timezone.utc)
    ts = (int(now.timestamp()) // 900) * 900
    slug = f"btc-updown-15m-{ts}"

    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{BASE}/markets", params={"slug": slug, "limit": 5})
        market = r.json()[0] if r.json() else None
        if not market:
            print("no market found")
            return
        print(f"Market slug:    {market.get('slug')}")
        print(f"Market eventSlug: {market.get('eventSlug')}")
        print(f"Question:       {market.get('question')}")
        print(f"Condition ID:   {market.get('conditionId')}")
        events = market.get("events") or []
        print(f"Events: {len(events)}")
        for e in events[:3]:
            print(f"  - slug: {e.get('slug')}")
            print(f"    title: {e.get('title')}")
            print(f"    end: {e.get('endDate')}")

        if events:
            evt_slug = events[0].get("slug")
            print(f"\nQuerying event by slug: {evt_slug}")
            r2 = await c.get(f"{BASE}/events", params={"slug": evt_slug, "limit": 5})
            print(f"  events status: {r2.status_code}")
            if r2.status_code == 200:
                data = r2.json()
                if data:
                    e = data[0]
                    markets_in_event = e.get("markets", [])
                    print(f"  event has {len(markets_in_event)} markets")
                    for m in markets_in_event[:5]:
                        print(f"    market slug: {m.get('slug')}")


if __name__ == "__main__":
    asyncio.run(main())
