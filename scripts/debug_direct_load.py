"""
Test the strategy's direct Gamma loader without a TradingNode.

Reproduces the live code path that runs when Nautilus's provider doesn't
populate the cache. Uses the same httpx-based fetch + parse + cache.add
logic that's now in IntegratedBTCStrategy._load_instruments_via_gamma_direct.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()


def main() -> int:
    import httpx
    from nautilus_trader.adapters.polymarket.common.gamma_markets import (
        normalize_gamma_market_to_clob_format,
    )
    from nautilus_trader.adapters.polymarket.common.parsing import (
        parse_polymarket_instrument,
    )

    base_url = os.getenv(
        "GAMMA_API_URL", "https://gamma-api.polymarket.com"
    ).rstrip("/")
    now = datetime.now(timezone.utc)
    ts0 = (int(now.timestamp()) // 900) * 900
    slugs = [f"btc-updown-15m-{ts0 + i * 900}" for i in range(-1, 97)]
    print(f"Generated {len(slugs)} slugs starting at ts={ts0}")

    markets: list[dict] = []
    seen: set[str] = set()
    with httpx.Client(timeout=60.0) as client:
        for start in range(0, len(slugs), 50):
            chunk = slugs[start : start + 50]
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "slug": chunk,
                "limit": 100,
            }
            r = client.get(f"{base_url}/markets", params=params)
            if r.status_code != 200:
                print(f"  HTTP {r.status_code} {r.text[:200]}")
                continue
            rows = r.json()
            for m in rows:
                cid = m.get("conditionId")
                if cid in seen:
                    continue
                if cid:
                    seen.add(cid)
                markets.append(m)
    print(f"Fetched {len(markets)} markets")

    parsed = 0
    errors = 0
    sample = []
    for market in markets:
        try:
            normalized = normalize_gamma_market_to_clob_format(market)
            for token_info in normalized.get("tokens") or []:
                token_id = token_info.get("token_id")
                if not token_id:
                    continue
                outcome = token_info.get("outcome") or ""
                token_market_info = dict(normalized)
                token_market_info["outcome"] = outcome
                token_market_info["token_id"] = token_id
                inst = parse_polymarket_instrument(
                    market_info=token_market_info,
                    token_id=token_id,
                    outcome=outcome,
                    ts_init=None,
                )
                parsed += 1
                if len(sample) < 5:
                    sample.append((str(inst.id), token_market_info["market_slug"], outcome))
        except Exception as e:
            errors += 1
            print(f"  parse error for {market.get('slug', '?')}: {e}")

    print(f"Parsed {parsed} instruments ({errors} errors)")
    for s in sample:
        print(f"  {s[0]}  slug={s[1]}  outcome={s[2]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
