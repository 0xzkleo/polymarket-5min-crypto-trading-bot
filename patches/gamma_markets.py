"""
patches.gamma_markets
=====================
Monkey-patches for the Polymarket Nautilus adapter's Gamma API layer.

Fixes applied
-------------
1. ``gamma_markets.build_markets_query`` — properly serialises array parameters
   (slug, clob_token_ids, etc.) as repeated query params instead of
   comma-separated strings.

2. ``PolymarketInstrumentProvider.load_all_async`` — forces the provider to
   use the Gamma API instead of the CLOB API when ``use_gamma_markets=True``.

3. On Windows, ``nautilus_pyo3.HttpClient`` crashes with
   STATUS_ACCESS_VIOLATION (0xC0000005).  The replacement implementation uses
   ``httpx.AsyncClient`` which is fully safe on Windows.
"""
from __future__ import annotations

import logging
import os
import asyncio
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


async def _fetch_markets_httpx(filters: dict) -> list:
    """Fetch Gamma API markets via httpx (Windows-safe fallback)."""
    import httpx

    base_url = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/")
    limit = int(filters.get("limit", 500))
    offset = int(filters.get("offset", 0))

    params: dict = {}
    scalar_keys = (
        "active", "archived", "closed", "order", "ascending",
        "liquidity_num_min", "liquidity_num_max",
        "volume_num_min", "volume_num_max",
        "start_date_min", "start_date_max",
        "end_date_min", "end_date_max",
        "tag_id", "related_tags",
    )
    for key in scalar_keys:
        if key in filters and filters[key] is not None:
            params[key] = filters[key]

    array_keys = ("id", "slug", "clob_token_ids", "condition_ids", "question_ids")
    for key in array_keys:
        if key in filters and filters[key] is not None:
            value = filters[key]
            params[key] = list(value) if isinstance(value, (list, tuple)) else [value]

    all_markets: list = []
    async with httpx.AsyncClient(timeout=120.0) as client:
        while True:
            page_params = {**params, "limit": limit, "offset": offset}
            resp = await client.get(f"{base_url}/markets", params=page_params)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Gamma API error {resp.status_code}: {resp.text[:500]}"
                )
            data = resp.json()
            if isinstance(data, list):
                page = data
            elif isinstance(data, dict) and "data" in data:
                page = data.get("data") or []
            else:
                raise RuntimeError(f"Unexpected Gamma API response: {str(data)[:200]}")

            all_markets.extend(page)
            if len(page) < limit:
                break
            offset += limit

    return all_markets


def apply_gamma_markets_patch() -> bool:
    """
    Apply both Gamma API patches to the running Nautilus adapter.

    Returns ``True`` on success, ``False`` on failure.
    """
    try:
        from nautilus_trader.adapters.polymarket.common import gamma_markets
        from nautilus_trader.adapters.polymarket import providers

        logger.info("=" * 80)
        logger.info("Applying Polymarket Gamma-API patches")
        logger.info("=" * 80)

        # ── Patch 1: fix array parameter serialisation ────────────────────────

        def patched_build_markets_query(filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
            params: Dict[str, Any] = {}
            if not filters:
                return params

            if filters.get("is_active") is True:
                params["active"] = "true"
                params["archived"] = "false"
                params["closed"] = "false"

            scalar_keys = (
                "active", "archived", "closed", "limit", "offset",
                "order", "ascending",
                "liquidity_num_min", "liquidity_num_max",
                "volume_num_min", "volume_num_max",
                "start_date_min", "start_date_max",
                "end_date_min", "end_date_max",
                "tag_id", "related_tags",
            )
            for key in scalar_keys:
                if key in filters and filters[key] is not None:
                    params[key] = filters[key]

            array_keys = (
                "id", "slug", "clob_token_ids",
                "condition_ids", "question_ids", "market_maker_address",
            )
            for key in array_keys:
                if key in filters and filters[key] is not None:
                    value = filters[key]
                    params[key] = (
                        list(value) if isinstance(value, (tuple, list)) else [value]
                    )
                    if key == "slug" and params[key]:
                        logger.debug(f"Added {len(params[key])} slug filters")

            return params

        gamma_markets.build_markets_query = patched_build_markets_query
        logger.info("Patched gamma_markets.build_markets_query (array params)")

        # ── Patch 2: force Gamma API in load_all_async ────────────────────────

        async def patched_load_all_async(self, filters: dict | None = None) -> None:
            self._log.info("=" * 80)
            self._log.info("LOADING MARKETS VIA GAMMA API (PATCHED)")
            if filters:
                self._log.info(f"Filters: {filters}")
            self._log.info("=" * 80)

            if self._config.use_gamma_markets:
                await self._load_all_using_gamma_markets(filters)
            else:
                self._log.warning("Falling back to CLOB API (slow, may ignore filters)")
                await self._load_markets([], filters)

        async def _load_all_using_gamma_markets(self, filters: dict | None = None) -> None:
            import sys as _sys
            filters = (filters.copy() if filters is not None else {})
            if "limit" not in filters:
                filters["limit"] = 1000

            self._log.info(f"Requesting markets from Gamma API: {filters}")

            try:
                if _sys.platform == "win32":
                    markets = await _fetch_markets_httpx(filters)
                else:
                    markets = await gamma_markets.list_markets(
                        http_client=self._http_client,
                        filters=filters,
                        timeout=120.0,
                    )

                self._log.info(f"Gamma API returned {len(markets)} markets")

                if not markets:
                    self._log.warning("No markets found with current filters")
                    return

                btc_count = sum(1 for m in markets if "btc" in m.get("slug", "").lower())
                self._log.info(f"BTC markets in batch: {btc_count}")

                loaded_count = 0
                for market in markets:
                    try:
                        normalized = gamma_markets.normalize_gamma_market_to_clob_format(market)
                        slug = market.get("slug", "")
                        if "btc" in slug.lower() and "15m" in slug.lower():
                            self._log.info(f"Found BTC 15-min market: {slug}")
                        for token_info in normalized.get("tokens", []):
                            token_id = token_info["token_id"]
                            if not token_id:
                                continue
                            outcome = token_info["outcome"]
                            self._load_instrument(normalized, token_id, outcome)
                            loaded_count += 1
                    except Exception as e:
                        self._log.error(f"Error processing market {market.get('slug', '?')}: {e}")
                        continue

                self._log.info(f"Loaded {loaded_count} instruments from {len(markets)} markets")

            except Exception as e:
                self._log.error(f"Gamma API request failed: {e}")
                import traceback
                traceback.print_exc()

        providers.PolymarketInstrumentProvider.load_all_async = patched_load_all_async
        providers.PolymarketInstrumentProvider._load_all_using_gamma_markets = (
            _load_all_using_gamma_markets
        )

        logger.info("Patched PolymarketInstrumentProvider.load_all_async")
        logger.info("=" * 80)
        return True

    except ImportError as e:
        logger.error(f"Failed to import modules for gamma_markets patch: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to apply gamma_markets patch: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_patch() -> bool:
    """Verify that the gamma_markets patch is active."""
    try:
        from nautilus_trader.adapters.polymarket.common import gamma_markets
        from nautilus_trader.adapters.polymarket import providers

        logger.info("Verifying patches …")

        test_filters = {
            "active": True,
            "closed": False,
            "archived": False,
            "slug": ("test-slug-1", "test-slug-2"),
            "end_date_min": "2026-01-01T00:00:00Z",
        }
        params = gamma_markets.build_markets_query(test_filters)
        logger.info(f"  build_markets_query output: {params}")

        has_patched = hasattr(
            providers.PolymarketInstrumentProvider, "_load_all_using_gamma_markets"
        )
        logger.info(f"  Provider has patched method: {has_patched}")
        return has_patched

    except Exception as e:
        logger.error(f"Patch verification failed: {e}")
        return False
