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
   The patched loader chunks slug batches (URL-safe), uses ``httpx`` on
   Windows (avoiding the pyo3 ``HttpClient`` STATUS_ACCESS_VIOLATION crash),
   and copies the token's outcome into ``info['outcome']`` so the strategy
   can identify YES/NO without relying on insertion order.

3. ``PolymarketDataLoader._fetch_event_by_slug`` — Windows-safe httpx
   fallback so the ``event_slug_builder`` initialisation path also works.

All logs go through loguru so they appear in the main bot log file alongside
the rest of the strategy output.
"""
from __future__ import annotations

import asyncio
import os
import sys
import traceback
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

# Max slugs per HTTP request. Gamma's `/markets?limit=100` page cap means
# anything larger gets paged anyway, so chunk small enough that the URL stays
# well under the 8 KB safe limit (each slug is ~28 chars + key overhead).
_SLUG_CHUNK = 50


def _is_win() -> bool:
    return sys.platform == "win32"


def _to_list(value: Any) -> Optional[List[Any]]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _chunked(items: Iterable[Any], size: int) -> Iterable[List[Any]]:
    chunk: List[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


async def _fetch_markets_httpx(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch Gamma API markets via httpx.

    Handles slug-array filters by chunking into URL-safe requests and unioning
    the results. Also pages through limit/offset until the response is short.
    """
    import httpx

    base_url = os.getenv(
        "GAMMA_API_URL", "https://gamma-api.polymarket.com"
    ).rstrip("/")
    endpoint = f"{base_url}/markets"

    base_params: Dict[str, Any] = {}
    scalar_keys = (
        "active", "archived", "closed", "order", "ascending",
        "liquidity_num_min", "liquidity_num_max",
        "volume_num_min", "volume_num_max",
        "start_date_min", "start_date_max",
        "end_date_min", "end_date_max",
        "tag_id", "related_tags",
    )
    for key in scalar_keys:
        if filters.get(key) is not None:
            base_params[key] = filters[key]

    # Coerce booleans to lowercase strings for Gamma's parser.
    for key in ("active", "archived", "closed", "ascending"):
        if isinstance(base_params.get(key), bool):
            base_params[key] = "true" if base_params[key] else "false"

    limit = int(filters.get("limit", 100))

    slug_values = _to_list(filters.get("slug"))
    other_arrays: Dict[str, List[Any]] = {}
    for key in ("id", "clob_token_ids", "condition_ids", "question_ids"):
        value = _to_list(filters.get(key))
        if value:
            other_arrays[key] = value

    request_groups: List[Dict[str, Any]] = []

    if slug_values:
        for chunk in _chunked(slug_values, _SLUG_CHUNK):
            params = {**base_params, "slug": chunk}
            params.update(other_arrays)
            request_groups.append(params)
    elif other_arrays:
        # Chunk the first array field that's present.
        first_key, first_val = next(iter(other_arrays.items()))
        for chunk in _chunked(first_val, _SLUG_CHUNK):
            params = {**base_params, first_key: chunk}
            for k, v in other_arrays.items():
                if k != first_key:
                    params[k] = v
            request_groups.append(params)
    else:
        request_groups.append(dict(base_params))

    all_markets: List[Dict[str, Any]] = []
    seen_condition_ids: set[str] = set()

    async with httpx.AsyncClient(timeout=60.0) as client:
        for idx, group in enumerate(request_groups, 1):
            offset = 0
            page = 0
            while True:
                page += 1
                page_params = {**group, "limit": limit, "offset": offset}
                logger.debug(
                    f"[gamma] request {idx}/{len(request_groups)} page {page} "
                    f"keys={list(page_params.keys())} "
                    f"slug_count={len(page_params.get('slug', [])) if isinstance(page_params.get('slug'), list) else 1}"
                )
                resp = await client.get(endpoint, params=page_params)
                if resp.status_code != 200:
                    logger.error(
                        f"[gamma] HTTP {resp.status_code} on /markets "
                        f"(group {idx}, page {page}): {resp.text[:300]}"
                    )
                    raise RuntimeError(
                        f"Gamma API error {resp.status_code}: {resp.text[:300]}"
                    )

                data = resp.json()
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict) and "data" in data:
                    rows = data.get("data") or []
                else:
                    logger.error(f"[gamma] unexpected response: {str(data)[:200]}")
                    rows = []

                # De-dupe across overlapping chunks/pages.
                for market in rows:
                    cid = market.get("conditionId")
                    if cid and cid in seen_condition_ids:
                        continue
                    if cid:
                        seen_condition_ids.add(cid)
                    all_markets.append(market)

                if len(rows) < limit:
                    break
                offset += limit
                # Gamma rejects offset > 10000.
                if offset >= 10000:
                    logger.warning("[gamma] offset cap (10000) reached, stopping page loop")
                    break

    logger.info(
        f"[gamma] httpx fetched {len(all_markets)} markets "
        f"across {len(request_groups)} request group(s)"
    )
    return all_markets


async def _fetch_event_by_slug_httpx(slug: str) -> Dict[str, Any]:
    """Windows-safe replacement for PolymarketDataLoader._fetch_event_by_slug."""
    import httpx

    base_url = os.getenv(
        "GAMMA_API_URL", "https://gamma-api.polymarket.com"
    ).rstrip("/")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{base_url}/events", params={"slug": slug})
        if resp.status_code == 404:
            raise ValueError(f"Event with slug '{slug}' not found")
        if resp.status_code != 200:
            raise RuntimeError(
                f"HTTP request failed with status {resp.status_code}: "
                f"{resp.text[:200]}"
            )
        events = resp.json()
        if not events:
            raise ValueError(f"Event with slug '{slug}' not found")
        return events[0]


def apply_gamma_markets_patch() -> bool:
    """Apply Gamma API + slug-array + Windows-safe patches. Idempotent-ish."""
    try:
        from nautilus_trader.adapters.polymarket import providers
        from nautilus_trader.adapters.polymarket import loaders
        from nautilus_trader.adapters.polymarket.common import gamma_markets

        logger.info("=" * 80)
        logger.info("Applying Polymarket Gamma-API patches (loguru-instrumented)")
        logger.info(f"  platform: {sys.platform}  using_httpx_fallback: {_is_win()}")
        logger.info("=" * 80)

        # ── Patch 1: array-friendly build_markets_query ───────────────────
        def patched_build_markets_query(
            filters: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
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
                if filters.get(key) is not None:
                    params[key] = filters[key]

            array_keys = (
                "id", "slug", "clob_token_ids",
                "condition_ids", "question_ids", "market_maker_address",
            )
            for key in array_keys:
                value = filters.get(key)
                if value is None:
                    continue
                params[key] = list(value) if isinstance(value, (tuple, list)) else [value]

            return params

        gamma_markets.build_markets_query = patched_build_markets_query
        logger.info("[gamma] patched build_markets_query (array params)")

        # ── Patch 2: full load_all_async + Windows-safe fetch ─────────────
        async def patched_load_all_async(
            self,
            filters: Optional[Dict[str, Any]] = None,
        ) -> None:
            logger.info("=" * 80)
            logger.info("[gamma] LOAD_ALL_ASYNC invoked (patched)")
            logger.info(
                f"[gamma] filters keys: {list(filters.keys()) if filters else []}"
            )
            logger.info(
                f"[gamma] use_gamma_markets={getattr(self._config, 'use_gamma_markets', None)}"
            )
            logger.info("=" * 80)

            if getattr(self._config, "use_gamma_markets", False):
                await self._load_all_using_gamma_markets(filters)
            else:
                logger.warning("[gamma] use_gamma_markets=False, falling back to CLOB")
                await self._load_markets([], filters)

            logger.info(
                f"[gamma] LOAD_ALL_ASYNC complete — provider has {self.count} instruments"
            )

        async def patched_load_all_using_gamma_markets(
            self,
            filters: Optional[Dict[str, Any]] = None,
        ) -> None:
            filters = filters.copy() if filters else {}
            if "limit" not in filters:
                filters["limit"] = 100

            logger.info(
                f"[gamma] _load_all_using_gamma_markets: requesting "
                f"slug_count={len(filters.get('slug', [])) if isinstance(filters.get('slug'), (list, tuple)) else 0}"
            )

            try:
                if _is_win():
                    markets = await _fetch_markets_httpx(filters)
                else:
                    markets = await gamma_markets.list_markets(
                        http_client=self._http_client,
                        filters=filters,
                        timeout=60.0,
                    )
            except Exception as exc:
                logger.error(f"[gamma] fetch failed: {exc}")
                logger.error(traceback.format_exc())
                return

            logger.info(f"[gamma] received {len(markets)} markets from Gamma API")

            if not markets:
                logger.warning("[gamma] zero markets returned — check filters/slugs")
                return

            btc_count = sum(
                1 for m in markets if "btc" in (m.get("slug") or "").lower()
            )
            logger.info(f"[gamma] BTC markets in batch: {btc_count}")

            loaded = 0
            errors = 0

            for market in markets:
                try:
                    normalized = gamma_markets.normalize_gamma_market_to_clob_format(
                        market
                    )
                    slug = market.get("slug") or ""
                    tokens = normalized.get("tokens", []) or []
                    for token_info in tokens:
                        token_id = token_info.get("token_id")
                        if not token_id:
                            continue
                        outcome = token_info.get("outcome") or ""

                        # Each token gets its own info dict so info['outcome']
                        # is correct per-leg (the strategy reads this to
                        # distinguish YES vs NO).
                        token_market_info = dict(normalized)
                        token_market_info["outcome"] = outcome
                        token_market_info["token_id"] = token_id

                        self._load_instrument(token_market_info, token_id, outcome)
                        loaded += 1
                except Exception as exc:
                    errors += 1
                    logger.error(
                        f"[gamma] failed processing market {market.get('slug', '?')}: {exc}"
                    )

            logger.info(
                f"[gamma] _load_all_using_gamma_markets done: "
                f"loaded={loaded} errors={errors} provider_count={self.count}"
            )

        providers.PolymarketInstrumentProvider.load_all_async = patched_load_all_async
        providers.PolymarketInstrumentProvider._load_all_using_gamma_markets = (
            patched_load_all_using_gamma_markets
        )
        logger.info("[gamma] patched PolymarketInstrumentProvider.load_all_async")

        # ── Patch 3: Windows-safe event_slug_builder loader ───────────────
        if _is_win():
            async def patched_fetch_event_by_slug(slug, http_client=None):
                return await _fetch_event_by_slug_httpx(slug)

            loaders.PolymarketDataLoader._fetch_event_by_slug = staticmethod(
                patched_fetch_event_by_slug
            )
            logger.info(
                "[gamma] patched PolymarketDataLoader._fetch_event_by_slug "
                "(Windows-safe httpx)"
            )

        logger.info("=" * 80)
        logger.info("[gamma] all patches applied successfully")
        logger.info("=" * 80)
        return True

    except ImportError as exc:
        logger.error(f"[gamma] failed to import nautilus modules: {exc}")
        return False
    except Exception as exc:
        logger.error(f"[gamma] failed to apply patches: {exc}")
        logger.error(traceback.format_exc())
        return False


def verify_patch() -> bool:
    """Verify the gamma_markets patch is active."""
    try:
        from nautilus_trader.adapters.polymarket import providers
        from nautilus_trader.adapters.polymarket.common import gamma_markets

        test_filters = {
            "active": True,
            "closed": False,
            "slug": ("test-slug-1", "test-slug-2"),
        }
        params = gamma_markets.build_markets_query(test_filters)
        is_array = isinstance(params.get("slug"), list)
        logger.info(f"[gamma] verify: build_markets_query slug is list: {is_array}")

        has_patched = (
            getattr(
                providers.PolymarketInstrumentProvider.load_all_async,
                "__name__",
                "",
            )
            == "patched_load_all_async"
        )
        logger.info(f"[gamma] verify: load_all_async is patched: {has_patched}")
        return has_patched and is_array

    except Exception as exc:
        logger.error(f"[gamma] verify failed: {exc}")
        return False
