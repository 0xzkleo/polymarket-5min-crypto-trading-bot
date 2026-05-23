"""
patches.market_orders
=====================
DEPRECATED — kept only for backwards compatibility with older imports.

The previous version of this module monkey-patched
``PolymarketExecutionClient._submit_market_order`` because the bot used to
submit BUY market orders with token quantities (``quote_quantity=False``)
and Polymarket actually expects USD amounts.

That patch imported ``MarketOrderArgs`` from ``py_clob_client`` (V1), which
is missing the ``builder_code`` and ``metadata`` fields that
``py_clob_client_v2`` and Nautilus's adapter now require — producing
``AttributeError('MarketOrderArgs' object has no attribute 'builder_code')``
when an order was submitted.

The strategy now submits BUY orders with ``quote_quantity=True`` and the
USD amount as the order quantity (precision = 6 for microUSDC). Nautilus's
native ``_submit_market_order`` then constructs ``MarketOrderArgsV2``
correctly, including ``builder_code`` and ``user_usdc_balance``, and the
order goes through without any monkey-patching.

This module is now a no-op. ``apply_market_order_patch()`` simply logs that
the native path is in use and returns ``True`` so callers (``bot.runner``)
don't think anything went wrong.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Kept so ``bot.strategy._place_real_order`` (and any other callers) can do
# ``from patches.market_orders import _patch_applied`` without raising
# ImportError. The strategy no longer depends on it for correctness.
_patch_applied: bool = True


def apply_market_order_patch() -> bool:
    """
    No-op shim — the bot uses Nautilus's native V2 market-order submission.

    Returns ``True`` so legacy callers treat the bot as ready to trade.
    """
    logger.info(
        "patches.market_orders: native V2 path active (no monkey-patch needed)"
    )
    return True
