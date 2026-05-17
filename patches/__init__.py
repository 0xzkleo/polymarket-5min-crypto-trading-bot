"""
patches — Monkey-patches for the Polymarket Nautilus adapter.

Two patches are applied at startup (before Nautilus is imported):

1. ``gamma_markets``   — fixes array parameter handling in the Gamma API query
   builder and forces ``load_all_async`` to use the Gamma API on Windows
   (avoids ``STATUS_ACCESS_VIOLATION`` in ``nautilus_pyo3.HttpClient``).

2. ``market_orders``   — intercepts BUY market orders and converts the token
   quantity to a USD amount sourced from the ``MARKET_BUY_USD`` env var.
"""
from patches.gamma_markets import apply_gamma_markets_patch, verify_patch
from patches.market_orders import apply_market_order_patch

__all__ = [
    "apply_gamma_markets_patch",
    "verify_patch",
    "apply_market_order_patch",
]
