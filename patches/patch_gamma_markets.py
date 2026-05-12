"""Load Gamma / provider monkey-patch implementation from the repo root module."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_IMPL = _ROOT / "patch_gamma_markets.py"

_spec = importlib.util.spec_from_file_location("polymarket_ai.patch_gamma_impl", _IMPL)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

apply_gamma_markets_patch = _mod.apply_gamma_markets_patch
verify_patch = _mod.verify_patch

__all__ = ("apply_gamma_markets_patch", "verify_patch")
