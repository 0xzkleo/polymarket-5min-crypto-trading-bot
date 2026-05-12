"""Phase 3: run Nautilus core tests (`core/nautilus_core/test_nautilus.py`)."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    target = ROOT / "core" / "nautilus_core" / "test_nautilus.py"
    sys.argv = [str(target)] + sys.argv[1:]
    sys.path.insert(0, str(ROOT))
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
