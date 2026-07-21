#!/usr/bin/env python
"""Executable wrapper for :mod:`tools.queue_bench`."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.queue_bench.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
