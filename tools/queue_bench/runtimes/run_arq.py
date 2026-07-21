# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "arq==0.28.0",
#   "redis==5.3.1",
# ]
# ///
"""Run the shared benchmark child in the pinned ARQ environment."""

import sys
from pathlib import Path


def main() -> int:
    """Load and run the shared child protocol.

    Returns:
        Child process exit code.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from tools.queue_bench.child import main as child_main

    return child_main()


if __name__ == "__main__":
    raise SystemExit(main())
