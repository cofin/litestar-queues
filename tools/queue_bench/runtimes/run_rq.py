# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "redis==8.0.1",
#   "rq==2.10.0",
# ]
# ///
"""Run the shared benchmark child in the pinned RQ environment."""

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
