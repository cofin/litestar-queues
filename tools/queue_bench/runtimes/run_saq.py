# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "litestar-saq[psycopg]==0.8.0",
#   "redis==7.4.1",
#   "saq[postgres,redis]",
# ]
# ///
"""Run the shared benchmark child against the latest available SAQ release."""

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
