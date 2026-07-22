"""Smoke coverage for the argument-identity microbenchmark tool."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.benchmark_identity import run_identity_benchmark  # noqa: E402


def test_identity_benchmark_covers_required_shapes() -> "None":
    rows = run_identity_benchmark(iterations=1)
    shapes = {row.shape for row in rows}
    assert shapes == {
        "string-1KiB",
        "string-100KiB",
        "string-1MiB",
        "nested-mapping-1k-keys",
        "nested-mapping-10k-keys",
    }
    for row in rows:
        assert row.payload_bytes > 0
        assert row.median_ms >= 0.0
