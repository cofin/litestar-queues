#!/usr/bin/env python
"""Microbenchmark for canonical argument-identity digest cost.

Measures the ``unique_by="arguments"`` canonical-JSON + SHA-256 digest for the
payload shapes the task-uniqueness spec calls out: 1 KiB, 100 KiB, and 1 MiB
string payloads, plus nested 1,000-key and 10,000-key mappings. Each row records
the canonical payload size in bytes and the median digest duration. Argument
*values* are never logged -- only their shape label, byte size, and timing.

Run directly::

    python tools/benchmark_identity.py
"""

import inspect
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from litestar_queues._identity import arguments_identity

_KIB = 1024
_MIB = 1024 * 1024


def _bench(payload: object) -> None:
    """Signature target whose single parameter carries the benchmark payload."""


_SIGNATURE = inspect.signature(_bench)


@dataclass(frozen=True, slots=True)
class IdentityMeasurement:
    """One benchmark row: shape label, payload size, and median digest time."""

    shape: str
    payload_bytes: int
    median_ms: float


def _payloads() -> "list[tuple[str, object]]":
    return [
        ("string-1KiB", "x" * _KIB),
        ("string-100KiB", "x" * (100 * _KIB)),
        ("string-1MiB", "x" * _MIB),
        ("nested-mapping-1k-keys", {f"k{i}": {"v": i, "t": "x"} for i in range(1_000)}),
        ("nested-mapping-10k-keys", {f"k{i}": {"v": i, "t": "x"} for i in range(10_000)}),
    ]


def run_identity_benchmark(*, iterations: int = 50) -> "list[IdentityMeasurement]":
    """Return digest measurements for every benchmark payload shape.

    Args:
        iterations: Number of digest samples per shape.

    Returns:
        One :class:`IdentityMeasurement` per payload shape.
    """
    measurements: "list[IdentityMeasurement]" = []
    for shape, payload in _payloads():
        payload_bytes = 0
        samples: "list[float]" = []
        for _ in range(max(1, iterations)):
            start = time.perf_counter()
            identity = arguments_identity("bench.task", _SIGNATURE, (payload,), {})
            samples.append((time.perf_counter() - start) * 1000.0)
            payload_bytes = identity.payload_bytes
        measurements.append(
            IdentityMeasurement(shape=shape, payload_bytes=payload_bytes, median_ms=statistics.median(samples))
        )
    return measurements


def main() -> int:
    """Print the identity digest benchmark table.

    Returns:
        Process exit code.
    """
    print(f"{'shape':<26} {'payload_bytes':>14} {'median_ms':>12}")
    for row in run_identity_benchmark():
        print(f"{row.shape:<26} {row.payload_bytes:>14} {row.median_ms:>12.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
