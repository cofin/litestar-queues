"""Stable task declarations for operator-deployed managed benchmarks."""

from litestar_queues import task


@task("queue_bench.managed_noop", queue="queue_benchmark_managed")
async def managed_noop(payload: str) -> int:
    """Return the payload size after managed delivery."""
    return len(payload)


__all__ = ("managed_noop",)
