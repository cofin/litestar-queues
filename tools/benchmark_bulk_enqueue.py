"""Micro-benchmark comparing per-row ``enqueue`` against bulk ``enqueue_many``.

Run with the default in-memory aiosqlite adapter::

    uv run python tools/benchmark_bulk_enqueue.py --rows 2000

Point it at a containerized adapter to measure the native fast paths
(asyncpg COPY, DuckDB Arrow ingest, etc.)::

    uv run python tools/benchmark_bulk_enqueue.py --adapter asyncpg \
        --dsn postgresql://queue:queue@localhost:5432/queue --rows 5000

The script prints rows/second for each path and the bulk speedup. It is a
developer tool, not part of the test suite; the contract suite asserts
correctness while this captures throughput.
"""

import argparse
import asyncio
import time
from collections.abc import Callable
from tempfile import TemporaryDirectory
from typing import Any

from litestar_queues import EnqueueSpec
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend


def _aiosqlite_config(directory: str) -> Any:
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    return AiosqliteConfig(connection_config={"database": f"{directory}/benchmark.db"})


def _asyncpg_config(dsn: str) -> Any:
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    return AsyncpgConfig(connection_config={"dsn": dsn})


def _duckdb_config(directory: str) -> Any:
    from sqlspec.adapters.duckdb import DuckDBConfig

    return DuckDBConfig(connection_config={"database": f"{directory}/benchmark.duckdb"})


_ADAPTERS: dict[str, Callable[[str, str | None], Any]] = {
    "aiosqlite": lambda directory, _dsn: _aiosqlite_config(directory),
    "duckdb": lambda directory, _dsn: _duckdb_config(directory),
    "asyncpg": lambda _directory, dsn: _asyncpg_config(dsn or ""),
}


async def _time_path(label: str, rows: int, run: Callable[[], Any]) -> float:
    start = time.perf_counter()
    await run()
    elapsed = time.perf_counter() - start
    print(f"  {label:<14} {rows / elapsed:>12,.0f} rows/s  ({elapsed:.3f}s)")
    return elapsed


async def _run(adapter: str, dsn: str | None, rows: int) -> None:
    with TemporaryDirectory() as directory:
        config = _ADAPTERS[adapter](directory, dsn)

        single_backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=config))
        await single_backend.open()
        try:
            print(f"adapter={adapter} rows={rows}\n")

            async def per_row() -> None:
                for index in range(rows):
                    await single_backend.enqueue("bench.task", args=(index,), kwargs={"n": index})

            single_elapsed = await _time_path("per-row", rows, per_row)
        finally:
            await single_backend.close()

        bulk_backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=config))
        await bulk_backend.open()
        try:
            specs = [EnqueueSpec(task_name="bench.task", args=(index,), kwargs={"n": index}) for index in range(rows)]

            async def bulk() -> None:
                await bulk_backend.enqueue_many(specs)

            bulk_elapsed = await _time_path("enqueue_many", rows, bulk)
        finally:
            await bulk_backend.close()

    if bulk_elapsed > 0:
        print(f"\n  speedup: {single_elapsed / bulk_elapsed:.1f}x")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", choices=sorted(_ADAPTERS), default="aiosqlite")
    parser.add_argument("--dsn", default=None, help="Connection DSN for server adapters (e.g. asyncpg).")
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    asyncio.run(_run(args.adapter, args.dsn, args.rows))


if __name__ == "__main__":
    main()
