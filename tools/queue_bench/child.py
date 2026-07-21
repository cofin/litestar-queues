"""Isolated benchmark child process protocol."""

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from importlib import import_module
from importlib.metadata import distributions
from typing import Any

from tools.queue_bench.adapters import AdapterRequest
from tools.queue_bench.models import RawSample

ADAPTER_MODULES = {
    "litestar-queues": "tools.queue_bench.adapters.litestar_queues",
    "litestar-saq": "tools.queue_bench.adapters.saq",
    "raw-saq": "tools.queue_bench.adapters.saq",
    "arq": "tools.queue_bench.adapters.arq",
    "taskiq": "tools.queue_bench.adapters.taskiq",
    "dramatiq": "tools.queue_bench.adapters.sync_competitors",
    "rq": "tools.queue_bench.adapters.sync_competitors",
    "celery": "tools.queue_bench.adapters.sync_competitors",
}


async def execute(request: AdapterRequest) -> RawSample:
    """Execute an adapter and convert its correctness result to the wire model.

    Returns:
        Versioned raw sample for the parent process.
    """
    module = import_module(ADAPTER_MODULES[request.system])
    result = await module.run(request)
    valid, error = result.validate(request)
    metadata: dict[str, Any] = dict(result.metadata)
    metadata["packages"] = {
        distribution.metadata["Name"]: distribution.version
        for distribution in distributions()
        if distribution.metadata["Name"]
    }
    return RawSample(
        system=request.system,
        backend=request.backend,
        scenario=request.scenario,
        sample_index=request.sample_index,
        duration_seconds=result.duration_seconds,
        operations=request.operations,
        valid=valid,
        counters=result.counters,
        error=error,
        metadata=metadata,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True, help="JSON child request")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        request = AdapterRequest.from_dict(json.loads(args.request))
        sample = asyncio.run(execute(request))
    except Exception:
        import traceback

        traceback.print_exc()
        return 1
    payload = json.dumps(
        sample.__dict__ if hasattr(sample, "__dict__") else _sample_dict(sample), separators=(",", ":")
    )
    output = sys.__stdout__ or sys.stdout
    output.write(payload + "\n")
    output.flush()
    return 0


def _sample_dict(sample: RawSample) -> dict[str, Any]:
    return {
        "system": sample.system,
        "backend": sample.backend,
        "scenario": sample.scenario,
        "sample_index": sample.sample_index,
        "duration_seconds": sample.duration_seconds,
        "operations": sample.operations,
        "valid": sample.valid,
        "counters": sample.counters,
        "error": sample.error,
        "metadata": sample.metadata,
    }


if __name__ == "__main__":
    raise SystemExit(main())
