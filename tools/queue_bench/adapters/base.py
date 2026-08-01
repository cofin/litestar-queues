"""Shared adapter request and correctness contracts."""

import asyncio
import math
from collections.abc import Awaitable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from tools.queue_bench.profiles import (
    BackendVariant,
    ProfileName,
    parse_backend_variant,
    parse_profile_name,
    validate_profile_parameters,
)


@dataclass(frozen=True, slots=True)
class AdapterRequest:
    system: str
    backend: str
    dsn: str
    scenario: str
    profile: ProfileName
    backend_variant: BackendVariant
    operations: int
    payload_size: int
    concurrency: int
    namespace: str
    sample_index: int
    parameters: Mapping[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 60.0

    @property
    def payload(self) -> str:
        return "x" * self.payload_size

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AdapterRequest":
        profile = parse_profile_name(str(value["profile"]))
        return cls(
            system=str(value["system"]),
            backend=str(value["backend"]),
            dsn=str(value["dsn"]),
            scenario=str(value["scenario"]),
            profile=profile,
            backend_variant=parse_backend_variant(str(value["backend_variant"])),
            operations=int(value["operations"]),
            payload_size=int(value["payload_size"]),
            concurrency=int(value["concurrency"]),
            namespace=str(value["namespace"]),
            sample_index=int(value["sample_index"]),
            parameters=validate_profile_parameters(profile, dict(value.get("parameters", {}))),
            timeout_seconds=float(value.get("timeout_seconds", 60.0)),
        )


@dataclass(frozen=True, slots=True)
class AdapterResult:
    duration_seconds: float
    counters: dict[str, int]
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized_counters(self) -> dict[str, int]:
        """Return the common counter schema, including legacy adapter aliases."""
        counters = dict(self.counters)
        legacy_enqueued = counters.pop("enqueued", None)
        if legacy_enqueued is not None:
            counters.setdefault("requests", legacy_enqueued)
            counters.setdefault("records", legacy_enqueued)
            counters.setdefault("failed", 0)
            counters.setdefault("retried", 0)
        return counters

    def validate(self, request: AdapterRequest) -> tuple[bool, str | None]:
        counters = self.normalized_counters()
        expected = _expected_counters(request)
        for name, count in expected.items():
            if name not in counters:
                return False, f"required counter {name!r} is missing"
            if counters.get(name) != count:
                return False, f"expected {count} {name}, got {counters.get(name, 0)}"
        return True, None


def _expected_counters(request: AdapterRequest) -> dict[str, int]:
    operations = request.operations
    terminal = {
        "requests": operations,
        "records": operations,
        "started": operations,
        "completed": operations,
        "failed": 0,
        "retried": 0,
        "remaining": 0,
    }
    if request.scenario in {"enqueue", "enqueue-concurrent"}:
        return {**terminal, "started": 0, "completed": 0, "remaining": operations}
    if request.scenario == "enqueue-many":
        batch_size = int(request.parameters.get("batch_size", 100))
        return {
            **terminal,
            "requests": math.ceil(operations / batch_size),
            "started": 0,
            "completed": 0,
            "remaining": operations,
        }
    if request.scenario == "delayed-lateness":
        return {**terminal, "scheduled": operations, "not_early": operations}
    if request.scenario == "retry-once":
        return {**terminal, "started": operations * 2, "retried": operations}
    if request.scenario == "idle":
        return {
            "requests": 0,
            "records": 0,
            "started": 0,
            "completed": 0,
            "failed": 0,
            "retried": 0,
            "remaining": 0,
            "idle_observations": 1,
        }
    return terminal


async def gather_bounded(awaitables: Iterable[Awaitable[Any]], *, limit: int) -> list[Any]:
    """Await work without exceeding the configured benchmark concurrency.

    Returns:
        Results in input order.
    """
    semaphore = asyncio.Semaphore(max(1, limit))

    async def run_one(awaitable: Awaitable[Any]) -> Any:
        async with semaphore:
            return await awaitable

    return list(await asyncio.gather(*(run_one(awaitable) for awaitable in awaitables)))


__all__ = ("AdapterRequest", "AdapterResult", "gather_bounded")
