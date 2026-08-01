"""Shared adapter request and correctness contracts."""

import asyncio
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

    def validate(self, request: AdapterRequest) -> tuple[bool, str | None]:
        expected = request.operations
        if self.counters.get("enqueued") != expected:
            return False, f"expected {expected} enqueued, got {self.counters.get('enqueued', 0)}"
        if request.scenario == "roundtrip":
            if self.counters.get("started") != expected:
                return False, f"expected {expected} started, got {self.counters.get('started', 0)}"
            if self.counters.get("completed") != expected:
                return False, f"expected {expected} completed, got {self.counters.get('completed', 0)}"
            if self.counters.get("remaining") != 0:
                return False, f"expected no remaining jobs, got {self.counters.get('remaining', 0)}"
        elif self.counters.get("remaining") != expected:
            return False, f"expected {expected} remaining jobs, got {self.counters.get('remaining', 0)}"
        return True, None


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
