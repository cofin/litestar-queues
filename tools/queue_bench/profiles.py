"""Benchmark profile and backend-variant contracts."""

import json
import math
from collections.abc import Mapping
from typing import Any, Literal, cast

ProfileName = Literal[
    "core",
    "rich",
    "heartbeat",
    "events",
    "uniqueness",
    "maintenance",
    "advanced-alchemy",
    "cloud-tasks",
    "cloud-run-jobs",
]
BackendVariant = Literal["default", "psycopg", "asyncpg"]

PROFILE_NAMES: tuple[ProfileName, ...] = (
    "core",
    "rich",
    "heartbeat",
    "events",
    "uniqueness",
    "maintenance",
    "advanced-alchemy",
    "cloud-tasks",
    "cloud-run-jobs",
)
BACKEND_VARIANTS: tuple[BackendVariant, ...] = ("default", "psycopg", "asyncpg")
MAX_PRODUCER_CONCURRENCY = 32
CORE_SCENARIOS: tuple[str, ...] = (
    "enqueue",
    "enqueue-concurrent",
    "enqueue-many",
    "roundtrip",
    "delayed-lateness",
    "retry-once",
    "idle",
)
FEATURE_SCENARIOS: tuple[str, ...] = ("heartbeat", "events")
SCENARIOS: tuple[str, ...] = (*CORE_SCENARIOS, *FEATURE_SCENARIOS)
COMPETITOR_SCENARIOS: tuple[str, ...] = ("enqueue", "roundtrip")

# Scenario-owning child tasks extend these allowlists when they add parameters.
_PROFILE_PARAMETER_KEYS: dict[ProfileName, frozenset[str]] = {
    "core": frozenset({"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}),
    "rich": frozenset({"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}),
    "heartbeat": frozenset({"heartbeat_interval", "observation_seconds"}),
    "events": frozenset({"mode"}),
    "uniqueness": frozenset({"mode"}),
    "maintenance": frozenset({"limit", "record_count"}),
    "advanced-alchemy": frozenset({"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}),
    "cloud-tasks": frozenset(),
    "cloud-run-jobs": frozenset(),
}


def parse_profile_name(value: str) -> ProfileName:
    """Validate and narrow an untyped profile value."""
    if value not in PROFILE_NAMES:
        msg = f"unsupported profile: {value}"
        raise ValueError(msg)
    return value


def parse_backend_variant(value: str) -> BackendVariant:
    """Validate and narrow an untyped backend variant value."""
    if value not in BACKEND_VARIANTS:
        msg = f"unsupported backend variant: {value}"
        raise ValueError(msg)
    return value


def parse_parameter_overrides(values: list[str]) -> dict[str, Any]:
    """Parse repeatable ``KEY=JSON`` command-line values.

    Returns:
        JSON-native parameter values keyed by profile parameter name.
    """
    parameters: dict[str, Any] = {}
    for value in values:
        key, separator, raw_value = value.partition("=")
        if not separator or not key:
            msg = f"invalid profile parameter {value!r}; expected KEY=JSON"
            raise ValueError(msg)
        if key in parameters:
            msg = f"duplicate profile parameter: {key}"
            raise ValueError(msg)
        try:
            parameters[key] = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            msg = f"invalid JSON value for profile parameter {key!r}"
            raise ValueError(msg) from exc
    return parameters


def validate_profile_parameters(profile: ProfileName, parameters: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-native copy after validating keys for the selected profile.

    Returns:
        Validated mutable data suitable for the child-process JSON request.
    """
    if any(not isinstance(key, str) for key in parameters):
        msg = "profile parameter keys must be strings"
        raise TypeError(msg)
    unknown = set(parameters) - _PROFILE_PARAMETER_KEYS[profile]
    if unknown:
        msg = f"unsupported {profile} profile parameters: {', '.join(sorted(unknown))}"
        raise ValueError(msg)
    try:
        encoded = json.dumps(dict(parameters), allow_nan=False)
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        msg = "profile parameters must contain only finite JSON-native values"
        raise ValueError(msg) from exc
    if not isinstance(decoded, dict):  # pragma: no cover - dict input guarantees this branch is unreachable.
        msg = "profile parameters must be a mapping"
        raise TypeError(msg)
    if profile in {"core", "rich", "advanced-alchemy"}:
        _validate_core_parameters(decoded)
    elif profile == "heartbeat":
        _validate_heartbeat_parameters(decoded)
    elif profile == "events":
        _validate_events_parameters(decoded)
    elif profile == "uniqueness":
        _validate_uniqueness_parameters(decoded)
    return cast("dict[str, Any]", decoded)


def _validate_core_parameters(parameters: Mapping[str, Any]) -> None:
    batch_size = parameters.get("batch_size")
    if batch_size is not None and (type(batch_size) is not int or batch_size < 1):
        msg = "batch_size must be an integer of at least 1"
        raise ValueError(msg)
    producer_concurrency = parameters.get("producer_concurrency")
    if producer_concurrency is not None and (
        type(producer_concurrency) is not int or not 1 <= producer_concurrency <= MAX_PRODUCER_CONCURRENCY
    ):
        msg = "producer_concurrency must be an integer from 1 through 32"
        raise ValueError(msg)
    for name in ("delay_seconds", "idle_duration_seconds"):
        value = parameters.get(name)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value) or value <= 0
        ):
            msg = f"{name} must be a positive finite number"
            raise ValueError(msg)


def _validate_heartbeat_parameters(parameters: Mapping[str, Any]) -> None:
    for name in ("heartbeat_interval", "observation_seconds"):
        value = parameters.get(name)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value) or value <= 0
        ):
            msg = f"{name} must be a positive finite number"
            raise ValueError(msg)


def _validate_events_parameters(parameters: Mapping[str, Any]) -> None:
    mode = parameters.get("mode")
    if mode is not None and mode not in {"disabled", "live-only", "durable-history"}:
        msg = "mode must be one of: disabled, live-only, durable-history"
        raise ValueError(msg)


def _validate_uniqueness_parameters(parameters: Mapping[str, Any]) -> None:
    mode = parameters.get("mode")
    if mode not in {"none", "explicit-key", "unique-by-task", "unique-by-arguments"}:
        msg = "mode must be one of: none, explicit-key, unique-by-task, unique-by-arguments"
        raise ValueError(msg)


__all__ = (
    "BACKEND_VARIANTS",
    "COMPETITOR_SCENARIOS",
    "CORE_SCENARIOS",
    "FEATURE_SCENARIOS",
    "PROFILE_NAMES",
    "SCENARIOS",
    "BackendVariant",
    "ProfileName",
    "parse_backend_variant",
    "parse_parameter_overrides",
    "parse_profile_name",
    "validate_profile_parameters",
)
