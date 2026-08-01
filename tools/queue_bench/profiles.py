"""Benchmark profile and backend-variant contracts."""

import json
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

# Scenario-owning child tasks extend these allowlists when they add parameters.
_PROFILE_PARAMETER_KEYS: dict[ProfileName, frozenset[str]] = {
    "core": frozenset({"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}),
    "rich": frozenset({"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}),
    "heartbeat": frozenset({"observation_seconds", "task_count"}),
    "events": frozenset({"mode"}),
    "uniqueness": frozenset({"mode"}),
    "maintenance": frozenset({"limit", "record_count"}),
    "advanced-alchemy": frozenset(
        {"batch_size", "delay_seconds", "idle_duration_seconds", "producer_concurrency"}
    ),
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
    return cast("dict[str, Any]", decoded)


__all__ = (
    "BACKEND_VARIANTS",
    "PROFILE_NAMES",
    "BackendVariant",
    "ProfileName",
    "parse_backend_variant",
    "parse_parameter_overrides",
    "parse_profile_name",
    "validate_profile_parameters",
)
