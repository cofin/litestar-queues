"""Shared local-service selection for benchmark runs."""

from tools.dev_infra import ServiceConfig, _service_configs

SUPPORTED_BACKENDS = frozenset({"postgres", "redis", "valkey"})


def parse_dsn_overrides(values: list[str]) -> dict[str, str]:
    """Parse repeatable BACKEND=URL options.

    Returns:
        Mapping from backend name to external URL.
    """
    overrides: dict[str, str] = {}
    for value in values:
        backend, separator, dsn = value.partition("=")
        if not separator or not backend or not dsn:
            msg = f"DSN override must use BACKEND=URL, got {value!r}"
            raise ValueError(msg)
        if backend not in SUPPORTED_BACKENDS:
            msg = f"unsupported DSN backend {backend!r}"
            raise ValueError(msg)
        if backend in overrides:
            msg = f"duplicate DSN override for {backend!r}"
            raise ValueError(msg)
        overrides[backend] = dsn
    return overrides


def select_local_services(backends: list[str], dsn_overrides: dict[str, str]) -> list[ServiceConfig]:
    """Return selected local services that do not have external DSNs."""
    requested = set(backends)
    unknown = requested - SUPPORTED_BACKENDS
    if unknown:
        msg = f"unsupported backends: {', '.join(sorted(unknown))}"
        raise ValueError(msg)
    return [service for service in _service_configs() if service.key in requested and service.key not in dsn_overrides]


__all__ = ("parse_dsn_overrides", "select_local_services")
