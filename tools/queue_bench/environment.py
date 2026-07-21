"""Environment capture and secret redaction."""

import os
import platform
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SENSITIVE_KEYS = frozenset({"api_key", "authorization", "password", "secret", "token"})


def redact_url(value: str) -> str:
    """Redact credentials and query values from a URL.

    Returns:
        Safe URL for artifacts and human output.
    """
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return value
    hostname = parsed.hostname or ""
    host = f"[{hostname}]" if ":" in hostname else hostname
    port = f":{parsed.port}" if parsed.port is not None else ""
    if parsed.password is not None:
        username = parsed.username or ""
        netloc = f"{username}:***@{host}{port}"
    else:
        netloc = parsed.netloc
    query = urlencode([(key, "***") for key, _ in parse_qsl(parsed.query, keep_blank_values=True)])
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Recursively redact secret-bearing mappings and URLs.

    Returns:
        Redacted JSON-compatible value.
    """
    if key is not None and key.lower() in SENSITIVE_KEYS:
        return "***"
    if isinstance(value, dict):
        return {str(item_key): redact_data(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [redact_data(item) for item in value]
    if isinstance(value, tuple):
        return [redact_data(item) for item in value]
    if isinstance(value, str) and "://" in value:
        return redact_url(value)
    return value


def capture_environment(*, packages: list[str], network_class: str) -> dict[str, Any]:
    """Capture reproducibility metadata without leaking environment secrets.

    Returns:
        Environment manifest suitable for a benchmark result.
    """
    package_versions = {package: _package_version(package) for package in packages}
    git = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, check=False, text=True, timeout=5)
    git_status = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, check=False, text=True, timeout=5
    )
    environment = {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "cpu": platform.processor(),
        "cpu_count": os.cpu_count(),
        "git_sha": git.stdout.strip() if git.returncode == 0 else "unknown",
        "git_dirty": bool(git_status.stdout.strip()) if git_status.returncode == 0 else None,
        "network_class": network_class,
        "packages": package_versions,
    }
    try:
        import psutil  # type: ignore[import-untyped]
    except ImportError:
        environment["memory_bytes"] = None
    else:
        environment["memory_bytes"] = psutil.virtual_memory().total
    return environment


def _package_version(package: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return "not-installed"


__all__ = ("capture_environment", "redact_data", "redact_url")
