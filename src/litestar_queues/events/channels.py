"""Channel naming helpers for queue events."""

import re
import unicodedata
from typing import ClassVar

__all__ = ("QueueChannels",)

_INVALID_CHARS = re.compile(r"[^a-z0-9_.:-]+")
_INVALID_CHARS_NO_COLON = re.compile(r"[^a-z0-9_.-]+")
_REPEATED_UNDERSCORES = re.compile(r"_+")


class QueueChannels:
    """Canonical channel name factories for queue event scopes."""

    prefix: ClassVar[str] = "litestar_queues"

    @classmethod
    def task(cls, task_id: str, *, topic: str = "events") -> str:
        """Return the channel for task-scoped events."""
        return f"{cls.prefix}:task:{_normalize_part(task_id)}:{_normalize_part(topic)}"

    @classmethod
    def queue(cls, queue: str, *, topic: str = "events") -> str:
        """Return the channel for queue-scoped events."""
        return f"{cls.prefix}:queue:{_normalize_part(queue)}:{_normalize_part(topic)}"

    @classmethod
    def worker(cls, worker_id: str, *, topic: str = "events") -> str:
        """Return the channel for worker-scoped events."""
        return f"{cls.prefix}:worker:{_normalize_part(worker_id)}:{_normalize_part(topic)}"

    @classmethod
    def global_channel(cls, *, topic: str = "events") -> str:
        """Return the global queue event channel."""
        return f"{cls.prefix}:global:{_normalize_part(topic)}"

    @classmethod
    def custom(cls, scope_key: str, *, topic: str = "events") -> str:
        """Return a custom queue event channel."""
        return f"{cls.prefix}:custom:{_normalize_part(scope_key, allow_colon=True)}:{_normalize_part(topic)}"


def _normalize_part(value: str, *, allow_colon: bool = False) -> str:
    normalized = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").strip().lower()
    pattern = _INVALID_CHARS if allow_colon else _INVALID_CHARS_NO_COLON
    normalized = pattern.sub("_", normalized)
    normalized = _REPEATED_UNDERSCORES.sub("_", normalized).strip("_")
    return normalized or "unknown"
