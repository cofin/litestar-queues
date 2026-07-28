"""Runtime namespace rendering for package-owned identifiers."""

import re
from dataclasses import dataclass

from litestar_queues.exceptions import QueueConfigurationError

__all__ = ("DEFAULT_NAMESPACE", "QueueNamespace")

DEFAULT_NAMESPACE = "litestar_queues"
_NAMESPACE_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")


@dataclass(frozen=True, slots=True)
class QueueNamespace:
    """Validated root for package-owned runtime identifiers."""

    root: str = DEFAULT_NAMESPACE

    def __post_init__(self) -> None:
        """Reject roots that cannot render consistently across target formats."""
        if not _NAMESPACE_PATTERN.fullmatch(self.root):
            msg = (
                "QueueConfig.namespace must start with a lowercase letter and contain only "
                "lowercase letters, digits, and single underscores between non-empty segments."
            )
            raise QueueConfigurationError(msg)

    def metric(self, *parts: str) -> str:
        """Render an OpenTelemetry or Prometheus identifier."""
        return self._join(".", self.root, *parts)

    def logger(self, *parts: str) -> str:
        """Render a runtime logger name."""
        return self._join(".", self.root, *parts)

    def channel(self, *parts: str) -> str:
        """Render a pub/sub or event channel."""
        return self._join(":", self.root, *parts)

    def key(self, *parts: str) -> str:
        """Render a storage key."""
        return self._join(":", self.root, *parts)

    def database_channel(self, *parts: str) -> str:
        """Render a database notification channel."""
        return self._join("_", self.root, *parts)

    def registration(self, *parts: str) -> str:
        """Render a Litestar state, dependency, or route registration."""
        root = "queue" if self.is_default else self.root
        return self._join("_", root, *parts)

    def environment(self, *parts: str) -> str:
        """Render an environment-variable name."""
        return self._join("_", self.root.upper(), *(part.upper() for part in parts))

    def resource(self, *parts: str) -> str:
        """Render a process, thread, or filesystem resource name."""
        root = self.root.replace("_", "-")
        return self._join("-", root, *(part.replace("_", "-") for part in parts))

    def coordination(self, *parts: str) -> str:
        """Render a distributed coordination name with legacy compatibility."""
        root = "queue" if self.is_default else self.root.replace("_", "-")
        return self._join("-", root, *(part.replace("_", "-") for part in parts))

    def package_task(self, *parts: str) -> str:
        """Render a package-owned built-in task name with legacy compatibility."""
        if self.is_default:
            return self._join(".", *parts)
        return self.metric(*parts)

    @property
    def is_default(self) -> bool:
        """Return whether this root is the compatibility namespace."""
        return self.root == DEFAULT_NAMESPACE

    @staticmethod
    def _join(separator: str, *parts: str) -> str:
        if not parts or any(not part for part in parts):
            msg = "Namespace identifier parts must be non-empty strings."
            raise QueueConfigurationError(msg)
        return separator.join(parts)
