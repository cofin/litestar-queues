__all__ = (
    "JobCancelledError",
    "MissingDependencyError",
    "NonRetryableError",
    "QueueConfigurationError",
    "QueueError",
    "QueueEventBufferFull",
    "TaskIdentityError",
    "TaskPayloadTooLargeError",
    "job_cancelled",
    "non_retryable",
)


class QueueError(Exception):
    """Base exception for litestar-queues errors."""


class QueueConfigurationError(QueueError):
    """Raised when queue backend configuration is invalid."""


class TaskIdentityError(QueueError):
    """Raised when task uniqueness identity cannot be derived.

    Signals that ``unique_by="arguments"`` was requested for a call whose bound
    arguments cannot be represented by the package's canonical JSON identity
    contract (for example non-finite floats or non-JSON objects). Uniqueness
    identity never falls back to pickle or ``repr()``; the caller must supply an
    explicit key or pass identity-friendly arguments instead.
    """


class TaskPayloadTooLargeError(QueueError):
    """Raised when a canonical argument-identity payload exceeds the configured limit."""

    def __init__(self, *, actual_bytes: "int", max_bytes: "int") -> "None":
        """Initialize the payload-size error.

        Args:
            actual_bytes: Measured size of the canonical identity payload.
            max_bytes: Configured ``QueueConfig.max_task_payload_bytes`` limit.
        """
        self.actual_bytes = actual_bytes
        self.max_bytes = max_bytes
        super().__init__(
            f"Task argument-identity payload is {actual_bytes} bytes, which exceeds the configured "
            f"max_task_payload_bytes of {max_bytes} bytes. Externalize large payloads to object or "
            "database storage and pass a stable identifier, or raise max_task_payload_bytes."
        )


class QueueEventBufferFull(QueueError):  # noqa: N818
    """Raised when queue event buffering cannot accept another event."""


class NonRetryableError(QueueError):
    """Raised by a task to mark the current failure as permanent."""


class JobCancelledError(QueueError):
    """Raised by a task to cooperatively mark itself cancelled."""


def non_retryable(message: "str") -> "None":
    """Raise a non-retryable task failure.

    Raises:
        NonRetryableError: Always raised with the provided message.
    """
    raise NonRetryableError(message)


def job_cancelled(message: "str" = "Task cancelled") -> "None":
    """Raise a cooperative task cancellation.

    Raises:
        JobCancelledError: Always raised with the provided message.
    """
    raise JobCancelledError(message)


class MissingDependencyError(QueueError, ImportError):
    """Raised when a required optional dependency is not installed."""

    def __init__(self, package: "str", install_package: "str | None" = None) -> "None":
        """Initialize missing dependency error.

        Args:
            package: The missing import package.
            install_package: Optional package or extra to install.
        """
        install_name = install_package or package
        super().__init__(
            f"Package {package!r} is not installed but required. Install it with "
            f"'pip install litestar-queues[{install_name}]' or install {install_name!r} separately."
        )
