__all__ = (
    "JobCancelledError",
    "MissingDependencyError",
    "NonRetryableError",
    "QueueConfigurationError",
    "QueueError",
    "job_cancelled",
    "non_retryable",
)


class QueueError(Exception):
    """Base exception for litestar-queues errors."""


class QueueConfigurationError(QueueError):
    """Raised when queue backend configuration is invalid."""


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
