"""Shared fixtures for the Cloud Tasks unit tests."""

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

SHARED_STORAGE = "selftest-shared-storage"

_BASE: "dict[str, Any]" = {
    "project_id": "example-project",
    "location": "us-central1",
    "queue_id": "queue-consumer",
    "service_url": "https://queue-consumer-abcdef-uc.a.run.app",
    "service_account_email": "queues@example-project.iam.gserviceaccount.com",
    "trust_platform_auth": True,
}


@pytest.fixture
def shared_storage() -> "Iterator[str]":
    """Register an in-process backend whose name is not process-local.

    Cloud Tasks rejects ``memory`` and ``ephemeral``, so these tests need a
    selector it treats as shared without standing up a real server.

    Yields:
        The registered queue backend name.
    """
    from litestar_queues.backends.factory import _queue_backend_registry
    from litestar_queues.backends.memory import InMemoryQueueBackend

    _queue_backend_registry[SHARED_STORAGE] = type("SharedStandInBackend", (InMemoryQueueBackend,), {})
    yield SHARED_STORAGE
    _queue_backend_registry.pop(SHARED_STORAGE, None)


@pytest.fixture
def cloud_tasks_config() -> "Callable[..., CloudTasksExecutionConfig]":
    """Return a builder for a valid Cloud Tasks config.

    Returns:
        A callable applying keyword overrides to the valid baseline.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

    def build(**overrides: "Any") -> "CloudTasksExecutionConfig":
        return CloudTasksExecutionConfig(**{**_BASE, **overrides})

    return build
