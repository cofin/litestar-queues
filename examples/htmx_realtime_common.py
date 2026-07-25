"""Small shared helpers for the copyable realtime examples."""

import os

from litestar_queues import WorkerConfig

__all__ = ("example_worker_config",)


def example_worker_config() -> WorkerConfig:
    """Return worker configuration for an in-process or standalone example worker.

    The examples run a worker inside each ASGI process so a single
    ``uvicorn``/``granian`` command is enough to see live events. Set
    ``LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER=0`` to run ``litestar queues run``
    as a separate process instead.
    """
    in_process = os.getenv("LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER") != "0"
    return WorkerConfig(placement="asgi" if in_process else "external", graceful_shutdown_timeout=5)
