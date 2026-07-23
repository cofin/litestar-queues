"""Small shared helpers for the copyable realtime examples."""

import os

from litestar_queues import WorkerConfig

__all__ = ("example_worker_config",)


def example_worker_config() -> WorkerConfig:
    """Return worker configuration for an in-app or standalone example worker."""
    return WorkerConfig(
        run_in_app=os.getenv("LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER") != "0", graceful_shutdown_timeout=5
    )
