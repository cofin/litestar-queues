"""Small shared helpers for the copyable realtime examples."""

import os

from litestar_queues import WorkerConfig

__all__ = ("example_worker_config",)


def example_worker_config() -> WorkerConfig:
    """Return worker configuration for the selected example placement.

    The examples default to an ASGI-owned worker so their process-local
    Channels backend can display live events. Set
    ``LITESTAR_QUEUES_EXAMPLE_PLACEMENT`` to ``server``, ``asgi``, or
    ``external`` when exercising another topology.
    """
    placement = os.getenv("LITESTAR_QUEUES_EXAMPLE_PLACEMENT", "asgi")
    if placement not in {"server", "asgi", "external"}:
        msg = "LITESTAR_QUEUES_EXAMPLE_PLACEMENT must be server, asgi, or external"
        raise ValueError(msg)
    return WorkerConfig(placement=placement, graceful_shutdown_timeout=5)
