"""Queue backend public re-exports.

Optional backends (``advanced_alchemy``, ``sqlspec``, ``redis``, ``valkey``) are
NOT re-exported here. Users must import them explicitly from their respective
submodules, e.g. ``from litestar_queues.backends.sqlspec import SQLSpecQueueBackend``.
This keeps the top-level package importable without the optional driver extras
installed.
"""

from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.factory import (
    get_queue_backend,
    get_queue_backend_class,
    list_queue_backends,
    queue_backend,
)
from litestar_queues.backends.memory import InMemoryQueueBackend

__all__ = (
    "BaseQueueBackend",
    "InMemoryQueueBackend",
    "get_queue_backend",
    "get_queue_backend_class",
    "list_queue_backends",
    "queue_backend",
)
