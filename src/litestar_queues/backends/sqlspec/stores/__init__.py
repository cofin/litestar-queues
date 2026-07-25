"""SQLSpec queue store selection.

Every adapter defines its store in its own module, named after the adapter
(``asyncpg.py``, ``oracledb.py``, ...). Import a concrete store from there.
This package exports only the base class and the factory, so selecting one
adapter never imports another adapter's driver.
"""

from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store

__all__ = ("SQLSpecQueueStore", "create_queue_store")
