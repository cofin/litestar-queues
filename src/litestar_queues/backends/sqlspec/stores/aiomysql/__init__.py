"""aiomysql SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.aiomysql.store import AiomysqlQueueStore

__all__ = ("AiomysqlQueueStore",)
