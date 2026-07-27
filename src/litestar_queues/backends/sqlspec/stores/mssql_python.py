"""mssql-python SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores._dialects import MssqlQueueStore

__all__ = ("MssqlPythonQueueStore",)


class MssqlPythonQueueStore(MssqlQueueStore):
    """mssql-python SQLSpec queue statement store."""

    __slots__ = ()

    select_stream_chunk_size = 100
