"""spanner SQLSpec queue store."""

from litestar_queues.backends.sqlspec.stores.spanner.store import SpannerQueueStore

__all__ = ("SpannerQueueStore",)
