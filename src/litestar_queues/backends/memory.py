from typing import ClassVar

from litestar_queues.backends.base import BaseStorageBackend

__all__ = ("InMemoryStorageBackend",)


class InMemoryStorageBackend(BaseStorageBackend):
    """Memory storage backend placeholder for scaffold tests and examples."""

    __slots__ = ()

    records: ClassVar[list[object]] = []
    """Class-level storage reserved for the Chapter 2 memory backend."""

    @classmethod
    def clear(cls) -> None:
        """Clear stored records."""
        cls.records.clear()
