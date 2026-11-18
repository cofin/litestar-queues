"""asyncpg SQLSpec queue store."""

from collections.abc import Mapping
from typing import Any, ClassVar

from sqlspec.utils.serializers import from_json

from litestar_queues.backends.sqlspec.stores._dialects import PostgresQueueStore

__all__ = ("AsyncpgQueueStore",)


class AsyncpgQueueStore(PostgresQueueStore):
    """asyncpg-specific SQLSpec queue statement store."""

    __slots__ = ()

    table_storage_parameters: "ClassVar[bool]" = True

    def serialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Serialize JSON column values as UTF-8 bytes for SQLSpec's asyncpg JSONB codec.

        SQLSpec's asyncpg JSON/JSONB codecs JSON-encode ``str`` inputs while passing
        ``bytes`` payloads through verbatim as UTF-8 JSON. Returning UTF-8 JSON bytes
        prevents double-encoding of arrays and scalar strings while encoding ``None``
        as ``b"null"`` rather than SQL ``NULL`` on ``NOT NULL`` JSONB columns.

        Returns:
            UTF-8 JSON ``bytes`` when asyncpg JSON codecs are active on a native JSON
            column, or a JSON ``str`` otherwise.
        """
        normalized = list(value) if isinstance(value, tuple) else value
        serialized = self._serialize_json(normalized)
        if canonical in self._native_json_columns and self._json_codecs_enabled():
            return serialized.encode("utf-8")
        return serialized

    def deserialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Deserialize JSON column values returned by asyncpg.

        Returns:
            The decoded Python JSON value.
        """
        if value is None:
            return None
        if canonical in self._native_json_columns and not self._json_codecs_enabled():
            if isinstance(value, (bytearray, memoryview)):
                return from_json(bytes(value))
            if isinstance(value, (str, bytes)):
                return from_json(value)
        return super().deserialize_json(canonical, value)

    def _json_codecs_enabled(self) -> "bool":
        features = getattr(self._config, "driver_features", None)
        if isinstance(features, Mapping):
            return bool(features.get("enable_json_codecs", True))
        return True
