"""psqlpy SQLSpec queue store."""

from typing import Any, ClassVar

from litestar_queues.backends.sqlspec.stores._families import PostgresQueueStore

__all__ = ("PsqlpyQueueStore",)


class PsqlpyQueueStore(PostgresQueueStore):
    """psqlpy-specific SQLSpec queue statement store."""

    __slots__ = ()

    auto_native_json_columns: "ClassVar[frozenset[str]]" = frozenset()
    table_storage_parameters: "ClassVar[bool]" = True

    def __init__(
        self, config: "Any", *, native_json_columns: "frozenset[str] | None" = None, **kwargs: "Any"
    ) -> "None":
        super().__init__(
            config,
            native_json_columns=native_json_columns or frozenset({"args_json", "kwargs_json", "metadata_json"}),
            **kwargs,
        )

    def _result_json_type(self, column_name: "str") -> "str":
        del column_name
        return self._text_type()

    def serialize_json(self, canonical: "str", value: "Any") -> "Any":
        """Serialize psqlpy PyJSON values using Python containers.

        Returns:
            A Python container for native PyJSON columns or JSON text otherwise.
        """
        if canonical in self._native_json_columns:
            if isinstance(value, tuple):
                return list(value)
            if isinstance(value, (dict, list)):
                return value
            return self._serialize_json(value)
        return self._serialize_json(value)
