"""duckdb SQLSpec queue store."""

from typing import TYPE_CHECKING, Any, ClassVar

from litestar_queues.backends.sqlspec.stores.base import BulkHeartbeatStatement, SQLSpecQueueStore

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from litestar_queues.backends.sqlspec._typing import DatetimeParam

__all__ = ("DuckDBQueueStore",)


class DuckDBQueueStore(SQLSpecQueueStore):
    """duckdb-specific SQLSpec queue statement store."""

    __slots__ = ()

    data_dictionary_dialect: "ClassVar[str | None]" = "duckdb"
    bind_datetime_as_naive_utc: "ClassVar[bool]" = True
    supports_bulk_touch_heartbeats: "ClassVar[bool]" = True

    def bulk_touch_heartbeats(
        self, *, touches: "Sequence[Mapping[str, Any]]", heartbeat_at: "DatetimeParam"
    ) -> "BulkHeartbeatStatement | None":
        """Return one positional fenced bulk heartbeat UPDATE for DuckDB."""
        if not touches:
            return None

        parameters: "list[Any]" = []
        value_rows: "list[str]" = []
        for touch in touches:
            parameters.extend((touch["task_id"], touch["expected_retry_count"], touch["metadata_json"]))
            value_rows.append(
                f"(CAST(? AS {self._id_type()}), "
                f"CAST(? AS {self._integer_type()}), "
                f"CAST(? AS {self._metadata_json_type('metadata_json')}))"
            )

        parameters.append(heartbeat_at)
        target = "target"
        source = "heartbeat_updates"
        id_col = self._quoted_col("id")
        status_col = self._quoted_col("status")
        retry_count_col = self._quoted_col("retry_count")
        heartbeat_col = self._quoted_col("heartbeat_at")
        metadata_col = self._quoted_col("metadata_json")
        values_sql = ", ".join(value_rows)
        statement = f"""
WITH {source}(task_id, expected_retry_count, metadata_json) AS (
    VALUES {values_sql}
)
UPDATE {self._quoted_table_name()} AS {target}
SET {heartbeat_col} = CAST(? AS {self._timestamp_type()}),
    {metadata_col} = CASE
        WHEN {source}.metadata_json IS NULL THEN {target}.{metadata_col}
        ELSE COALESCE((
            SELECT json_group_object(merged.key, merged.value)
            FROM (
                SELECT existing.key, existing.value
                FROM json_each({target}.{metadata_col}) AS existing
                WHERE existing.key NOT IN (
                    SELECT patch_keys.key
                    FROM json_each({source}.metadata_json) AS patch_keys
                )
                UNION ALL
                SELECT patch.key, patch.value
                FROM json_each({source}.metadata_json) AS patch
            ) AS merged
        ), CAST('{{}}' AS {self._metadata_json_type("metadata_json")}))
    END
FROM {source}
WHERE {target}.{id_col} = {source}.task_id
  AND {target}.{status_col} = 'running'
  AND ({source}.expected_retry_count IS NULL OR {target}.{retry_count_col} = {source}.expected_retry_count)
RETURNING {target}.{id_col} AS id
""".strip()  # noqa: S608
        return BulkHeartbeatStatement(sql=statement, parameters=parameters)
