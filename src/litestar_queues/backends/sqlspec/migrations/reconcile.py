"""Schema reconciliation and teardown entrypoint for Litestar Queues SQLSpec tables."""

from collections.abc import Mapping
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_async, ensure_schema_sync, metadata_has_index
from sqlspec.migrations.utils import resolve_default_schema, validate_builtin_reconcile_adapter

from litestar_queues.backends.sqlspec.event_log import create_event_log_store
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.maintenance import create_maintenance_store
from litestar_queues.backends.sqlspec.reservation import create_task_reservation_store
from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, validate_table_name
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.factory import create_queue_store
from litestar_queues.events import EventHistoryExtraColumn
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from sqlspec.migrations.context import MigrationContext

__all__ = (
    "down",
    "downgrade",
    "reconcile",
    "reconcile_events_queue_async",
    "reconcile_events_queue_sync",
    "reconcile_stores_async",
    "reconcile_stores_sync",
)

_UNSUPPORTED_RECONCILE_ADAPTERS = frozenset({"spanner", "db2"})
_COCKROACH_ADAPTERS = frozenset({"cockroach_asyncpg", "cockroach_psycopg"})


def reconcile_stores_sync(
    driver: "Any", stores: "Sequence[SQLSpecQueueStore]", *, config: "Any" = None, validate_adapter: "bool" = False
) -> "None":
    """Reconcile enabled queue stores sequentially on a synchronous SQLSpec driver.

    Why:
        Sharing this loop between ``reconcile(context)`` and backend schema
        initialization keeps driver-bound capability preparation, empty-DDL
        skipping for unmanaged stores, strict additive ``ensure_schema_sync``,
        catalog-checked companion index repair, and store storage-option
        delegation consistent in one place without committing runner transactions.
    """
    if validate_adapter:
        _validate_reconcile_adapter(config)
    for store in stores:
        store.prepare_schema_sync(driver)
        ddl = store.create_table_statement()
        if ddl.strip() == "":
            continue
        target = SchemaTarget.from_ddl(store.table_name, ddl, dialect=store.sqlglot_dialect())
        ensure_schema_sync(driver, (target,), manage_schema=True, create_schema=True, commit=False, strict=True)
        _reconcile_companion_indexes_sync(driver, config, store, target)
        reconcile_options = getattr(store, "reconcile_storage_options_sync", None)
        if callable(reconcile_options):
            reconcile_options(driver)


async def reconcile_stores_async(
    driver: "Any", stores: "Sequence[SQLSpecQueueStore]", *, config: "Any" = None, validate_adapter: "bool" = False
) -> "None":
    """Reconcile enabled queue stores sequentially on an asynchronous SQLSpec driver.

    Why:
        Mirrors ``reconcile_stores_sync`` for coroutine drivers so async dialect
        capability checks, companion index repair, and store storage-option
        convergence run on the active runner driver without committing.
    """
    if validate_adapter:
        _validate_reconcile_adapter(config)
    for store in stores:
        await store.prepare_schema_async(driver)
        ddl = store.create_table_statement()
        if ddl.strip() == "":
            continue
        target = SchemaTarget.from_ddl(store.table_name, ddl, dialect=store.sqlglot_dialect())
        await ensure_schema_async(driver, (target,), manage_schema=True, create_schema=True, commit=False, strict=True)
        await _reconcile_companion_indexes_async(driver, config, store, target)
        await _invoke_optional_async(store, "reconcile_storage_options_async", driver)


def reconcile_events_queue_sync(driver: "Any", events_store: "Any", backend_name: "str | None") -> "None":
    """Reconcile SQLSpec's durable events queue table on a synchronous driver."""
    manage_schema, create_schema = _resolve_events_schema_flags(events_store)
    if not manage_schema:
        return
    get_schema_targets = getattr(events_store, "get_schema_targets", None)
    if callable(get_schema_targets):
        _invoke_optional_sync(events_store, "prepare_schema_sync", driver)
        targets = cast("Sequence[SchemaTarget]", get_schema_targets())
        ensure_schema_sync(driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True)
        _invoke_optional_sync(events_store, "reconcile_companion_objects_sync", driver)
        native_targets_fn = getattr(events_store, "native_backend_targets", None)
        if backend_name is not None and callable(native_targets_fn):
            native_targets = native_targets_fn(backend_name)
            _invoke_optional_sync(
                events_store,
                "reconcile_native_backend_sync",
                driver,
                backend_name,
                native_targets,
                create_schema=create_schema,
            )
        return
    if create_schema:
        for statement in events_store.create_statements():
            if statement.strip():
                driver.execute_script(statement)


async def reconcile_events_queue_async(driver: "Any", events_store: "Any", backend_name: "str | None") -> "None":
    """Reconcile SQLSpec's durable events queue table on an asynchronous driver."""
    manage_schema, create_schema = _resolve_events_schema_flags(events_store)
    if not manage_schema:
        return
    get_schema_targets = getattr(events_store, "get_schema_targets", None)
    if callable(get_schema_targets):
        await _invoke_optional_async(events_store, "prepare_schema_async", driver)
        targets = cast("Sequence[SchemaTarget]", get_schema_targets())
        await ensure_schema_async(
            driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True
        )
        await _invoke_optional_async(events_store, "reconcile_companion_objects_async", driver)
        native_targets_fn = getattr(events_store, "native_backend_targets", None)
        if backend_name is not None and callable(native_targets_fn):
            native_targets = native_targets_fn(backend_name)
            await _invoke_optional_async(
                events_store,
                "reconcile_native_backend_async",
                driver,
                backend_name,
                native_targets,
                create_schema=create_schema,
            )
        return
    if create_schema:
        for statement in events_store.create_statements():
            if statement.strip():
                await driver.execute_script(statement)


def reconcile(context: "MigrationContext") -> "Awaitable[None] | None":
    """Reconcile all enabled queue tables on ``context.driver`` without committing."""
    config, driver, validated = _validate_context(context)
    _validate_reconcile_adapter(config)
    stores = _resolve_stores(validated, config)
    if _is_async_driver(validated):
        return reconcile_stores_async(driver, stores, config=config)
    reconcile_stores_sync(driver, stores, config=config)
    return None


def downgrade(context: "MigrationContext") -> "Awaitable[None] | None":
    """Drop all enabled queue tables in reverse order on ``context.driver``."""
    config, driver, validated = _validate_context(context)
    _validate_reconcile_adapter(config)
    stores = _resolve_stores(validated, config)
    if _is_async_driver(validated):
        return _downgrade_async(driver, stores)
    _downgrade_sync(driver, stores)
    return None


down = downgrade


def _validate_context(context: "MigrationContext | None") -> "tuple[Any, Any, MigrationContext]":
    if context is None:
        msg = "Queue schema reconciliation requires a MigrationContext with SQLSpec adapter configuration."
        raise MigrationError(msg)
    config = getattr(context, "config", None)
    if config is None:
        msg = "Queue schema reconciliation requires SQLSpec adapter configuration on MigrationContext."
        raise MigrationError(msg)
    driver = getattr(context, "driver", None)
    if driver is None:
        msg = "Queue schema reconciliation requires an active database driver on MigrationContext."
        raise MigrationError(msg)
    return config, driver, context


def _explicit_queue_store_class(config: "Any") -> "type[SQLSpecQueueStore] | None":
    if config is None:
        return None
    explicit_store = getattr(config, "queue_store_class", None)
    if not isinstance(explicit_store, type):
        explicit_store = getattr(type(config), "queue_store_class", None)
    if isinstance(explicit_store, type) and issubclass(explicit_store, SQLSpecQueueStore):
        return explicit_store
    return None


def _validate_reconcile_adapter(config: "Any") -> "None":
    if _explicit_queue_store_class(config) is not None:
        return
    adapter = _adapter_name(config)
    if adapter in _UNSUPPORTED_RECONCILE_ADAPTERS:
        msg = f"SQLSpec schema reconciliation is not supported for adapter {adapter!r}."
        raise MigrationError(msg)
    try:
        validate_builtin_reconcile_adapter(config)
    except MigrationError:
        raise
    except (QueueConfigurationError, SQLSpecError, ValueError, TypeError) as exc:
        raise MigrationError(str(exc)) from exc


def _resolve_stores(context: "MigrationContext", config: "Any") -> "list[SQLSpecQueueStore]":
    try:
        config_ext = getattr(config, "extension_config", None) or {}
        queue_settings: dict[str, Any] = {
            **dict(config_ext.get(QUEUE_EXTENSION_NAME, {}) or {}),
            **dict(getattr(context, "extension_config", None) or {}),
        }
        raw_queue_table = queue_settings.get("queue_table_name", DEFAULT_TABLE_NAME)
        queue_table_name = validate_table_name(str(DEFAULT_TABLE_NAME if raw_queue_table is None else raw_queue_table))
        stores: list[SQLSpecQueueStore] = [
            create_queue_store(
                cast("Any", config),
                table_name=queue_table_name,
                column_map=queue_settings.get("column_map"),
                manage_schema=True,
                terminal_ttl=queue_settings.get("terminal_ttl"),
            )
        ]
        if bool(queue_settings.get("event_history_enabled")):
            configured_event_table = queue_settings.get("event_history_table_name")
            event_history_table_name = str(configured_event_table) if configured_event_table is not None else None
            extra_columns = _build_extra_columns(queue_settings.get("event_history_extra_columns"))
            stores.append(
                create_event_log_store(
                    cast("Any", config),
                    queue_table_name=queue_table_name,
                    event_history_table_name=event_history_table_name,
                    manage_schema=True,
                    extra_columns=extra_columns,
                )
            )
        configured_maintenance = queue_settings.get("maintenance_table_name")
        maintenance_table_name = str(configured_maintenance) if configured_maintenance is not None else None
        stores.append(
            create_maintenance_store(
                cast("Any", config),
                queue_table_name=queue_table_name,
                maintenance_table_name=maintenance_table_name,
                manage_schema=True,
            )
        )
        configured_reservation = queue_settings.get("task_reservation_table_name")
        task_reservation_table_name = str(configured_reservation) if configured_reservation is not None else None
        stores.append(
            create_task_reservation_store(
                cast("Any", config),
                queue_table_name=queue_table_name,
                task_reservation_table_name=task_reservation_table_name,
                manage_schema=True,
            )
        )
    except MigrationError:
        raise
    except (QueueConfigurationError, SQLSpecError, KeyError, ValueError, TypeError) as exc:
        raise MigrationError(str(exc)) from exc
    return stores


def _build_extra_columns(declared: "Any") -> "tuple[EventHistoryExtraColumn, ...]":
    if not declared:
        return ()
    columns: list[EventHistoryExtraColumn] = []
    for entry in declared:
        if isinstance(entry, EventHistoryExtraColumn):
            columns.append(entry)
        elif isinstance(entry, Mapping):
            columns.append(
                EventHistoryExtraColumn(
                    name=str(entry["name"]), source=str(entry["source"]), indexed=bool(entry.get("indexed", False))
                )
            )
        else:
            msg = f"Invalid event_history_extra_columns entry: {entry!r}"
            raise MigrationError(msg)
    return tuple(columns)


def _is_async_driver(context: "MigrationContext") -> "bool":
    return bool(getattr(context, "is_async_driver", False))


def _is_cockroach_target(driver: "Any", config: "Any", store: "SQLSpecQueueStore") -> "bool":
    if getattr(store, "data_dictionary_dialect", None) == "cockroachdb":
        return True
    if getattr(getattr(driver, "data_dictionary", None), "dialect", None) == "cockroachdb":
        return True
    effective_config = config if config is not None else getattr(store, "_config", None)
    return effective_config is not None and _adapter_name(effective_config) in _COCKROACH_ADAPTERS


def _resolve_index_query_schema(
    driver: "Any", config: "Any", store: "SQLSpecQueueStore", target: "SchemaTarget"
) -> "str | None":
    if target.schema is not None:
        return target.schema
    if not _is_cockroach_target(driver, config, store):
        return None
    effective_config = config if config is not None else getattr(store, "_config", None)
    default_schema = resolve_default_schema(getattr(effective_config, "migration_config", None))
    if default_schema is not None:
        return default_schema
    resolve_schema = getattr(getattr(driver, "data_dictionary", None), "resolve_schema", None)
    if callable(resolve_schema):
        resolved = resolve_schema(None)
        if isinstance(resolved, str) and resolved:
            return resolved
    return "public"


def _missing_companion_indexes(
    companions: "Sequence[tuple[str, str]]", rows: "Sequence[Any]", target: "SchemaTarget"
) -> "list[tuple[str, str]]":
    return [
        (index_name, ddl)
        for index_name, ddl in companions
        if not metadata_has_index(rows, target.table_name, index_name, schema_name=target.schema)
    ]


def _raise_unresolved_indexes(
    missing: "Sequence[tuple[str, str]]", after_rows: "Sequence[Any]", target: "SchemaTarget"
) -> "None":
    unresolved = [
        index_name
        for index_name, _ddl in missing
        if not metadata_has_index(after_rows, target.table_name, index_name, schema_name=target.schema)
    ]
    if unresolved:
        joined = ", ".join(unresolved)
        msg = (
            f"Companion index reconciliation failed for table {target.table_name!r} "
            f"(schema={target.schema!r}): unresolved index(es) {joined}."
        )
        raise MigrationError(msg)


def _reconcile_companion_indexes_sync(
    driver: "Any", config: "Any", store: "SQLSpecQueueStore", target: "SchemaTarget"
) -> "None":
    companions = store.companion_indexes()
    if not companions:
        return
    query_schema = _resolve_index_query_schema(driver, config, store, target)
    rows = driver.data_dictionary.get_indexes(driver, table=target.table_name, schema=query_schema)
    missing = _missing_companion_indexes(companions, rows, target)
    if not missing:
        return
    for _index_name, ddl in missing:
        driver.execute_script(ddl)
    after_rows = driver.data_dictionary.get_indexes(driver, table=target.table_name, schema=query_schema)
    _raise_unresolved_indexes(missing, after_rows, target)


async def _reconcile_companion_indexes_async(
    driver: "Any", config: "Any", store: "SQLSpecQueueStore", target: "SchemaTarget"
) -> "None":
    companions = store.companion_indexes()
    if not companions:
        return
    query_schema = _resolve_index_query_schema(driver, config, store, target)
    rows = await driver.data_dictionary.get_indexes(driver, table=target.table_name, schema=query_schema)
    missing = _missing_companion_indexes(companions, rows, target)
    if not missing:
        return
    for _index_name, ddl in missing:
        await driver.execute_script(ddl)
    after_rows = await driver.data_dictionary.get_indexes(driver, table=target.table_name, schema=query_schema)
    _raise_unresolved_indexes(missing, after_rows, target)


def _resolve_events_schema_flags(events_store: "Any") -> "tuple[bool, bool]":
    flags_fn = getattr(events_store, "_schema_management_flags", None)
    if callable(flags_fn):
        manage_schema, create_schema = cast("tuple[bool, bool]", flags_fn())
        return bool(manage_schema), bool(create_schema)
    return True, True


def _invoke_optional_sync(target: "Any", method_name: "str", *args: "Any", **kwargs: "Any") -> "None":
    method = getattr(target, method_name, None)
    if callable(method):
        method(*args, **kwargs)


async def _invoke_optional_async(target: "Any", method_name: "str", *args: "Any", **kwargs: "Any") -> "None":
    method = getattr(target, method_name, None)
    if callable(method):
        result = method(*args, **kwargs)
        if isawaitable(result):
            await result


def _downgrade_sync(driver: "Any", stores: "Sequence[SQLSpecQueueStore]") -> "None":
    for store in reversed(stores):
        for statement in store.drop_statements():
            if statement.strip():
                driver.execute_script(statement)


async def _downgrade_async(driver: "Any", stores: "Sequence[SQLSpecQueueStore]") -> "None":
    for store in reversed(stores):
        for statement in store.drop_statements():
            if statement.strip():
                await driver.execute_script(statement)
