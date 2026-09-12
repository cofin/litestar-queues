"""SQLSpec queue extension configuration."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    event_history_table_name_for,
    maintenance_table_name_for,
    migration_directory,
    resolve_column_map,
    task_reservation_table_name_for,
    validate_table_name,
)
from litestar_queues.events import validate_event_history_extra_columns

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig
    from litestar_queues.events import EventHistoryExtraColumn

__all__ = (
    "QUEUE_EXTENSION_NAME",
    "configure_events_migration_extension",
    "configure_queue_migration_extension",
    "queue_migration_directory",
)

QUEUE_EXTENSION_NAME = "litestar_queues"
_EVENTS_EXTENSION_NAME = "events"


def configure_events_migration_extension(
    sqlspec_config: "SQLSpecConfig",
    *,
    backend: "str | None",
    queue_table: "str | None" = None,
    manage_schema: "bool" = True,
) -> "None":
    """Register SQLSpec's events queue migration for native wakeup provisioning.

    Writing the events extension settings makes SQLSpec auto-include its bundled
    events queue migration on migrate-up, so a capability-native backend gets its
    durable events queue table with no manual step. Existing events settings are
    preserved; only unset keys are filled in.

    The events queue table is package-owned, so ``manage_schema=False`` leaves
    nothing registered: any events settings and packaged revision already on the
    config are removed, so an application that owns its schema receives no
    packaged revision whatever ran before. That holds whether or not a durable
    events table would otherwise be provisioned.

    A ``backend`` of ``None`` means no durable events table is needed and leaves
    the config untouched. The events extension is SQLSpec's own rather than this
    package's, so an application may have registered it for its own use, and a
    transport that needs no events table is not grounds for removing it.
    """
    if not manage_schema:
        _deregister_extension(sqlspec_config, _EVENTS_EXTENSION_NAME)
        return
    if backend is None:
        return
    extension_config = dict(sqlspec_config.extension_config or {})
    events_settings = dict(extension_config.get(_EVENTS_EXTENSION_NAME, {}) or {})
    events_settings.setdefault("backend", backend)
    if queue_table is not None:
        events_settings.setdefault("queue_table", queue_table)
    extension_config[_EVENTS_EXTENSION_NAME] = events_settings
    sqlspec_config.extension_config = extension_config
    sqlspec_config.set_migration_config(dict(sqlspec_config.migration_config or {}))


def queue_migration_directory() -> "Path":
    """Return the queue extension migration directory."""
    return migration_directory()


def configure_queue_migration_extension(
    sqlspec_config: "SQLSpecConfig",
    *,
    queue_table_name: "str" = DEFAULT_TABLE_NAME,
    event_history_enabled: "bool" = False,
    event_history_table_name: "str | None" = None,
    event_history_extra_columns: "Sequence[EventHistoryExtraColumn]" = (),
    maintenance_table_name: "str | None" = None,
    task_reservation_table_name: "str | None" = None,
    column_map: "Mapping[str, str] | None" = None,
    manage_schema: "bool" = True,
) -> "None":
    """Register the packaged queue migrations with SQLSpec's extension runner.

    ``manage_schema=False`` declares that the application owns the queue schema,
    so nothing stays registered: no extension settings, no migration directory,
    and therefore no packaged revision for SQLSpec to discover or apply. A
    registration written by an earlier call on the same config is removed, so the
    postcondition holds regardless of call order.
    """
    if not manage_schema:
        _deregister_extension(sqlspec_config, QUEUE_EXTENSION_NAME)
        return
    queue_settings = _configure_extension_settings(
        sqlspec_config,
        queue_table_name=queue_table_name,
        event_history_enabled=event_history_enabled,
        event_history_table_name=event_history_table_name,
        event_history_extra_columns=event_history_extra_columns,
        maintenance_table_name=maintenance_table_name,
        task_reservation_table_name=task_reservation_table_name,
        column_map=column_map,
    )
    commands = sqlspec_config.get_migration_commands()
    commands.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    runner = commands.runner
    runner.extension_migrations[QUEUE_EXTENSION_NAME] = queue_migration_directory()
    runner.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    if runner.context is not None:
        runner.context.extension_config = commands.extension_configs


def _deregister_extension(sqlspec_config: "SQLSpecConfig", extension_name: "str") -> "None":
    """Remove every trace of an extension registration from a SQLSpec config.

    ``SQLSpecConfig`` caches its ``MigrationCommands``, so settings written by an
    earlier ``manage_schema=True`` call outlive a later skip and keep the packaged
    revision discoverable. Removal covers the cached commands, the migration
    runner, the config's extension settings, and SQLSpec's auto-included extension
    list, and does nothing when the extension was never registered.
    """
    extension_config = sqlspec_config.extension_config or {}
    if extension_name in extension_config:
        remaining = dict(extension_config)
        del remaining[extension_name]
        sqlspec_config.extension_config = remaining
    migration_config = sqlspec_config.migration_config or {}
    included = migration_config.get("include_extensions")
    if included is not None and extension_name in included:
        migration_config["include_extensions"] = [name for name in included if name != extension_name]
    commands = sqlspec_config.get_migration_commands()
    commands.extension_configs.pop(extension_name, None)
    runner = commands.runner
    runner.extension_configs.pop(extension_name, None)
    runner.extension_migrations.pop(extension_name, None)
    if runner.context is not None:
        runner.context.extension_config = commands.extension_configs


def _configure_extension_settings(
    sqlspec_config: "SQLSpecConfig",
    *,
    queue_table_name: "str",
    event_history_enabled: "bool" = False,
    event_history_table_name: "str | None" = None,
    event_history_extra_columns: "Sequence[EventHistoryExtraColumn]" = (),
    maintenance_table_name: "str | None" = None,
    task_reservation_table_name: "str | None" = None,
    column_map: "Mapping[str, str] | None" = None,
) -> "dict[str, Any]":
    extension_config = dict(sqlspec_config.extension_config or {})
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_settings.pop("table_name", None)
    queue_settings["queue_table_name"] = validate_table_name(queue_table_name)
    if event_history_enabled:
        queue_settings["event_history_enabled"] = True
        queue_settings["event_history_table_name"] = validate_table_name(
            event_history_table_name or event_history_table_name_for(queue_table_name)
        )
        queue_settings["event_history_extra_columns"] = tuple(
            {"name": column.name, "source": column.source, "indexed": column.indexed}
            for column in validate_event_history_extra_columns(event_history_extra_columns)
        )
    queue_settings["maintenance_table_name"] = validate_table_name(
        maintenance_table_name or maintenance_table_name_for(queue_table_name)
    )
    queue_settings["task_reservation_table_name"] = validate_table_name(
        task_reservation_table_name or task_reservation_table_name_for(queue_table_name)
    )
    if column_map is not None:
        queue_settings["column_map"] = resolve_column_map(column_map)
    extension_config[QUEUE_EXTENSION_NAME] = queue_settings
    sqlspec_config.extension_config = extension_config
    return queue_settings
