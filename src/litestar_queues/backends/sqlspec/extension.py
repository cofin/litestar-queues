"""SQLSpec queue extension configuration."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    event_history_table_name_for,
    maintenance_table_name_for,
    migration_directory,
    task_reservation_table_name_for,
    validate_table_name,
)

if TYPE_CHECKING:
    from pathlib import Path

    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig

__all__ = (
    "QUEUE_EXTENSION_NAME",
    "configure_events_migration_extension",
    "configure_queue_migration_extension",
    "queue_migration_directory",
)

QUEUE_EXTENSION_NAME = "litestar_queues"
_EVENTS_EXTENSION_NAME = "events"


def configure_events_migration_extension(
    sqlspec_config: "SQLSpecConfig", *, backend: "str", queue_table: "str | None" = None
) -> "None":
    """Register SQLSpec's events queue migration for native wakeup provisioning.

    Writing the events extension settings makes SQLSpec auto-include its bundled
    events queue migration on migrate-up, so a capability-native backend gets its
    durable events queue table with no manual step. Existing events settings are
    preserved; only unset keys are filled in.
    """
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
    maintenance_table_name: "str | None" = None,
    task_reservation_table_name: "str | None" = None,
) -> "None":
    """Register the packaged queue migrations with SQLSpec's extension runner."""
    queue_settings = _configure_extension_settings(
        sqlspec_config,
        queue_table_name=queue_table_name,
        event_history_enabled=event_history_enabled,
        event_history_table_name=event_history_table_name,
        maintenance_table_name=maintenance_table_name,
        task_reservation_table_name=task_reservation_table_name,
    )
    commands = sqlspec_config.get_migration_commands()
    commands.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    runner = commands.runner
    runner.extension_migrations[QUEUE_EXTENSION_NAME] = queue_migration_directory()
    runner.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    if runner.context is not None:
        runner.context.extension_config = commands.extension_configs


def _configure_extension_settings(
    sqlspec_config: "SQLSpecConfig",
    *,
    queue_table_name: "str",
    event_history_enabled: "bool" = False,
    event_history_table_name: "str | None" = None,
    maintenance_table_name: "str | None" = None,
    task_reservation_table_name: "str | None" = None,
) -> "dict[str, Any]":
    extension_config = dict(sqlspec_config.extension_config or {})
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_settings["table_name"] = validate_table_name(queue_table_name)
    if event_history_enabled:
        queue_settings["event_history_enabled"] = True
        queue_settings["event_history_table_name"] = validate_table_name(
            event_history_table_name or event_history_table_name_for(queue_table_name)
        )
    queue_settings["maintenance_table_name"] = validate_table_name(
        maintenance_table_name or maintenance_table_name_for(queue_table_name)
    )
    queue_settings["task_reservation_table_name"] = validate_table_name(
        task_reservation_table_name or task_reservation_table_name_for(queue_table_name)
    )
    return queue_settings
