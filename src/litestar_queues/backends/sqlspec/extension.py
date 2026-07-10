"""SQLSpec queue extension configuration."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.sqlspec.schema import (
    DEFAULT_TABLE_NAME,
    event_log_table_name_for,
    migration_directory,
    validate_table_name,
)

if TYPE_CHECKING:
    from pathlib import Path

    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig

__all__ = ("QUEUE_EXTENSION_NAME", "configure_queue_migration_extension", "queue_migration_directory")

QUEUE_EXTENSION_NAME = "litestar_queues"


def queue_migration_directory() -> "Path":
    """Return the queue extension migration directory."""
    return migration_directory()


def configure_queue_migration_extension(
    sqlspec_config: "SQLSpecConfig",
    *,
    queue_table_name: "str" = DEFAULT_TABLE_NAME,
    event_log_enabled: "bool" = False,
    event_log_table_name: "str | None" = None,
) -> "None":
    """Register the packaged queue migration with SQLSpec's extension runner."""
    queue_settings = _configure_extension_settings(
        sqlspec_config,
        queue_table_name=queue_table_name,
        event_log_enabled=event_log_enabled,
        event_log_table_name=event_log_table_name,
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
    event_log_enabled: "bool" = False,
    event_log_table_name: "str | None" = None,
) -> "dict[str, Any]":
    extension_config = dict(sqlspec_config.extension_config or {})
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_settings["table_name"] = validate_table_name(queue_table_name)
    if event_log_enabled:
        queue_settings["event_log_enabled"] = True
        queue_settings["event_log_table_name"] = validate_table_name(
            event_log_table_name or event_log_table_name_for(queue_table_name)
        )
    return queue_settings
