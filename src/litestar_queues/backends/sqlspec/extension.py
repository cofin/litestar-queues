"""SQLSpec queue extension configuration."""

from typing import TYPE_CHECKING, Any

from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, migration_directory, validate_table_name

if TYPE_CHECKING:
    from pathlib import Path

    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig

__all__ = ("QUEUE_EXTENSION_NAME", "configure_queue_migration_extension", "queue_migration_directory")

QUEUE_EXTENSION_NAME = "litestar_queues"


def queue_migration_directory() -> "Path":
    """Return the queue extension migration directory."""
    return migration_directory()


def configure_queue_migration_extension(
    sqlspec_config: "SQLSpecConfig", *, table_name: "str" = DEFAULT_TABLE_NAME
) -> "None":
    """Register the packaged queue migration with SQLSpec's extension runner."""
    queue_settings = _configure_extension_settings(sqlspec_config, table_name=table_name)
    commands = sqlspec_config.get_migration_commands()
    commands.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    runner = commands.runner
    runner.extension_migrations[QUEUE_EXTENSION_NAME] = queue_migration_directory()
    runner.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    if runner.context is not None:
        runner.context.extension_config = commands.extension_configs


def _configure_extension_settings(sqlspec_config: "SQLSpecConfig", *, table_name: "str") -> "dict[str, Any]":
    extension_config = dict(sqlspec_config.extension_config or {})
    queue_settings = dict(extension_config.get(QUEUE_EXTENSION_NAME, {}) or {})
    queue_settings["table_name"] = validate_table_name(table_name)
    return queue_settings
