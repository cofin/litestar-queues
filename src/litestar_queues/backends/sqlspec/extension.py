"""SQLSpec queue extension configuration."""

from pathlib import Path
from typing import Any, cast

from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME, migration_directory, validate_table_name

__all__ = ("QUEUE_EXTENSION_NAME", "configure_queue_migration_extension", "queue_migration_directory")

QUEUE_EXTENSION_NAME = "litestar_queues"


def queue_migration_directory() -> Path:
    """Return the queue extension migration directory."""
    return migration_directory()


def configure_queue_migration_extension(sqlspec_config: Any, *, table_name: str = DEFAULT_TABLE_NAME) -> None:
    """Register the packaged queue migration with SQLSpec's extension runner."""
    queue_settings = _configure_extension_settings(sqlspec_config, table_name=table_name)
    commands = sqlspec_config.get_migration_commands()
    commands.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    runner = commands.runner
    runner.extension_migrations[QUEUE_EXTENSION_NAME] = queue_migration_directory()
    runner.extension_configs[QUEUE_EXTENSION_NAME] = queue_settings

    if runner.context is not None:
        runner.context.extension_config = commands.extension_configs


def _configure_extension_settings(sqlspec_config: Any, *, table_name: str) -> dict[str, Any]:
    extension_config = dict(cast("dict[str, Any]", getattr(sqlspec_config, "extension_config", {}) or {}))
    queue_settings = dict(cast("dict[str, Any]", extension_config.get(QUEUE_EXTENSION_NAME, {}) or {}))
    queue_settings["table_name"] = validate_table_name(table_name)
    extension_config[QUEUE_EXTENSION_NAME] = queue_settings
    sqlspec_config.extension_config = extension_config

    migration_config = dict(cast("dict[str, Any]", getattr(sqlspec_config, "migration_config", {}) or {}))
    include_extensions = migration_config.get("include_extensions")
    if include_extensions is None:
        include_list: list[str] = []
    elif isinstance(include_extensions, tuple):
        include_list = list(include_extensions)
    else:
        include_list = list(cast("list[str]", include_extensions))

    if QUEUE_EXTENSION_NAME not in include_list:
        include_list.append(QUEUE_EXTENSION_NAME)
    migration_config["include_extensions"] = include_list
    sqlspec_config.set_migration_config(migration_config)
    return queue_settings
