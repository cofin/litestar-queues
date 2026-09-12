from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
    from pathlib import Path

    from sqlspec.extensions.events import AsyncEventChannel


class _EventChannel:
    _backend_name = "poll_queue"

    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, channel: str, *_args: Any) -> None:
        self.published.append(channel)

    async def shutdown(self) -> None:
        return None


@pytest.mark.anyio
async def test_sqlspec_events_extension_does_not_select_worker_wakeup_transport() -> None:
    """Worker wakeup transport has one typed selection path."""
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"backend": "poll_queue"}}
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config))

    await backend.open()
    try:
        assert backend.capabilities.supports_worker_wakeups is False
        assert backend.capabilities.wakeup_backend is None
    finally:
        await backend.close()


@pytest.mark.anyio
async def test_sqlspec_legacy_queue_settings_do_not_override_typed_worker_wakeups() -> None:
    """Only SQLSpecWorkerWakeupConfig controls wakeup enablement and channel naming."""
    channel = _EventChannel()
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={QUEUE_EXTENSION_NAME: {"notifications": False, "wakeup_channel": "legacy"}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config,
            worker_wakeups=SQLSpecWorkerWakeupConfig(channel=cast("AsyncEventChannel", channel), channel_name="typed"),
        )
    )

    await backend.open()
    try:
        await backend.notify_new_task(QueuedTaskRecord(task_name="tasks.typed_wakeup"))
        assert backend.capabilities.supports_worker_wakeups is True
        assert channel.published == ["typed"]
    finally:
        await backend.close()


def test_the_sqlspec_backend_registers_its_own_migrations_through_the_plugin() -> None:
    """QueuePlugin reaches SQLSpec migrations through the backend-owned hook."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

    sqlspec_config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "legacy_jobs"}},
    )
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, queue_table_name="jobs")

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    queue_settings = commands.extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings["queue_table_name"] == "jobs"
    assert "table_name" not in queue_settings
    assert queue_settings["maintenance_table_name"] == "jobs_maintenance"
    assert QUEUE_EXTENSION_NAME in commands.runner.extension_migrations


def test_only_backends_that_own_migrations_advertise_the_hook() -> None:
    """The plugin's extension point is a protocol, not a hard-coded backend list."""
    from litestar_queues.backends.redis import RedisBackendConfig
    from litestar_queues.config import MigrationConfiguringBackend

    assert isinstance(SQLSpecBackendConfig(sqlspec_config=AiosqliteConfig()), MigrationConfiguringBackend)
    assert not isinstance(RedisBackendConfig(url="redis://localhost:6379/0"), MigrationConfiguringBackend)


@pytest.mark.anyio
async def test_sqlspec_worker_control_publishes_on_its_own_channel() -> None:
    """The control hint rides the events channel under its own NOTIFY identifier."""
    channel = _EventChannel()
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            sqlspec_config=sqlspec_config,
            worker_wakeups=SQLSpecWorkerWakeupConfig(channel=cast("AsyncEventChannel", channel)),
        )
    )

    await backend.open()
    try:
        await backend.notify_worker_control("worker-a")
        assert channel.published == ["litestar_queues_worker_control"]
    finally:
        await backend.close()


@pytest.mark.anyio
async def test_sqlspec_worker_control_falls_back_to_polling_without_wakeups() -> None:
    """A polling-only adapter keeps the base no-op publish and poll wait."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=sqlspec_config))

    await backend.open()
    try:
        assert backend.capabilities.supports_worker_wakeups is False
        await backend.notify_worker_control("worker-a")
        assert await backend.wait_for_worker_control(worker_id="worker-a", timeout=0) is False
    finally:
        await backend.close()


def test_dispatch_checked_mapping_is_carried_into_migrations() -> None:
    from litestar_queues import QueueConfig

    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"unrelated": {"enabled": True}}
    )
    backend_config = SQLSpecBackendConfig(sqlspec_config=config, column_map={"dispatch_checked_at": "last_scan"})
    backend_config.configure_migrations(QueueConfig(queue_backend=backend_config))
    settings = cast("dict[str, Any]", config.extension_config[QUEUE_EXTENSION_NAME])
    assert settings["column_map"]["dispatch_checked_at"] == "last_scan"
    assert config.extension_config["unrelated"] == {"enabled": True}


def test_dispatch_checked_store_mapping_precedence() -> None:
    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension
    from litestar_queues.backends.sqlspec.stores import create_queue_store

    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(config, column_map={"dispatch_checked_at": "configured_check"})
    configure_queue_migration_extension(config)
    inherited = create_queue_store(config)
    overridden = create_queue_store(config, column_map={"dispatch_checked_at": "explicit_check"})
    assert "configured_check" in inherited.dispatch_repair_index_sql()
    assert "explicit_check" in overridden.dispatch_repair_index_sql()


def test_manage_schema_false_registers_no_packaged_queue_migration() -> None:
    """An application that owns its schema receives no packaged queue revision."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_registers_no_packaged_events_migration() -> None:
    """The package-owned events queue table is schema-managing and follows the same rule."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        manage_schema=False,
    )

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    assert "events" not in (sqlspec_config.extension_config or {})


def test_manage_schema_true_still_registers_both_packaged_migrations() -> None:
    """Owning the schema keeps packaged migrations registered with unchanged settings."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        queue_table_name="jobs",
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
    )

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    queue_settings = commands.extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings["queue_table_name"] == "jobs"
    assert queue_settings["maintenance_table_name"] == "jobs_maintenance"
    assert queue_settings["task_reservation_table_name"] == "jobs_reservation"
    assert QUEUE_EXTENSION_NAME in commands.runner.extension_migrations
    assert commands.runner.extension_configs[QUEUE_EXTENSION_NAME] == queue_settings
    events_settings = cast("dict[str, Any]", (sqlspec_config.extension_config or {})["events"])
    assert events_settings["backend"] == "poll_queue"


def test_manage_schema_false_on_the_direct_extension_call_registers_nothing() -> None:
    """The documented standalone migration path honors adopter-owned schema too."""
    from litestar_queues.backends.sqlspec.extension import (
        configure_events_migration_extension,
        configure_queue_migration_extension,
    )

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs", manage_schema=False)
    configure_events_migration_extension(sqlspec_config, backend="poll_queue", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert sqlspec_config.extension_config in (None, {})


def test_manage_schema_false_removes_an_earlier_queue_registration() -> None:
    """A registration made before the plugin runs does not survive adopter-owned schema."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config)
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_on_the_direct_extension_call_removes_an_earlier_registration() -> None:
    """The documented standalone recipe is undone by a later adopter-owned call."""
    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs")
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_removes_an_earlier_events_registration() -> None:
    """The packaged events queue migration is deregistered the same way."""
    from litestar_queues.backends.sqlspec.extension import configure_events_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    configure_events_migration_extension(sqlspec_config, backend="poll_queue", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    assert "events" not in commands.extension_configs
    assert "events" not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_configs
    assert "events" not in (sqlspec_config.extension_config or {})
    assert "events" not in (sqlspec_config.migration_config or {}).get("include_extensions", [])


def test_manage_schema_false_removes_an_earlier_events_registration_through_the_plugin() -> None:
    """The plugin path deregisters an events registration made before it ran."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.backends.sqlspec.extension import configure_events_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        manage_schema=False,
    )

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    assert "events" not in commands.extension_configs
    assert "events" not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_configs
    assert "events" not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_removes_an_earlier_events_registration_without_wakeups() -> None:
    """Disabled wakeups do not exempt an events registration from adopter-owned schema."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.backends.sqlspec.extension import configure_events_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, worker_wakeups=None, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    assert "events" not in commands.extension_configs
    assert "events" not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_configs
    assert "events" not in (sqlspec_config.extension_config or {})


def test_an_adopter_events_registration_survives_a_transport_that_needs_no_events_table() -> None:
    """A polling adapter leaves SQLSpec's own events extension registration alone."""
    from litestar import Litestar

    from litestar_queues import QueueConfig, QueuePlugin
    from litestar_queues.backends.sqlspec.extension import configure_events_migration_extension

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    events_settings = cast("dict[str, Any]", (sqlspec_config.extension_config or {})["events"])
    assert events_settings["backend"] == "poll_queue"


@pytest.mark.anyio
async def test_manage_schema_switching_to_false_leaves_the_applied_revision_alone(tmp_path: "Path") -> None:
    """Switching an already-migrated database to adopter-owned schema is a no-op.

    SQLSpec's ``auto_sync`` reconciliation does not delete the applied
    ``ext_litestar_queues_0001`` row when the revision stops being discoverable,
    and it drops no packaged table: the second ``migrate_up`` simply finds
    nothing to do.
    """
    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension

    database = str(tmp_path / "queue.db")
    script_location = tmp_path / "migrations"
    script_location.mkdir()

    def make_config() -> AiosqliteConfig:
        return AiosqliteConfig(
            connection_config={"database": database}, migration_config={"script_location": str(script_location)}
        )

    async def applied_revisions(config: AiosqliteConfig) -> "list[str]":
        async with config.provide_session() as driver:
            rows = await driver.select("SELECT version_num FROM ddl_migrations ORDER BY version_num")
        return [str(row["version_num"]) for row in rows]

    async def table_names(config: AiosqliteConfig) -> "list[str]":
        async with config.provide_session() as driver:
            rows = await driver.select("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [str(row["name"]) for row in rows]

    owned = make_config()
    configure_queue_migration_extension(owned, queue_table_name="queue_task")
    await owned.migrate_up(echo=False)
    try:
        assert await applied_revisions(owned) == ["ext_litestar_queues_0001"]
        migrated_tables = await table_names(owned)
    finally:
        await owned.close_pool()
    assert "queue_task" in migrated_tables

    adopter = make_config()
    configure_queue_migration_extension(adopter, queue_table_name="queue_task", manage_schema=False)
    await adopter.migrate_up(echo=False)
    try:
        assert await applied_revisions(adopter) == ["ext_litestar_queues_0001"]
        assert await table_names(adopter) == migrated_tables
    finally:
        await adopter.close_pool()
