import inspect
from typing import TYPE_CHECKING, Any, cast, get_args

import pytest

pytest.importorskip("sqlspec")

from litestar import Litestar
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueuePlugin
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.backends.sqlspec import extension as sqlspec_extension
from litestar_queues.backends.sqlspec import typing as sqlspec_typing
from litestar_queues.backends.sqlspec.extension import (
    DEFAULT_MIGRATION_MODE,
    QUEUE_EXTENSION_NAME,
    VALID_MIGRATION_MODES,
    configure_events_migration_extension,
    configure_queue_migration_extension,
    queue_migration_directory,
    validate_migration_mode,
)
from litestar_queues.exceptions import QueueConfigurationError
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
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={QUEUE_EXTENSION_NAME: {"table_name": "legacy_jobs"}},
    )
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, queue_table_name="jobs")

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    queue_settings = commands.extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings["queue_table_name"] == "jobs"
    assert "table_name" not in queue_settings
    assert queue_settings["maintenance_table_name"] == "jobs_maintenance"
    assert QUEUE_EXTENSION_NAME in reconciler_names
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations


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
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in reconciler_names
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
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        queue_table_name="jobs",
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
    )

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    queue_settings = commands.extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings["queue_table_name"] == "jobs"
    assert queue_settings["maintenance_table_name"] == "jobs_maintenance"
    assert queue_settings["task_reservation_table_name"] == "jobs_reservation"
    assert QUEUE_EXTENSION_NAME in reconciler_names
    assert "events" in reconciler_names
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_migrations
    assert commands.runner.extension_configs[QUEUE_EXTENSION_NAME] == queue_settings
    events_settings = cast("dict[str, Any]", (sqlspec_config.extension_config or {})["events"])
    assert events_settings["backend"] == "poll_queue"


@pytest.mark.parametrize(
    ("migration_mode", "expect_reconciler", "expect_versioned"),
    [(None, True, False), ("reconcile", True, False), ("versioned", False, True)],
)
def test_plugin_discovery_partitions_extensions_by_migration_mode(
    migration_mode: Any, expect_reconciler: bool, expect_versioned: bool
) -> None:
    """QueuePlugin discovery partitions queue and events extensions by migration_mode."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_kwargs: dict[str, Any] = {
        "sqlspec_config": sqlspec_config,
        "queue_table_name": "jobs",
        "worker_wakeups": SQLSpecWorkerWakeupConfig(transport="poll_queue"),
    }
    if migration_mode is not None:
        backend_kwargs["migration_mode"] = migration_mode
    backend_config = SQLSpecBackendConfig(**backend_kwargs)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert (QUEUE_EXTENSION_NAME in reconciler_names) is expect_reconciler
    assert ("events" in reconciler_names) is expect_reconciler
    assert (QUEUE_EXTENSION_NAME in commands.runner.extension_migrations) is expect_versioned
    assert ("events" in commands.runner.extension_migrations) is expect_versioned
    assert QUEUE_EXTENSION_NAME in commands.extension_configs
    assert "events" in commands.extension_configs


def test_manage_schema_false_on_the_direct_extension_call_registers_nothing() -> None:
    """The documented standalone migration path honors adopter-owned schema too."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs", manage_schema=False)
    configure_events_migration_extension(sqlspec_config, backend="poll_queue", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert "events" not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in reconciler_names
    assert "events" not in reconciler_names
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert sqlspec_config.extension_config in (None, {})


def test_manage_schema_false_removes_an_earlier_queue_registration() -> None:
    """A registration made before the plugin runs does not survive adopter-owned schema."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config)
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in reconciler_names
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_on_the_direct_extension_call_removes_an_earlier_registration() -> None:
    """The documented standalone recipe is undone by a later adopter-owned call."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs")
    configure_queue_migration_extension(sqlspec_config, queue_table_name="jobs", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in reconciler_names
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_configs
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_removes_an_earlier_events_registration() -> None:
    """The packaged events queue migration is deregistered the same way."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    configure_events_migration_extension(sqlspec_config, backend="poll_queue", manage_schema=False)

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert "events" not in commands.extension_configs
    assert "events" not in reconciler_names
    assert "events" not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_configs
    assert "events" not in (sqlspec_config.extension_config or {})
    assert "events" not in (sqlspec_config.migration_config or {}).get("include_extensions", [])


def test_manage_schema_false_removes_an_earlier_events_registration_through_the_plugin() -> None:
    """The plugin path deregisters an events registration made before it ran."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        manage_schema=False,
    )

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert "events" not in commands.extension_configs
    assert "events" not in reconciler_names
    assert "events" not in commands.runner.extension_migrations
    assert "events" not in commands.runner.extension_configs
    assert "events" not in (sqlspec_config.extension_config or {})


def test_manage_schema_false_removes_an_earlier_events_registration_without_wakeups() -> None:
    """Disabled wakeups do not exempt an events registration from adopter-owned schema."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    configure_events_migration_extension(sqlspec_config, backend="poll_queue")
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, worker_wakeups=None, manage_schema=False)

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert "events" not in commands.extension_configs
    assert "events" not in reconciler_names
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
    configure_queue_migration_extension(owned, queue_table_name="queue_task", migration_mode="versioned")
    await owned.migrate_up(echo=False)
    try:
        assert await applied_revisions(owned) == ["ext_litestar_queues_0001"]
        migrated_tables = await table_names(owned)
    finally:
        await owned.close_pool()
    assert "queue_task" in migrated_tables

    adopter = make_config()
    configure_queue_migration_extension(
        adopter, queue_table_name="queue_task", manage_schema=False, migration_mode="versioned"
    )
    await adopter.migrate_up(echo=False)
    try:
        assert await applied_revisions(adopter) == ["ext_litestar_queues_0001"]
        assert await table_names(adopter) == migrated_tables
    finally:
        await adopter.close_pool()


@pytest.mark.anyio
async def test_migration_registration_toggle_preserves_adopter_options_and_history(tmp_path: "Path") -> None:
    from litestar_queues.backends.sqlspec.extension import configure_queue_migration_extension

    scripts = tmp_path / "application_migrations"
    scripts.mkdir()
    unrelated = tmp_path / "other_migrations"
    unrelated.mkdir()
    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "toggle.db")},
        migration_config={"script_location": str(scripts), "version_table_name": "application_revisions"},
    )
    config.add_extension_migrations("unrelated", unrelated, {"application_option": "preserved"})
    config.get_migration_commands()
    configure_queue_migration_extension(config, queue_table_name="jobs", manage_schema=False)
    try:
        await config.migrate_up(echo=False)
        async with config.provide_session() as driver:
            assert await driver.select("SELECT version_num FROM application_revisions") == []
            assert await driver.select("SELECT name FROM sqlite_master WHERE name='jobs'") == []

        configure_queue_migration_extension(config, queue_table_name="jobs", migration_mode="versioned")
        commands = config.get_migration_commands()
        assert commands.extension_configs[QUEUE_EXTENSION_NAME]["queue_table_name"] == "jobs"
        await config.migrate_up(echo=False)
        async with config.provide_session() as driver:
            before = await driver.select("SELECT * FROM application_revisions ORDER BY version_num")
            tables = await driver.select("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        assert [row["version_num"] for row in before] == ["ext_litestar_queues_0001"]

        for manage_schema in (False, False, True):
            config.get_migration_commands()
            configure_queue_migration_extension(
                config, queue_table_name="jobs", manage_schema=manage_schema, migration_mode="versioned"
            )
            commands = config.get_migration_commands()
            assert (QUEUE_EXTENSION_NAME in commands.runner.extension_migrations) is manage_schema
            unrelated_settings = cast("dict[str, Any]", config.extension_config["unrelated"])
            assert unrelated_settings["application_option"] == "preserved"
            assert config.migration_config["script_location"] == str(scripts)
            assert config.migration_config["version_table_name"] == "application_revisions"
            assert "unrelated" in config.migration_config["include_extensions"]
            await config.migrate_up(echo=False)
            async with config.provide_session() as driver:
                assert await driver.select("SELECT * FROM application_revisions ORDER BY version_num") == before
                assert await driver.select("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") == tables
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("symbol_name", "expected_literal_args"), [("ExtensionMigrationMode", ("versioned", "reconcile"))]
)
def test_sqlspec_typing_facade_exports_extension_migration_mode(
    symbol_name: str, expected_literal_args: tuple[str, ...]
) -> None:
    """The SQLSpec typing facade exposes ExtensionMigrationMode with versioned and reconcile literals."""
    assert symbol_name in sqlspec_typing.__all__
    assert get_args(getattr(sqlspec_typing, symbol_name)) == expected_literal_args


@pytest.mark.parametrize(
    ("param_name", "expected_kind", "expected_default", "expected_annotation"),
    [
        ("name", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty, str),
        ("migrations_path", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty, "str | Path"),
        ("settings", inspect.Parameter.POSITIONAL_OR_KEYWORD, None, "dict[str, Any] | None"),
        ("migration_mode", inspect.Parameter.KEYWORD_ONLY, None, "ExtensionMigrationMode | None"),
    ],
)
def test_sqlspec_config_protocol_add_extension_migrations_signature(
    param_name: str, expected_kind: Any, expected_default: Any, expected_annotation: Any
) -> None:
    """SQLSpecConfig.add_extension_migrations accepts keyword-only migration_mode."""
    params = inspect.signature(sqlspec_typing.SQLSpecConfig.add_extension_migrations).parameters

    assert param_name in params
    parameter = params[param_name]
    assert parameter.kind is expected_kind
    assert parameter.default is expected_default
    assert parameter.annotation == expected_annotation


class _SpySQLSpecConfig:
    """Minimal spy double recording extension migration calls and config mutations."""

    __slots__ = ("calls", "extension_config", "migration_config", "removed")

    def __init__(self, extension_config: "dict[str, Any] | None" = None) -> None:
        self.extension_config: dict[str, Any] = {} if extension_config is None else dict(extension_config)
        self.migration_config: dict[str, Any] = {}
        self.calls: list[tuple[str, Any, dict[str, Any] | None, Any]] = []
        self.removed: list[str] = []

    def add_extension_migrations(
        self, name: str, migrations_path: Any, settings: "dict[str, Any] | None" = None, *, migration_mode: Any = None
    ) -> None:
        self.calls.append((name, migrations_path, None if settings is None else dict(settings), migration_mode))
        merged = dict(self.extension_config.get(name) or {})
        if settings:
            merged.update(settings)
        merged["migrations_path"] = migrations_path
        if migration_mode is not None:
            merged["migration_mode"] = migration_mode
        self.extension_config[name] = merged

    def remove_extension_migrations(self, name: str) -> bool:
        self.removed.append(name)
        return self.extension_config.pop(name, None) is not None


@pytest.mark.parametrize(
    ("symbol_name", "expected_value"),
    [("DEFAULT_MIGRATION_MODE", "reconcile"), ("VALID_MIGRATION_MODES", frozenset({"versioned", "reconcile"}))],
)
def test_extension_module_exports_migration_mode_constants(symbol_name: str, expected_value: Any) -> None:
    """The SQLSpec extension module exports migration mode defaults, valid set, and validator."""
    assert symbol_name in sqlspec_extension.__all__
    assert "validate_migration_mode" in sqlspec_extension.__all__
    assert getattr(sqlspec_extension, symbol_name) == expected_value


@pytest.mark.parametrize("mode", ["versioned", "reconcile"])
def test_validate_migration_mode_accepts_valid_modes(mode: str) -> None:
    """validate_migration_mode returns valid literal mode strings unchanged."""
    assert validate_migration_mode(mode) == mode
    assert mode in VALID_MIGRATION_MODES


@pytest.mark.parametrize("bad_mode", [True, False, None, "", "auto", 123])
def test_validate_migration_mode_rejects_invalid_values(bad_mode: Any) -> None:
    """validate_migration_mode rejects booleans, None, and unknown mode strings."""
    with pytest.raises(QueueConfigurationError, match="Invalid SQLSpec migration_mode"):
        validate_migration_mode(bad_mode)


@pytest.mark.parametrize("bad_mode", [True, False, "", "auto", 123])
@pytest.mark.parametrize("manage_schema", [True, False])
@pytest.mark.parametrize("backend", ["poll_queue", None])
def test_configure_events_migration_extension_rejects_invalid_migration_mode_immediately(
    bad_mode: Any, manage_schema: bool, backend: "str | None"
) -> None:
    """Invalid non-None migration_mode fails before removal or early return."""
    spy_cfg = _SpySQLSpecConfig({"events": {"backend": "custom"}})

    with pytest.raises(QueueConfigurationError, match="Invalid SQLSpec migration_mode"):
        configure_events_migration_extension(
            cast("Any", spy_cfg), backend=backend, manage_schema=manage_schema, migration_mode=bad_mode
        )

    assert spy_cfg.calls == []
    assert spy_cfg.removed == []
    assert spy_cfg.extension_config == {"events": {"backend": "custom"}}


@pytest.mark.parametrize("bad_mode", [True, False, "", "auto", 123])
@pytest.mark.parametrize("manage_schema", [True, False])
def test_configure_queue_migration_extension_rejects_invalid_migration_mode_immediately(
    bad_mode: Any, manage_schema: bool
) -> None:
    """Invalid non-None migration_mode fails before queue registration or removal."""
    spy_cfg = _SpySQLSpecConfig({QUEUE_EXTENSION_NAME: {"queue_table_name": "existing"}})

    with pytest.raises(QueueConfigurationError, match="Invalid SQLSpec migration_mode"):
        configure_queue_migration_extension(cast("Any", spy_cfg), manage_schema=manage_schema, migration_mode=bad_mode)

    assert spy_cfg.calls == []
    assert spy_cfg.removed == []
    assert spy_cfg.extension_config == {QUEUE_EXTENSION_NAME: {"queue_table_name": "existing"}}


@pytest.mark.parametrize("mode", [None, "versioned", "reconcile"])
def test_configure_events_migration_extension_forwards_migration_mode_spy(mode: Any) -> None:
    """configure_events_migration_extension resolves None and forwards migration_mode via add_extension_migrations."""
    spy_cfg = _SpySQLSpecConfig()
    expected_mode = DEFAULT_MIGRATION_MODE if mode is None else mode

    configure_events_migration_extension(
        cast("Any", spy_cfg), backend="poll_queue", queue_table="custom_events", migration_mode=mode
    )

    assert spy_cfg.calls == [
        (
            "events",
            "sqlspec.extensions.events:migrations",
            {"backend": "poll_queue", "queue_table": "custom_events"},
            expected_mode,
        )
    ]
    assert spy_cfg.extension_config["events"] == {
        "backend": "poll_queue",
        "queue_table": "custom_events",
        "migrations_path": "sqlspec.extensions.events:migrations",
        "migration_mode": expected_mode,
    }


@pytest.mark.parametrize("mode", [None, "versioned", "reconcile"])
def test_configure_queue_migration_extension_forwards_migration_mode_spy(mode: Any) -> None:
    """configure_queue_migration_extension resolves None and forwards migration_mode via add_extension_migrations."""
    spy_cfg = _SpySQLSpecConfig()
    expected_mode = DEFAULT_MIGRATION_MODE if mode is None else mode

    configure_queue_migration_extension(cast("Any", spy_cfg), queue_table_name="jobs", migration_mode=mode)

    assert len(spy_cfg.calls) == 1
    name, migrations_path, settings, recorded_mode = spy_cfg.calls[0]
    assert name == QUEUE_EXTENSION_NAME
    assert migrations_path == queue_migration_directory()
    assert settings is not None
    assert settings["queue_table_name"] == "jobs"
    assert settings["maintenance_table_name"] == "jobs_maintenance"
    assert settings["task_reservation_table_name"] == "jobs_reservation"
    assert recorded_mode == expected_mode
    assert spy_cfg.extension_config[QUEUE_EXTENSION_NAME]["migration_mode"] == expected_mode


@pytest.mark.parametrize("mode", [None, "versioned", "reconcile"])
def test_configure_events_preserves_custom_migrations_path_and_none_backend(mode: Any) -> None:
    """Adopter events migrations_path is forwarded and backend=None leaves adopter config untouched."""
    expected_mode = DEFAULT_MIGRATION_MODE if mode is None else mode
    custom_cfg = _SpySQLSpecConfig({"events": {"migrations_path": "custom.pkg:migrations"}})

    configure_events_migration_extension(cast("Any", custom_cfg), backend="poll_queue", migration_mode=mode)

    assert custom_cfg.calls == [
        (
            "events",
            "custom.pkg:migrations",
            {"migrations_path": "custom.pkg:migrations", "backend": "poll_queue"},
            expected_mode,
        )
    ]

    preserved_cfg = _SpySQLSpecConfig({"events": {"backend": "custom", "migrations_path": "adopter.pkg:migrations"}})
    configure_events_migration_extension(cast("Any", preserved_cfg), backend=None, migration_mode=mode)

    assert preserved_cfg.calls == []
    assert preserved_cfg.removed == []
    assert preserved_cfg.extension_config == {
        "events": {"backend": "custom", "migrations_path": "adopter.pkg:migrations"}
    }


@pytest.mark.parametrize("mode", [None, "versioned", "reconcile"])
def test_configure_extensions_manage_schema_false_removes_without_adding(mode: Any) -> None:
    """manage_schema=False removes registrations without writing stale migration_mode."""
    spy_cfg = _SpySQLSpecConfig({
        "events": {"backend": "poll_queue", "migration_mode": "reconcile"},
        QUEUE_EXTENSION_NAME: {"queue_table_name": "jobs", "migration_mode": "reconcile"},
    })

    configure_events_migration_extension(
        cast("Any", spy_cfg), backend="poll_queue", manage_schema=False, migration_mode=mode
    )
    configure_queue_migration_extension(cast("Any", spy_cfg), manage_schema=False, migration_mode=mode)

    assert spy_cfg.calls == []
    assert spy_cfg.removed == ["events", QUEUE_EXTENSION_NAME]
    assert spy_cfg.extension_config == {}


@pytest.mark.parametrize(
    ("mode", "expected_mode", "expect_reconciler", "expect_versioned"),
    [
        (None, "reconcile", True, False),
        ("reconcile", "reconcile", True, False),
        ("versioned", "versioned", False, True),
    ],
)
def test_aiosqlite_get_migration_commands_includes_migration_mode(
    mode: Any, expected_mode: str, expect_reconciler: bool, expect_versioned: bool
) -> None:
    """Live AiosqliteConfig partitions queue and events extensions by migration_mode."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})

    configure_events_migration_extension(sqlspec_config, backend="poll_queue", migration_mode=mode)
    configure_queue_migration_extension(sqlspec_config, migration_mode=mode)

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert commands.extension_configs["events"]["migration_mode"] == expected_mode
    assert commands.extension_configs[QUEUE_EXTENSION_NAME]["migration_mode"] == expected_mode
    assert ("events" in reconciler_names) is expect_reconciler
    assert (QUEUE_EXTENSION_NAME in reconciler_names) is expect_reconciler
    assert ("events" in commands.runner.extension_migrations) is expect_versioned
    assert (QUEUE_EXTENSION_NAME in commands.runner.extension_migrations) is expect_versioned


def test_sqlspec_backend_config_default_migration_mode() -> None:
    """SQLSpecBackendConfig defaults migration_mode to reconcile."""
    config = SQLSpecBackendConfig()

    assert config.migration_mode == "reconcile"
    assert config.migration_mode == DEFAULT_MIGRATION_MODE


@pytest.mark.parametrize("mode", ["versioned", "reconcile"])
def test_sqlspec_backend_config_accepts_valid_migration_mode(mode: Any) -> None:
    """SQLSpecBackendConfig accepts versioned and reconcile migration modes."""
    config = SQLSpecBackendConfig(migration_mode=mode)

    assert config.migration_mode == mode


@pytest.mark.parametrize("bad_mode", [True, False, None, "", "auto", 123])
def test_sqlspec_backend_config_rejects_invalid_migration_mode(bad_mode: Any) -> None:
    """SQLSpecBackendConfig rejects invalid, boolean, and None migration_mode values in __post_init__."""
    with pytest.raises(QueueConfigurationError, match="Invalid SQLSpec migration_mode"):
        SQLSpecBackendConfig(migration_mode=bad_mode)


@pytest.mark.parametrize("mode", ["versioned", "reconcile"])
def test_sqlspec_backend_config_configure_migrations_forwards_mode_to_both_extensions(mode: Any) -> None:
    """configure_migrations forwards migration_mode to events then litestar_queues in order."""
    spy_cfg = _SpySQLSpecConfig()
    backend_config = SQLSpecBackendConfig(
        sqlspec_config=cast("Any", spy_cfg),
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        migration_mode=mode,
    )

    backend_config.configure_migrations(QueueConfig(queue_backend=backend_config))

    assert [(name, path, recorded_mode) for name, path, _settings, recorded_mode in spy_cfg.calls] == [
        ("events", "sqlspec.extensions.events:migrations", mode),
        (QUEUE_EXTENSION_NAME, queue_migration_directory(), mode),
    ]
    assert spy_cfg.extension_config["events"]["migration_mode"] == mode
    assert spy_cfg.extension_config[QUEUE_EXTENSION_NAME]["migration_mode"] == mode


@pytest.mark.parametrize("mode", ["versioned", "reconcile"])
@pytest.mark.parametrize("pre_registered", [False, True])
def test_sqlspec_backend_config_manage_schema_false_removes_both_in_both_modes(mode: Any, pre_registered: bool) -> None:
    """manage_schema=False removes both extension registrations under both migration modes."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    if pre_registered:
        configure_events_migration_extension(sqlspec_config, backend="poll_queue", migration_mode="versioned")
        configure_queue_migration_extension(sqlspec_config, migration_mode="versioned")

    backend_config = SQLSpecBackendConfig(
        sqlspec_config=sqlspec_config,
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="poll_queue"),
        manage_schema=False,
        migration_mode=mode,
    )
    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    reconciler_names = {descriptor.name for descriptor in commands.extension_reconcilers}
    assert "events" not in commands.extension_configs
    assert QUEUE_EXTENSION_NAME not in commands.extension_configs
    assert "events" not in reconciler_names
    assert QUEUE_EXTENSION_NAME not in reconciler_names
    assert "events" not in commands.runner.extension_migrations
    assert QUEUE_EXTENSION_NAME not in commands.runner.extension_migrations
    assert "events" not in (sqlspec_config.extension_config or {})
    assert QUEUE_EXTENSION_NAME not in (sqlspec_config.extension_config or {})
