from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("sqlspec")

from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.models import QueuedTaskRecord

if TYPE_CHECKING:
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

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, queue_table_name="jobs")

    Litestar(plugins=[QueuePlugin(QueueConfig(queue_backend=backend_config))])

    commands = sqlspec_config.get_migration_commands()
    queue_settings = commands.extension_configs[QUEUE_EXTENSION_NAME]
    assert queue_settings["table_name"] == "jobs"
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
