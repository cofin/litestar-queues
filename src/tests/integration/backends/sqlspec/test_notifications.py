"""Notification-channel tests for the SQLSpec queue backend.

Covers ``event_channel`` wiring, derivation from extension config, and
explicit-override precedence. Tests pin to the aiosqlite adapter so the
StubAsyncEventChannel can be injected without bringing up a real LISTEN/NOTIFY
service.
"""

import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar import Litestar
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

from litestar_queues import QueueConfig, QueuePlugin
from litestar_queues.backends.sqlspec import SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from tests.integration.backends.sqlspec.conftest import StubAsyncEventChannel

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


async def test_sqlspec_backend_event_channel_notifications_wake_waiters(
    tmp_path: "Path", sqlite_config_factory: Any
) -> None:
    event_channel = StubAsyncEventChannel()
    backend = SQLSpecQueueBackend(
        sqlspec_config=sqlite_config_factory(tmp_path / "notifications.db"),
        event_channel=cast("Any", event_channel),
        notification_channel="queue_notifications",
    )

    await backend.open()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        record = await backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "table_queue"
        assert backend.capabilities.notifications_durable is True
        assert event_channel.published == [
            (
                "queue_notifications",
                {
                    "task_id": str(record.id),
                    "task_name": "tasks.notified",
                    "queue": "critical",
                    "execution_backend": "local",
                },
                {"event_type": "litestar_queues.task_available"},
            )
        ]
        assert event_channel.acked == ["event-1"]
    finally:
        await backend.close()


async def test_sqlspec_backend_derives_sqlspec_event_channel_from_config(tmp_path: "Path") -> None:
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "derived-notifications.db")},
        extension_config={
            "events": {
                "backend": "table_queue",
                "poll_interval": 0.01,
                "queue_table": "queue_events",
            },
        },
    )
    backend = SQLSpecQueueBackend(
        sqlspec_config=sqlspec_config,
        create_schema=False,
        run_migrations=True,
        notifications=True,
        notification_channel="derived_notifications",
    )

    await backend.open()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        await backend.enqueue("tasks.derived_notified")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "table_queue"
        assert backend.capabilities.notifications_durable is True
    finally:
        await backend.close()


async def test_sqlspec_backend_notification_channel_uses_extension_config_with_explicit_override(
    tmp_path: "Path",
) -> None:
    extension_channel = StubAsyncEventChannel()
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "extension-notifications.db")},
        extension_config={
            QUEUE_EXTENSION_NAME: {
                "notification_channel": "extension_notifications",
            },
        },
    )
    extension_backend = SQLSpecQueueBackend(sqlspec_config=sqlspec_config, event_channel=cast("Any", extension_channel))
    await extension_backend.open()
    try:
        await extension_backend.enqueue("tasks.extension_notified")
    finally:
        await extension_backend.close()

    explicit_channel = StubAsyncEventChannel()
    explicit_backend = SQLSpecQueueBackend(
        sqlspec_config=sqlspec_config,
        event_channel=cast("Any", explicit_channel),
        notification_channel="explicit_notifications",
        table_name="explicit_notification_queue",
    )
    await explicit_backend.open()
    try:
        await explicit_backend.enqueue("tasks.explicit_notified")
    finally:
        await explicit_backend.close()

    assert extension_channel.published[0][0] == "extension_notifications"
    assert explicit_channel.published[0][0] == "explicit_notifications"


async def test_sqlspec_backend_uses_user_registered_litestar_sqlspec_plugin(
    tmp_path: "Path", sqlite_config_factory: Any
) -> None:
    sqlspec = SQLSpec()
    sqlspec_config = sqlite_config_factory(tmp_path / "litestar.db")
    sqlspec.add_config(sqlspec_config)

    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="sqlspec",
            queue_backend_config={
                "sqlspec": sqlspec,
                "create_schema": False,
            },
            initialize_schedules=False,
        )
    )

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec), plugin])

    assert "db_session" in app.dependencies
    assert "AiosqliteDriver" in app.signature_namespace
