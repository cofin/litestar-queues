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
