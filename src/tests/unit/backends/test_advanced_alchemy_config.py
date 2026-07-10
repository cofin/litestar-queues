"""Advanced Alchemy backend configuration tests."""

from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("sqlalchemy")

if TYPE_CHECKING:
    from collections.abc import Callable


def test_advanced_alchemy_config_defaults_to_singular_queue_task_model() -> "None":
    """Default Advanced Alchemy config should use the built-in queue task model."""
    from litestar_queues.backends.advanced_alchemy import QueueTaskModel, SQLAlchemyBackendConfig

    config = SQLAlchemyBackendConfig()

    assert config.model_class is QueueTaskModel
    assert QueueTaskModel.__tablename__ == "litestar_queue_task"


class _FakeAsyncpgConnection:
    """Minimal asyncpg connection double for listener lifecycle tests."""

    __slots__ = ("added", "closed", "removed")

    def __init__(self) -> "None":
        self.added: "list[tuple[str, Callable[..., None]]]" = []
        self.removed: "list[tuple[str, Callable[..., None]]]" = []
        self.closed = False

    def is_closed(self) -> "bool":
        return self.closed

    async def add_listener(self, channel: "str", callback: "Callable[..., None]") -> "None":
        self.added.append((channel, callback))

    async def remove_listener(self, channel: "str", callback: "Callable[..., None]") -> "None":
        self.removed.append((channel, callback))

    async def close(self) -> "None":
        self.closed = True


@pytest.mark.anyio
async def test_asyncpg_listener_retains_one_read_across_timeouts() -> "None":
    from litestar_queues.backends.advanced_alchemy.backend import _AsyncpgNotificationListener

    connections: "list[_FakeAsyncpgConnection]" = []

    class _FakeConnectListener(_AsyncpgNotificationListener):
        async def _connect(self) -> "_FakeAsyncpgConnection":
            connection = _FakeAsyncpgConnection()
            connections.append(connection)
            return connection

    listener = _FakeConnectListener(dsn="postgresql://ignored/db", channel="lq")

    assert await listener.wait(timeout=0.01) is False
    assert await listener.wait(timeout=0.01) is False
    assert _has_pending(listener) is True
    # One connection, one LISTEN registration, one retained event read.
    assert len(connections) == 1
    assert len(connections[0].added) == 1

    # A notification delivered between waits wakes the retained read exactly once.
    listener._handle_notification(connections[0], 0, "lq", "tasks")
    assert await listener.wait(timeout=0.01) is True
    assert _has_pending(listener) is False
    assert len(connections[0].added) == 1
    assert await listener.wait(timeout=0.01) is False

    await listener.close()
    assert _has_pending(listener) is False
    assert connections[0].closed is True
    assert len(connections[0].removed) == 1


def _has_pending(listener: "Any") -> "bool":
    # Read through a function so mypy does not narrow the property to a literal.
    return bool(listener._pending_read.has_pending)
