"""Advanced Alchemy backend configuration and notification-listener tests."""

from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("advanced_alchemy")
pytest.importorskip("sqlalchemy")

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable


def test_advanced_alchemy_config_defaults_to_singular_queue_task_model() -> "None":
    """Default Advanced Alchemy config should use the built-in queue task model."""
    from litestar_queues.backends.advanced_alchemy import QueueTaskModel, SQLAlchemyBackendConfig

    config = SQLAlchemyBackendConfig()

    assert config.model_class is QueueTaskModel
    assert QueueTaskModel.__tablename__ == "queue_task"


def test_create_notification_listener_selects_driver_specific_listener() -> "None":
    """The factory dispatches on the exact SQLAlchemy driver name."""
    from litestar_queues.backends.advanced_alchemy._notifications import (
        SUPPORTED_NOTIFY_DRIVERS,
        _AsyncpgNotificationListener,
        _PsycopgNotificationListener,
        create_notification_listener,
    )

    asyncpg_listener = create_notification_listener(
        connection_string="postgresql+asyncpg://user:secret@localhost/db", channel="lq"
    )
    psycopg_listener = create_notification_listener(
        connection_string="postgresql+psycopg://user:secret@localhost/db", channel="lq"
    )

    assert isinstance(asyncpg_listener, _AsyncpgNotificationListener)
    assert isinstance(psycopg_listener, _PsycopgNotificationListener)
    assert frozenset({"postgresql+asyncpg", "postgresql+psycopg"}) == SUPPORTED_NOTIFY_DRIVERS


def test_create_notification_listener_rejects_unsupported_driver_without_leaking_secret() -> "None":
    from litestar_queues.backends.advanced_alchemy._notifications import create_notification_listener
    from litestar_queues.exceptions import QueueConfigurationError

    with pytest.raises(QueueConfigurationError) as exc_info:
        create_notification_listener(connection_string="mysql+asyncmy://user:secret@localhost/db", channel="lq")

    assert "secret" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# asyncpg listener (moved behind the shared protocol, semantics unchanged)
# ---------------------------------------------------------------------------


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
    from litestar_queues.backends.advanced_alchemy._notifications import _AsyncpgNotificationListener

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


# ---------------------------------------------------------------------------
# psycopg listener
# ---------------------------------------------------------------------------


class _FakePumpError(Exception):
    """Stand-in for a psycopg OperationalError raised by a dropped pump."""


class _FakePsycopgConnection:
    """Minimal psycopg ``AsyncConnection`` double for listener lifecycle tests."""

    __slots__ = ("_auto_deliver", "_fail", "_signal", "autocommit", "closed", "executed")

    def __init__(self, *, fail_pump: "bool" = False, auto_deliver: "bool" = False) -> "None":
        self.autocommit = False
        self.closed = False
        self.executed: "list[str]" = []
        self._auto_deliver = auto_deliver
        self._fail = fail_pump
        self._signal = __import__("asyncio").Event()

    async def execute(self, statement: "Any") -> "None":
        # ``statement`` is a psycopg ``Composed``; record the rendered SQL so
        # tests can assert safe identifier quoting.
        self.executed.append(statement.as_string(None))

    async def notifies(
        self, *, timeout: "float | None" = None, stop_after: "int | None" = None
    ) -> "AsyncIterator[Any]":
        if self._fail:
            msg = "connection lost"
            raise _FakePumpError(msg)
        if not self._auto_deliver:
            await self._signal.wait()
            self._signal.clear()
        yield object()

    async def close(self) -> "None":
        self.closed = True

    def deliver(self) -> "None":
        self._signal.set()


def _psycopg_connect_factory(
    connections: "list[_FakePsycopgConnection]", *specs: "dict[str, bool]"
) -> "Callable[..., Any]":
    """Return a fake ``AsyncConnection.connect`` yielding fakes per ``specs``."""

    async def connect(conninfo: "str", *, autocommit: "bool" = False, **_kwargs: "Any") -> "_FakePsycopgConnection":
        spec = specs[len(connections)] if len(connections) < len(specs) else {}
        connection = _FakePsycopgConnection(
            fail_pump=spec.get("fail_pump", False), auto_deliver=spec.get("auto_deliver", False)
        )
        connection.autocommit = autocommit
        connections.append(connection)
        return connection

    return connect


@pytest.mark.anyio
async def test_psycopg_listener_opens_autocommit_and_composes_safe_identifier(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    import psycopg

    from litestar_queues.backends.advanced_alchemy._notifications import _PsycopgNotificationListener

    connections: "list[_FakePsycopgConnection]" = []
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", staticmethod(_psycopg_connect_factory(connections)))

    listener = _PsycopgNotificationListener(conninfo="postgresql://user:secret@localhost/db", channel="lq-notify")
    await listener.start()

    assert len(connections) == 1
    assert connections[0].autocommit is True
    # Identifier is quoted, never interpolated -- a dash cannot break out.
    assert connections[0].executed == ['LISTEN "lq-notify"']


@pytest.mark.anyio
async def test_psycopg_listener_retains_one_pump_across_timeouts(monkeypatch: "pytest.MonkeyPatch") -> "None":
    import psycopg

    from litestar_queues.backends.advanced_alchemy._notifications import _PsycopgNotificationListener

    connections: "list[_FakePsycopgConnection]" = []
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", staticmethod(_psycopg_connect_factory(connections)))

    listener = _PsycopgNotificationListener(conninfo="postgresql://ignored/db", channel="lq")

    assert await listener.wait(timeout=0.01) is False
    assert await listener.wait(timeout=0.01) is False
    assert _has_pending(listener) is True
    # One connection, one LISTEN, one retained notification pump.
    assert len(connections) == 1
    assert connections[0].executed == ['LISTEN "lq"']

    connections[0].deliver()
    assert await listener.wait(timeout=0.5) is True
    assert _has_pending(listener) is False
    assert len(connections) == 1

    await listener.close()
    assert _has_pending(listener) is False
    assert connections[0].closed is True
    assert connections[0].executed[-1] == 'UNLISTEN "lq"'


@pytest.mark.anyio
async def test_psycopg_listener_reconnects_and_relistens_after_pump_failure(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    import psycopg

    from litestar_queues.backends.advanced_alchemy._notifications import _PsycopgNotificationListener

    connections: "list[_FakePsycopgConnection]" = []
    monkeypatch.setattr(
        psycopg.AsyncConnection,
        "connect",
        staticmethod(_psycopg_connect_factory(connections, {"fail_pump": True}, {"auto_deliver": True})),
    )

    listener = _PsycopgNotificationListener(conninfo="postgresql://ignored/db", channel="lq")

    # First wait: the pump raises; the dead connection is torn down and False
    # is returned so the worker reconciles + retries.
    assert await listener.wait(timeout=0.5) is False
    assert connections[0].closed is True
    assert _has_pending(listener) is False

    # Second wait: bounded reconnect rebuilds the connection and re-LISTENs.
    assert await listener.wait(timeout=0.5) is True
    assert len(connections) == 2
    assert connections[1].executed[0] == 'LISTEN "lq"'
    assert connections[1].closed is False

    await listener.close()


@pytest.mark.anyio
async def test_psycopg_listener_missing_dependency_raises_only_when_path_used(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    import importlib

    from litestar_queues.backends.advanced_alchemy import _notifications
    from litestar_queues.exceptions import QueueConfigurationError

    real_import_module = importlib.import_module

    def blocked_import(name: "str") -> "Any":
        if name == "psycopg" or name.startswith("psycopg."):
            raise ImportError(name)
        return real_import_module(name)

    monkeypatch.setattr(_notifications, "import_module", blocked_import)

    # Construction is lazy: no psycopg import happens until the pump path runs.
    listener = _notifications.create_notification_listener(
        connection_string="postgresql+psycopg://user:secret@localhost/db", channel="lq"
    )

    with pytest.raises(QueueConfigurationError) as exc_info:
        await listener.start()

    message = str(exc_info.value)
    assert "psycopg" in message
    # Connection secrets must never appear in the error surface.
    assert "secret" not in message
    assert "postgresql://" not in message


@pytest.mark.anyio
async def test_psycopg_listener_double_close_and_partial_setup_leave_no_state(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    import psycopg

    from litestar_queues.backends.advanced_alchemy._notifications import _PsycopgNotificationListener

    connections: "list[_FakePsycopgConnection]" = []
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", staticmethod(_psycopg_connect_factory(connections)))

    listener = _PsycopgNotificationListener(conninfo="postgresql://ignored/db", channel="lq")
    await listener.wait(timeout=0.01)
    assert _has_pending(listener) is True

    await listener.close()
    await listener.close()
    assert _has_pending(listener) is False
    assert listener._connection is None
    assert connections[0].closed is True

    # Partial setup: LISTEN fails -> the just-opened connection is closed and no
    # listener state (connection or retained pump) survives.
    failing: "list[_FakePsycopgConnection]" = []

    class _FailingListenConnection(_FakePsycopgConnection):
        __slots__ = ()

        async def execute(self, statement: "Any") -> "None":
            msg = "listen failed"
            raise _FakePumpError(msg)

    async def failing_connect(
        conninfo: "str", *, autocommit: "bool" = False, **_kwargs: "Any"
    ) -> "_FakePsycopgConnection":
        connection = _FailingListenConnection()
        connection.autocommit = autocommit
        failing.append(connection)
        return connection

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", staticmethod(failing_connect))
    partial = _PsycopgNotificationListener(conninfo="postgresql://ignored/db", channel="lq")
    with pytest.raises(_FakePumpError):
        await partial.start()

    assert partial._connection is None
    assert _has_pending(partial) is False
    assert failing[0].closed is True


@pytest.mark.anyio
async def test_psycopg_listener_cancel_during_wait_retains_read_then_close_cleans_up(
    monkeypatch: "pytest.MonkeyPatch",
) -> "None":
    import asyncio

    import psycopg

    from litestar_queues.backends.advanced_alchemy._notifications import _PsycopgNotificationListener

    connections: "list[_FakePsycopgConnection]" = []
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", staticmethod(_psycopg_connect_factory(connections)))

    listener = _PsycopgNotificationListener(conninfo="postgresql://ignored/db", channel="lq")
    waiter = asyncio.ensure_future(listener.wait(timeout=None))
    await asyncio.sleep(0.05)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    # Cancelling the outer wait retains the in-flight pump for reuse.
    assert _has_pending(listener) is True
    assert len(connections) == 1
    assert connections[0].closed is False

    await listener.close()
    assert _has_pending(listener) is False
    assert connections[0].closed is True


def _has_pending(listener: "Any") -> "bool":
    # Read through a function so mypy does not narrow the property to a literal.
    return bool(listener._pending_read.has_pending)
