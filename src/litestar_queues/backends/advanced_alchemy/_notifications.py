"""Driver-specific PostgreSQL LISTEN/NOTIFY listeners for Advanced Alchemy wakeups.

A queue backend that has opted into PostgreSQL notifications owns exactly one
dedicated listener connection per driver. The listener protocol below is the
only seam the backend knows about: it owns setup, a single retained wait,
reconnect-on-failure, and deterministic close. It does not own task
persistence, marker publication (``SELECT pg_notify(...)`` stays in the
backend), retry policy, or public configuration.

Notifications are hints. The backend reconciles the durable task table after
every :meth:`NotificationListener.start` (including reconnects), so a duplicate
or lost marker affects only wakeup latency, never correctness.

Connection strings are treated as secrets: they are never placed in exception
messages, and listeners carry no ``__dict__``/``repr`` that would leak them.
"""

import asyncio
from contextlib import suppress
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from sqlalchemy.engine import make_url

from litestar_queues.backends._notification_wait import PendingNativeRead
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

__all__ = ("SUPPORTED_NOTIFY_DRIVERS", "NotificationListener", "create_notification_listener")

_ASYNCPG_DRIVER = "postgresql+asyncpg"
_PSYCOPG_DRIVER = "postgresql+psycopg"

SUPPORTED_NOTIFY_DRIVERS: "frozenset[str]" = frozenset({_ASYNCPG_DRIVER, _PSYCOPG_DRIVER})
"""Exact SQLAlchemy driver names that expose a dedicated wakeup listener."""


@runtime_checkable
class NotificationListener(Protocol):
    """Structural protocol for a dedicated PostgreSQL wakeup listener.

    Implementations own a single driver connection plus at most one retained
    notification read. ``start`` is idempotent and re-establishes ``LISTEN``
    after a lost connection; ``wait`` reuses the retained read across timeouts;
    ``close`` tears the connection and pending read down deterministically.
    """

    async def start(self) -> "None":
        """Ensure the dedicated connection is open and subscribed."""
        ...

    async def wait(self, timeout: "float | None") -> "bool":
        """Wait up to ``timeout`` seconds for one wakeup marker.

        Returns:
            True when a marker was observed; False on timeout or a recovered
            pump failure.
        """
        ...

    async def close(self) -> "None":
        """Close the connection and cancel any retained read."""
        ...


def create_notification_listener(*, connection_string: "str", channel: "str") -> "NotificationListener":
    """Return the dedicated listener for the connection string's exact driver.

    Args:
        connection_string: The adopter's SQLAlchemy connection string. Only its
            driver name selects the listener; the value is otherwise treated as
            a secret.
        channel: The PostgreSQL ``LISTEN``/``NOTIFY`` channel identifier.

    Returns:
        A driver-specific :class:`NotificationListener`.

    Raises:
        QueueConfigurationError: If the driver has no dedicated listener.
    """
    url = make_url(connection_string)
    driver = url.drivername
    if driver == _ASYNCPG_DRIVER:
        return _AsyncpgNotificationListener(dsn=_neutral_conninfo(url), channel=channel)
    if driver == _PSYCOPG_DRIVER:
        return _PsycopgNotificationListener(conninfo=_neutral_conninfo(url), channel=channel)
    msg = "SQLAlchemyBackend PostgreSQL notifications require a postgresql+asyncpg or postgresql+psycopg driver."
    raise QueueConfigurationError(msg)


def _neutral_conninfo(url: "URL") -> "str":
    """Render a driver-neutral ``postgresql://`` connection string.

    The driver suffix is stripped so both asyncpg (``connect(dsn=...)``) and
    psycopg (``AsyncConnection.connect(conninfo)``) accept the same value.

    Returns:
        A ``postgresql://`` connection string with the driver suffix removed.
    """
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


class _AsyncpgNotificationListener:
    """Dedicated asyncpg LISTEN connection for Advanced Alchemy wakeups."""

    __slots__ = ("_channel", "_connection", "_dsn", "_event", "_pending_read")

    def __init__(self, *, dsn: "str", channel: "str") -> "None":
        self._dsn = dsn
        self._channel = channel
        self._event = asyncio.Event()
        self._connection: "Any | None" = None
        self._pending_read = PendingNativeRead()

    async def start(self) -> "None":
        connection = self._connection
        if connection is not None and not connection.is_closed():
            return
        await self.close()
        connection = await self._connect()
        await connection.add_listener(self._channel, self._handle_notification)
        self._connection = connection

    async def wait(self, timeout: "float | None") -> "bool":
        await self.start()
        if not self._pending_read.has_pending and self._event.is_set():
            self._event.clear()
            return True
        task = await self._pending_read.race(self._event.wait, timeout)
        if task is None:
            return False
        task.result()
        self._event.clear()
        return True

    async def close(self) -> "None":
        await self._pending_read.aclose()
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        with suppress(Exception):
            await connection.remove_listener(self._channel, self._handle_notification)
        with suppress(Exception):
            await connection.close()

    async def _connect(self) -> "Any":
        try:
            asyncpg = import_module("asyncpg")
        except ImportError as exc:
            msg = "SQLAlchemyBackendConfig.worker_wakeups=True for postgresql+asyncpg requires asyncpg."
            raise QueueConfigurationError(msg) from exc
        return await cast("Any", asyncpg).connect(dsn=self._dsn)

    def _handle_notification(self, _connection: "Any", _pid: "int", _channel: "str", _payload: "str") -> "None":
        self._event.set()


class _PsycopgNotificationListener:
    """Dedicated autocommit psycopg ``AsyncConnection`` for Advanced Alchemy wakeups.

    LISTEN/NOTIFY delivery requires the connection to be outside a transaction,
    so the connection is opened with ``autocommit=True``. Exactly one
    ``connection.notifies(stop_after=1)`` pump is retained across waits by the
    shared :class:`PendingNativeRead`; a pump failure resets the connection so
    the next :meth:`start` reconnects and re-subscribes.
    """

    __slots__ = ("_channel", "_connection", "_conninfo", "_pending_read")

    def __init__(self, *, conninfo: "str", channel: "str") -> "None":
        self._conninfo = conninfo
        self._channel = channel
        self._connection: "Any | None" = None
        self._pending_read = PendingNativeRead()

    async def start(self) -> "None":
        connection = self._connection
        if connection is not None and not connection.closed:
            return
        await self.close()
        connection = await self._connect()
        try:
            await connection.execute(self._listen_statement())
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        self._connection = connection

    async def wait(self, timeout: "float | None") -> "bool":
        await self.start()
        task = await self._pending_read.race(self._read_one, timeout)
        if task is None:
            return False
        if task.exception() is not None:
            # The pump failed (dropped connection). Reset so the next start()
            # reconnects and re-LISTENs; the backend reconciles the durable
            # table before the next wait, so no marker is lost.
            await self.close()
            return False
        return True

    async def close(self) -> "None":
        await self._pending_read.aclose()
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        with suppress(Exception):
            await connection.execute(self._unlisten_statement())
        with suppress(Exception):
            await connection.close()

    async def _connect(self) -> "Any":
        try:
            psycopg = import_module("psycopg")
        except ImportError as exc:
            msg = "SQLAlchemyBackendConfig.worker_wakeups=True for postgresql+psycopg requires psycopg."
            raise QueueConfigurationError(msg) from exc
        return await cast("Any", psycopg).AsyncConnection.connect(self._conninfo, autocommit=True)

    async def _read_one(self) -> "None":
        connection = self._connection
        if connection is None:
            return
        async for _notify in connection.notifies(stop_after=1):
            return

    def _listen_statement(self) -> "Any":
        return self._compose("LISTEN {}")

    def _unlisten_statement(self) -> "Any":
        return self._compose("UNLISTEN {}")

    def _compose(self, template: "str") -> "Any":
        psycopg_sql = import_module("psycopg.sql")
        return cast("Any", psycopg_sql).SQL(template).format(cast("Any", psycopg_sql).Identifier(self._channel))
