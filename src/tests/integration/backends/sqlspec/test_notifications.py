"""Notification-channel tests for the SQLSpec queue backend.

Covers ``event_channel`` wiring, derivation from extension config,
explicit-override precedence, and the per-adapter wakeup-transport gate
(``notify`` / ``notify_queue`` / ``poll_queue`` / ``polling``). Adapter-agnostic
cases pin to the aiosqlite adapter so the StubAsyncEventChannel can be injected
without bringing up a real LISTEN/NOTIFY service; one case exercises the real
Postgres NOTIFY path against a live container.
"""

import asyncio
import contextlib
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import pytest

pytest.importorskip("aiosqlite")
pytest.importorskip("sqlspec")

from litestar import Litestar
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.extensions.litestar import SQLSpecPlugin

from litestar_queues import QueueConfig, QueuePlugin
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend
from litestar_queues.backends.sqlspec.backend import _adapter_notify_transport
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.exceptions import QueueConfigurationError
from tests.integration.backends.sqlspec.conftest import StubAsyncEventChannel

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from pytest import FixtureRequest
    from pytest_databases.docker.oracle import OracleService
    from sqlspec.extensions.events import AsyncEventChannel

    from litestar_queues.backends.sqlspec._typing import SQLSpecManager, SQLSpecSessionConfig
    from tests.integration._backends import PostgresService
    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio

_ORACLE_AQ_QUEUE_TABLE = "LQ_EVENTS_AQ_TABLE"
_ORACLE_AQ_QUEUE = "LQ_EVENTS_AQ_QUEUE"
_ORACLE_TXEVENTQ_QUEUE = "LQ_EVENTS_TXQ"


class RecordingPollIntervalEventChannel(StubAsyncEventChannel):
    """Stub channel that records the poll interval passed to ``iter_events``."""

    __slots__ = ("poll_intervals",)

    def __init__(self, backend_name: "str" = "poll_queue") -> "None":
        super().__init__(backend_name=backend_name)
        self.poll_intervals: "list[float | None]" = []

    async def iter_events(self, channel: "str", *, poll_interval: "float | None" = None) -> "Any":
        self.poll_intervals.append(poll_interval)
        async for event in super().iter_events(channel, poll_interval=poll_interval):
            yield event


class CountingIterEventsChannel(StubAsyncEventChannel):
    """Stub channel that counts how many event iterators are created."""

    __slots__ = ("iter_events_calls",)

    def __init__(self, backend_name: "str" = "poll_queue") -> "None":
        super().__init__(backend_name=backend_name)
        self.iter_events_calls = 0

    async def iter_events(self, channel: "str", *, poll_interval: "float | None" = None) -> "Any":
        self.iter_events_calls += 1
        async for event in super().iter_events(channel, poll_interval=poll_interval):
            yield event


class FailingIterEventsChannel(CountingIterEventsChannel):
    """Stub channel whose first ``anext`` raises to model a driver read failure."""

    __slots__ = ("fail_next",)

    def __init__(self, backend_name: "str" = "poll_queue") -> "None":
        super().__init__(backend_name=backend_name)
        self.fail_next = True

    async def iter_events(self, channel: "str", *, poll_interval: "float | None" = None) -> "Any":
        self.iter_events_calls += 1
        if self.fail_next:
            self.fail_next = False
            msg = "event stream boom"
            raise RuntimeError(msg)
        async for event in StubAsyncEventChannel.iter_events(self, channel, poll_interval=poll_interval):
            yield event


def _oracle_sync_config(
    oracle_service: "OracleService", *, user: "str | None" = None, password: "str | None" = None
) -> "Any":
    from sqlspec.adapters.oracledb import OracleSyncConfig

    return OracleSyncConfig(
        connection_config={
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": user or oracle_service.user,
            "password": password or oracle_service.password,
        }
    )


def _oracle_async_config(oracle_service: "OracleService", *, backend_name: "str", aq_queue: "str") -> "Any":
    from sqlspec.adapters.oracledb import OracleAsyncConfig

    return OracleAsyncConfig(
        connection_config={
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
            "min": 1,
            "max": 5,
        },
        extension_config={"events": {"backend": backend_name, "aq_queue": aq_queue, "aq_wait_seconds": 1}},
    )


@contextlib.contextmanager
def _classic_aq_queue(oracle_service: "OracleService") -> "Iterator[None]":
    """Provision a classic Advanced Queuing queue for the Oracle notification smoke.

    Yields:
        Control while the AQ queue exists.
    """

    config = _oracle_sync_config(oracle_service)
    try:
        with config.provide_session() as session:
            session.execute_script(
                f"""
                DECLARE
                    table_count INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO table_count
                    FROM user_queue_tables
                    WHERE queue_table = '{_ORACLE_AQ_QUEUE_TABLE}';
                    IF table_count = 0 THEN
                        dbms_aqadm.create_queue_table(
                            queue_table => '{_ORACLE_AQ_QUEUE_TABLE}',
                            queue_payload_type => 'JSON'
                        );
                    END IF;
                    BEGIN
                        dbms_aqadm.create_queue(
                            queue_name => '{_ORACLE_AQ_QUEUE}',
                            queue_table => '{_ORACLE_AQ_QUEUE_TABLE}'
                        );
                    EXCEPTION
                        WHEN OTHERS THEN
                            IF SQLCODE != -24005 THEN
                                RAISE;
                            END IF;
                    END;
                    dbms_aqadm.start_queue(queue_name => '{_ORACLE_AQ_QUEUE}');
                END;
                """
            )
            session.commit()
    except SQLSpecError as exc:
        with suppress(Exception):
            config.close_pool()
        pytest.skip(f"Oracle AQ queue provisioning unavailable: {exc}")
    try:
        yield
    finally:
        with suppress(Exception), config.provide_session() as session:
            session.execute_script(
                f"""
                    BEGIN
                        BEGIN
                            dbms_aqadm.stop_queue(queue_name => '{_ORACLE_AQ_QUEUE}');
                        EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN
                            dbms_aqadm.drop_queue(queue_name => '{_ORACLE_AQ_QUEUE}');
                        EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN
                            dbms_aqadm.drop_queue_table(queue_table => '{_ORACLE_AQ_QUEUE_TABLE}');
                        EXCEPTION WHEN OTHERS THEN NULL; END;
                    END;
                    """
            )
        with suppress(Exception):
            config.close_pool()


@contextlib.contextmanager
def _txeventq_queue(oracle_service: "OracleService") -> "Iterator[None]":
    """Provision a Transactional Event Queue for the Oracle notification smoke.

    Yields:
        Control while the TxEventQ queue exists.
    """

    config = _oracle_sync_config(oracle_service)
    try:
        with config.provide_session() as session:
            session.execute_script(
                f"""
                DECLARE
                    queue_count INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO queue_count
                    FROM user_queues
                    WHERE name = '{_ORACLE_TXEVENTQ_QUEUE}';
                    IF queue_count = 0 THEN
                        dbms_aqadm.create_transactional_event_queue(
                            queue_name => '{_ORACLE_TXEVENTQ_QUEUE}',
                            queue_payload_type => 'JSON',
                            multiple_consumers => FALSE
                        );
                    END IF;
                    dbms_aqadm.start_queue(queue_name => '{_ORACLE_TXEVENTQ_QUEUE}');
                END;
                """
            )
            session.commit()
    except SQLSpecError as exc:
        with suppress(Exception):
            config.close_pool()
        pytest.skip(f"Oracle TxEventQ provisioning unavailable: {exc}")
    try:
        yield
    finally:
        with suppress(Exception), config.provide_session() as session:
            session.execute_script(
                f"""
                    BEGIN
                        BEGIN
                            dbms_aqadm.stop_queue(queue_name => '{_ORACLE_TXEVENTQ_QUEUE}');
                        EXCEPTION WHEN OTHERS THEN NULL; END;
                        BEGIN
                            dbms_aqadm.drop_transactional_event_queue(queue_name => '{_ORACLE_TXEVENTQ_QUEUE}');
                        EXCEPTION WHEN OTHERS THEN NULL; END;
                    END;
                    """
            )
        with suppress(Exception):
            config.close_pool()


@pytest.fixture(scope="session")
def oracle_aq_privileges(request: "FixtureRequest") -> "None":
    """Grant the app user Oracle AQ privileges when the 23ai fixture is available."""

    pytest.importorskip("oracledb")
    try:
        oracle_service = cast("OracleService", request.getfixturevalue("oracle_23ai_service"))
    except pytest.FixtureLookupError:
        pytest.skip("Oracle 23ai fixture is not available")
    system_password = getattr(oracle_service, "system_password", None)
    if system_password is None:
        pytest.skip("Oracle service fixture does not expose a system password for AQ grants")

    config = _oracle_sync_config(oracle_service, user="system", password=cast("str", system_password))
    app_user = oracle_service.user
    try:
        with config.provide_session() as session:
            for grant in (
                f"GRANT aq_administrator_role, aq_user_role TO {app_user}",
                f"GRANT EXECUTE ON dbms_aq TO {app_user}",
            ):
                session.execute_script(grant)
            session.commit()
    except SQLSpecError as exc:
        pytest.skip(f"Oracle AQ privileges unavailable: {exc}")
    finally:
        with suppress(Exception):
            config.close_pool()


async def test_sqlspec_backend_event_channel_notifications_wake_waiters(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = StubAsyncEventChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "notifications.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="queue_notifications",
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        await backend.enqueue("tasks.notified", queue="critical", execution_backend="local")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "poll_queue"
        assert backend.capabilities.notifications_durable is True
        assert event_channel.published == [
            ("queue_notifications", {"event": "task_available"}, {"event_type": "litestar_queues.task_available"})
        ]
        assert event_channel.acked == ["event-1"]
    finally:
        await backend.close()


async def test_sqlspec_backend_reuses_one_event_stream_across_timeouts(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = CountingIterEventsChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "reuse.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="reuse",
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert await backend.wait_for_notifications(timeout=0.05) is False
        assert await backend.wait_for_notifications(timeout=0.05) is False
        assert await backend.wait_for_notifications(timeout=0.05) is False
        assert event_channel.iter_events_calls == 1
        assert backend._pending_read.has_pending is True

        await backend.enqueue("tasks.reuse", queue="critical", execution_backend="local")

        # A notification arriving after timeouts wakes the retained read without a new iterator.
        assert await backend.wait_for_notifications(timeout=1) is True
        assert event_channel.iter_events_calls == 1
        assert event_channel.acked == ["event-1"]
        assert bool(backend._pending_read.has_pending) is False
    finally:
        await backend.close()


async def test_sqlspec_backend_close_while_reading_leaves_no_stream(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = CountingIterEventsChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "close-reading.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="close_reading",
        )
    )

    await backend.open()
    await backend.create_schema()
    assert await backend.wait_for_notifications(timeout=0.05) is False
    assert backend._pending_read.has_pending is True

    await backend.close()
    assert bool(backend._pending_read.has_pending) is False
    assert backend._event_stream is None
    # Double close is idempotent.
    await backend.close()


async def test_sqlspec_backend_read_error_resets_stream_and_recovers(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = FailingIterEventsChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "read-error.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="read_error",
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        with pytest.raises(RuntimeError, match="event stream boom"):
            await backend.wait_for_notifications(timeout=1)
        assert backend._event_stream is None
        assert bool(backend._pending_read.has_pending) is False

        # A bounded re-establishment builds a fresh stream that reconciles normally.
        await backend.enqueue("tasks.recover", execution_backend="local")
        assert await backend.wait_for_notifications(timeout=1) is True
        assert event_channel.iter_events_calls == 2
    finally:
        await backend.close()


async def test_sqlspec_backend_worker_timeout_does_not_set_event_poll_interval(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = RecordingPollIntervalEventChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "worker-timeout.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="worker_timeout",
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=0.25))
        await backend.enqueue("tasks.worker_timeout")

        assert await waiter is True
        assert event_channel.poll_intervals == [None]
    finally:
        await backend.close()


async def test_sqlspec_backend_event_poll_interval_is_passed_to_event_channel(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    event_channel = RecordingPollIntervalEventChannel()
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / "event-poll-interval.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
            notification_channel="event_poll_interval",
            event_poll_interval=0.01,
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=0.25))
        await backend.enqueue("tasks.event_poll_interval")

        assert await waiter is True
        assert event_channel.poll_intervals == [0.01]
    finally:
        await backend.close()


async def test_sqlspec_backend_derives_sqlspec_event_channel_from_config(tmp_path: "Path") -> "None":
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "derived-notifications.db")},
        extension_config={"events": {"backend": "poll_queue", "poll_interval": 0.01, "queue_table": "queue_events"}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config, notifications=True, notification_channel="derived_notifications"
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=1))
        await backend.enqueue("tasks.derived_notified")

        assert await waiter is True
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "poll_queue"
        assert backend.capabilities.notifications_durable is True
    finally:
        await backend.close()


async def test_sqlspec_backend_notification_channel_uses_extension_config_with_explicit_override(
    tmp_path: "Path",
) -> "None":
    extension_channel = StubAsyncEventChannel()
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "extension-notifications.db")},
        extension_config={QUEUE_EXTENSION_NAME: {"notification_channel": "extension_notifications"}},
    )
    extension_backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config, event_channel=cast("AsyncEventChannel", extension_channel)
        )
    )
    await extension_backend.open()
    await extension_backend.create_schema()
    try:
        await extension_backend.enqueue("tasks.extension_notified")
    finally:
        await extension_backend.close()

    explicit_channel = StubAsyncEventChannel()
    explicit_backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            event_channel=cast("AsyncEventChannel", explicit_channel),
            notification_channel="explicit_notifications",
            queue_table_name="explicit_notification_queue",
        )
    )
    await explicit_backend.open()
    await explicit_backend.create_schema()
    try:
        await explicit_backend.enqueue("tasks.explicit_notified")
    finally:
        await explicit_backend.close()

    assert extension_channel.published[0][0] == "extension_notifications"
    assert explicit_channel.published[0][0] == "explicit_notifications"


async def test_sqlspec_backend_uses_user_registered_litestar_sqlspec_plugin(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    sqlspec = SQLSpec()
    sqlspec_config = sqlite_config_factory(tmp_path / "litestar.db")
    sqlspec.add_config(sqlspec_config)

    plugin = QueuePlugin(QueueConfig(queue_backend=SQLSpecBackendConfig(sqlspec=sqlspec), initialize_schedules=False))

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec), plugin])

    assert "db_session" in app.dependencies
    assert "AiosqliteDriver" in app.signature_namespace
    assert QUEUE_EXTENSION_NAME in sqlspec_config.get_migration_commands().extension_configs


def test_queue_plugin_registers_events_migration_for_capable_adapter() -> "None":
    """A capability-native adapter auto-registers SQLSpec's events queue migration.

    Registering the events extension makes SQLSpec provision the durable events
    queue table on migrate-up, so a zero-config native-wakeup backend works on a
    fresh database with no manual step. Incapable adapters register nothing.
    """
    from click import Group

    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    capable_config = AsyncpgConfig(
        connection_config={"host": "localhost", "port": 5432, "user": "u", "password": "p", "database": "d"}
    )
    QueuePlugin(
        QueueConfig(queue_backend=SQLSpecBackendConfig(config=capable_config), initialize_schedules=False)
    ).on_cli_init(Group())
    assert (capable_config.extension_config or {}).get("events") == {"backend": "notify_queue"}
    assert "events" in capable_config.migration_config.get("include_extensions", [])

    polling_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    QueuePlugin(
        QueueConfig(queue_backend=SQLSpecBackendConfig(config=polling_config), initialize_schedules=False)
    ).on_cli_init(Group())
    assert "events" not in (polling_config.extension_config or {})


async def test_sqlspec_backend_migration_path_provisions_events_table(postgres_service: "PostgresService") -> "None":
    """The plugin's migration registration provisions the events table for native wakeups.

    Runs the SQLSpec migration path only (no ``create_schema``): the plugin
    registers both the queue and the durable events queue migration, ``migrate_up``
    creates both tables, and a bare asyncpg backend then wakes via NOTIFY on a
    fresh database. Uses the default table names, which the queue migration
    provisions.
    """
    from click import Group

    from litestar_queues.backends.sqlspec.schema import DEFAULT_TABLE_NAME

    pytest.importorskip("asyncpg")

    # Start from a clean slate: another test or run may share this database.
    cleanup_backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=_postgres_config("asyncpg", postgres_service))
    )
    await cleanup_backend.open()
    await _drop_postgres_tables(cleanup_backend, DEFAULT_TABLE_NAME, "ddl_migrations")

    sqlspec_config = _postgres_config("asyncpg", postgres_service)
    backend_config = SQLSpecBackendConfig(config=sqlspec_config)
    QueuePlugin(QueueConfig(queue_backend=backend_config, initialize_schedules=False)).on_cli_init(Group())
    await sqlspec_config.migrate_up(echo=False)

    backend = SQLSpecQueueBackend(backend_config=backend_config)
    await backend.open()
    # No create_schema: the migration path already provisioned both tables.
    try:
        assert backend.capabilities.notification_backend == "notify_queue"

        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=5))
        await asyncio.sleep(0.3)
        start = time.monotonic()
        await backend.enqueue("tasks.migrate_wake")
        assert await waiter is True
        assert time.monotonic() - start < 0.9
    finally:
        await _drop_postgres_tables(backend, DEFAULT_TABLE_NAME, "ddl_migrations")


@pytest.mark.parametrize(
    ("adapter_name", "expected_transport"),
    [
        ("asyncpg", "notify_queue"),
        ("psycopg", "notify_queue"),
        ("psqlpy", "notify_queue"),
        ("cockroach_asyncpg", "polling"),
        ("cockroach_psycopg", "polling"),
        ("aiosqlite", "polling"),
        ("sqlite", "polling"),
        ("duckdb", "poll_queue"),
        ("asyncmy", "polling"),
        ("aiomysql", "polling"),
        ("pymysql", "polling"),
        ("mysqlconnector", "polling"),
        ("oracledb", "polling"),
        (None, "polling"),
    ],
)
def test_adapter_notify_transport_capability_gate(adapter_name: "str | None", expected_transport: "str") -> "None":
    """The wakeup transport is gated by adapter knowledge.

    Every real Postgres driver (asyncpg, psycopg, psqlpy) gets the durable native
    ``notify_queue`` LISTEN/NOTIFY hybrid; DuckDB uses the in-process durable
    ``poll_queue``; every other family reports ``polling``.
    """
    assert _adapter_notify_transport(adapter_name) == expected_transport


@pytest.mark.parametrize(
    ("notifications_requested", "transport", "expected"),
    [
        # Default (None): on whenever the resolved transport is capability-native.
        (None, "notify_queue", True),
        (None, "poll_queue", True),
        (None, "polling", False),
        # Explicit opt-out is preserved even on a capable transport.
        (False, "notify_queue", False),
        (False, "polling", False),
        # Explicit opt-in enables when capable and degrades to polling otherwise.
        (True, "notify_queue", True),
        (True, "polling", False),
    ],
)
def test_notifications_should_enable_matrix(
    tmp_path: "Path", notifications_requested: "bool | None", transport: "str", expected: "bool"
) -> "None":
    """Native wakeups are default-on when capable; ``False`` opts out; polling stays polling."""
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=AiosqliteConfig(connection_config={"database": ":memory:"}))
    )
    assert backend._notifications_should_enable(notifications_requested, transport) is expected


def test_notify_transport_config_rejects_unknown_value() -> "None":
    with pytest.raises(QueueConfigurationError):
        SQLSpecBackendConfig(notify_transport="not-a-transport")


def test_queue_plugin_registers_sqlspec_migrations_for_cli() -> "None":
    """CLI initialization exposes queue migrations to SQLSpec's database command."""
    from click import Group

    sqlspec_config = AiosqliteConfig(connection_config={"database": ":memory:"})
    plugin = QueuePlugin(QueueConfig(queue_backend=SQLSpecBackendConfig(config=sqlspec_config)))

    plugin.on_cli_init(Group())

    assert QUEUE_EXTENSION_NAME in sqlspec_config.get_migration_commands().extension_configs


@pytest.mark.parametrize("transport", ("listen_notify", "listen_notify_durable", "table_queue"))
def test_notify_transport_config_rejects_legacy_public_names(transport: "str") -> "None":
    with pytest.raises(QueueConfigurationError):
        SQLSpecBackendConfig(notify_transport=transport)


@pytest.mark.parametrize("transport", ("listen_notify", "listen_notify_durable", "table_queue"))
async def test_sqlspec_backend_rejects_legacy_extension_event_backend(transport: "str", tmp_path: "Path") -> "None":
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / f"legacy-{transport}.db")},
        extension_config={"events": {"backend": transport}},
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config, notifications=True))

    with pytest.raises(QueueConfigurationError, match="expected one of"):
        await backend.open()


@pytest.mark.parametrize("transport", ("aq", "notify", "notify_queue", "poll_queue", "txeventq"))
def test_sqlspec_backend_forwards_canonical_extension_event_backend(transport: "str") -> "None":
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"events": {"backend": transport}}
    )
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config, notifications=True))

    assert backend._select_notify_transport(sqlspec_config, {}, {"backend": transport}) == transport


@pytest.mark.parametrize("transport", ("aq", "txeventq"))
def test_notify_transport_config_accepts_oracle_event_backends(transport: "str") -> "None":
    """Oracle AQ and TxEventQ backend names are valid SQLSpec event transports."""
    config = SQLSpecBackendConfig(notify_transport=transport)

    assert config.notify_transport == transport


@pytest.mark.parametrize("backend_name", ("aq", "txeventq"))
async def test_oracle_event_backend_names_are_reported_durable(
    backend_name: "str", tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """Injected Oracle event channels report durable queue wakeups."""
    event_channel = StubAsyncEventChannel(backend_name=backend_name)
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlite_config_factory(tmp_path / f"{backend_name}-durable.db"),
            event_channel=cast("AsyncEventChannel", event_channel),
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == backend_name
        assert backend.capabilities.notifications_durable is True
    finally:
        await backend.close()


@pytest.mark.xdist_group("oracle")
@pytest.mark.parametrize(
    ("event_backend", "aq_queue"), [("aq", _ORACLE_AQ_QUEUE), ("txeventq", _ORACLE_TXEVENTQ_QUEUE)]
)
async def test_sqlspec_backend_oracle_event_transports_wake_waiters(
    request: "FixtureRequest", event_backend: "str", aq_queue: "str"
) -> "None":
    """Oracle AQ and TxEventQ wake ``wait_for_notifications`` through SQLSpec events."""

    pytest.importorskip("oracledb")
    oracle_service = cast("OracleService", request.getfixturevalue("oracle_23ai_service"))
    request.getfixturevalue("oracle_aq_privileges")

    queue_manager = _classic_aq_queue(oracle_service) if event_backend == "aq" else _txeventq_queue(oracle_service)
    table_name = f"LQ_NOTIFY_{event_backend.upper()}"
    sqlspec_config = _oracle_async_config(oracle_service, backend_name=event_backend, aq_queue=aq_queue)
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            queue_table_name=table_name,
            notifications=True,
            notify_transport=event_backend,
            event_settings={"aq_queue": aq_queue, "aq_wait_seconds": 1},
            event_poll_interval=0.1,
        )
    )

    with queue_manager:
        await backend.open()
        await backend.create_schema()
        try:
            assert backend.capabilities.supports_notifications is True
            assert backend.capabilities.notification_backend == event_backend
            assert backend.capabilities.notifications_durable is True

            waiter = asyncio.create_task(backend.wait_for_notifications(timeout=10))
            await asyncio.sleep(0.5)
            await backend.enqueue(f"tasks.oracle_{event_backend}_wake")
            assert await waiter is True
        finally:
            from litestar_queues.backends.sqlspec.backend import _bridge_session

            with suppress(Exception):
                assert backend._sqlspec is not None
                assert backend._sqlspec_config is not None
                async with _bridge_session(
                    cast("SQLSpecManager", backend._sqlspec), cast("SQLSpecSessionConfig", backend._sqlspec_config)
                ) as driver:
                    await driver.execute_script(f'DROP TABLE IF EXISTS "{table_name}"')
            await backend.close()


async def test_sqlspec_backend_non_notify_adapter_polls(
    tmp_path: "Path", sqlite_config_factory: "SqliteConfigFactory"
) -> "None":
    """A non-notify adapter degrades requested notifications to polling."""
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlite_config_factory(tmp_path / "polling.db"), notifications=True)
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is False
        assert backend.capabilities.notification_backend is None

        start = time.monotonic()
        woke = await backend.wait_for_notifications(timeout=0.05)
        elapsed = time.monotonic() - start

        assert woke is False
        assert elapsed >= 0.04
    finally:
        await backend.close()


async def test_sqlspec_backend_notify_transport_override_enables_poll_queue(tmp_path: "Path") -> "None":
    """An explicit ``notify_transport`` overrides the adapter's polling default."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": str(tmp_path / "override-poll-queue.db")})
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            notify_transport="poll_queue",
            notification_channel="override_poll_queue",
            event_poll_interval=0.01,
        )
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "poll_queue"
        assert backend.capabilities.notifications_durable is True

        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=2))
        await backend.enqueue("tasks.override_poll_queue")
        assert await waiter is True
    finally:
        await backend.close()


async def test_sqlspec_backend_duckdb_defaults_to_poll_queue(tmp_path: "Path") -> "None":
    """A bare DuckDB config gets the durable ``poll_queue`` transport with zero config.

    DuckDB is embedded with no LISTEN/NOTIFY, so its capability-native default is
    the in-process durable table queue. ``create_schema`` alone provisions the
    events queue table, so no manual migration is needed.
    """
    pytest.importorskip("duckdb")
    from sqlspec.adapters.duckdb import DuckDBConfig

    sqlspec_config = DuckDBConfig(connection_config={"database": str(tmp_path / "duckdb-poll-queue.db")})
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(config=sqlspec_config, event_poll_interval=0.01))

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "poll_queue"
        assert backend.capabilities.notifications_durable is True

        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=2))
        await backend.enqueue("tasks.duckdb_poll_queue")
        assert await waiter is True
    finally:
        await backend.close()


async def test_sqlspec_backend_notify_transport_polling_overrides_extension_config(tmp_path: "Path") -> "None":
    """``queue_backend_config`` polling override beats an ``extension_config`` events default."""
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "override-polling.db")},
        extension_config={"events": {"backend": "poll_queue", "poll_interval": 0.01}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlspec_config, notify_transport="polling")
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is False
        assert backend.capabilities.notification_backend is None
    finally:
        await backend.close()


async def test_sqlspec_backend_postgres_notify_queue_wakes(
    postgres_service: "PostgresService", tmp_path: "Path"
) -> "None":
    """A bare asyncpg config wakes via NOTIFY with a durable fallback, no missed bursts.

    No notification settings and no manual migration step: ``create_schema`` alone
    provisions the events queue table and native ``notify_queue`` wakeups are on by
    default because asyncpg is capability-native.
    """
    pytest.importorskip("asyncpg")
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    sqlspec_config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlspec_config, queue_table_name="lq_notify_asyncpg")
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "notify_queue"
        assert backend.capabilities.notifications_durable is True

        # NOTIFY wake: subscribe first, then enqueue, and measure latency.
        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=5))
        await asyncio.sleep(0.2)
        start = time.monotonic()
        await backend.enqueue("tasks.pg_wake")
        assert await waiter is True
        assert time.monotonic() - start < 1.0

        # Burst: every enqueue must remain observable via the durable fallback.
        burst = 5
        for index in range(burst):
            await backend.enqueue("tasks.pg_burst", kwargs={"index": index})
        for _ in range(burst):
            assert await backend.wait_for_notifications(timeout=5) is True
    finally:
        from litestar_queues.backends.sqlspec.backend import _bridge_session

        with suppress(Exception):
            assert backend._sqlspec is not None
            assert backend._sqlspec_config is not None
            async with _bridge_session(
                cast("SQLSpecManager", backend._sqlspec), cast("SQLSpecSessionConfig", backend._sqlspec_config)
            ) as driver:
                for ddl in ('DROP TABLE IF EXISTS "lq_notify_asyncpg"', 'DROP TABLE IF EXISTS "sqlspec_event_queue"'):
                    await driver.execute_script(ddl)
            await backend.close()


def _postgres_config(adapter_name: "str", postgres_service: "PostgresService") -> "Any":
    """Return a bare SQLSpec config for a Postgres driver against the live service."""
    if adapter_name == "asyncpg":
        pytest.importorskip("asyncpg")
        from sqlspec.adapters.asyncpg import AsyncpgConfig

        return AsyncpgConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
            }
        )
    if adapter_name == "psycopg":
        pytest.importorskip("psycopg")
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig

        return PsycopgAsyncConfig(
            connection_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "dbname": postgres_service.database,
            }
        )
    pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy import PsqlpyConfig

    return PsqlpyConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "username": postgres_service.user,
            "password": postgres_service.password,
            "db_name": postgres_service.database,
        }
    )


async def _drop_postgres_tables(backend: "SQLSpecQueueBackend", *table_names: "str") -> "None":
    from litestar_queues.backends.sqlspec.backend import _bridge_session

    with suppress(Exception):
        assert backend._sqlspec is not None
        assert backend._sqlspec_config is not None
        async with _bridge_session(
            cast("SQLSpecManager", backend._sqlspec), cast("SQLSpecSessionConfig", backend._sqlspec_config)
        ) as driver:
            for table_name in (*table_names, "sqlspec_event_queue"):
                await driver.execute_script(f'DROP TABLE IF EXISTS "{table_name}"')
    await backend.close()


@pytest.mark.parametrize("adapter_name", ["asyncpg", "psycopg", "psqlpy"])
async def test_sqlspec_backend_postgres_native_wakeup_default_on(
    adapter_name: "str", postgres_service: "PostgresService"
) -> "None":
    """Every Postgres driver gets native NOTIFY wakeups from a bare, zero-config backend.

    No notification settings and no manual migration: ``create_schema`` provisions
    the events queue table, and the enqueue wakes the waiter via LISTEN/NOTIFY far
    faster than the default one-second poll interval.
    """
    sqlspec_config = _postgres_config(adapter_name, postgres_service)
    table_name = f"lq_native_{adapter_name}"
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlspec_config, queue_table_name=table_name)
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "notify_queue"
        assert backend.capabilities.notifications_durable is True

        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=5))
        await asyncio.sleep(0.3)
        start = time.monotonic()
        await backend.enqueue(f"tasks.{adapter_name}_wake")
        assert await waiter is True
        # A real NOTIFY wake resolves well under the 1s default poll interval.
        assert time.monotonic() - start < 0.9
    finally:
        await _drop_postgres_tables(backend, table_name)


@pytest.mark.parametrize("adapter_name", ["asyncpg", "psycopg", "psqlpy"])
async def test_sqlspec_backend_postgres_notifications_false_forces_polling(
    adapter_name: "str", postgres_service: "PostgresService"
) -> "None":
    """``notifications=False`` opts a capability-native Postgres driver out to polling."""
    sqlspec_config = _postgres_config(adapter_name, postgres_service)
    table_name = f"lq_optout_{adapter_name}"
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(config=sqlspec_config, queue_table_name=table_name, notifications=False)
    )

    await backend.open()
    await backend.create_schema()
    try:
        assert backend.capabilities.supports_notifications is False
        assert backend.capabilities.notification_backend is None

        start = time.monotonic()
        assert await backend.wait_for_notifications(timeout=0.05) is False
        assert time.monotonic() - start >= 0.04
    finally:
        await _drop_postgres_tables(backend, table_name)
