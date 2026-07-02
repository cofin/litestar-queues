"""Notification-channel tests for the SQLSpec queue backend.

Covers ``event_channel`` wiring, derivation from extension config,
explicit-override precedence, and the per-adapter wakeup-transport gate
(``listen_notify_durable`` / ``table_queue`` / ``polling``). Adapter-agnostic
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

    from tests.integration._backends import PostgresService
    from tests.integration.backends.sqlspec.conftest import SqliteConfigFactory

pytestmark = pytest.mark.anyio

_ORACLE_AQ_QUEUE_TABLE = "LQ_EVENTS_AQ_TABLE"
_ORACLE_AQ_QUEUE = "LQ_EVENTS_AQ_QUEUE"
_ORACLE_TXEVENTQ_QUEUE = "LQ_EVENTS_TXQ"


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


async def test_sqlspec_backend_derives_sqlspec_event_channel_from_config(tmp_path: "Path") -> "None":
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "derived-notifications.db")},
        extension_config={"events": {"backend": "table_queue", "poll_interval": 0.01, "queue_table": "queue_events"}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            create_schema=False,
            run_migrations=True,
            notifications=True,
            notification_channel="derived_notifications",
        )
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
            table_name="explicit_notification_queue",
        )
    )
    await explicit_backend.open()
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

    plugin = QueuePlugin(
        QueueConfig(
            queue_backend=SQLSpecBackendConfig(sqlspec=sqlspec, create_schema=False), initialize_schedules=False
        )
    )

    app = Litestar(plugins=[SQLSpecPlugin(sqlspec), plugin])

    assert "db_session" in app.dependencies
    assert "AiosqliteDriver" in app.signature_namespace


@pytest.mark.parametrize(
    ("adapter_name", "expected_transport"),
    [
        ("asyncpg", "listen_notify_durable"),
        ("psycopg", "table_queue"),
        ("psqlpy", "table_queue"),
        ("aiosqlite", "polling"),
        ("sqlite", "polling"),
        ("duckdb", "polling"),
        ("asyncmy", "polling"),
        ("aiomysql", "polling"),
        ("mysqlconnector", "polling"),
        ("oracledb", "polling"),
        (None, "polling"),
    ],
)
def test_adapter_notify_transport_capability_gate(adapter_name: "str | None", expected_transport: "str") -> "None":
    """The wakeup transport is gated by adapter knowledge.

    Postgres-over-asyncpg gets the durable LISTEN/NOTIFY hybrid; psycopg/psqlpy
    fall back to the durable table queue until their LISTEN/NOTIFY path lands
    upstream; every other family polls.
    """
    assert _adapter_notify_transport(adapter_name) == expected_transport


def test_notify_transport_config_rejects_unknown_value() -> "None":
    with pytest.raises(QueueConfigurationError):
        SQLSpecBackendConfig(notify_transport="not-a-transport")


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
            table_name=table_name,
            notifications=True,
            notify_transport=event_backend,
            event_settings={"aq_queue": aq_queue, "aq_wait_seconds": 1},
            event_poll_interval=0.1,
        )
    )

    with queue_manager:
        await backend.open()
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
                async with _bridge_session(backend._sqlspec, backend._sqlspec_config) as driver:
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


async def test_sqlspec_backend_notify_transport_override_enables_table_queue(tmp_path: "Path") -> "None":
    """An explicit ``notify_transport`` overrides the adapter's polling default."""
    sqlspec_config = AiosqliteConfig(connection_config={"database": str(tmp_path / "override-table-queue.db")})
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            notify_transport="table_queue",
            create_schema=False,
            run_migrations=True,
            notification_channel="override_table_queue",
            event_poll_interval=0.01,
        )
    )

    await backend.open()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "table_queue"
        assert backend.capabilities.notifications_durable is True

        waiter = asyncio.create_task(backend.wait_for_notifications(timeout=2))
        await backend.enqueue("tasks.override_table_queue")
        assert await waiter is True
    finally:
        await backend.close()


async def test_sqlspec_backend_notify_transport_polling_overrides_extension_config(tmp_path: "Path") -> "None":
    """``queue_backend_config`` polling override beats an ``extension_config`` events default."""
    sqlspec_config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "override-polling.db")},
        extension_config={"events": {"backend": "table_queue", "poll_interval": 0.01}},
    )
    backend = SQLSpecQueueBackend(
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config, notify_transport="polling", create_schema=False, run_migrations=True
        )
    )

    await backend.open()
    try:
        assert backend.capabilities.supports_notifications is False
        assert backend.capabilities.notification_backend is None
    finally:
        await backend.close()


async def test_sqlspec_backend_postgres_listen_notify_durable_wakes(
    postgres_service: "PostgresService", tmp_path: "Path"
) -> "None":
    """Postgres workers wake via NOTIFY with a durable fallback, no missed bursts."""
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
        backend_config=SQLSpecBackendConfig(
            config=sqlspec_config,
            table_name="lq_notify_asyncpg",
            notifications=True,
            create_schema=False,
            run_migrations=True,
            notification_channel="lq_asyncpg_wake",
            event_poll_interval=0.05,
        )
    )

    await backend.open()
    try:
        assert backend.capabilities.supports_notifications is True
        assert backend.capabilities.notification_backend == "listen_notify_durable"
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
            async with _bridge_session(backend._sqlspec, backend._sqlspec_config) as driver:
                for ddl in (
                    'DROP TABLE IF EXISTS "lq_notify_asyncpg"',
                    'DROP TABLE IF EXISTS "sqlspec_async_events"',
                    'DROP TABLE IF EXISTS "ddl_migrations"',
                ):
                    await driver.execute_script(ddl)
        await backend.close()
