"""One cross-backend contract for durable event-history query, summary, and retention.

Covers every filter and combination, both stable orders, offset/limit and empty
pages, scoped summaries, latest-sequence ties, worst-level aggregation, and
filtered bounded retention convergence, on all six backend families.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from litestar_queues import EventDeliveryConfig, QueueConfig, QueueService, WorkerConfig
from litestar_queues.events import (
    EventHistoryConfig,
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventQuery,
    QueueEventsConfig,
)
from litestar_queues.events.history import QueueEventLog
from litestar_queues.exceptions import QueueConfigurationError

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
FAMILIES = ("memory", "ephemeral", "sqlspec", "advanced_alchemy", "redis", "valkey")
_HISTORY_BACKEND: pytest.StashKey[Any] = pytest.StashKey()


@pytest.fixture
def history_config(request: pytest.FixtureRequest) -> EventHistoryConfig:
    return getattr(request, "param", EventHistoryConfig(batch_size=1, flush_interval=60))


@pytest.fixture(params=FAMILIES)
async def event_log(request: pytest.FixtureRequest, history_config: EventHistoryConfig) -> AsyncIterator[QueueEventLog]:  # noqa: PLR0915
    """Yield an opened event log for one backend family."""
    import uuid

    family = request.param
    config = history_config

    if family == "memory":
        from litestar_queues.backends.memory import InMemoryQueueBackend

        backend_mem = InMemoryQueueBackend()
        await backend_mem.open()
        if hasattr(backend_mem, "create_schema"):
            await backend_mem.create_schema()
        log_mem = backend_mem.get_event_log(config)
        assert log_mem is not None
        request.node.stash[_HISTORY_BACKEND] = backend_mem
        yield log_mem
        await backend_mem.close()

    elif family == "ephemeral":
        from litestar_queues.backends.ephemeral import EphemeralQueueBackend
        from litestar_queues.backends.ephemeral.server import EphemeralServerContext
        from litestar_queues.config import QueueConfig

        with EphemeralServerContext(nonce="test-nonce"):
            backend_eph = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))
            await backend_eph.open()
            if hasattr(backend_eph, "create_schema"):
                await backend_eph.create_schema()
            log_eph = backend_eph.get_event_log(config)
            assert log_eph is not None
            request.node.stash[_HISTORY_BACKEND] = backend_eph
            yield log_eph
            await backend_eph.close()

    elif family == "sqlspec":
        pytest.importorskip("sqlspec")
        pytest.importorskip("psycopg")
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig

        from litestar_queues import QueueConfig, WorkerConfig
        from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

        try:
            postgres_service = request.getfixturevalue("postgres_service")
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Docker service not available: {e}")

        from tests.integration._names import table_name_for_test

        table = table_name_for_test("queue_task", "sqlspec", request.node.nodeid)

        backend_config = SQLSpecBackendConfig(
            sqlspec_config=PsycopgAsyncConfig(
                connection_config={
                    "host": postgres_service.host,
                    "port": postgres_service.port,
                    "user": postgres_service.user,
                    "password": postgres_service.password,
                    "dbname": postgres_service.database,
                }
            ),
            queue_table_name=table,
        )
        backend_sql = SQLSpecQueueBackend(
            config=QueueConfig(
                worker=WorkerConfig(placement="external"),
                queue_backend=backend_config,
                events=QueueEventsConfig(history=config),
            ),
            backend_config=backend_config,
        )
        await backend_sql.open()
        if hasattr(backend_sql, "create_schema"):
            await backend_sql.create_schema()
        log_sql = backend_sql.get_event_log(config)
        assert log_sql is not None
        request.node.stash[_HISTORY_BACKEND] = backend_sql
        yield log_sql
        await backend_sql.close()

    elif family == "advanced_alchemy":
        pytest.importorskip("advanced_alchemy")
        pytest.importorskip("asyncpg")
        from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

        from litestar_queues.backends.advanced_alchemy import SQLAlchemyBackend, SQLAlchemyBackendConfig

        try:
            postgres = request.getfixturevalue("postgres_service")
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Docker service not available: {e}")

        aa_service = SQLAlchemyAsyncConfig(
            connection_string=f"postgresql+asyncpg://{postgres.user}:{postgres.password}@{postgres.host}:{postgres.port}/{postgres.database}"
        )

        from advanced_alchemy.base import UUIDAuditBase

        from litestar_queues.backends.advanced_alchemy import QueueEventHistoryModelMixin
        from tests.integration._names import table_name_for_test
        from tests.integration.backends.advanced_alchemy._aa_schema import create_tables

        event_history_table_name = table_name_for_test("queue_event_history", "aa", request.node.nodeid)
        event_history_model_class = type(
            f"IntegrationEventHistory_{request.node.nodeid.replace('-', '_').replace(':', '_')}",
            (UUIDAuditBase, QueueEventHistoryModelMixin),
            {"__module__": __name__, "__tablename__": event_history_table_name},
        )

        await create_tables(aa_service, event_history_model_class)

        backend_aa = SQLAlchemyBackend(
            backend_config=SQLAlchemyBackendConfig(
                sqlalchemy_config=aa_service, event_history_model_class=event_history_model_class
            )
        )
        await backend_aa.open()
        log_aa = backend_aa.get_event_log(config)
        assert log_aa is not None
        request.node.stash[_HISTORY_BACKEND] = backend_aa
        try:
            yield log_aa
        finally:
            try:
                await backend_aa.close()
            finally:
                engine = aa_service.get_engine()
                try:
                    async with engine.begin() as connection:
                        await connection.run_sync(
                            UUIDAuditBase.metadata.tables[event_history_table_name].drop, checkfirst=True
                        )
                finally:
                    await engine.dispose()

    elif family == "redis":
        pytest.importorskip("redis.asyncio")
        from litestar_queues.backends.redis import RedisBackendConfig, RedisQueueBackend

        try:
            redis_service = request.getfixturevalue("redis_service")
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Docker service not available: {e}")

        prefix = f"litestar_queues:test:redis:{uuid.uuid4().hex}"
        backend_redis = RedisQueueBackend(
            backend_config=RedisBackendConfig(
                url=f"redis://{redis_service.host}:{redis_service.port}/{redis_service.db}",
                key_prefix=prefix,
                worker_wakeups=True,
                wakeup_channel=f"{prefix}:notifications",
            )
        )
        await backend_redis.open()
        if hasattr(backend_redis, "create_schema"):
            await backend_redis.create_schema()
        log_redis = backend_redis.get_event_log(config)
        assert log_redis is not None
        request.node.stash[_HISTORY_BACKEND] = backend_redis
        yield log_redis
        await backend_redis.close()

    elif family == "valkey":
        pytest.importorskip("valkey.asyncio")
        from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

        try:
            valkey_service = request.getfixturevalue("valkey_service")
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Docker service not available: {e}")

        prefix = f"litestar_queues:test:valkey:{uuid.uuid4().hex}"
        backend_valkey = ValkeyQueueBackend(
            backend_config=ValkeyBackendConfig(
                url=f"redis://{valkey_service.host}:{valkey_service.port}/{valkey_service.db}",
                key_prefix=prefix,
                worker_wakeups=True,
                wakeup_channel=f"{prefix}:notifications",
            )
        )
        await backend_valkey.open()
        if hasattr(backend_valkey, "create_schema"):
            await backend_valkey.create_schema()
        log_valkey = backend_valkey.get_event_log(config)
        assert log_valkey is not None
        request.node.stash[_HISTORY_BACKEND] = backend_valkey
        yield log_valkey
        await backend_valkey.close()

    else:
        pytest.fail(f"Unknown family: {family}")


async def _raw_history_contains(backend: Any, log: Any, event_id: str) -> bool:
    """Read committed storage directly without invoking a writer's query/flush."""
    name = type(log).__name__
    if name == "InMemoryQueueEventLog":
        async with log._lock:
            return any(record.event_id == event_id for record in log._records)
    if name == "EphemeralQueueEventLog":
        return bool(
            await backend._run(
                lambda connection: connection.execute(
                    "SELECT 1 FROM queue_event WHERE event_id = ?", (event_id,)
                ).fetchone()
            )
        )
    if name == "SQLSpecQueueEventLog":
        async with log._session_factory() as reader:
            return bool(await reader.select(log._store.select_existing_event_ids([event_id])))
    if name == "AdvancedAlchemyQueueEventLog":
        async with backend._event_log_service() as reader:
            _, rows = await reader.query_events(QueueEventQuery())
            return any(record.event_id == event_id for record in rows)
    client = backend._client
    return (
        bool(await client.exists(backend._event_log_event_key(event_id)))
        and await client.zscore(backend._event_log_global_key(), event_id) is not None
    )


@pytest.mark.parametrize(
    "history_config", [EventHistoryConfig(batch_size=20, flush_interval=0.02, strict=True)], indirect=True
)
@pytest.mark.parametrize("history_only", [False, True])
async def test_service_owned_sparse_history_commits_before_live(
    event_log: QueueEventLog, history_config: EventHistoryConfig, request: pytest.FixtureRequest, history_only: bool
) -> None:
    backend = request.node.stash[_HISTORY_BACKEND]
    received = asyncio.Event()
    event = QueueEvent(type="task.log", scope="task", task_id="sparse-service", message="sparse")

    class Sink:
        async def publish(self, delivered: QueueEvent, *, channels: Sequence[str]) -> None:
            assert await _raw_history_contains(backend, event_log, delivered.id)
            received.set()

    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            execution_backend="immediate",
            events=QueueEventsConfig(
                history=history_config, delivery=None if history_only else EventDeliveryConfig(sinks=(Sink(),))
            ),
        ),
        queue_backend=backend,
    )
    async with service:
        await service.get_event_publisher().publish(event)
        if history_only:
            deadline = asyncio.get_running_loop().time() + 3
            while not await _raw_history_contains(backend, event_log, event.id):
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
        else:
            await asyncio.wait_for(received.wait(), timeout=3)
    assert service.get_event_log() is None


async def seed_events(event_log: QueueEventLog) -> None:
    events = [
        QueueEvent(
            id="e1",
            occurred_at=BASE,
            type="task.log",
            level="info",
            message="one",
            scope="task",
            scope_key="acme",
            entity=QueueEventEntityRef(type="invoice", id="42"),
            actor=QueueEventActor(type="user", id="u1"),
            sequence=1,
            payload={"stage": "load", "duration_ms": 10},
        ),
        QueueEvent(
            id="e2",
            occurred_at=BASE + timedelta(seconds=1),
            type="task.log",
            level="error",
            message="two",
            scope="task",
            scope_key="acme",
            entity=QueueEventEntityRef(type="invoice", id="42"),
            actor=QueueEventActor(type="user", id="u1"),
            sequence=2,
            payload={"stage": "load", "duration_ms": 5},
        ),
        QueueEvent(
            id="e3",
            occurred_at=BASE + timedelta(seconds=2),
            type="task.progress",
            level="info",
            message="three",
            scope="task",
            scope_key="acme",
            entity=QueueEventEntityRef(type="invoice", id="99"),
            actor=QueueEventActor(type="user", id="u2"),
            sequence=3,
            payload={"stage": "write"},
        ),
        QueueEvent(
            id="e4",
            occurred_at=BASE + timedelta(seconds=3),
            type="task.log",
            level="warning",
            message="four",
            scope="task",
            scope_key="other",
            entity=QueueEventEntityRef(type="invoice", id="42"),
            actor=QueueEventActor(type="user", id="u1"),
            sequence=4,
            payload={"stage": "load", "duration_ms": 1},
        ),
        QueueEvent(
            id="e5",
            occurred_at=BASE + timedelta(seconds=4),
            type="audit.export",
            scope="custom",
            scope_key="acme",
            sequence=5,
            payload={},
        ),
        QueueEvent(
            id="tie-a-id",
            occurred_at=BASE + timedelta(seconds=5),
            type="task.log",
            level="debug",
            message="tie-a",
            scope="task",
            scope_key="acme",
            entity=QueueEventEntityRef(type="invoice", id="42"),
            actor=QueueEventActor(type="user", id="u1"),
            sequence=6,
            payload={"stage": "load"},
        ),
        QueueEvent(
            id="tie-b-id",
            occurred_at=BASE + timedelta(seconds=5),
            type="task.log",
            level="debug",
            message="tie-b",
            scope="task",
            scope_key="acme",
            entity=QueueEventEntityRef(type="invoice", id="42"),
            actor=QueueEventActor(type="user", id="u1"),
            sequence=6,
            payload={"stage": "load"},
        ),
    ]
    for event in events:
        await event_log.publish_event(event)
    await event_log.flush_events()


@pytest.mark.parametrize(
    ("filter_args", "expected_ids"),
    [
        ({"task_id": None}, ["e1", "e2", "e3", "e4", "e5", "tie-a-id", "tie-b-id"]),
        ({"event_type": "task.progress"}, ["e3"]),
        ({"scope": "task"}, ["e1", "e2", "e3", "e4", "tie-a-id", "tie-b-id"]),
        ({"scope_key": "acme"}, ["e1", "e2", "e3", "e5", "tie-a-id", "tie-b-id"]),
        ({"entity": "invoice:99"}, ["e3"]),
        ({"level": "warning"}, ["e4"]),
    ],
)
async def test_single_filters(event_log: QueueEventLog, filter_args: dict[str, Any], expected_ids: list[str]) -> None:
    await seed_events(event_log)
    page = await event_log.query_events(QueueEventQuery(**filter_args))
    assert [r.event_id for r in page.items] == expected_ids


@pytest.mark.parametrize(
    ("filter_args", "expected_ids"),
    [
        ({"scope_key": "acme", "level": "error"}, ["e2"]),
        ({"entity": "invoice:42", "event_type": "task.log"}, ["e1", "e2", "e4", "tie-a-id", "tie-b-id"]),
        ({"scope": "custom", "scope_key": "acme"}, ["e5"]),
        ({"task_name": None, "entity": "invoice:42", "level": "info"}, ["e1"]),
        ({"scope_key": "other", "level": "error"}, []),
    ],
)
async def test_filter_combinations(
    event_log: QueueEventLog, filter_args: dict[str, Any], expected_ids: list[str]
) -> None:
    await seed_events(event_log)
    page = await event_log.query_events(QueueEventQuery(**filter_args))
    assert [r.event_id for r in page.items] == expected_ids


async def test_stable_ascending_and_descending_order(event_log: QueueEventLog) -> None:
    await seed_events(event_log)
    page_asc = await event_log.query_events(QueueEventQuery(order="asc"))
    expected_ids = ["e1", "e2", "e3", "e4", "e5", "tie-a-id", "tie-b-id"]
    assert [r.event_id for r in page_asc.items] == expected_ids

    page_desc = await event_log.query_events(QueueEventQuery(order="desc"))
    assert [r.event_id for r in page_desc.items] == list(reversed(expected_ids))


async def test_offset_limit_and_empty_pages(event_log: QueueEventLog) -> None:
    await seed_events(event_log)
    expected_ids = ["e1", "e2", "e3", "e4", "e5", "tie-a-id", "tie-b-id"]

    page1 = await event_log.query_events(QueueEventQuery(limit=3))
    assert [r.event_id for r in page1.items] == expected_ids[:3]
    assert page1.total == len(expected_ids)

    page2 = await event_log.query_events(QueueEventQuery(offset=3, limit=3))
    assert [r.event_id for r in page2.items] == expected_ids[3:6]
    assert page2.total == len(expected_ids)

    assert [r.event_id for r in page1.items] + [r.event_id for r in page2.items] == expected_ids[:6]

    page_empty = await event_log.query_events(QueueEventQuery(offset=99))
    assert page_empty.items == []
    assert page_empty.total == len(expected_ids)


async def test_scoped_summaries(event_log: QueueEventLog) -> None:
    await seed_events(event_log)
    summaries = await event_log.summarize_stages(QueueEventQuery(scope_key="acme"))

    assert [s.stage for s in summaries] == [None, "load", "write"]

    # None stage (e5)
    assert summaries[0].event_count == 1
    assert summaries[0].total_duration_ms == 0.0

    # load stage (e1, e2, tie-a, tie-b)
    assert summaries[1].event_count == 4
    assert summaries[1].total_duration_ms == 15.0  # e1 (10) + e2 (5)

    # write stage (e3)
    assert summaries[2].event_count == 1
    assert summaries[2].total_duration_ms == 0.0


async def test_latest_sequence_ties_and_worst_level(event_log: QueueEventLog) -> None:
    await seed_events(event_log)
    summaries = await event_log.summarize_stages(QueueEventQuery(scope_key="acme"))

    # load stage
    load = summaries[1]
    assert load.stage == "load"
    assert load.latest_sequence == 6
    assert load.latest_message == "tie-b"  # Tie broken by tie-b-id > tie-a-id
    assert load.worst_level == "error"

    # write stage
    write = summaries[2]
    assert write.stage == "write"
    assert write.worst_level == "info"

    # None stage
    none_st = summaries[0]
    assert none_st.stage is None
    assert none_st.worst_level is None


async def test_summary_rejects_pagination(event_log: QueueEventLog) -> None:
    await seed_events(event_log)
    with pytest.raises(QueueConfigurationError):
        await event_log.summarize_stages(QueueEventQuery(limit=5))
    with pytest.raises(QueueConfigurationError):
        await event_log.summarize_stages(QueueEventQuery(offset=5))


async def test_filtered_bounded_retention_converges(event_log: QueueEventLog) -> None:
    await seed_events(event_log)

    before = BASE + timedelta(days=1)
    match = QueueEventQuery(scope_key="acme")

    d1 = await event_log.cleanup_events(before=before, match=match, limit=2)
    assert d1 == 2

    d2 = await event_log.cleanup_events(before=before, match=match, limit=2)
    assert d2 == 2

    # Run it until convergence
    while await event_log.cleanup_events(before=before, match=match, limit=2) > 0:
        pass

    # all 'acme' events should be deleted
    page = await event_log.query_events(QueueEventQuery(scope_key="acme"))
    assert page.items == []

    page = await event_log.query_events(QueueEventQuery())
    assert [r.event_id for r in page.items] == ["e4"]


async def test_exclude_implements_first_match_wins(event_log: QueueEventLog) -> None:
    await seed_events(event_log)

    deleted = await event_log.cleanup_events(
        before=BASE + timedelta(days=1),
        match=QueueEventQuery(scope_key="acme"),
        exclude=(QueueEventQuery(level="error"),),
        limit=10,
    )
    assert deleted > 0

    page = await event_log.query_events(QueueEventQuery(scope_key="acme"))
    assert [r.event_id for r in page.items] == ["e2"]


async def test_unmatched_records_are_never_deleted(event_log: QueueEventLog) -> None:
    await seed_events(event_log)

    deleted = await event_log.cleanup_events(
        before=BASE + timedelta(days=1), match=QueueEventQuery(scope_key="nobody"), limit=10
    )

    assert deleted == 0
    page = await event_log.query_events(QueueEventQuery())
    assert len(page.items) == 7
