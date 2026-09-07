"""Live IPC notifications must follow history visible to an independent process."""

import asyncio
import multiprocessing
import traceback
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlspec.adapters.psycopg import PsycopgAsyncConfig

from litestar_queues import EventDeliveryConfig, QueueConfig, QueueService, WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend, SQLSpecWorkerWakeupConfig
from litestar_queues.events import EventHistoryConfig, QueueEvent, QueueEventQuery, QueueEventsConfig
from tests.integration._names import table_name_for_test

if TYPE_CHECKING:
    from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLog

pytestmark = pytest.mark.anyio


def _backend_config(connection: dict[str, Any], table: str) -> SQLSpecBackendConfig:
    return SQLSpecBackendConfig(
        sqlspec_config=PsycopgAsyncConfig(connection_config=connection),
        queue_table_name=table,
        event_history_table_name=f"{table}_history",
        maintenance_table_name=f"{table}_maintenance",
        task_reservation_table_name=f"{table}_reservation",
        worker_wakeups=SQLSpecWorkerWakeupConfig(transport="polling"),
    )


async def _wait_message(pipe: Any, process: Any = None) -> Any:
    async def receive() -> Any:
        while not pipe.poll():
            if process is not None and process.exitcode is not None:
                pytest.fail(f"Writer exited before notification: {process.exitcode}")
            await asyncio.sleep(0.01)
        return pipe.recv()

    return await asyncio.wait_for(receive(), timeout=15)


def _writer(connection: dict[str, Any], table: str, scenario: str, live: Any, control: Any, allow_write: Any) -> None:  # noqa: PLR0917
    async def run() -> None:
        class Sink:
            async def publish(self, event: QueueEvent, *, channels: Sequence[str]) -> None:
                live.send(event.id)

        config = QueueConfig(
            queue_backend=_backend_config(connection, table),
            execution_backend="immediate",
            worker=WorkerConfig(placement="external"),
            initialize_schedules=False,
            events=QueueEventsConfig(
                history=EventHistoryConfig(
                    batch_size=20, flush_interval=0.02 if scenario in {"sparse", "failure"} else 60, strict=False
                ),
                delivery=EventDeliveryConfig(buffer=None, sinks=(Sink(),), strict=True),
            ),
        )
        async with QueueService(config) as service:
            if scenario == "failure":
                log = service.get_event_log()
                assert log is not None
                buffer = cast("SQLSpecQueueEventLog", log)._buffer
                original_write = buffer._write_batch
                reported = False

                async def controlled_write(batch: Any) -> None:
                    nonlocal reported
                    if not allow_write.is_set():
                        if not reported:
                            control.send("write_failed")
                            reported = True
                        message = "controlled persistence failure"
                        raise RuntimeError(message)
                    await original_write(batch)

                buffer._write_batch = controlled_write
            publisher = service.get_event_publisher()
            await publisher.publish(QueueEvent(id="ordinary", type="task.log", task_id="process-task", scope="task"))
            if scenario == "terminal":
                await publisher.publish(
                    QueueEvent(id="terminal", type="task.completed", task_id="process-task", scope="task")
                )
            if scenario != "close":
                assert await _wait_message(control) == "stop"
        control.send("closed")

    try:
        asyncio.run(run())
    except BaseException:
        control.send(traceback.format_exc())
        raise
    finally:
        live.close()
        control.close()


@pytest.mark.parametrize("scenario", ["sparse", "terminal", "close", "failure"])
async def test_live_notification_has_committed_history_in_another_process(  # noqa: PLR0915
    postgres_service: Any, request: pytest.FixtureRequest, scenario: str
) -> None:
    connection = {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "dbname": postgres_service.database,
    }
    table = table_name_for_test("history_process", "pg", request.node.nodeid)
    backend_config = _backend_config(connection, table)
    history = EventHistoryConfig(batch_size=20, flush_interval=60)
    reader = SQLSpecQueueBackend(
        config=QueueConfig(queue_backend=backend_config, events=QueueEventsConfig(history=history)),
        backend_config=backend_config,
    )
    context = multiprocessing.get_context("spawn")
    live_reader, live_writer = context.Pipe(duplex=False)
    parent_control, child_control = context.Pipe()
    allow_write = context.Event()
    process = context.Process(
        target=_writer, args=(connection, table, scenario, live_writer, child_control, allow_write)
    )
    started = False
    opened = False
    try:
        await reader.open()
        opened = True
        await reader.create_schema()
        log = reader.get_event_log(history)
        assert log is not None
        process.start()
        started = True
        live_writer.close()
        child_control.close()
        if scenario == "failure":
            assert await _wait_message(parent_control, process) == "write_failed"
            assert not live_reader.poll()
            page = await log.query_events(QueueEventQuery(task_id="process-task"))
            assert page.total == 0
            assert page.items == []
            assert not live_reader.poll()
            allow_write.set()
        expected = ["ordinary", "terminal"] if scenario == "terminal" else ["ordinary"]
        for event_id in expected:
            assert await _wait_message(live_reader, process) == event_id
            # This reader owns a different pool and no reference to the writer's buffer.
            page = await log.query_events(QueueEventQuery(task_id="process-task"))
            assert event_id in {row.event_id for row in page.items}
        if scenario != "close":
            parent_control.send("stop")
        assert await _wait_message(parent_control) == "closed"
        await asyncio.to_thread(process.join, 5)
        assert process.exitcode == 0
    finally:
        allow_write.set()
        if started and process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 5)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, 5)
        for pipe in (live_reader, live_writer, parent_control, child_control):
            pipe.close()
        try:
            if opened:
                sqlspec_config = cast("PsycopgAsyncConfig", backend_config.sqlspec_config)
                async with sqlspec_config.provide_session() as session:
                    for suffix in ("_history", "_maintenance", "_reservation", ""):
                        await session.execute(f'DROP TABLE IF EXISTS "{table}{suffix}" CASCADE')
                    await session.commit()
        finally:
            try:
                await reader.close()
            finally:
                process.close()
