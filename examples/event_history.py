"""Publish and query one event using process-local history."""

# ruff: noqa: S101, T201 -- This runnable example verifies and prints its result.

import asyncio

from litestar_queues import QueueConfig, QueueService, WorkerConfig
from litestar_queues.events import (
    EventHistoryConfig,
    QueueEvent,
    QueueEventActor,
    QueueEventEntityRef,
    QueueEventQuery,
    QueueEventsConfig,
)

__all__ = ("main",)


async def main() -> None:
    config = QueueConfig(
        queue_backend="memory",
        execution_backend="immediate",
        worker=WorkerConfig(placement="external"),
        events=QueueEventsConfig(history=EventHistoryConfig()),
    )
    async with QueueService(config) as service:
        event_log = service.get_event_log()
        assert event_log is not None
        await service.get_event_publisher().publish(
            QueueEvent(
                type="task.log",
                scope="custom",
                scope_key="batch-42",
                entity=QueueEventEntityRef(type="dataset", id="user-csv"),
                payload={"stage": "processing"},
                task_id="import-42",
                message="importing",
                actor=QueueEventActor(type="user", id="u-1", name="Alice"),
            )
        )
        await event_log.flush_events()
        page = await event_log.query_events(
            QueueEventQuery(scope="custom", scope_key="batch-42", entity="dataset:user-csv"), extra={"actor_id": "u-1"}
        )
        assert page.total == len(page.items) == 1
        assert page.items[0].stage == "processing"
        assert page.items[0].entity == "dataset:user-csv"
        print(page.total)


if __name__ == "__main__":
    asyncio.run(main())
