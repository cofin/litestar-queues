import asyncio
from tests.unit.test_service import *
async def run():
    order = []
    event_log = _LifecycleEventLog(order)
    service = QueueService(
        QueueConfig(
            worker=WorkerConfig(placement="external"),
            queue_backend="memory",
            events=QueueEventsConfig(history=EventHistoryConfig()),
            task_dependency_provider=_LifecycleDependencyProvider(order),
        ),
        queue_backend=_LifecycleQueueBackend(order, event_log),
        execution_backend=_LifecycleExecutionBackend(order, fail_open=True),
    )
    try:
        await service.open()
    except Exception:
        pass
    print("ORDER:")
    for o in order:
        print(o)

asyncio.run(run())
