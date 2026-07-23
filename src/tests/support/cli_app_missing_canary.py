"""App factory used by the scheduler-health missing-canary test."""

from litestar import Litestar

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig


def create_app() -> "Litestar":
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            execution_backend="immediate",
            worker=WorkerConfig(run_in_app=False),
            task_modules=(),
            scheduler_canary_task="not.a.registered.task",
        )
    )
    return Litestar(plugins=[plugin])


app = create_app()
