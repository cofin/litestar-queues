"""App factory used by the scheduler-health missing-canary test."""

from litestar import Litestar

from litestar_queues import QueueConfig, QueuePlugin


def create_app() -> "Litestar":
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            execution_backend="immediate",
            in_app_worker=False,
            task_modules=(),
            scheduler_canary_task="not.a.registered.task",
        )
    )
    return Litestar(plugins=[plugin])


app = create_app()
