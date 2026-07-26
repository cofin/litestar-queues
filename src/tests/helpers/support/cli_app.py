"""Litestar app factory used by CLI tests.

Pointed at via ``LITESTAR_APP=tests.helpers.support.cli_app:app``.
"""

from litestar import Litestar

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig


def create_app() -> "Litestar":
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            execution_backend="immediate",
            worker=WorkerConfig(placement="external"),
            task_modules=("tests.helpers.queue_tasks",),
            scheduler_canary_task="support_ping",
        )
    )
    return Litestar(plugins=[plugin])


app = create_app()
