"""Litestar app factory used by CLI tests.

Pointed at via ``LITESTAR_APP=tests.support.cli_app:app``.
"""
from litestar import Litestar

from litestar_queues import QueueConfig, QueuePlugin


def create_app() -> Litestar:
    plugin = QueuePlugin(
        QueueConfig(
            queue_backend="memory",
            execution_backend="immediate",
            start_worker=False,
            task_modules=("tests._factories.queue_tasks",),
            scheduler_canary_task="support_ping",
        )
    )
    return Litestar(plugins=[plugin])


app = create_app()
