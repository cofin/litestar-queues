"""Litestar app factory used by the standalone-worker CLI tests.

Pointed at via ``LITESTAR_APP=tests.helpers.support.cli_worker_app:app``.

``litestar queues run`` is the external worker, so it requires storage that
outlives and is visible outside a single process. This app uses a file-backed
SQLite database whose path comes from ``LITESTAR_QUEUES_TEST_DB`` so the test
that spawns it can point it at a temporary directory.

The schema is provisioned the way an application would provision it: the
backend config registers its own migrations through the
``MigrationConfiguringBackend`` hook, then SQLSpec runs them.
"""

import asyncio
import os
import tempfile
from inspect import isawaitable
from pathlib import Path
from typing import TYPE_CHECKING, cast

from litestar import Litestar
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from litestar_queues import QueueConfig, QueuePlugin, WorkerConfig
from litestar_queues.backends.sqlspec import SQLSpecBackendConfig

if TYPE_CHECKING:
    from litestar_queues.backends.sqlspec._typing import SQLSpecConfig

DATABASE_ENV_VAR = "LITESTAR_QUEUES_TEST_DB"


def database_path() -> "Path":
    """Return the queue database path for this invocation.

    Returns:
        The configured path, or a stable temporary file when unset.
    """
    configured = os.environ.get(DATABASE_ENV_VAR)
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "litestar-queues-cli-test.db"


def _queue_config() -> "QueueConfig":
    return QueueConfig(
        queue_backend=SQLSpecBackendConfig(
            sqlspec_config=AiosqliteConfig(connection_config={"database": str(database_path())})
        ),
        execution_backend="local",
        worker=WorkerConfig(placement="external", poll_interval=0.05),
        task_modules=("tests.helpers.queue_tasks",),
        scheduler_canary_task="support_ping",
    )


def _migrate(config: "QueueConfig") -> "None":
    """Run the migrations the backend registers for itself."""
    backend_config = config.queue_backend
    assert isinstance(backend_config, SQLSpecBackendConfig)
    backend_config.configure_migrations(config)
    assert backend_config.sqlspec_config is not None
    sqlspec_config = cast("SQLSpecConfig", backend_config.sqlspec_config)

    async def upgrade() -> "None":
        result = sqlspec_config.migrate_up(echo=False)
        if isawaitable(result):
            await result

    asyncio.run(upgrade())


def create_app() -> "Litestar":
    config = _queue_config()
    _migrate(config)
    return Litestar(plugins=[QueuePlugin(config)])


app = create_app()
