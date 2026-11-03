<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cofin/litestar-queues/main/docs/assets/banner-inline-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/cofin/litestar-queues/main/docs/assets/banner-inline-light.svg">
    <img alt="Litestar Queues" src="https://raw.githubusercontent.com/cofin/litestar-queues/main/docs/assets/banner-inline-light.svg" width="520">
  </picture>
  <p><strong>Task queues, workers, schedules, realtime event streams, and storage/execution backends for Litestar.</strong></p>
  <p>
    <a href="https://pypi.org/project/litestar-queues/"><img src="https://img.shields.io/pypi/v/litestar-queues?color=1d4ed8" alt="PyPI"></a>
    <a href="https://pypi.org/project/litestar-queues/"><img src="https://img.shields.io/pypi/pyversions/litestar-queues?color=1d4ed8" alt="Python"></a>
    <a href="https://github.com/cofin/litestar-queues/actions/workflows/ci.yml"><img src="https://github.com/cofin/litestar-queues/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://cofin.github.io/litestar-queues/"><img src="https://img.shields.io/badge/docs-cofin.github.io-1d4ed8" alt="Documentation"></a>
    <a href="https://github.com/cofin/litestar-queues/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/litestar-queues?color=1d4ed8" alt="License"></a>
  </p>
  <p>
    <a href="https://cofin.github.io/litestar-queues/getting-started/quickstart/">Quickstart</a> ·
    <a href="https://cofin.github.io/litestar-queues/architecture/">Architecture</a> ·
    <a href="https://cofin.github.io/litestar-queues/backends/">Storage Backends</a> ·
    <a href="https://cofin.github.io/litestar-queues/execution/">Execution &amp; Operations</a> ·
    <a href="https://cofin.github.io/litestar-queues/events/">Events &amp; Streams</a> ·
    <a href="https://cofin.github.io/litestar-queues/reference/">API Reference</a>
  </p>
</div>

---

`litestar-queues` decouples **authoritative task storage** (`BaseQueueBackend`) from **execution delivery** (`BaseExecutionBackend`). Persist tasks, retries, uniqueness reservations, and event history in SQLSpec (PostgreSQL, MySQL, MariaDB, SQLite, DuckDB, CockroachDB, Spanner, Oracle, SQL Server), Advanced Alchemy, Redis, or Valkey while dispatching execution across local workers, Google Cloud Tasks, Google Cloud Run Jobs, Pub/Sub, SQS, RabbitMQ, or Kafka using an ID-only wire payload (`{"version": 1, "task_id": "<uuid>"}`).

## Install

```bash
uv add litestar-queues
```

Add optional extras for your storage or execution backend (`sqlspec`, `sqlalchemy`, `redis`, `valkey`, `cloud-tasks`, `cloud-run`, `pubsub`, `sqs`, `rabbitmq`, `kafka`, `otel`, `prometheus`).

## Quickstart

```python
from uuid import UUID

from litestar import Litestar, get, post
from litestar.di import NamedDependency
from litestar.exceptions import NotFoundException
from litestar.params import FromPath, FromQuery

from litestar_queues import QueueConfig, QueuePlugin, QueueService, task


@task(queue="reports")
async def render_report(report_id: str, requested_by: str) -> dict[str, str]:
    """Render an analytics report in the background."""
    return {"report_id": report_id, "requested_by": requested_by, "status": "ready"}


@post("/reports/{report_id:str}")
async def start_report(
    report_id: FromPath[str],
    queue_service: NamedDependency[QueueService],
    requested_by: FromQuery[str] = "team@example.com",
) -> dict[str, str]:
    """Enqueue a background report job and return its tracking ID."""
    handle = await queue_service.enqueue(render_report, report_id, requested_by=requested_by)
    return {"task_id": str(handle.id), "status": handle.status or "pending"}


@get("/tasks/{task_id:uuid}")
async def get_report_status(task_id: FromPath[UUID], queue_service: NamedDependency[QueueService]) -> dict[str, object]:
    """Inspect the status and return value of a queued task."""
    record = await queue_service.get_task(task_id)
    if record is None:
        raise NotFoundException(detail=f"Task {task_id} not found")
    return {
        "task_id": str(record.id),
        "status": record.status,
        "is_terminal": record.is_terminal,
        "result": record.result,
    }


app = Litestar(route_handlers=[start_report, get_report_status], plugins=[QueuePlugin(config=QueueConfig())])
```

Start the server and its supervised worker process:

```bash
LITESTAR_APP=app:app uv run litestar run --reload
```

Enqueue a report job and inspect its status:

```bash
curl -X POST http://127.0.0.1:8000/reports/q3-revenue
curl http://127.0.0.1:8000/tasks/<task_id>
```

## Capabilities

| Area | Highlights |
| :--- | :--- |
| **Storage Backends** | Zero-config `EphemeralQueueBackend` (WAL SQLite), `InMemoryQueueBackend`, `SQLSpecQueueBackend` (9+ relational databases & third-party adapters), `SQLAlchemyBackend` (Advanced Alchemy), `RedisQueueBackend`, and `ValkeyQueueBackend`. |
| **Execution & Workers** | `WorkerConfig(placement="server" \| "asgi" \| "external")`, multi-backend routing (`QueueConfig.execution_backends`), Google Cloud Tasks push callbacks (`CloudTasksOIDCGuard`), Cloud Run Jobs, Pub/Sub, SQS, RabbitMQ, and Kafka. |
| **Tasks & Schedules** | Priority queues, retries with exponential/jittered backoff, argument-identity uniqueness & forever reservations, cooperative & remote cancellation, cron/interval `ScheduleConfig`, and attempt-scoped `TaskDependencyProvider`. |
| **Events & Observability** | Live SSE & WebSocket event streams (`EventStreamConfig`), durable event history (`EventHistoryConfig`, `QueueEventQuery`, `EventHistoryExtraColumn`), and low-cardinality OpenTelemetry, Prometheus, and SQLCommenter telemetry (`ObservabilityConfig`). |

## Documentation

- [Getting Started & Installation](https://cofin.github.io/litestar-queues/getting-started/)
- [Core Concepts & Guides](https://cofin.github.io/litestar-queues/learn/)
- [Storage Backends (SQLSpec, Advanced Alchemy, Redis, Valkey)](https://cofin.github.io/litestar-queues/backends/)
- [Execution Backends, Bounded Maintenance & CLI](https://cofin.github.io/litestar-queues/execution/)
- [Task Events, Realtime Streams, History & Observability](https://cofin.github.io/litestar-queues/events/)
- [Architecture & Runnable Examples](https://cofin.github.io/litestar-queues/architecture/)
- [API & CLI Reference](https://cofin.github.io/litestar-queues/reference/)

## Contributing & License

Run `make install`, `make test-unit`, `make lint`, and `make docs` before submitting changes; see the [Contributing & Testing guide](https://cofin.github.io/litestar-queues/reference/contributing/) for details. `litestar-queues` supports Python 3.11 through 3.15 and is distributed under the [MIT License](https://github.com/cofin/litestar-queues/blob/main/LICENSE).
