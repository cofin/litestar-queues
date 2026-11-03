# Quickstart

This guide walks through creating a runnable Litestar application that defines a background task, enqueues work from an HTTP route handler, and inspects task results.

## Define a Task and Register the Plugin

Create `app.py` with the following code:

```python title="app.py"
--8<-- "docs/examples/getting_started/quickstart_app.py"
```

### What Happens Under the Hood

1. **`@task(queue="reports")`** wraps `render_report` in a `Task` bound to the `"reports"` queue and registers it in the task registry while preserving its direct async callability.
2. **`QueueConfig()`** defaults to `queue_backend="ephemeral"` (a zero-config, cross-process WAL SQLite database) and `WorkerConfig(placement="server")`.
3. **`QueuePlugin`** registers `QueueService` for dependency injection (`queue_service: NamedDependency[QueueService]`) and hooks into the Litestar CLI so `litestar run` automatically supervises a dedicated worker subprocess.

## Run the Application

Start the development server using the Litestar CLI:

```bash
litestar --app app:app run --reload
```

When `litestar run` starts with `WorkerConfig(placement="server")`, `ServerWorkerSupervisor` spawns a child `litestar queues run` process that claims work across all queues.

## Enqueue a Task and Inspect the Result

In another terminal, enqueue a report job via `POST /reports/q4-revenue`:

```bash
curl -X POST "http://127.0.0.1:8000/reports/q4-revenue?requested_by=analytics@example.com"
```

```json
{
  "task_id": "2c8c9a14-6d3c-4f2b-9f1a-8e3e4d7a1b20",
  "status": "pending"
}
```

Query the task status and return value using the returned `task_id`:

```bash
curl "http://127.0.0.1:8000/tasks/2c8c9a14-6d3c-4f2b-9f1a-8e3e4d7a1b20"
```

```json
{
  "task_id": "2c8c9a14-6d3c-4f2b-9f1a-8e3e4d7a1b20",
  "status": "completed",
  "is_terminal": true,
  "result": {
    "report_id": "q4-revenue",
    "requested_by": "analytics@example.com",
    "status": "ready"
  }
}
```

## Next Steps

- Understand the [Core Mental Model](concepts.md) separating authoritative task storage from execution dispatch.
- Configure [Tasks & Enqueuing](../learn/tasks.md) and [Task Options, Retries & Uniqueness](../learn/task-options.md).
- Move to production storage with [SQLSpec](../backends/sqlspec.md), [Advanced Alchemy](../backends/advanced-alchemy.md), or [Redis & Valkey](../backends/redis-valkey.md).
