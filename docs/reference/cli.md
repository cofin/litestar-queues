# CLI Reference

This page is deterministically generated from [`litestar_queues.cli.queues_group`](https://github.com/cofin/litestar-queues/blob/main/src/litestar_queues/cli.py) via `uv run python tools/docs/generate_cli_docs.py`.

All commands run under the Litestar CLI `queues` group:

```bash
litestar --app app:app queues <command> [OPTIONS]
```

| Command | Category | Summary |
| --- | --- | --- |
| [`litestar queues run`](#litestar-queues-run) | Worker & Consumer Execution | Start a standalone worker fleet. |
| [`litestar queues run-consumer`](#litestar-queues-run-consumer) | Worker & Consumer Execution | Start a continuous external broker consumer. |
| [`litestar queues run-task`](#litestar-queues-run-task) | Worker & Consumer Execution | Run one queued record by id (external-executor consumer). By default reads the task id QUEUES_TASK_ID and QUEUES_CONFIG_FACTORY from the environment; the options below override those defaults so a job can be run by hand. |
| [`litestar queues run-maintenance`](#litestar-queues-run-maintenance) | Maintenance, Status & Diagnostics | Run one bounded maintenance pass (external reconcile, stale recovery, and retention) and exit. Thresholds and limits come from QueueConfig.maintenance; this command never starts a worker or runs due work. |
| [`litestar queues status`](#litestar-queues-status) | Maintenance, Status & Diagnostics | Show queue status counts. |
| [`litestar queues list`](#litestar-queues-list) | Maintenance, Status & Diagnostics | List queued task records with filtering and pagination. |
| [`litestar queues scheduler-health`](#litestar-queues-scheduler-health) | Maintenance, Status & Diagnostics | Exit non-zero if the scheduler canary task has not completed within the window. |

## Worker & Consumer Execution

Commands for running queue workers, broker push/pull consumers, and one-shot serverless task containers.

### `litestar queues run`

Start a standalone worker fleet.

```bash
litestar queues run [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--queue` | `text` | `None` | Queue name to process. Repeatable. |
| `--max-concurrency` | `integer range` | `None` | Override WorkerConfig.max_concurrency for this run. |
| `--drain-timeout` | `float range` | `None` | Seconds to wait for in-flight tasks to drain after SIGTERM/SIGINT. Defaults to QueueConfig.worker.graceful_shutdown_timeout. |

### `litestar queues run-consumer`

Start a continuous external broker consumer.

```bash
litestar queues run-consumer [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--backend` | `Choice` | *required* | Configured broker backend to consume. (Choices: `kafka`, `pubsub`, `rabbitmq`, `sqs`) |
| `--max-concurrency` | `integer range` | `None` | Command option. |
| `--drain-timeout` | `float range` | `None` | Command option. |

### `litestar queues run-task`

Run one queued record by id (external-executor consumer). By default reads the task id QUEUES_TASK_ID and QUEUES_CONFIG_FACTORY from the environment; the options below override those defaults so a job can be run by hand.

```bash
litestar queues run-task [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--task-id` | `text` | `None` | Run the queued record with this id (local one-shot). |
| `--config-factory` | `text` | `None` | ``module:callable`` returning a QueueConfig or QueueService. |
| `--task-modules` | `text` | `None` | Comma-separated modules to import before running the task. |

## Maintenance, Status & Diagnostics

Commands for executing the 6-phase queue maintenance sweep, inspecting backend health and task listings, and checking recurring scheduler heartbeats.

### `litestar queues run-maintenance`

Run one bounded maintenance pass (external reconcile, stale recovery, and retention) and exit. Thresholds and limits come from QueueConfig.maintenance; this command never starts a worker or runs due work.

```bash
litestar queues run-maintenance [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--phase` | `Choice` | `None` | Maintenance phase to run. Repeatable; defaults to every configured phase. Only narrows configuration. (Choices: `expire`, `external`, `stale`, `schedules`, `terminal`, `events`) |
| `--json` | `bool` | `False` | Emit machine-readable JSON. |

### `litestar queues status`

Show queue status counts.

```bash
litestar queues status [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--queue` | `text` | `None` | Count only records in this queue. |
| `--json` | `bool` | `False` | Emit machine-readable JSON. |

### `litestar queues list`

List queued task records with filtering and pagination.

```bash
litestar queues list [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--status` | `Choice` | `None` | Filter by task status. Repeatable. (Choices: `pending`, `scheduled`, `running`, `completed`, `failed`, `cancelled`, `expired`) |
| `--queue` | `text` | `None` | Filter by logical queue name. |
| `--task-name` | `text` | `None` | Filter by registered task name. |
| `--key` | `text` | `None` | Filter by deduplication key. |
| `--limit` | `integer range` | `50` | Maximum records to return (default 50). |
| `--offset` | `integer range` | `0` | Zero-based starting record offset (default 0). |
| `--json` | `bool` | `False` | Emit machine-readable JSON. |

### `litestar queues scheduler-health`

Exit non-zero if the scheduler canary task has not completed within the window.

```bash
litestar queues scheduler-health [OPTIONS]
```

**Options**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--minutes` | `integer range` | `5` | Staleness threshold in minutes (default 5). |

## Public Python CLI Surface

::: litestar_queues.cli
    options:
      members:
        - queues_group
        - register
        - run_command
        - run_consumer_command
        - run_task_command
        - run_maintenance_command
        - status_command
        - scheduler_health_command
        - list_command
