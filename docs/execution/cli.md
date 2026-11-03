# CLI Operations Guide

When `QueuePlugin` is registered on a `Litestar` application, it extends the `litestar` CLI with the `litestar queues` command group (implemented in `litestar_queues.cli`).

For full auto-generated option tables of every command and flag, see the [CLI Reference](../reference/cli.md).

## Command Summary

| Command | Target Role | Description |
| :--- | :--- | :--- |
| `litestar queues run` | Local Worker / Broker Dispatcher | Starts a long-lived `Worker` claiming due tasks from storage (or dispatching ID-only records to configured message brokers) |
| `litestar queues run-consumer` | Broker Consumer Fleet | Runs a long-lived pull consumer for `--backend pubsub`, `sqs`, `rabbitmq`, or `kafka` |
| `litestar queues run-task` | Serverless One-Shot Job | Claims and executes a single task record by `--task-id` or `QUEUES_TASK_ID` (also runnable via `python -m litestar_queues.consumer`) |
| `litestar queues run-maintenance` | Scheduled Maintenance Runner | Executes one finite, token-fenced 6-phase `QueueMaintenanceService` pass |
| `litestar queues status` | Operator Diagnostics | Displays queue record counts by status (`pending`, `scheduled`, `running`, `completed`, `failed`, `cancelled`, `expired`), registered tasks, and schedules |
| `litestar queues scheduler-health` | Operator Diagnostics | Verifies recurring schedule health and checks the configured `QueueConfig.scheduler_canary_task` |

## Running Standalone Workers (`litestar queues run`)

Use `litestar queues run` when `WorkerConfig(placement="external")` separates worker pods or VMs from your web tier:

```bash
litestar --app my_app.main:app queues run \
  --queue critical \
  --queue default \
  --max-concurrency 24 \
  --drain-timeout 30
```

By default (`WorkerConfig.run_app_lifespan=True`), `litestar queues run`, `run-consumer`, and `run-maintenance` enter `QueuePlugin.standalone_app_lifespan` so database pools and dependencies configured on your `Litestar` app lifespan are initialized before the command runs. Set `WorkerConfig(run_app_lifespan=False)` when tasks manage all external resources through `task_dependency_provider`.

## Signal Handling & Graceful Shutdown Escalation

Long-lived worker and broker consumer commands install deterministic POSIX signal handlers for `SIGTERM` and `SIGINT`:

1. **First Signal (`SIGTERM` or `SIGINT`)**: Initiates graceful shutdown. The worker stops claiming new records and waits up to `WorkerConfig.graceful_shutdown_timeout` (`30.0s`) for in-flight tasks to complete.
2. **Post-Drain Cancellation**: Tasks still running after `graceful_shutdown_timeout` are cancelled and given `WorkerConfig.final_cancel_timeout` (`5.0s`) to exit `task_dependency_provider` scopes. If `WorkerConfig.requeue_on_shutdown=True`, interrupted tasks are returned to `pending` (up to `max_interruptions`).
3. **Second Signal / Watchdog**: A second `SIGINT`/`SIGTERM` (or exceeding `WorkerConfig.hard_exit_timeout`) forces immediate termination.

## Bootstrapping Without a Litestar App (`QUEUES_CONFIG_FACTORY`)

In minimal worker or Cloud Run Job containers that do not import the full HTTP web application, set `QUEUES_CONFIG_FACTORY` to a `module:callable` returning a `QueueConfig` and `LITESTAR_QUEUES_TASK_MODULES` to a comma-separated list of task modules:

```bash
export QUEUES_CONFIG_FACTORY="my_app.queues:build_queue_config"
export LITESTAR_QUEUES_TASK_MODULES="my_app.tasks,my_app.billing.jobs"
python -m litestar_queues.consumer --task-id 2c8d8f72-6a4b-4c5c-9c82-5e3f21a09b41
```
