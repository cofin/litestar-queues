# Bounded Queue Maintenance

[`QueueMaintenanceService`](../reference/execution-and-workers.md#litestar_queues.maintenance.QueueMaintenanceService) (configured via [`QueueMaintenanceConfig`](../reference/execution-and-workers.md#litestar_queues.maintenance.QueueMaintenanceConfig)) executes a finite, bounded pass of repair and retention work under token-fenced distributed ownership and a wall-clock time budget, then returns.

Maintenance never starts a polling worker loop, never executes due application tasks, and never auto-provisions external schedulers or database schema out of band.

## Runnable Configuration Example

```python title="docs/examples/execution/maintenance_config.py"
--8<-- "docs/examples/execution/maintenance_config.py"
```

## Maintenance Phase Execution Order

Every maintenance run evaluates bounded phases in the fixed `PHASE_ORDER`:

| Order | Phase Name | Limit Field | Enablement Threshold | What the Phase Does |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `expire` | `expire_limit` (`1000`) | Always enabled | Transitions overdue `pending` and `scheduled` tasks whose `expires_at` has passed to `expired` |
| 2 | `external` | `external_limit` (`100`) | Enabled when any configured execution backend has `is_external == True` | Repairs unfinished external dispatch reservations and reconciles running Cloud Tasks / Cloud Run Jobs / broker attempts |
| 3 | `stale` | `stale_limit` (`100`) | `stale_after: float \| None = None` | Recovers `running` tasks whose `heartbeat_at` is older than `stale_after` seconds (requeuing or failing them according to retry policy) |
| 4 | `schedules` | `schedules_limit` (`100`) | Enabled when `QueueConfig.initialize_schedules=True` and recurring `@task` schedules exist | Creates missing `scheduled:<task-name>` occurrences and repairs undispatched active occurrences |
| 5 | `terminal` | `terminal_limit` (`1000`) | `terminal_retention: float \| None = None` | Deletes terminal task records (`completed`, `failed`, `cancelled`, `expired`) older than `terminal_retention` seconds without touching `TaskReservation` |
| 6 | `events` | `event_limit` (`1000`) | `event_retention_rules: tuple[QueueEventRetentionRule, ...] = ()` | Deletes durable `QueueEventLogRecord` rows matching ordered `QueueEventRetentionRule` age policies |

### Non-Destructive Defaults

Destructive cleanup and stale-recovery thresholds (`stale_after`, `terminal_retention`, and `event_retention_rules`) default to `None` (or `()`), leaving their phases skipped until explicitly configured.

## Distributed Ownership & Coordination Storage

Before running any phase, `QueueMaintenanceService` acquires a token-fenced distributed lease (`MAINTENANCE_NAME = "queue-maintenance"`) from the storage backend for `coordination_timeout` seconds (default `360.0s`).

- **Budget Invariant**: `coordination_timeout` must strictly exceed `time_budget` (default `300.0s`) so distributed ownership outlives the entire maintenance run; violating this raises `QueueConfigurationError`.
- **Concurrent Invocations**: If another process already holds active maintenance ownership, `QueueMaintenanceService.run()` returns immediately with `outcome="already_running"` and `acquired=False`.
- **In-Memory Backend Rejection**: Because `InMemoryQueueBackend` is process-local and cannot coordinate across processes, invoking `QueueMaintenanceService` against the in-memory backend raises `QueueConfigurationError`. Use a persistent backend (`SQLSpecQueueBackend`, `SQLAlchemyBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, or `EphemeralQueueBackend`).

### Schema Ownership & Migration Requirements

| Storage Backend | Coordination & Reservation Tables | Migration / Schema Provisioning |
| :--- | :--- | :--- |
| **SQLSpec (`SQLSpecBackendConfig`)** | `queue_maintenance`, `queue_task_reservation` | Included in migration `0001_create_queue_tasks` (`configure_queue_migration_extension`) and `await backend.create_schema()` |
| **Advanced Alchemy (`SQLAlchemyBackendConfig`)** | `QueueMaintenanceModel` (`QueueMaintenanceModelMixin`), `QueueTaskReservationModel` (`QueueTaskReservationModelMixin`) | Managed via your application's Alembic migration history |
| **Redis & Valkey (`RedisBackendConfig`, `ValkeyBackendConfig`)** | `{prefix}:maintenance:<name>`, `{prefix}:task_reservations` | Managed automatically via atomic Lua scripts |

## Running Maintenance via CLI & Exit Codes

Run all enabled phases or narrow execution to specific phases with `litestar queues run-maintenance`:

```bash
litestar --app my_app.main:app queues run-maintenance --json
litestar --app my_app.main:app queues run-maintenance --phase expire --phase external --phase schedules
```

Passing `--phase` only narrows which phases are evaluated; a phase whose retention threshold (`stale_after`, `terminal_retention`, or `event_retention_rules`) is unconfigured remains `"skipped"`.

| CLI Exit Code | `MaintenanceOutcome` | Meaning |
| :--- | :--- | :--- |
| `0` | `"completed"` or `"already_running"` | All enabled phases completed within budget, or another runner already holds active ownership |
| `1` | `"failed"` | One or more phases encountered an error (reported as sanitized `maintenance_phase_failed:<ExceptionType>`) |
| `2` | `"partial"` | A phase hit its batch limit (`limit_reached=True`) or `time_budget` elapsed before all enabled phases ran |

## Authenticated HTTP Maintenance Endpoint

For serverless deployments where triggering an authenticated HTTP request from an external scheduler is simpler than launching a container job, set `QueueMaintenanceConfig(mount_route=True, ...)`. `QueuePlugin` mounts `POST /_litestar-queues/maintenance` (or `/_<namespace>/maintenance`) protected by at least one of:

- `verify_oidc=True` with `oidc_audience="https://..."` (and optional `oidc_service_account_email`) using `CloudTasksOIDCGuard`
- `trust_platform_auth=True` when platform IAM already authenticates callers
- `guards=(...)` with custom Litestar `Guard` callables

The HTTP endpoint returns `200 OK` for `"completed"` / `"already_running"`, `207 Multi-Status` for `"partial"`, and `503 Service Unavailable` for `"failed"`.

## Recommended Scheduling Cadence & Launch Surfaces

Because workers and native backend TTL policies (`terminal_ttl`) already handle routine operations, external maintenance is designed for infrequent periodic sweeps (such as every six hours with `0 */6 * * *`, or daily). Cloud Run Jobs, Kubernetes CronJobs, systemd timers, and Cloud Scheduler are supported external launch surfaces; container orchestration itself is not part of the core design of `QueueMaintenanceService`.

=== "Kubernetes CronJob"

    ```yaml
    apiVersion: batch/v1
    kind: CronJob
    metadata:
      name: litestar-queues-maintenance
    spec:
      schedule: "0 */6 * * *"
      concurrencyPolicy: Forbid
      jobTemplate:
        spec:
          template:
            spec:
              restartPolicy: OnFailure
              containers:
                - name: maintenance
                  image: my-org/my-app:latest
                  command:
                    - litestar
                    - --app
                    - my_app.main:app
                    - queues
                    - run-maintenance
                    - --json
    ```

=== "Cloud Scheduler -> HTTP Maintenance Route"

    ```bash
    gcloud scheduler jobs create http litestar-queues-maintenance \
      --location=us-central1 \
      --schedule="0 */6 * * *" \
      --uri="https://worker-service-uc.a.run.app/_litestar-queues/maintenance" \
      --http-method=POST \
      --oidc-service-account-email="scheduler-invoker@my-gcp-project.iam.gserviceaccount.com" \
      --oidc-token-audience="https://worker-service-uc.a.run.app"
    ```

=== "systemd Timer"

    ```ini
    [Unit]
    Description=Run bounded litestar-queues maintenance every 6 hours

    [Timer]
    OnCalendar=*-*-* 00/6:00:00
    Persistent=true

    [Install]
    WantedBy=timers.target
    ```
