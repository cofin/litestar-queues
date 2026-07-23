# Changelog

Notable changes to Litestar Queues are recorded here. This project is still
pre-release, so minor releases may make intentional API breaks.

## [0.5.0] - Unreleased

Version 0.5.0 consolidates the public configuration API before its first stable
release. It does not retain aliases, deprecations, or compatibility shims for
the replaced pre-release surface.

### Breaking changes

- Event delivery, realtime streams, and event history now share one
  `QueueEventsConfig` group. Capability objects enable their respective
  features, Channels resolution has explicit precedence, and custom delivery
  sinks are additive.
- Worker startup and runtime settings now live in `WorkerConfig`, which is also
  passed directly to `Worker`; CLI options override a copied configuration.
- Task submission and durable uniqueness use the clearer `TaskRequest` and
  `TaskReservation` vocabulary. Successful-task logging uses the positive
  `log_success` option, and argument identity limits use
  `max_argument_identity_bytes` with `TaskIdentityTooLargeError`.
- Redis, Valkey, Advanced Alchemy, and SQLSpec worker-wakeup settings now use a
  consistent vocabulary. SQLSpec has one explicit transport path and supports
  disabling wakeups with `worker_wakeups=None`.
- Dead Cloud Run, scheduling, observability, state-key, and event configuration
  fields were removed. The supported Advanced Alchemy names remain
  `SQLAlchemyBackend` and `SQLAlchemyBackendConfig`.
- The default relational tables are `queue_task`,
  `queue_task_event_history`, `queue_task_reservation`, and
  `queue_maintenance`. SQLSpec creates the complete schema through migration
  `0001`; there is no follow-up migration for this unreleased schema.

### Added

- Added `litestar queues run-task` for one-record external executor dispatch.
- Added task uniqueness policies with argument-based identities, durable
  forever reservations, and explicit administrative reset support.
- Added bounded maintenance configuration and
  `litestar queues run-maintenance` for external reconciliation, stale-task
  recovery, terminal retention, and event-history retention.
- Added adaptive worker polling, richer heartbeat and progress lifecycle
  handling, and backend-native worker wakeups where supported.
- Added Litestar signature-namespace coverage for the consolidated public
  queue, worker, event, observability, and backend types.

### Changed

- Raised the SQLSpec requirement to 0.56.0, adopted its authoritative DML row
  counts, removed obsolete adapter workarounds, and added `mssql-python`
  coverage.
- Added Advanced Alchemy psycopg notification wakeups and kept package
  observability from duplicating SQLSpec queue-domain telemetry while retaining
  SQL statement telemetry.
- Updated configuration, worker, backend, event, observability, scheduling,
  maintenance, and example documentation for the consolidated API.
- Updated every realtime demo to enable Vite explicitly and opt into allowed
  unauthenticated access, keeping discovery and asset-status output quiet.

### Fixed

- Preserved standalone Redis and Valkey example worker settings while allowing
  the CI topology runner to disable in-app workers.
- Trusted the self-signed SQL Server certificate in the `mssql-python` test
  adapter and handled wrapped Advanced Alchemy integrity errors during
  concurrent task reservation.
- Ensured SQLSpec-native queue telemetry is suppressed only when the package
  observability runtime is actually enabled.

Earlier release notes remain available for
[v0.4.0](https://github.com/cofin/litestar-queues/releases/tag/v0.4.0),
[v0.3.0](https://github.com/cofin/litestar-queues/releases/tag/v0.3.0),
[v0.2.0](https://github.com/cofin/litestar-queues/releases/tag/v0.2.0), and
[v0.1.0](https://github.com/cofin/litestar-queues/releases/tag/v0.1.0).

[0.5.0]: https://github.com/cofin/litestar-queues/compare/v0.4.0...v0.5.0
