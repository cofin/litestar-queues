====================
Cloud Run Deployment
====================

This guide covers the generic Cloud Run pattern for ``litestar-queues``.
Replace the project, region, service, job, and import-path placeholders with
your own values.

The core model
==============

Cloud Run deployments work best when you keep three responsibilities separate:

.. code-block:: text

   ┌─────────────┐   enqueue()    ┌──────────────┐   dispatch()   ┌───────────────┐
   │ Web service │───────────────▶│ Queue store  │◀──────────────▶│ Dispatcher    │
   │ (your API)  │  writes a      │ (shared DB)  │   a running    │ worker loop   │
   └─────────────┘  pending row   └──────────────┘   worker reads  └───────┬───────┘
                                                            pending rows    │
                                                                             │ run_job
                                                                             ▼
                                                                     ┌───────────────┐
                                                                     │ Cloud Run Job │
                                                                     │ executes task │
                                                                     └───────────────┘

The important rule is simple:

* ``enqueue()`` writes a pending queue record.
* ``dispatch()`` happens only inside a running worker loop.
* ``execute`` happens inside the Cloud Run Job container.

If no dispatcher process is running, records routed to ``cloudrun`` stay
pending forever.

Realtime fan-out is a separate concern
--------------------------------------

If the web service and dispatcher are separate Cloud Run processes, do not use
``MemoryChannelsBackend`` for browser events. It is process-local even when the
queue records are stored in a shared database. Configure a shared Redis
Channels Streams backend (or a PostgreSQL Channels backend when that is the
existing stack) in both processes, with the same channel key prefix and event
contract. Redis/Valkey queue notifications only wake the dispatcher; they do
not carry SSE or WebSocket event payloads.

Keep browser authentication and proxy behavior explicit: same-origin relative
stream URLs are the simplest deployment, while a separate frontend origin
needs the corresponding CORS, cookie, WebSocket upgrade, and proxy timeout
configuration. A 403 on an enqueue POST is an auth/CSRF problem, not a
Channels delivery failure. A stream error after a successful enqueue is a
Channels, origin, or proxy problem.

Topology and security
---------------------

.. list-table::
   :header-rows: 1

   * - Component
     - Required shared state
     - Security boundary
   * - Web service
     - Persistent queue backend
     - Authenticate enqueue routes and protect queue credentials.
   * - Dispatcher service
     - Same queue backend and Cloud Run configuration
     - Grant only ``run.jobs.runWithOverrides``-equivalent access.
   * - Cloud Run Job
     - Same queue backend and task modules
     - Use a dedicated service account and least-privilege data access.
   * - Browser event replicas
     - Explicit shared Channels transport
     - Authorize stream scopes; queue wakeups are not browser events.

Recommended topology
====================

Use one web service, one always-on dispatcher service, and one Cloud Run Job
that actually runs the queued task.

The web service should keep ``in_app_worker=False`` so it only enqueues
records. The dispatcher service owns the worker loop.

.. code-block:: python

   from litestar_queues import QueueConfig, task
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
   from litestar_queues.execution.cloudrun import CloudRunExecutionConfig


   @task("reports.render", execution_backend="cloudrun", execution_profile="heavy")
   async def render_report(report_id: str) -> None:
       ...


   queue_config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(config=...),
       execution_backend=CloudRunExecutionConfig(
           project_id="my-project",
           region="my-region",
           job_name="my-worker-job",
           profiles={"heavy": "my-worker-job-heavy"},
       ),
       task_modules=("myapp.tasks",),
   )

The same queue backend must be reachable from the web service, the dispatcher,
and the Cloud Run Job. The dispatcher needs the typed
``CloudRunExecutionConfig`` so it can resolve the job name for each record.

Example deployment commands:

.. code-block:: bash

   # Web service: accepts requests and enqueues records
   gcloud run deploy my-api \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-api:TAG \
     --region my-region \
     --add-cloudsql-instances my-project:my-region:my-db

   # Dedicated dispatcher: keeps polling and dispatching records
   gcloud run deploy my-queue-dispatcher \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-api:TAG \
     --region my-region \
     --command litestar \
     --args queues,run \
     --min-instances 1 \
     --no-cpu-throttling \
     --add-cloudsql-instances my-project:my-region:my-db

The dispatcher service should keep CPU available between requests. That is why
the recommended pattern uses ``--min-instances 1`` together with
``--no-cpu-throttling``.

In-app worker alternative
==========================

You can run the dispatcher inside the API process instead of splitting it out.
This is simpler, but Cloud Run only keeps background polling healthy when the
service stays warm and CPU is not throttled.

.. code-block:: python

   from litestar_queues import QueueConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
   from litestar_queues.execution.cloudrun import CloudRunExecutionConfig


   queue_config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(config=...),
       execution_backend=CloudRunExecutionConfig(
           project_id="my-project",
           region="my-region",
           job_name="my-worker-job",
       ),
       in_app_worker=True,
   )

.. code-block:: bash

   gcloud run deploy my-api \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-api:TAG \
     --region my-region \
     --min-instances 1 \
     --no-cpu-throttling \
     --add-cloudsql-instances my-project:my-region:my-db

Use this only for low-volume deployments. The dedicated dispatcher service is
the safer default because web and background capacity can scale independently.

Cloud Run Job worker
====================

The Cloud Run Job is the process that executes the queued task. The shipped
console script is ``litestar-queues-cloudrun-worker``. The equivalent module
invocation is ``python -m litestar_queues.execution.cloudrun.entrypoint``.

The worker loads the shared queue configuration, imports the configured task
modules, claims the persisted record, executes the task, and exits with a
deterministic code. The entry point resolves the config factory before it reads
``TASK_ID``, so custom ``env_prefix`` values are honored end-to-end.

Environment contract
--------------------

The following names use the default ``env_prefix`` value,
``LITESTAR_QUEUES``. If you change ``CloudRunExecutionConfig.env_prefix``, the
prefix changes everywhere.

.. list-table::
   :header-rows: 1

   * - Env var
     - Required
     - Meaning
   * - ``LITESTAR_QUEUES_CONFIG_FACTORY``
     - Yes
     - ``module:attr`` or dotted path returning the shared-DB ``QueueConfig``
       or ``QueueService``. Without it, the worker cannot rebuild the queue
       service from the job container.
   * - ``LITESTAR_QUEUES_TASK_MODULES``
     - Recommended
     - Comma-separated modules to import so ``@task`` registrations exist.
       The entrypoint merges these with ``config.task_modules``.
   * - ``LITESTAR_QUEUES_TASK_ID``
     - Injected
     - The queue record UUID. The dispatcher sets this automatically through
       container overrides.
   * - ``LITESTAR_QUEUES_TASK_NAME``
     - Injected
     - The task name stored on the queue record.
   * - ``LITESTAR_QUEUES_TASK_ARGS``
     - Injected
     - JSON-encoded positional arguments.
   * - ``LITESTAR_QUEUES_TASK_KWARGS``
     - Injected
     - JSON-encoded keyword arguments.
   * - ``LITESTAR_QUEUES_EXECUTION_BACKEND``
     - Injected
     - Set to ``cloudrun`` by the dispatcher.
   * - ``LITESTAR_QUEUES_EXECUTION_PROFILE``
     - As applicable
     - Present when the record uses a profile and the dispatcher selected a
       profile-specific Cloud Run Job.

Profile-based job selection
---------------------------

Use ``execution_profile`` when different task families should run in different
Cloud Run Jobs. The dispatcher resolves the job name in this order:

1. ``profiles[record.execution_profile]``
2. ``job_name``
3. ``profiles["default"]``

If no job name can be resolved, dispatch fails before any Cloud Run execution
is created.

Example job command:

.. code-block:: bash

   gcloud run jobs create my-worker-job \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-worker:TAG \
     --region my-region \
     --command litestar-queues-cloudrun-worker \
     --set-cloudsql-instances my-project:my-region:my-db \
     --set-env-vars LITESTAR_QUEUES_CONFIG_FACTORY=myapp.queues:create_config,\
     LITESTAR_QUEUES_TASK_MODULES=myapp.tasks \
     --task-timeout 3600s

The task timeout should match the longest task you expect the Job to run.
Set the queue timeout and the Cloud Run Job timeout to the same ceiling, or to
values where the Job timeout is at least as long as the queue timeout.

If you need a different prefix for the worker environment variables, set it on
``CloudRunExecutionConfig.env_prefix`` and use the matching prefix when you
deploy the Job.

IAM
===

The dispatcher or API service account needs permission to run the worker Job
with container overrides. The simplest grant is ``roles/run.developer`` on the
worker Job, because it includes ``run.jobs.runWithOverrides``.

.. code-block:: bash

   gcloud run jobs add-iam-policy-binding my-worker-job \
     --region my-region \
     --member serviceAccount:my-dispatcher-sa@my-project.iam.gserviceaccount.com \
     --role roles/run.developer

If you use a custom IAM role instead, make sure it includes
``run.jobs.runWithOverrides``.

Database connectivity
=====================

The web service, dispatcher, and Job must all reach the same queue database.
That requirement is separate from the Cloud Run Job itself.

If you use Cloud SQL:

* attach the instance to the web service,
* attach the same instance to the dispatcher service, and
* attach the same instance to the Job.

Example:

.. code-block:: bash

   gcloud run deploy my-api \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-api:TAG \
     --region my-region \
     --add-cloudsql-instances my-project:my-region:my-db

   gcloud run deploy my-queue-dispatcher \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-api:TAG \
     --region my-region \
     --command litestar \
     --args queues,run \
     --add-cloudsql-instances my-project:my-region:my-db

   gcloud run jobs create my-worker-job \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-worker:TAG \
     --region my-region \
     --set-cloudsql-instances my-project:my-region:my-db

Use the connection form your driver expects. The important part is that all
three processes point at the same durable queue store.

Dispatch failure behavior
==========================

By default, dispatch failures surface in logs and events and the record stays
routed to ``cloudrun``. The default fallback backend is ``None``.

If you want the older reroute behavior, set
``fallback_execution_backend="local"`` explicitly and run a local worker that
can consume the fallback queue.

Transient Cloud Run status probe failures are treated as still running so
reconciliation does not create false terminal states. If the remote execution
is missing, reconciliation marks the record failed so retryable work can run
again.

Scheduling
==========

Scheduled work still needs a dispatcher. You have two common options:

* use ``@task(schedule=...)`` so the dispatcher creates the pending records on
  a schedule, or
* use Cloud Scheduler to call an endpoint that enqueues the task.

In both cases, enqueueing only writes the record. The dispatcher is what turns
that record into a Cloud Run Job execution.

Troubleshooting
===============

* **Jobs never trigger** - check the sequence in order:
  * the dispatcher service is running,
  * the record's execution backend is ``cloudrun``,
  * the dispatcher uses a typed ``CloudRunExecutionConfig``,
  * the job name resolves from ``execution_profile`` or ``job_name``,
  * the dispatcher service account can run the Job with overrides, and
  * the web service, dispatcher, and Job all point at the same database.
* **``Extension litestar_queues not found`` appears during SQLSpec startup** -
  this is usually a SQLSpec extension-discovery warning. Verify that the queue
  migration path is registered correctly, and do not assume the warning means
  the queue schema is absent.
* **The worker exits with a non-zero code** - the exit codes are deterministic.
  The most useful ones are ``4`` for missing record, ``6`` for claim lost,
  ``7`` for cancellation, and ``8`` for missing ``CONFIG_FACTORY``.

See also
========

* :doc:`../backends` for the backend configuration surface.
* :doc:`../workers` for local worker behavior and in-app worker placement.
