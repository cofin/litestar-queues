====================
Cloud Run Deployment
====================

This guide shows a general Cloud Run setup for ``litestar-queues``. Replace the
project, region, service, job, and import-path placeholders with your values.

The core model
==============

A Cloud Run deployment has three separate responsibilities:

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

Each part has one job:

* ``enqueue()`` writes a pending queue record.
* A running worker loop calls ``dispatch()``.
* The Cloud Run Job container executes the task.

The **dispatcher** is the worker loop that starts Cloud Run Jobs. Without a
running dispatcher, records routed to ``cloudrun`` remain pending.

Realtime fan-out is a separate concern
--------------------------------------

If the web service and dispatcher run in separate processes, do not use
``MemoryChannelsBackend`` for browser events. It works in one process only,
even when queue records use a shared database. Configure the same shared
Channels backend in both processes. Use Redis Channels Streams, or PostgreSQL
Channels when it matches the existing stack, with the same channel prefix and
event contract. Redis/Valkey queue notifications only wake the dispatcher.
They do not carry SSE or WebSocket events.

Configure browser authentication and proxies explicitly. Same-origin relative
stream URLs are the simplest choice. A separate frontend origin also needs the
right CORS, cookie, WebSocket upgrade, and proxy timeout settings. A ``403`` on
an enqueue POST points to authentication or CSRF, not Channels. If enqueueing
succeeds but the stream fails, check Channels, the origin, and the proxy.

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
that runs the queued task.

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

The web service, dispatcher, and Cloud Run Job must all reach the same queue
backend. The dispatcher needs ``CloudRunExecutionConfig`` to choose the Cloud
Run Job for each record.

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

The dispatcher must keep CPU available while it waits for tasks. Therefore,
the recommended command uses both ``--min-instances 1`` and
``--no-cpu-throttling``.

In-app worker alternative
==========================

You can run the dispatcher in the API process. This uses fewer services, but
background polling works only while the service stays warm and has CPU.

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

Use this only for low-volume deployments. A dedicated dispatcher is the safer
default because web and background capacity can scale separately.

Cloud Run Job worker
====================

The Cloud Run Job executes the queued task. Run it with ``litestar queues
execute``. The command reads the dispatch envelope and
``LITESTAR_QUEUES_CONFIG_FACTORY`` from the container environment, resolves the
shared queue service, claims the saved record, runs the task, and exits with a
defined code.

The command resolves the config factory before decoding the prefixed dispatch
envelope, so a custom ``env_prefix`` works throughout the process.

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
       or ``QueueService``. Without it, the consumer cannot rebuild the queue
       service from the job container.
   * - ``LITESTAR_QUEUES_TASK_MODULES``
     - Recommended
     - Comma-separated modules to import so ``@task`` registrations exist.
       The consumer merges these with ``config.task_modules``.
   * - ``LITESTAR_QUEUES_DISPATCH_ENVELOPE``
     - Injected
     - The versioned dispatch envelope (camelCase JSON: the routing subset of
       the queue record). The dispatcher sets this automatically through
       container overrides. The consumer decodes it and re-fetches the live
       record by id.

Profile-based job selection
---------------------------

Use ``execution_profile`` to send different task families to different Cloud
Run Jobs. The dispatcher chooses the job name in this order:

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
     --command litestar \
     --args queues,execute \
     --set-cloudsql-instances my-project:my-region:my-db \
     --set-env-vars LITESTAR_QUEUES_CONFIG_FACTORY=myapp.queues:create_config,\
     LITESTAR_QUEUES_TASK_MODULES=myapp.tasks \
     --task-timeout 3600s

Set the timeout for the longest expected task. The Cloud Run Job timeout must
be at least as long as the queue timeout. Using the same value for both is the
simplest option.

If you need a different prefix for the worker environment variables, set it on
``CloudRunExecutionConfig.env_prefix`` and use the matching prefix when you
deploy the Job.

IAM
===

The dispatcher or API service account must be allowed to run the worker Job
with container overrides. The simplest grant is ``roles/run.developer`` on
that Job because it includes ``run.jobs.runWithOverrides``.

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
Creating the Cloud Run Job does not provide that connection.

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

Use the connection format required by your driver. All three processes must
point to the same persistent queue store.

Dispatch failure behavior
==========================

By default, Litestar Queues reports dispatch failures in logs and events. The
record remains routed to ``cloudrun``. The default fallback backend is
``None``.

If you want the older reroute behavior, set
``fallback_execution_backend="local"`` explicitly and run a local worker that
can consume the fallback queue.

A temporary Cloud Run status-check failure is treated as still running. This
prevents a false final state. If the remote execution is missing, the status
check marks the record failed so retryable work can run again.

Scheduling
==========

Scheduled work still needs a dispatcher. Choose one of these options:

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
