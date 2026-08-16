======================
Cloud Tasks Deployment
======================

Cloud Tasks holds each queued record and calls your consumer service when it is
due. Nothing polls, so nothing has to stay running: both your API and your
consumer can scale to zero, and Google is the thing that remembers there is work
to do.

This is the topology to reach for when your background work is HTTP-shaped and
finishes in minutes rather than hours. If it does not, read
:ref:`cloud-tasks-when-not-to` before going further.

.. mermaid::

   flowchart LR
       api["Your API<br/>(any process)"]
       store[("Queue store<br/>(shared DB)")]
       tasks["Cloud Tasks<br/>holds the delivery"]
       consumer["Consumer<br/>(Cloud Run)"]

       api -- "enqueue() writes the record" --> store
       store -- "creates a delivery" --> tasks
       tasks -- "POST" --> consumer
       consumer -- "claims, runs, settles" --> store

The delivery carries one thing: the record's id. Arguments, metadata, and
results never cross the network — the consumer re-reads all of it from the queue
store, which is why payload size, secrets in arguments, and the queue's schema
stay out of the transport entirely.

Installing
==========

.. code-block:: bash

   pip install "litestar-queues[cloud-tasks]"

The consumer side does not need the extra. Serving deliveries is ordinary HTTP;
only creating them talks to Google.

What you provision first
========================

**The package never creates infrastructure.** It creates deliveries on a queue
that already exists, using an identity that already has permission. Everything
below is done once, by you, before any record is enqueued.

Create the queue:

.. code-block:: bash

   gcloud services enable cloudtasks.googleapis.com

   gcloud tasks queues create my-queue \
     --location my-region \
     --max-attempts 5 \
     --max-backoff 300s

The queue's own retry policy is a second, independent retry layer above the
queue's ``retries``. See :ref:`cloud-tasks-two-retry-layers`.

Deploy the consumer with no public access and no minimum instances:

.. code-block:: bash

   gcloud run deploy my-consumer \
     --image REGION-docker.pkg.dev/my-project/my-repo/my-app:TAG \
     --region my-region \
     --no-allow-unauthenticated \
     --min-instances 0 \
     --timeout 900 \
     --add-cloudsql-instances my-project:my-region:my-db

``--timeout`` must exceed the longest task plus room to answer. It is the outer
bound; see :ref:`cloud-tasks-deadlines`.

Create the delivery identity and let it invoke the consumer:

.. code-block:: bash

   gcloud iam service-accounts create queue-delivery

   gcloud run services add-iam-policy-binding my-consumer \
     --region my-region \
     --member serviceAccount:queue-delivery@my-project.iam.gserviceaccount.com \
     --role roles/run.invoker

Let whatever enqueues records manage tasks on that queue, and nothing else:

.. code-block:: bash

   gcloud tasks queues add-iam-policy-binding my-queue \
     --location my-region \
     --member serviceAccount:my-api@my-project.iam.gserviceaccount.com \
     --role roles/cloudtasks.enqueuer

   gcloud iam service-accounts add-iam-policy-binding \
     queue-delivery@my-project.iam.gserviceaccount.com \
     --member serviceAccount:my-api@my-project.iam.gserviceaccount.com \
     --role roles/iam.serviceAccountUser

The second binding is the one people miss: creating a task that carries an OIDC
token means acting as the delivery identity, which needs
``roles/iam.serviceAccountUser`` on it.

Configuring
===========

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
   from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

   # Any SQLSpec adapter config works; this one points at Cloud SQL for PostgreSQL.
   sqlspec_config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://queue:secret@127.0.0.1:5432/appdb"}
   )

   queue_config = QueueConfig(
       queue_backend=SQLSpecBackendConfig(sqlspec_config=sqlspec_config),
       execution_backend=CloudTasksExecutionConfig(
           project_id="my-project",
           location="my-region",
           queue_id="my-queue",
           service_url="https://my-consumer-abc123-uc.a.run.app",
           service_account_email="queue-delivery@my-project.iam.gserviceaccount.com",
           trust_platform_auth=True,
       ),
       worker=WorkerConfig(placement="external"),
       task_modules=("myapp.tasks",),
   )

Every process — the API, the consumer, anything else that enqueues — uses this
same configuration. ``placement="external"`` means no worker loop starts
anywhere, which is the point.

Storage has to be shared. The in-memory and ephemeral backends are rejected at
startup, because a delivery arriving at another instance would find nothing.

The delivery route
==================

A queue configured for Cloud Tasks registers one route on your application at
``/_litestar-queues/cloud-tasks`` by default, or the equivalent path derived
from ``QueueConfig.namespace`` (for example ``/_myapp/cloud-tasks``). Set
``route_path`` to move it. You do not
write it, mount it, or wire it up.

Cloud Tasks delivery resource names use ``lq-`` with the default namespace and
the configured namespace otherwise (for example ``myapp-``). Set
``delivery_name_prefix`` when infrastructure needs an explicit prefix.

It is never open by default. Either assert Cloud Run's own IAM with
``trust_platform_auth=True`` — which is a statement that the service is
deployed ``--no-allow-unauthenticated``, so every request has already been
authenticated by the platform — or pass your own ``guards``, or both. Delivery
headers are never treated as authentication: anyone can send them.

What the route answers, and why
-------------------------------

Cloud Tasks reads exactly one thing from the response: whether it was 2xx.
Everything else means *deliver this again*.

.. list-table::
   :header-rows: 1

   * - Outcome
     - Answer
     - Why
   * - Task succeeded, failed for the last time, or was cancelled
     - ``204``
     - Durable. Redelivery cannot change it.
   * - Task failed with attempts left
     - ``204``
     - The retry already has its own delivery.
   * - Another consumer owns the record
     - ``204``
     - Running it again would double the work.
   * - The record does not exist
     - ``204``
     - It never will. Retrying forever is the only alternative.
   * - The queue store could not be reached
     - ``503``
     - Says nothing about the task; redelivery is the repair.
   * - The retry's delivery could not be created
     - ``503``
     - This request arriving again is what gives the record a new delivery.
   * - Malformed body, or unauthenticated
     - ``400`` / ``401``
     - A deployment fault. Retrying it is what makes it visible.

.. _cloud-tasks-two-retry-layers:

Two retry layers
================

There are two, and confusing them causes real duplication:

* **The queue's ``retries``** governs task failures. When a task raises and has
  attempts left, the consumer settles that delivery with a ``204`` and creates a
  *new* delivery for the next attempt.
* **The Cloud Tasks queue's retry policy** governs *delivery* failures — a
  ``503``, a timeout, an instance that died. Google redelivers the same task.

Set ``--max-attempts`` on the queue high enough to ride out a cold start or a
brief database outage, and low enough that a genuinely broken consumer stops
being called. It is not a substitute for task retries and does not know anything
about your task.

.. _cloud-tasks-deadlines:

Deadlines and horizons
======================

These are Google's, not the package's, and the package refuses configurations
that cannot work rather than letting them fail later:

* **A delivery's HTTP deadline is between 15 seconds and 1800 seconds** (30
  minutes). ``dispatch_deadline`` is validated against that range at startup.
* **The task's own timeout must fit inside it, with room to answer.** Cloud
  Tasks stops waiting at the deadline; if the task is still running then, the
  delivery is counted as failed and redelivered while the first attempt is still
  going. Every task on a Cloud Tasks queue needs a finite timeout below the
  deadline.
* **A delivery cannot be scheduled more than 30 days ahead.** A recurrence whose
  next occurrence lands past that ceiling is refused where it is written, rather
  than accepted and never run.
* **Cloud Run's request timeout must exceed the dispatch deadline**, or the
  platform will cut the request before Google stops waiting for it.

.. _cloud-tasks-when-not-to:

When not to use this
====================

Work that genuinely runs longer than the 30-minute delivery ceiling does not
belong here. Split it, or run it on Cloud Run Jobs with the
:doc:`cloud-run` dispatcher topology, which has no such bound. A task that
occasionally takes 40 minutes will not fail cleanly — it will be redelivered
mid-flight, repeatedly.

Delivery guarantees
===================

Delivery is **at-least-once**. Duplicates are ordinary, not exceptional: a
response Google never received, a retry policy doing its job, a maintenance pass
racing a consumer.

The queue makes this safe at the record level. A delivery claims the record
under a fence, so a second delivery for the same attempt finds it already owned
and is acknowledged without running anything. What the queue cannot make safe is
what your task does to the outside world. **Side effects have to be
idempotent** — charge by an idempotency key, upsert instead of insert, check
before you send.

.. _cloud-tasks-repair:

When a delivery goes missing
============================

Nothing polls these records. If a delivery disappears while its record is still
active, the record waits forever and no error is ever raised — which is the one
failure mode this topology has that a polled queue does not.

Deliveries disappear for ordinary reasons: a create call that errored after
Google had already accepted it, an operator purging the queue, a retention
window closing before the schedule time arrived.

Bounded maintenance repairs them. It re-checks active records against the
transport and re-creates the deliveries that are gone, sharing the existing
external phase's budget:

.. code-block:: bash

   litestar queues run-maintenance --phase external

Each invocation runs one bounded pass and exits, so run it on a schedule — a
cron entry, a ``systemd`` timer, or a Cloud Scheduler job that starts the
container — sized to how quickly you need a lost record noticed. Drop
``--phase external`` to run every configured phase in the same pass. Repair is
also the reason maintenance matters more here than on a polled queue, where a
missed record is picked up by the next poll.

Costs
=====

Scale-to-zero removes the always-on dispatcher instance. It does not remove the
durable store behind the queue: a Cloud SQL instance or a Redis/Valkey deployment
costs the same whether or not anything is running, and on a low-volume queue the
**database is the bill**, not Cloud Tasks.

A normal successful task is at least **two operations**: one create and one
delivery. A task that retries once is four. Recurring schedules create one per
occurrence, and maintenance adds a lookup per active record it examines.

As of July 2026, Cloud Tasks bills per operation with a monthly free tier well
above what a typical application queue uses. Prices and free-tier sizes change;
check the `Cloud Tasks pricing page
<https://cloud.google.com/tasks/pricing>`_ before sizing a budget, and treat any
number written here as the date it carries.

Throughput
==========

The default dispatch ceiling is 500 tasks per second per queue, raisable on
request. That is the delivery rate, not the creation rate.

The package names every delivery explicitly, which is what makes an ambiguous
create call recoverable — a create that failed after Google accepted it leaves a
handle to find the delivery by. Named tasks are de-duplicated against a
tombstone that Google keeps for a period after a task completes or is deleted,
so the package never reuses a name: each attempt gets a fresh random suffix.
De-duplication has a documented throughput cost relative to unnamed tasks, so
confirm the current published limits before planning a high-rate queue.

Cancellation
============

The Cloud Tasks backend implements remote execution cancellation. Because Cloud Tasks holds a future delivery, cancellation is implemented as an API ``delete_task`` operation using the ``execution_ref`` stored on the queue record. If the delivery is already in flight, it cannot be deleted, so the operation succeeds idempotently but leaves the running task to cooperative cancellation.

Testing
=======

**There is no Cloud Tasks emulator.** Google publishes no local API for it, and
the local development server does not simulate one. Anything that claims
otherwise is a third-party stand-in with its own fidelity gaps.

Test your tasks the way you test any others — the consumer route is ordinary
HTTP and the record is ordinary storage. What cannot be tested locally is the
transport itself: that a cold instance wakes, that a delayed delivery is held for
exactly as long as it should be, that a redelivery finds the record already
owned. Those need a real project.

Not the same as its neighbours
==============================

Four Google products are all "the thing that runs work later", and they are not
substitutes:

.. list-table::
   :header-rows: 1

   * - Product
     - What it actually is
   * - **Cloud Tasks**
     - A queue of individual HTTP deliveries, each targeted, retried, and
       scheduled on its own. This is what this backend uses.
   * - **Cloud Scheduler**
     - Fixed cron. It fires on a clock, not per unit of work, and has no queue.
       Use it to *trigger* maintenance, not to deliver tasks.
   * - **Pub/Sub**
     - Fan-out messaging with subscriptions and ordering keys. Built for many
       consumers of one event, not one delivery of one record.
   * - **Eventarc**
     - Routes CloudEvents from Google services to a target. It standardizes
       event envelopes; it is not a queue and does not schedule per-task
       deliveries.
   * - **Cloud Run Jobs**
     - A container run to completion, with no HTTP request holding it open, so
       no 30-minute ceiling. The right home for genuinely long work.

Troubleshooting
===============

* **Records stay pending and nothing is delivered** — the enqueueing identity
  probably cannot create tasks or cannot act as the delivery identity. Check
  ``roles/cloudtasks.enqueuer`` on the queue *and*
  ``roles/iam.serviceAccountUser`` on the delivery service account. Dispatch
  failures are reported as events and logs, with the record's id.
* **Deliveries return 401 or 403** — the delivery identity is missing
  ``roles/run.invoker`` on the consumer service, or the configured
  ``service_url`` is not the consumer's own URL, which makes the OIDC audience
  wrong.
* **The same task runs twice** — expected under at-least-once if the task is not
  idempotent, and only surprising if you assumed otherwise. If the record itself
  shows two attempts, the task is exceeding its deadline and being redelivered
  mid-flight; lower the task timeout or raise ``dispatch_deadline``.
* **A recurring schedule stopped** — its next occurrence probably landed past the
  30-day horizon. The refusal is reported where the occurrence was written.
* **Everything works except tasks silently never run** — check that the consumer
  image actually imports the task modules. A consumer that does not have a task
  registered retires the record instead of running it.

See also
========

* :doc:`cloud-run` for the dispatcher and Cloud Run Jobs topologies.
* :doc:`../maintenance` for the bounded maintenance phases repair shares.
* :doc:`../../reference/observability` for the dispatch, delivery, and repair metrics.
