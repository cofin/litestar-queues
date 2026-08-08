Google Cloud Pub/Sub dispatch
=============================

Use Pub/Sub to route queued task identifiers to continuously running consumers.
Install the official client first:

.. code-block:: bash

   pip install "litestar-queues[pubsub]"

Create one topic and one pull subscription, then configure their short resource
names alongside a shared, persistent queue backend:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.execution.pubsub import PubSubExecutionConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://redis:6379/0"),
       execution_backend=PubSubExecutionConfig(
           project_id="my-project",
           topic_id="litestar-queues-tasks",
           subscription_id="litestar-queues-workers",
       ),
       worker=WorkerConfig(placement="external"),
   )

Run one dispatcher and any number of consumers with the same application
configuration and queue database:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run
   LITESTAR_APP=app:app litestar queues run-consumer --backend pubsub --max-concurrency 10

Pub/Sub is the execution backend, not the queue backend. It receives only the
UTF-8 task UUID plus a private ``litestar_queues_attempt`` attribute. Arguments,
results, retry state, schedules, leases, and task definitions remain in shared
queue storage. Consequently, process-local memory storage is unsuitable for a
real multi-process deployment.

Delivery and recovery
---------------------

Each message is fenced to the exact persisted retry generation and dispatch
attempt. The consumer acknowledges a delivery only after its queue outcome is
durable. Pub/Sub acknowledgement-deadline extensions reduce duplicate delivery
during long tasks, but never replace the queue backend's heartbeat lease or
stale-work recovery.

Run ``litestar queues run-maintenance`` on a bounded schedule so a stale
dispatch reference can be rotated and republished if a publish outcome was
ambiguous or a delivery disappeared.

Local emulator
--------------

Use Google's official Pub/Sub emulator for local development and tests. Point
the execution config at the emulator's host and enable plaintext gRPC:

.. code-block:: python

   PubSubExecutionConfig(
       project_id="local-project",
       topic_id="tasks",
       subscription_id="workers",
       api_endpoint="127.0.0.1:8085",
       api_insecure=True,
   )

Create the topic and subscription in the emulator before starting the
dispatcher. ``api_insecure=True`` is accepted only when ``api_endpoint`` is
explicit, preventing an accidental plaintext production connection.

IAM
---

The dispatcher needs ``pubsub.topics.publish`` on the topic. Consumers need
``pubsub.subscriptions.consume`` on the subscription. Resource creation should
normally remain with deployment automation rather than the application runtime.
