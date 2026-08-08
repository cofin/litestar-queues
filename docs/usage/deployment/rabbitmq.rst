RabbitMQ Dispatch
=================

Install the optional client on Python 3.11 or newer:

.. code-block:: bash

   pip install "litestar-queues[rabbitmq]"

RabbitMQ 4.3 or newer is required. Configure a persistent queue backend and
use RabbitMQ only for execution delivery:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://redis:6379/0"),
       execution_backend=RabbitMQExecutionConfig(
           amqp_url="amqps://queues_user:secret@rabbit.example/queues",
       ),
       worker=WorkerConfig(placement="external"),
   )

Run a dispatcher and one or more consumers against the same persistent queue
storage:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run
   LITESTAR_APP=app:app litestar queues run-consumer --backend rabbitmq --max-concurrency 32

The broker message contains only the task UUID and an opaque attempt header.
Arguments, results, scheduling, retries, cancellation, and terminal state stay
in the queue backend. RabbitMQ delivery is therefore a wakeup and routing slip,
not the task record.

Topology and permissions
------------------------

By default the backend declares one durable, non-exclusive, non-auto-delete
quorum queue. Its namespace-derived name ends in ``-rabbitmq``. Set an explicit
``queue_name`` when deployment policy owns the resource name, or set
``declare_queue=False`` to perform a passive existence check instead. The
RabbitMQ principal needs connect permission on the vhost and configure, write,
and read permissions for the queue. Prefer ``amqps`` outside a trusted private
network; credentials and the AMQP URL are never attached to telemetry.

RabbitMQ 4.3 quorum queues provide strict priority. Stored priority is clamped
to the broker range ``0..31`` after dispatch; storage still decides whether a
record is due and eligible. The default ``delayed_retry_type="returned"``
applies linear 1--30 second backoff to explicit transient nacks without the
delayed-message plugin. Set it to ``disabled`` when a queue policy owns retry
behavior.

Set ``consumer_timeout_ms`` only when broker-side protection from stuck
consumers is required. It must exceed the longest legitimate task duration,
otherwise RabbitMQ can return healthy in-flight work for redelivery. Consumers
ack only after the corresponding durable queue transition; storage fences make
redelivery safe.
