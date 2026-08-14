============================
Broker execution transports
============================

Amazon SQS, RabbitMQ, Google Cloud Pub/Sub, and Apache Kafka are
**execution transports**.
They decide *where* a task runs, not *where it is stored*. Everything about a
task -- its arguments, result, retry count, schedule, lease, and final state --
lives in a persistent queue backend. The broker carries one thing: the UUID of
a record that is ready to run.

The transports work the same way and differ only in the details covered by
their own guides:

* :doc:`sqs` -- IAM permissions, standard versus FIFO queues, LocalStack.
* :doc:`rabbitmq` -- quorum-queue topology, vhost permissions, broker-managed
  retry delays.
* :doc:`pubsub` -- topic and subscription setup, and the official local
  emulator.
* :doc:`kafka` -- consumer groups, manual offset commits, and partitioning.

The shared model
================

Three parts have to exist, and all of them must reach the same persistent queue
backend:

#. **Anything that enqueues** -- usually your web application -- writes a
   pending queue record.
#. **One dispatcher** claims due records and publishes their identifiers to the
   broker.
#. **One or more consumers** receive an identifier, re-fetch the live record
   from queue storage, and execute the task.

Because the dispatcher runs as its own process, the application configures
``WorkerConfig(placement="external")`` so no worker loop starts inside the web
service.

A process-local queue backend cannot be used here: the consumer is a different
process and would find no record. Use Redis, Valkey, SQLSpec, or Advanced
Alchemy. The CLI refuses to start against ``memory`` or ``ephemeral`` storage
rather than failing later.

Running it
==========

Every deployment runs the same two commands, with the same application
configuration on both sides:

.. code-block:: bash

   # One dispatcher: claims due records and publishes identifiers
   LITESTAR_APP=app:app litestar queues run

   # Any number of consumers: receive identifiers and run the tasks
   LITESTAR_APP=app:app litestar queues run-consumer --backend sqs --max-concurrency 10

``--backend`` takes ``kafka``, ``pubsub``, ``rabbitmq``, or ``sqs``, and must
match the execution backend in your configuration -- the command exits with an
error if it does not, rather than consuming from a broker nobody publishes to.

``--max-concurrency`` bounds tasks in flight per consumer process and defaults
to ``WorkerConfig.max_concurrency``. ``--drain-timeout`` sets how long a
consumer waits for in-flight tasks after ``SIGTERM`` and defaults to
``WorkerConfig.graceful_shutdown_timeout``.

Delivery guarantees
===================

**Delivery is at-least-once, so tasks should be idempotent.** Every broker here
can redeliver a message, and the queue backend can also retry a record on its
own schedule.

Duplicate deliveries do not cause duplicate execution. The dispatcher stamps
each published message with a private ``litestar_queues_attempt`` reference --
an SQS message attribute, a RabbitMQ or Kafka header, or a Pub/Sub attribute
-- that
fences the delivery to one exact retry generation and dispatch attempt. A
consumer whose message does not match the persisted attempt discards it instead
of running the task a second time.

Consumers acknowledge a message only after the matching queue transition is
durable. Broker-side timers -- SQS visibility timeout, RabbitMQ consumer
timeout, Pub/Sub acknowledgement deadline, Kafka consumer offset -- are
crash-redelivery courtesy
only. They are not a lease on the work, and they never replace the queue
backend's own heartbeat, retry count, scheduling, or stale-work recovery.

Repairing lost dispatches
=========================

A publish can end with an unknown outcome, and a delivery can disappear. Run
:doc:`../maintenance` on a bounded schedule -- a cron every few hours is
enough -- so a stale dispatch reference is rotated and republished:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run-maintenance

Nothing else recovers those records, so treat this as part of the deployment
rather than an optional extra.
