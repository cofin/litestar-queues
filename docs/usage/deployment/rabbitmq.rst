=================
RabbitMQ dispatch
=================

Read :doc:`execution-transports` first: it covers the dispatcher/consumer
model, the two CLI commands, and the at-least-once delivery guarantee that
RabbitMQ shares with the other transports. This page covers only what is
specific to RabbitMQ.

RabbitMQ 4.3 or newer is required, as is Python 3.11 or newer.

Install and configure
=====================

.. code-block:: bash

   pip install "litestar-queues[rabbitmq]"

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.execution.rabbitmq import RabbitMQExecutionConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://redis:6379/0"),
       execution_backend=RabbitMQExecutionConfig(
           amqp_url="amqps://queues_user@rabbit.example/queues",
       ),
       worker=WorkerConfig(placement="external"),
   )

Then run ``litestar queues run`` and
``litestar queues run-consumer --backend rabbitmq``.

Prefer ``amqps`` outside a trusted private network. The AMQP URL carries the
credentials, and it is excluded from representations and telemetry so it does
not leak into logs.

Queue topology and permissions
==============================

By default the backend declares one durable, non-exclusive, non-auto-delete
quorum queue whose namespace-derived name ends in ``-rabbitmq``. Set
``queue_name`` when deployment policy owns the resource name, or
``declare_queue=False`` to perform a passive existence check instead of
declaring.

The RabbitMQ principal needs connect permission on the vhost, plus configure,
write, and read permissions on the queue.

Priority and broker-managed retries
===================================

RabbitMQ 4.3 quorum queues provide strict priority. Stored task priority is
clamped to the broker range ``0..31`` after dispatch; queue storage still
decides whether a record is due and eligible in the first place.

``delayed_retry_type`` defaults to ``"returned"``, which applies a linear
1--30 second backoff to explicitly nacked transient failures without needing
the delayed-message plugin. Tune the window with ``delayed_retry_min_ms`` and
``delayed_retry_max_ms``, or set ``delayed_retry_type="disabled"`` when a queue
policy owns retry behavior instead.

Consumer timeout
================

``consumer_timeout_ms`` is unset by default. Set it only when you need
broker-side protection from stuck consumers, and make it longer than the
longest legitimate task duration -- otherwise RabbitMQ returns healthy
in-flight work for redelivery.
