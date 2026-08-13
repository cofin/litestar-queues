===================
Amazon SQS dispatch
===================

Read :doc:`execution-transports` first: it covers the dispatcher/consumer
model, the two CLI commands, and the at-least-once delivery guarantee that SQS
shares with the other transports. This page covers only what is specific to
SQS.

Install and configure
=====================

.. code-block:: bash

   pip install "litestar-queues[sqs]"

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.execution.sqs import SqsExecutionConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://redis:6379/0"),
       execution_backend=SqsExecutionConfig(
           queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/tasks",
           region_name="us-east-1",
       ),
       worker=WorkerConfig(placement="external"),
   )

Then run ``litestar queues run`` and
``litestar queues run-consumer --backend sqs``.

Credentials come from the normal AWS credential chain and are deliberately not
configuration fields on ``SqsExecutionConfig``.

IAM
===

This is the part most likely to be missing on a first deployment. The two roles
need different permissions on the configured queue:

* the **dispatcher** needs ``sqs:SendMessage``;
* each **consumer** needs ``sqs:ReceiveMessage``, ``sqs:DeleteMessage``, and
  ``sqs:ChangeMessageVisibility``.

Standard and FIFO queues
========================

Standard queues are the default. Set ``fifo=True`` for a FIFO queue: the
attempt reference is then also sent as ``MessageDeduplicationId``, and a stable
bounded message group is derived from the persisted queue name. Set
``message_group_id`` to choose that group yourself.

Visibility timeout
==================

``visibility_timeout`` (60 seconds by default) is extended every
``visibility_extension_interval`` seconds while a task runs. This only reduces
duplicate deliveries. It is not a lease on the work: the queue backend's own
heartbeat remains what actually protects a running task.

LocalStack
==========

For local development, create a queue in LocalStack and point the config at it
with ``endpoint_url="http://localhost:4566"``, using the usual test credentials
through the AWS credential chain.
