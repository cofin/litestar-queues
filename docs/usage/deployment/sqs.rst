Amazon SQS Dispatch
===================

Install the optional client and configure SQS as execution placement:

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

SQS is an execution transport, not queue persistence. Run one dispatcher and
one or more consumers against the same persistent queue backend:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run
   LITESTAR_APP=app:app litestar queues run-consumer --backend sqs --max-concurrency 10

The dispatcher sends only the task UUID. Arguments, task names, results,
retries, schedules, and leases remain in queue storage. A private
``litestar_queues_attempt`` message attribute fences each delivery to the exact
persisted retry generation and dispatch attempt.

Standard and FIFO queues
------------------------

Standard queues are the default. Set ``fifo=True`` for a FIFO queue; the
attempt reference becomes ``MessageDeduplicationId`` and a stable bounded group
is derived from the persisted queue name. Set ``message_group_id`` to override
that group.

SQS visibility is only crash-redelivery courtesy. It never replaces the queue
backend heartbeat lease, retry count, scheduling, or stale recovery.

LocalStack
----------

For local development, create a queue in LocalStack and set
``endpoint_url="http://localhost:4566"``. Use the usual test credentials through
the AWS credential chain; credentials are intentionally not configuration
fields on ``SqsExecutionConfig``.

IAM
---

Dispatchers need ``sqs:SendMessage``. Consumers need ``sqs:ReceiveMessage``,
``sqs:DeleteMessage``, and ``sqs:ChangeMessageVisibility`` on the configured
queue.
