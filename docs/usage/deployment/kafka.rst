Kafka Dispatch
==============

Install the optional asynchronous client:

.. code-block:: bash

   pip install "litestar-queues[kafka]"

Configure durable shared queue storage, then use Kafka only as the execution
transport:

.. code-block:: python

   from litestar_queues import QueueConfig, WorkerConfig
   from litestar_queues.backends.redis import RedisBackendConfig
   from litestar_queues.execution.kafka import KafkaExecutionConfig

   queue_config = QueueConfig(
       queue_backend=RedisBackendConfig(url="redis://redis:6379/0"),
       execution_backend=KafkaExecutionConfig(
           bootstrap_servers="kafka:9092",
           topic="queue-dispatch",
           consumer_group="queue-workers",
       ),
       worker=WorkerConfig(placement="external"),
   )

Run one dispatcher and one or more members of the same consumer group:

.. code-block:: bash

   LITESTAR_APP=app:app litestar queues run
   LITESTAR_APP=app:app litestar queues run-consumer --backend kafka --max-concurrency 16

Kafka records contain only the UTF-8 task UUID and an opaque attempt header.
The queue backend remains authoritative for arguments, eligibility, retries,
cancellation, results, and terminal state.

Delivery and cancellation
-------------------------

The consumer disables automatic offset commits. It processes each partition
in offset order and commits the next offset only after the queue operation has
reported a durable outcome. A process failure before that commit redelivers the
record; retry-count and execution-reference fences prevent an old delivery from
claiming a newer attempt.

Kafka cannot remove one record from a log. Cancellation therefore updates the
durable task record, and a later delivery observes that terminal state without
executing the task. Scheduling and priority are also resolved before dispatch;
Kafka provides neither delayed delivery nor per-record priority here.

Use a dedicated topic and consumer group per queue deployment. Grant producers
write access to that topic and consumers read plus group access. Pass supported
aiokafka TLS or SASL keywords through ``producer_options`` and
``consumer_options``; bootstrap servers, group identity, and manual-commit mode
remain backend-owned and cannot be overridden through those mappings.
