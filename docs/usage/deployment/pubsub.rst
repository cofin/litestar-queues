=============================
Google Cloud Pub/Sub dispatch
=============================

Read :doc:`execution-transports` first: it covers the dispatcher/consumer
model, the two CLI commands, and the at-least-once delivery guarantee that
Pub/Sub shares with the other transports. This page covers only what is
specific to Pub/Sub.

Install and configure
=====================

.. code-block:: bash

   pip install "litestar-queues[pubsub]"

Create one topic and one **pull** subscription, then configure their short
resource names:

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

Then run ``litestar queues run`` and
``litestar queues run-consumer --backend pubsub``.

IAM
===

The dispatcher needs ``pubsub.topics.publish`` on the topic. Each consumer
needs ``pubsub.subscriptions.consume`` on the subscription. Creating the topic
and subscription belongs to deployment automation, not the application
runtime.

Local emulator
==============

Google's official Pub/Sub emulator works for local development and tests.
Point the execution config at the emulator's host and enable plaintext gRPC:

.. code-block:: python

   from litestar_queues.execution.pubsub import PubSubExecutionConfig

   execution_backend = PubSubExecutionConfig(
       project_id="local-project",
       topic_id="tasks",
       subscription_id="workers",
       api_endpoint="127.0.0.1:8085",
       api_insecure=True,
   )

Create the topic and subscription in the emulator before starting the
dispatcher. ``api_insecure=True`` is rejected unless ``api_endpoint`` is also
set, so a plaintext connection to production is not reachable by accident.
