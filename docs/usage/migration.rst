===============
Migration notes
===============

Use canonical queue terminology in application code and documentation:

.. list-table::
   :header-rows: 1

   * - Older wording
     - Current wording
   * - Storage backend
     - Queue backend
   * - Job notification/event bus
     - Worker wakeup hint
   * - Realtime job event
     - Task event
   * - Live event sink as history
     - Queue-backend event history

Task storage, worker wakeups, live task-event delivery, and saved event history
are separate features. Configure each one explicitly. Do not assume that one
transport provides the others.

When moving from memory to a persistent backend:

* use task arguments the backend can serialize;
* create its schema or data structures;
* disable the in-app worker if workers run separately; and
* refresh ``TaskResult`` before reading a later state.

See :doc:`backends` for the current support matrix and :doc:`event-history`
for event-history ownership.

SQLSpec event transport names
=============================

SQLSpec 0.55 removed the old ``listen_notify``, ``listen_notify_durable``, and
``table_queue`` event transport names. Litestar Queues removes them in the same
release rather than retaining aliases. Update configuration to ``notify``,
``notify_queue``, or ``poll_queue`` respectively. Oracle ``aq`` and
``txeventq`` names are unchanged.

Forever-uniqueness tombstones
=============================

``unique_until="forever"`` stores a compact tombstone (identity key plus the
originating task id/name and creation time) in a table separate from the queue
task table so ordinary terminal and event maintenance never removes it. See
:doc:`task-options` for the identity model.

* **SQLSpec**: the packaged initial extension migration
  (``0001_create_queue_tasks``) provisions the ``<queue_table>_uniqueness`` table
  alongside the queue table for every supported adapter. ``manage_schema``
  backends create it automatically. Override the table with
  ``SQLSpecBackendConfig.uniqueness_table_name``.
* **Advanced Alchemy**: schema ownership stays with the adopter. Compose the new
  :class:`~litestar_queues.backends.advanced_alchemy.QueueUniquenessModelMixin`
  into an application-owned model (the default is ``QueueUniquenessModel``) and
  create or migrate its table with your own Alembic or ``create_all``. Provide a
  custom model through ``SQLAlchemyBackendConfig.uniqueness_model_class``.
* **Redis / Valkey**: a namespaced ``{prefix}:uniqueness`` hash is created on
  demand; no schema step is required.

Tombstones are removed only by an explicit
:meth:`~litestar_queues.QueueService.reset_task_identity` call with the exact
effective key.
