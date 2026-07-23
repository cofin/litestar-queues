========================
Persistent storage setup
========================

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

Forever-uniqueness reservations
===============================

``unique_until="forever"`` stores a compact reservation (identity key plus the
originating task id/name and creation time) in a table separate from the queue
task table so ordinary terminal and event maintenance never removes it. See
:doc:`task-options` for the identity model.

* **SQLSpec**: the packaged ``0001_create_queue_tasks`` migration provisions
  ``queue_task_reservation`` by default. The explicit
  ``create_schema()`` development fallback also includes it when
  ``manage_schema=True``. Override it with
  ``SQLSpecBackendConfig.task_reservation_table_name``.
* **Advanced Alchemy**: schema ownership stays with the adopter. Compose the new
  :class:`~litestar_queues.backends.advanced_alchemy.QueueTaskReservationModelMixin`
  into an application-owned model (the default is ``QueueTaskReservationModel``) and
  create or migrate its table with your own Alembic or ``create_all``. Provide a
  custom model through ``SQLAlchemyBackendConfig.task_reservation_model_class``.
* **Redis / Valkey**: a namespaced ``{prefix}:task_reservations`` hash is created on
  demand; no schema step is required.

Reservations are removed only by an explicit
:meth:`~litestar_queues.QueueService.reset_task_identity` call with the exact
effective key.
