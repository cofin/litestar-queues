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

Queue persistence, worker wakeups, live task-event delivery, and durable event
history are separate capabilities. Configure each explicitly instead of
assuming one transport provides the others.

When moving from memory to a persistent backend, make task arguments
serializable, provision schema or data structures through the selected
backend, disable the in-app worker if workers run separately, and refresh
``TaskResult`` before reading later state.

See :doc:`backends` for the current support matrix and :doc:`event-history`
for event-history ownership.
