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
