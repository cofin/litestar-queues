# Events, Streams & History

API reference for event delivery, SSE/WebSocket streaming configuration, durable event history, task execution contexts, standalone producers, and event sinks.

<!-- Canonical event configuration modules:
litestar_queues.events.config
litestar_queues.events.history
litestar_queues.events.stream_config
-->

## Event & Stream Configuration

::: litestar_queues.events.QueueEventsConfig
    options:
      inherited_members: false

::: litestar_queues.events.EventDeliveryConfig
    options:
      inherited_members: false

::: litestar_queues.events.EventBufferConfig
    options:
      inherited_members: false

::: litestar_queues.events.EventStreamConfig
    options:
      inherited_members: false

## Durable Event History & Queries

::: litestar_queues.events.EventHistoryConfig
    options:
      inherited_members: false

::: litestar_queues.events.EventHistoryExtraColumn
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventLog
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.events.QueueEventLogRecord
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventStageSummary
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventRetentionRule
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventQuery
    options:
      inherited_members: false

## Task Context & Publish Helpers

::: litestar_queues.events.TaskExecutionContext
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.events.TaskBeatSink
    options:
      inherited_members: false

::: litestar_queues.events.bind_task_context

::: litestar_queues.events.bind_beat_sink

::: litestar_queues.events.get_current_task_context

::: litestar_queues.events.require_current_task_context

::: litestar_queues.events.beat

::: litestar_queues.events.publish_task_event

::: litestar_queues.events.publish_task_progress

::: litestar_queues.events.publish_task_log

## Event Models, Publisher, Producer & Sinks

::: litestar_queues.events.QueueEvent
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventActor
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventEntityRef
    options:
      inherited_members: false

::: litestar_queues.events.QueueEventPublisher
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.events.QueueEventProducer
    options:
      inherited_members: false
      show_source: false

::: litestar_queues.events.create_event_producer

::: litestar_queues.events.QueueEventSink
    options:
      inherited_members: false

::: litestar_queues.events.InMemoryQueueEventSink
    options:
      inherited_members: false

::: litestar_queues.events.ChannelsQueueEventSink
    options:
      inherited_members: false

::: litestar_queues.events.streaming.StreamMetrics
    options:
      inherited_members: false
