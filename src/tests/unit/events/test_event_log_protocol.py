"""The durable event-history protocol surface."""

from litestar_queues.events import QueueEventLog


def test_protocol_members() -> "None":
    members = {name for name in dir(QueueEventLog) if not name.startswith("_")} | set(
        getattr(QueueEventLog, "__annotations__", {})
    )
    # Some python 3.10 implementations of Protocol lack __protocol_attrs__
    if hasattr(QueueEventLog, "__protocol_attrs__"):
        members = set(QueueEventLog.__protocol_attrs__)

    assert members == {"publish_event", "flush_events", "query_events", "summarize_stages", "cleanup_events"}


def test_every_backend_event_log_satisfies_the_protocol() -> "None":
    from litestar_queues.backends.memory.event_log import InMemoryQueueEventLog

    def accepts(log: "QueueEventLog") -> "QueueEventLog":
        return log

    assert accepts(InMemoryQueueEventLog.__new__(InMemoryQueueEventLog)) is not None
