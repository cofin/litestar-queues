import inspect

from litestar_queues.events.history import QueueEventLog


def test_protocol_declares_extra() -> "None":
    signature = inspect.signature(QueueEventLog.query_events)
    assert "extra" in signature.parameters
    assert signature.parameters["extra"].default is None
