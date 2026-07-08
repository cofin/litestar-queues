from litestar_queues import events


def test_legacy_stream_queue_events_removed() -> None:
    assert "stream_queue_events" not in events.__all__
    assert not hasattr(events, "stream_queue_events")
