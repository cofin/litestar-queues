from pathlib import Path

DOCS = Path("docs/usage")


def test_lifecycle_docs_keep_heartbeat_progress_and_events_distinct() -> None:
    events = (DOCS / "events.rst").read_text(encoding="utf-8")
    recovery = (DOCS / "worker-recovery.rst").read_text(encoding="utf-8")

    for marker in ("Heartbeat", "Progress", "Custom event", "Lifecycle event"):
        assert marker in events
    assert "worker updates timestamps automatically" in events
    assert "beat(detail)" in events
    assert "should not also be copied into a generic custom event" in events
    assert "Heartbeat timestamps are automatic" in recovery


def test_lifecycle_docs_record_terminal_order_and_refresh_contract() -> None:
    recovery = (DOCS / "worker-recovery.rst").read_text(encoding="utf-8")
    results = (DOCS / "results.rst").read_text(encoding="utf-8")

    ordered = (
        "task body returns",
        "completed record and result",
        "task.completed",
        "flushes buffered",
        "schedules the next recurring run",
        "clears heartbeat ownership",
    )
    positions = [recovery.index(marker) for marker in ordered]
    assert positions == sorted(positions)
    for marker in ("will_retry=True", "will_retry=False", "claim loss", "stale failure"):
        assert marker in recovery
    assert "Refresh it after receiving a terminal event" in results
