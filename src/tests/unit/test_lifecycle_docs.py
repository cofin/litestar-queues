from pathlib import Path

DOCS = Path("docs/usage")


def test_lifecycle_docs_keep_heartbeat_progress_and_events_distinct() -> None:
    events = (DOCS / "events.rst").read_text(encoding="utf-8")
    workers = (DOCS / "workers.rst").read_text(encoding="utf-8")

    for marker in ("Heartbeat", "Progress", "Custom event", "Lifecycle event"):
        assert marker in events
    assert "worker updates timestamps automatically" in events
    assert "beat(detail)" in events
    assert "should not also be copied into a generic custom event" in events
    assert "Heartbeat timestamps are automatic" in workers


def test_lifecycle_docs_record_terminal_paths_and_refresh_contract() -> None:
    events = (DOCS / "events.rst").read_text(encoding="utf-8")
    workers = (DOCS / "workers.rst").read_text(encoding="utf-8")
    results = (DOCS / "results.rst").read_text(encoding="utf-8")

    # A consumer distinguishes an attempt failure from a terminal one, and the
    # separate terminal paths stay named. The internal completion ordering is
    # deliberately undocumented: none of it is observable from task code.
    assert "will_retry" in events
    assert "claim loss" in events
    assert "stale failure" in workers
    assert "Refresh it after receiving a terminal event" in results
