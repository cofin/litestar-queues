from pathlib import Path

import pytest

EVENTS_DOC = Path("docs/events/index.md")
WORKERS_DOC = Path("docs/learn/workers.md")
RESULTS_DOC = Path("docs/learn/results.md")


@pytest.mark.parametrize(
    "marker",
    [
        "Heartbeat",
        "Progress",
        "Custom event",
        "Lifecycle event",
        "worker updates timestamps automatically",
        "beat(detail)",
        "should not also be copied into a generic custom event",
    ],
)
def test_lifecycle_docs_keep_heartbeat_progress_and_events_distinct(marker: str) -> None:
    events = EVENTS_DOC.read_text(encoding="utf-8")
    workers = WORKERS_DOC.read_text(encoding="utf-8")

    assert marker in events
    assert "Heartbeat timestamps are automatic" in workers


def test_lifecycle_docs_record_terminal_paths_and_refresh_contract() -> None:
    events = EVENTS_DOC.read_text(encoding="utf-8")
    workers = WORKERS_DOC.read_text(encoding="utf-8")
    results = RESULTS_DOC.read_text(encoding="utf-8")

    assert "will_retry" in events
    assert "claim loss" in events
    assert "stale failure" in workers
    assert "Refresh it after receiving a terminal event" in results
