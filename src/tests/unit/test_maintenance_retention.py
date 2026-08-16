"""Ordered, filtered, bounded event-history retention."""

import pytest
from typing import Any

from litestar_queues.events import QueueEventQuery, QueueEventRetentionRule
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.maintenance import QueueMaintenanceConfig


def test_event_retention_is_rule_driven() -> "None":
    config = QueueMaintenanceConfig(
        event_retention_rules=(
            QueueEventRetentionRule(max_age=3600.0, match=QueueEventQuery(scope="task")),
            QueueEventRetentionRule(max_age=86400.0),
        )
    )

    assert len(config.event_retention_rules) == 2
    assert not hasattr(config, "event_retention")


def test_rejects_non_rule_entries() -> "None":
    with pytest.raises(QueueConfigurationError):
        QueueMaintenanceConfig(event_retention_rules=(3600.0,))  # type: ignore[arg-type]


def test_empty_rules_disable_the_events_phase() -> "None":
    config = QueueMaintenanceConfig()

    assert config.event_retention_rules == ()

@pytest.mark.anyio
async def test_rules_run_in_order_with_cumulative_excludes_and_shared_budget() -> "None":
    calls: "list[tuple[float, tuple[str, ...], int]]" = []

    class RecordingLog:
        async def publish_event(self, event: "object") -> "None": ...
        async def flush_events(self) -> "None": ...
        async def query_events(self, query: "object | None" = None) -> "object": ...
        async def summarize_stages(self, query: "object | None" = None) -> "list[object]":
            return []

        async def cleanup_events(
            self, *, before: "Any", match: "Any" = None, exclude: "Any" = (), limit: "Any" = None
        ) -> "int":
            calls.append((
                before.timestamp(),
                tuple(q.scope_key or "" for q in exclude),
                limit,
            ))
            return 2

    # drive QueueMaintenanceService with a stub QueueService exposing this log,
    # event_limit=5, and three rules with scope_key "a", "b", and no filter.
    from litestar_queues.maintenance import QueueMaintenanceService
    from unittest.mock import MagicMock
    import asyncio
    
    mock_service = MagicMock()
    mock_service.get_event_log.return_value = RecordingLog()
    mock_backend = MagicMock()
    mock_backend.capabilities.supports_maintenance = True
    
    # We must mock acquire_maintenance and release_maintenance
    async def _acquire(*args: "Any", **kwargs: "Any") -> bool: return True
    async def _release(*args: "Any", **kwargs: "Any") -> None: return None
    mock_backend.acquire_maintenance = _acquire
    mock_backend.release_maintenance = _release
    
    mock_service.get_queue_backend.return_value = mock_backend
    
    config = QueueMaintenanceConfig(
        event_limit=5,
        event_retention_rules=(
            QueueEventRetentionRule(max_age=3600, match=QueueEventQuery(scope_key="a")),
            QueueEventRetentionRule(max_age=7200, match=QueueEventQuery(scope_key="b")),
            QueueEventRetentionRule(max_age=86400),
        )
    )
    maint_service = QueueMaintenanceService(mock_service, config)
    
    await maint_service.run()

    # The rule limits should be 5, 3, 1 since each returns 2 deleted.
    # Excludes: (), ("a",), ("a", "b")
    
    assert [call[1] for call in calls] == [(), ("a",), ("a", "b")]
    assert [call[2] for call in calls] == [5, 3, 1]
