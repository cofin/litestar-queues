"""Redis event-history dimensions, query, summary, and filtered retention."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from litestar_queues.backends.redis.event_log import RedisQueueEventLog as ValkeyQueueEventLog
from litestar_queues.events import EventHistoryConfig, QueueEvent

if TYPE_CHECKING:
    from litestar_queues.backends.valkey import ValkeyQueueBackend

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _event(event_id: str, *, offset: int = 0, **kwargs: object) -> QueueEvent:
    return QueueEvent(
        id=event_id,
        type=kwargs.pop("type", "task.log"),  # type: ignore[arg-type]
        scope=kwargs.pop("scope", "task"),  # type: ignore[arg-type]
        task_id="t-1",
        task_name="tasks.demo",
        occurred_at=BASE + timedelta(seconds=offset),
        **kwargs,  # type: ignore[arg-type]
    )


async def test_record_hash_carries_dimensions(valkey_backend: "ValkeyQueueBackend") -> "None":
    log = valkey_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert isinstance(log, ValkeyQueueEventLog)
    await log.publish_event(_event("a", scope_key="acme", entity={"type": "invoice", "id": "42"}))
    await log.flush_events()

    client = await valkey_backend._get_client()
    mapping = await client.hgetall(valkey_backend._event_log_event_key("a"))
    decoded = {k.decode() if isinstance(k, bytes) else k: v for k, v in mapping.items()}

    assert decoded["scope_key"] in {"acme", b"acme"}
    assert decoded["entity"] in {"invoice:42", b"invoice:42"}


async def test_dimension_indexes_are_registered_and_cleaned(valkey_backend: "ValkeyQueueBackend") -> "None":
    log = valkey_backend.get_event_log(EventHistoryConfig(batch_size=1))
    assert isinstance(log, ValkeyQueueEventLog)
    await log.publish_event(_event("a", scope_key="acme", entity={"type": "invoice", "id": "42"}))
    await log.flush_events()

    from typing import Any, cast

    client = cast("Any", await valkey_backend._get_client())  # Assert index cards
    from litestar_queues.backends.redis.event_log import hashed_index_value

    scope_index = f"{valkey_backend._key_prefix}:events:scope_key:{hashed_index_value('acme')}"
    entity_index = f"{valkey_backend._key_prefix}:events:entity:{hashed_index_value('invoice:42')}"

    assert await client.zcard(scope_index) == 1
    assert await client.zcard(entity_index) == 1

    # Cleanup past its occurred_at
    cutoff = BASE + timedelta(hours=1)
    # Note: cleanup_events replaces cleanup_before
    await log.cleanup_events(before=cutoff)

    # Assert both index keys are gone
    assert await client.zcard(scope_index) == 0
    assert await client.zcard(entity_index) == 0
