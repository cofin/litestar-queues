"""Readiness helpers shared by Redis-protocol integration tests."""

from typing import TYPE_CHECKING, Any, cast

from tests.helpers._timing import wait_until

if TYPE_CHECKING:
    from litestar_queues.backends.redis import RedisQueueBackend


async def wait_for_channel_subscribers(
    backend: "RedisQueueBackend", channel: str, *, expected: int = 1, timeout: float = 2.0
) -> "None":
    """Wait until the server confirms the expected pub/sub subscription."""

    client = cast("Any", await backend._get_client())

    async def subscribed() -> "bool":
        response = await client.pubsub_numsub(channel)
        if isinstance(response, dict):
            return int(response.get(channel, 0)) >= expected
        for item in response or ():
            if isinstance(item, (list, tuple)) and len(item) >= 2 and str(item[0]) == channel:
                return int(item[1]) >= expected
        return False

    await wait_until(
        subscribed,
        timeout=timeout,
        message=f"channel {channel!r} did not reach {expected} subscriber(s)",
    )
