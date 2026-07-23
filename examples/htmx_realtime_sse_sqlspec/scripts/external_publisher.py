import asyncio
from typing import TYPE_CHECKING

from litestar_queues.events import create_event_producer

if TYPE_CHECKING:
    from litestar_queues import QueueConfig

__all__ = ("build_shared_broker_config", "main")


def build_shared_broker_config() -> "QueueConfig":
    """Return a QueueConfig that uses a cross-process Channels backend.

    The main demo uses MemoryChannelsBackend, which is intentionally
    process-local. Replace this placeholder with QueueEventsConfig(channels=..., delivery=EventDeliveryConfig())
    backed by Redis or SQLSpec Channels before running this script.
    """
    msg = "Configure Redis or SQLSpec Channels before publishing from an external process."
    raise RuntimeError(msg)


async def main() -> None:
    async with create_event_producer(build_shared_broker_config()) as queue_events:
        await queue_events.channel("demo:mission-control").publish(
            "mission.control",
            message="External publisher reached mission control",
            payload={"source": "external-publisher"},
            immediate=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
