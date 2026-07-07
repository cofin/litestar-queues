from typing import get_args

from litestar_queues.events._typing import ChannelsLike, ChannelsWaitPublishedManyBackend


def test_channels_like_requires_single_event_publish_capability() -> None:
    assert ChannelsWaitPublishedManyBackend not in get_args(ChannelsLike)
