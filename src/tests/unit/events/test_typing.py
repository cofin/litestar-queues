from typing import get_args

from litestar_queues.typing import ChannelsLike, ChannelsPublishManyBackend


def test_channels_like_accepts_public_batch_publish_capability() -> None:
    assert ChannelsPublishManyBackend in get_args(ChannelsLike)
