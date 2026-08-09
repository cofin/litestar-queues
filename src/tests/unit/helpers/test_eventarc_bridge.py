import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.integration.execution.pubsub.test_eventarc_push import (
    _close_transport_resources,
    _PayloadTooLargeError,
    _read_request_body,
)

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("length", ["-1", "not-an-int"])
async def test_bridge_rejects_invalid_content_length(length: "str") -> "None":
    with pytest.raises(ValueError, match="Content-Length"):
        await _read_request_body(asyncio.StreamReader(), {"content-length": length})


async def test_bridge_rejects_oversized_body_before_reading() -> "None":
    reader = AsyncMock(spec=asyncio.StreamReader)
    with pytest.raises(_PayloadTooLargeError):
        await _read_request_body(reader, {"content-length": str(64 * 1024 + 1)})
    reader.readexactly.assert_not_called()


async def test_bridge_times_out_incomplete_body(monkeypatch: "pytest.MonkeyPatch") -> "None":
    async def wait_for(awaitable: "Any", *, timeout: "float") -> "None":
        del timeout
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr("tests.integration.execution.pubsub.test_eventarc_push.asyncio.wait_for", wait_for)
    with pytest.raises(TimeoutError):
        await _read_request_body(asyncio.StreamReader(), {"content-length": "1"})


async def test_transport_cleanup_preserves_causal_exception() -> "None":
    publisher = AsyncMock()
    publisher.transport.close.side_effect = RuntimeError("publisher close")
    channel = AsyncMock()
    channel.close.side_effect = RuntimeError("channel close")

    await _close_transport_resources(publisher, channel, causal_exception=True)

    publisher.transport.close.assert_awaited_once()
    channel.close.assert_awaited_once()


async def test_transport_cleanup_surfaces_failure_without_causal_exception() -> "None":
    publisher = AsyncMock()
    publisher.transport.close.side_effect = RuntimeError("publisher close")
    channel = AsyncMock()

    with pytest.raises(RuntimeError, match="publisher close"):
        await _close_transport_resources(publisher, channel, causal_exception=False)
