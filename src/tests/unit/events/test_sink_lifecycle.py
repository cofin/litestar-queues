import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, Any

import pytest
from litestar.channels import ChannelsPlugin
from litestar.channels.backends.memory import MemoryChannelsBackend

from litestar_queues.events import CompositeQueueEventSink
from litestar_queues.events.channels_sink import ChannelsQueueEventSink

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events import QueueEvent

pytestmark = pytest.mark.anyio


class _LifecycleBackend:
    def __init__(self, calls: "list[str]") -> "None":
        self.calls = calls

    async def on_startup(self) -> "None":
        self.calls.append("startup")

    async def on_shutdown(self) -> "None":
        self.calls.append("shutdown")

    async def publish(self, data: "bytes", channels: "Sequence[str]") -> "None":
        del data, channels


class _RecordingChannelsPlugin(ChannelsPlugin):
    def __init__(self, calls: "list[tuple[str, object]]") -> "None":
        super().__init__(backend=MemoryChannelsBackend(), arbitrary_channels_allowed=True)
        self.calls = calls
        self.entered: "object | None" = None

    async def __aenter__(self) -> "Any":
        self.entered = object()
        self.calls.append(("enter", self.entered))
        return self.entered

    async def __aexit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_val: "BaseException | None",  # noqa: PYI036
        exc_tb: TracebackType | None,
    ) -> "None":
        self.calls.append(("exit", (exc_type, exc_val, exc_tb)))


class _LifecycleSink:
    def __init__(
        self,
        name: "str",
        calls: "list[str]",
        *,
        fail_open: "bool" = False,
        fail_close: "bool" = False,
        close_error: "BaseException | None" = None,
    ) -> "None":
        self.name = name
        self.calls = calls
        self.fail_open = fail_open
        self.fail_close = fail_close
        self.close_error = close_error

    async def open(self) -> "None":
        self.calls.append(f"{self.name}.open")
        if self.fail_open:
            msg = f"{self.name} open failed"
            raise RuntimeError(msg)

    async def close(self) -> "None":
        self.calls.append(f"{self.name}.close")
        if self.close_error is not None:
            raise self.close_error
        if self.fail_close:
            msg = f"{self.name} close failed"
            raise RuntimeError(msg)

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        del event, channels


async def test_channels_sink_does_not_manage_lifecycle_by_default() -> "None":
    calls: "list[str]" = []
    sink = ChannelsQueueEventSink(_LifecycleBackend(calls))

    await sink.open()
    await sink.close()

    assert calls == []


async def test_channels_sink_enters_exact_plugin_context_and_retains_result() -> "None":
    calls: "list[tuple[str, object]]" = []
    plugin = _RecordingChannelsPlugin(calls)
    sink = ChannelsQueueEventSink(plugin, manage_lifecycle=True)

    await sink.open()
    await sink.open()

    assert calls == [("enter", plugin.entered)]
    assert sink._lifecycle_resource is plugin.entered

    await sink.close()
    await sink.close()

    assert calls == [("enter", plugin.entered), ("exit", (None, None, None))]
    assert sink._lifecycle_resource is None


async def test_channels_sink_uses_public_backend_lifecycle_fallback() -> "None":
    calls: "list[str]" = []
    sink = ChannelsQueueEventSink(_LifecycleBackend(calls), manage_lifecycle=True)

    await sink.open()
    await sink.open()
    await sink.close()
    await sink.close()

    assert calls == ["startup", "shutdown"]


async def test_composite_opens_in_order_and_closes_in_reverse() -> "None":
    calls: "list[str]" = []
    composite = CompositeQueueEventSink([
        _LifecycleSink("one", calls),
        _LifecycleSink("two", calls),
        _LifecycleSink("three", calls),
    ])

    await composite.open()
    await composite.open()
    await composite.close()
    await composite.close()

    assert calls == ["one.open", "two.open", "three.open", "three.close", "two.close", "one.close"]


async def test_composite_rolls_back_partial_open() -> "None":
    calls: "list[str]" = []
    composite = CompositeQueueEventSink([
        _LifecycleSink("one", calls),
        _LifecycleSink("two", calls, fail_open=True),
        _LifecycleSink("three", calls),
    ])

    with pytest.raises(RuntimeError, match="two open failed"):
        await composite.open()

    assert calls == ["one.open", "two.open", "one.close"]


@pytest.mark.parametrize("strict", [False, True])
async def test_composite_close_attempts_every_sink_after_failures(*, strict: "bool") -> "None":
    calls: "list[str]" = []
    composite = CompositeQueueEventSink(
        [
            _LifecycleSink("one", calls, fail_close=True),
            _LifecycleSink("two", calls),
            _LifecycleSink("three", calls, fail_close=True),
        ],
        strict=strict,
    )
    await composite.open()

    if strict:
        with pytest.raises(RuntimeError, match="three close failed"):
            await composite.close()
    else:
        await composite.close()

    assert calls[-3:] == ["three.close", "two.close", "one.close"]


@pytest.mark.parametrize("strict", [False, True])
async def test_composite_close_never_swallows_cancellation_after_ordinary_failure(*, strict: "bool") -> "None":
    calls: "list[str]" = []
    composite = CompositeQueueEventSink(
        [
            _LifecycleSink("one", calls),
            _LifecycleSink("two", calls, close_error=asyncio.CancelledError()),
            _LifecycleSink("three", calls, fail_close=True),
        ],
        strict=strict,
    )
    await composite.open()

    with pytest.raises(asyncio.CancelledError):
        await composite.close()

    assert calls[-3:] == ["three.close", "two.close", "one.close"]
