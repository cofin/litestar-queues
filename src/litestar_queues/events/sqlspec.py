"""SQLSpec event-channel sink for external queue event producers."""

import asyncio
import inspect
from copy import deepcopy
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litestar_queues.events.chunking import QueueEventSizeEstimator
    from litestar_queues.events.models import QueueEvent

__all__ = ("SQLSpecQueueEventSink",)

_EVENT_EXTENSION_NAME = "events"
_POLLING_TRANSPORT = "polling"


class SQLSpecQueueEventSink:
    """Publish queue events through a SQLSpec sync or async event channel."""

    __slots__ = (
        "_backend_config",
        "_channel",
        "_max_payload_bytes",
        "_owns_channel",
        "_owns_sqlspec",
        "_payload_size_estimator",
        "_sqlspec",
        "_sqlspec_config",
    )

    def __init__(
        self,
        backend_config: "object",
        *,
        max_payload_bytes: "int | None" = None,
        payload_size_estimator: "QueueEventSizeEstimator | None" = None,
    ) -> "None":
        self._backend_config = backend_config
        self._channel = getattr(backend_config, "event_channel", None)
        self._owns_channel = self._channel is None
        self._sqlspec = getattr(backend_config, "sqlspec", None)
        self._owns_sqlspec = self._sqlspec is None
        self._sqlspec_config = getattr(backend_config, "config", None)
        self._max_payload_bytes = max_payload_bytes
        self._payload_size_estimator = payload_size_estimator

    async def open(self) -> "None":
        """Lifecycle hook kept for external producer symmetry."""

    async def close(self) -> "None":
        """Close only SQLSpec resources owned by this sink."""
        if self._owns_channel and self._channel is not None:
            await _invoke_channel_method(self._channel, "shutdown")
            self._channel = None
        if self._owns_sqlspec and self._sqlspec is not None:
            result = self._sqlspec.close_all_pools()
            if inspect.isawaitable(result):
                await result
            self._sqlspec = None

    async def publish(self, event: "QueueEvent", *, channels: "Sequence[str]") -> "None":
        """Publish one event to each requested SQLSpec channel."""
        for event_chunk in self._event_chunks(event):
            for channel in channels:
                await self._publish_one(event_chunk, channel=channel)

    async def publish_many(self, batch: "Sequence[tuple[QueueEvent, Sequence[str]]]") -> "None":
        """Publish an ordered producer batch through one SQLSpec operation."""
        events: "list[tuple[str, dict[str, Any], dict[str, object]]]" = []
        for event, channels in batch:
            for event_chunk in self._event_chunks(event):
                payload = event_chunk.to_dict()
                metadata = _metadata_for(event_chunk)
                events.extend((channel, payload, metadata) for channel in channels)
        if events:
            await _invoke_channel_method(self._get_event_channel(), "publish_many", events)

    def _event_chunks(self, event: "QueueEvent") -> "Sequence[QueueEvent]":
        if self._max_payload_bytes is None:
            return (event,)
        from litestar_queues.events.chunking import estimate_event_payload_bytes, split_event_batch_by_size

        estimator = self._payload_size_estimator or estimate_event_payload_bytes
        return split_event_batch_by_size(event, max_bytes=self._max_payload_bytes, size_estimator=estimator)

    async def _publish_one(self, event: "QueueEvent", *, channel: "str") -> "None":
        event_channel = self._get_event_channel()
        await _invoke_channel_method(event_channel, "publish", channel, event.to_dict(), _metadata_for(event))

    def _get_event_channel(self) -> "Any":
        if self._channel is None:
            sqlspec_config = self._get_sqlspec_config()
            self._apply_event_settings(sqlspec_config)
            self._channel = self._get_or_create_sqlspec().event_channel(sqlspec_config)
            self._owns_channel = True
        return self._channel

    def _get_or_create_sqlspec(self) -> "Any":
        if self._sqlspec is None:
            from sqlspec import SQLSpec

            self._sqlspec = SQLSpec()
        return self._sqlspec

    def _get_sqlspec_config(self) -> "Any":
        if self._sqlspec_config is not None:
            return self._sqlspec_config
        registered_configs = tuple(cast("dict[int, Any]", self._get_or_create_sqlspec().configs).values())
        if len(registered_configs) == 1:
            self._sqlspec_config = registered_configs[0]
            return self._sqlspec_config
        if len(registered_configs) > 1:
            from litestar_queues.exceptions import QueueConfigurationError

            msg = (
                "SQLSpec event producer received a SQLSpec manager with multiple configs; "
                "pass config to select the event database."
            )
            raise QueueConfigurationError(msg)
        from sqlspec.adapters.aiosqlite import AiosqliteConfig

        self._sqlspec_config = AiosqliteConfig()
        return self._sqlspec_config

    def _apply_event_settings(self, sqlspec_config: "Any") -> "None":
        extension_config = deepcopy(getattr(sqlspec_config, "extension_config", None) or {})
        event_settings = dict(extension_config.get(_EVENT_EXTENSION_NAME, {}))
        configured_settings = getattr(self._backend_config, "event_settings", None)
        if isinstance(configured_settings, dict):
            event_settings.update(configured_settings)
        backend_name = _event_backend_name(self._backend_config, event_settings)
        if backend_name is not None:
            event_settings["backend"] = backend_name
        queue_table = getattr(self._backend_config, "event_queue_table", None)
        if queue_table is not None:
            event_settings["queue_table"] = str(queue_table)
        poll_interval = getattr(self._backend_config, "event_poll_interval", None)
        if poll_interval is not None:
            event_settings["poll_interval"] = float(poll_interval)
        extension_config[_EVENT_EXTENSION_NAME] = event_settings
        sqlspec_config.extension_config = extension_config
        migration_config = dict(getattr(sqlspec_config, "migration_config", None) or {})
        set_migration_config = getattr(sqlspec_config, "set_migration_config", None)
        if set_migration_config is not None:
            set_migration_config(migration_config)
        else:
            sqlspec_config.migration_config = migration_config


def _event_backend_name(backend_config: "object", event_settings: "dict[str, Any]") -> "str | None":
    configured = getattr(backend_config, "event_backend", None)
    if configured is not None:
        return str(configured)
    configured = event_settings.get("backend")
    if configured is not None:
        return str(configured)
    transport = getattr(backend_config, "notify_transport", None)
    if transport is not None and transport != _POLLING_TRANSPORT:
        return str(transport)
    return None


def _metadata_for(event: "QueueEvent") -> "dict[str, object]":
    return {"event_type": event.type, "queue_event_id": event.id, "queue_event_scope": event.scope}


async def _invoke_channel_method(channel: "Any", method_name: "str", *args: "Any") -> "Any":
    """Invoke a sync or async SQLSpec channel method without blocking the event loop.

    Returns:
        The event-channel method result.
    """
    method = getattr(channel, method_name)
    if inspect.iscoroutinefunction(method):
        return await method(*args)
    result = await asyncio.to_thread(method, *args)
    if inspect.isawaitable(result):
        return await result
    return result
