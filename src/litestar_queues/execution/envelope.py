"""Universal, versioned dispatch envelope for external execution backends."""

from typing import TYPE_CHECKING, Any, cast

import msgspec

if TYPE_CHECKING:
    from litestar_queues.models import QueuedTaskRecord

__all__ = ("DISPATCH_ENVELOPE_VERSION", "DispatchEnvelope")

DISPATCH_ENVELOPE_VERSION = 1
"""Current dispatch-envelope schema version."""


class DispatchEnvelope(msgspec.Struct, rename="camel", kw_only=True):
    """Broker-agnostic routing slip a worker emits when dispatching a record to an external executor.

    A strict subset of :class:`~litestar_queues.models.QueuedTaskRecord`. It
    carries only what a remote consumer needs to locate and route the record;
    the live record in the queue backend stays the source of truth for result,
    heartbeat, and retry state. A consumer always re-fetches the record before
    executing it. The wire format is camelCase JSON.
    """

    task_id: "str"
    task_name: "str"
    queue: "str"
    execution_backend: "str"
    args: "tuple[Any, ...]" = ()
    kwargs: "dict[str, Any]" = msgspec.field(default_factory=dict)
    execution_profile: "str | None" = None
    version: "int" = DISPATCH_ENVELOPE_VERSION

    @classmethod
    def from_record(cls, record: "QueuedTaskRecord") -> "DispatchEnvelope":
        """Build an envelope from a queued task record.

        Returns:
            The dispatch envelope describing the record.
        """
        return cls(
            task_id=str(record.id),
            task_name=record.task_name,
            queue=record.queue,
            execution_backend=record.execution_backend,
            args=tuple(record.args),
            kwargs=dict(record.kwargs),
            execution_profile=record.execution_profile,
        )

    def to_dict(self) -> "dict[str, Any]":
        """Return the camelCase wire mapping for this envelope."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    def to_json(self) -> "bytes":
        """Return the envelope as camelCase JSON bytes."""
        return msgspec.json.encode(self)

    @classmethod
    def from_dict(cls, data: "dict[str, Any]") -> "DispatchEnvelope":
        """Build an envelope from a camelCase mapping, validating its version.

        Returns:
            The dispatch envelope.

        Raises:
            ValueError: If the envelope version is unsupported.
        """
        envelope = msgspec.convert(data, cls)
        _require_supported_version(envelope.version)
        return envelope

    @classmethod
    def from_json(cls, data: "str | bytes | bytearray") -> "DispatchEnvelope":
        """Build an envelope from camelCase JSON text or bytes, validating its version.

        Returns:
            The dispatch envelope.

        Raises:
            TypeError: If the decoded JSON value is not an object.
            ValueError: If the envelope version is unsupported.
        """
        payload = bytes(data) if isinstance(data, bytearray) else data
        decoded = msgspec.json.decode(payload)
        if not isinstance(decoded, dict):
            msg = "Dispatch envelope JSON must decode to an object"
            raise TypeError(msg)
        return cls.from_dict(cast("dict[str, Any]", decoded))


def _require_supported_version(version: "int") -> "None":
    if version != DISPATCH_ENVELOPE_VERSION:
        msg = f"Unsupported dispatch envelope version {version!r}; this build supports {DISPATCH_ENVELOPE_VERSION}."
        raise ValueError(msg)
