"""Versioned, pickle-free task uniqueness identity.

Identity keys are namespaced ``lq:u:{version}:{mode}:{sha256}`` strings. The
identity mode (``task`` or ``arguments``) and the canonical-format version are
part of the namespace so a future canonical change never silently collides with
an older key, and changing a task's ``unique_until`` never rehashes its identity.

Only ``unique_by="arguments"`` inspects call arguments. It binds the callable's
cached :class:`inspect.Signature`, applies defaults, strips the injected
``_task_context``, encodes ``{task, arguments, version}`` once as sorted, compact,
strict JSON, reuses those bytes for the optional payload-size guard and the
SHA-256 digest, then releases them. Pickle and ``repr()`` are never used, and no
global argument-to-digest cache is kept.
"""

import hashlib
import json
from inspect import Parameter
from typing import TYPE_CHECKING, Any, NamedTuple

from litestar_queues.exceptions import TaskIdentityError, TaskIdentityTooLargeError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from inspect import Signature

__all__ = ("IDENTITY_NAMESPACE", "IDENTITY_VERSION", "ArgumentsIdentity", "arguments_identity", "task_identity")

IDENTITY_NAMESPACE = "lq:u"
"""Stable namespace prefix for every derived uniqueness key."""

IDENTITY_VERSION = "v1"
"""Canonical identity format version embedded in every derived key."""

_TASK_CONTEXT_KEY = "_task_context"


class ArgumentsIdentity(NamedTuple):
    """Result of computing an argument-derived identity."""

    key: "str"
    payload_bytes: "int"


def _identity_key(mode: "str", digest: "str") -> "str":
    return f"{IDENTITY_NAMESPACE}:{IDENTITY_VERSION}:{mode}:{digest}"


def _encode(payload: "Any") -> "bytes":
    """Encode a canonical identity payload to strict, sorted, compact JSON bytes.

    Returns:
        UTF-8 encoded canonical JSON bytes.

    Raises:
        TaskIdentityError: If the payload is not representable as strict JSON
            (non-finite floats, non-JSON objects, or unorderable mapping keys).
    """
    try:
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        msg = (
            "Task arguments are not representable as canonical JSON identity material; "
            "supply an explicit key or pass JSON-compatible, finite arguments."
        )
        raise TaskIdentityError(msg) from exc
    return text.encode("utf-8")


def task_identity(task_name: "str") -> "str":
    """Return the derived ``unique_by="task"`` identity key for a task name.

    Never inspects, binds, serializes, or hashes call arguments.

    Returns:
        A namespaced, versioned ``task``-mode identity key.
    """
    digest = hashlib.sha256(_encode({"task": task_name, "version": IDENTITY_VERSION})).hexdigest()
    return _identity_key("task", digest)


def arguments_identity(
    task_name: "str",
    signature: "Signature",
    args: "tuple[Any, ...]",
    kwargs: "Mapping[str, Any]",
    *,
    max_payload_bytes: "int | None" = None,
) -> "ArgumentsIdentity":
    """Return the derived ``unique_by="arguments"`` identity for a call.

    Binds ``args``/``kwargs`` against ``signature``, applies defaults, excludes
    the injected ``_task_context``, encodes the versioned canonical payload once,
    reuses the encoded bytes for the optional size guard and SHA-256 digest, then
    releases them.

    Returns:
        The namespaced, versioned ``arguments``-mode identity key and the measured
        canonical payload size in bytes.

    Raises:
        TaskIdentityTooLargeError: If ``max_payload_bytes`` is set and the canonical
            payload exceeds it.
    """
    arguments = _bound_arguments(signature, args, kwargs)
    encoded = _encode({"arguments": arguments, "task": task_name, "version": IDENTITY_VERSION})
    payload_bytes = len(encoded)
    if max_payload_bytes is not None and payload_bytes > max_payload_bytes:
        raise TaskIdentityTooLargeError(actual_bytes=payload_bytes, max_bytes=max_payload_bytes)
    digest = hashlib.sha256(encoded).hexdigest()
    return ArgumentsIdentity(_identity_key("arguments", digest), payload_bytes)


def _bound_arguments(signature: "Signature", args: "tuple[Any, ...]", kwargs: "Mapping[str, Any]") -> "dict[str, Any]":
    bound = signature.bind(*args, **dict(kwargs))
    bound.apply_defaults()
    arguments = dict(bound.arguments)
    arguments.pop(_TASK_CONTEXT_KEY, None)
    for name, parameter in signature.parameters.items():
        if parameter.kind is Parameter.VAR_KEYWORD and name in arguments:
            var_keyword = dict(arguments[name])
            var_keyword.pop(_TASK_CONTEXT_KEY, None)
            arguments[name] = var_keyword
    return arguments
