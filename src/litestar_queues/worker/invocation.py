"""Identifies the Litestar CLI server invocation that owns this process tree.

The Litestar CLI enters ``CLIPlugin.server_lifespan`` once, around the whole
server command. That context publishes a fresh nonce and an exclusive marker
file; every ASGI process in the resulting tree can then tell whether it belongs
to an invocation that already owns a queue worker.

The marker path and the nonce travel in two separate environment variables so
nothing has to parse a delimiter out of a Windows drive letter, a path with
spaces, or a non-ASCII directory name.

This answers "was I launched correctly", nothing more. It is not a security
boundary, it never starts or stops a worker, and it deliberately performs no
liveness probe of the recorded owner: Uvicorn's reload/multi-worker modes and
alternative Litestar run-command plugins all insert legitimate intermediate
processes.
"""

import json
import logging
import os
import secrets
import signal
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Generator
    from types import FrameType

__all__ = (
    "MARKER_ENV_VAR",
    "MARKER_VERSION",
    "NONCE_ENV_VAR",
    "console_break_unwinds",
    "server_context",
    "server_context_active",
)

logger = logging.getLogger(__name__)

NONCE_ENV_VAR = "LITESTAR_QUEUES_SERVER_NONCE"
MARKER_ENV_VAR = "LITESTAR_QUEUES_SERVER_MARKER"
MARKER_VERSION = 1

_MARKER_NAME = "server.json"
_MARKER_FIELDS = frozenset({"version", "owner_pid", "nonce"})
_DIRECTORY_PREFIX = "litestar-queues-server-"
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_NONCE_BYTES = 16
_CLEANUP_DEADLINE = 5.0
_CLEANUP_RETRY = 0.05

_PREEXISTING_ERROR = (
    "A Litestar queue server context is already active in this process tree. Nested "
    "'litestar run' invocations cannot share one queue worker."
)
_CLEANUP_WARNING = "Could not remove the private queue server marker directory; it is left for inspection."

_ACTIVE_SERVER_CONTEXTS: "set[str]" = set()


def _remove(directory: "Path", marker: "Path") -> "None":
    """Unlink the marker and its private directory, retrying a bounded number of times."""
    deadline = time.monotonic() + _CLEANUP_DEADLINE
    while True:
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            if time.monotonic() >= deadline:
                break
            time.sleep(_CLEANUP_RETRY)
            continue
        break
    while True:
        try:
            directory.rmdir()
        except FileNotFoundError:
            return
        except OSError:
            if time.monotonic() >= deadline:
                logger.warning(_CLEANUP_WARNING)
                return
            time.sleep(_CLEANUP_RETRY)
            continue
        return


def _sys_platform() -> "str":
    return sys.platform


def _raise_keyboard_interrupt(signum: "int", frame: "FrameType | None") -> "None":
    """Handle a console break the way Python already handles a console interrupt.

    Raises:
        KeyboardInterrupt: Always.
    """
    del signum, frame
    raise KeyboardInterrupt


@contextmanager
def console_break_unwinds() -> "Generator[None]":
    """Let Ctrl+Break unwind the server process instead of killing it outright.

    Uvicorn re-raises whichever signal shut it down once it has restored the
    handler that was installed before it, so the console gets the exit status it
    expects. Python installs a handler for ``SIGINT`` but leaves ``SIGBREAK`` on
    the C default, and that default terminates the process on the spot with exit
    code 3. Everything wrapped around the server is then skipped, including the
    context that removes this invocation's private database.

    Giving ``SIGBREAK`` the same handler ``SIGINT`` already has turns Uvicorn's
    re-raise back into an ordinary unwind. Ctrl+C is unaffected: it arrives as
    ``SIGINT``, which was never the broken case.

    Yields:
        None: with Ctrl+Break routed through the interpreter.
    """
    console_break = getattr(signal, "SIGBREAK", None)
    if _sys_platform() != "win32" or console_break is None:
        yield
        return
    previous = signal.signal(console_break, _raise_keyboard_interrupt)
    try:
        yield
    finally:
        signal.signal(console_break, previous)


@contextmanager
def server_context() -> "Generator[str]":
    """Mark this process tree as owned for the lifetime of a server invocation.

    Yields:
        str: The invocation nonce, reused as the ephemeral database nonce.

    Raises:
        QueueConfigurationError: If a marker is already present in the environment.
    """
    if os.environ.get(NONCE_ENV_VAR) or os.environ.get(MARKER_ENV_VAR):
        raise QueueConfigurationError(_PREEXISTING_ERROR)
    nonce = secrets.token_hex(_NONCE_BYTES)
    directory = Path(tempfile.mkdtemp(prefix=_DIRECTORY_PREFIX))
    try:
        directory.chmod(_DIRECTORY_MODE)
    except OSError:
        logger.debug("Could not restrict the private queue server marker directory permissions.")
    marker = directory / _MARKER_NAME
    document = json.dumps({"version": MARKER_VERSION, "owner_pid": os.getpid(), "nonce": nonce})
    try:
        descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, _FILE_MODE)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(document)
    except BaseException:
        _remove(directory, marker)
        raise
    os.environ[NONCE_ENV_VAR] = nonce
    os.environ[MARKER_ENV_VAR] = str(marker)
    _ACTIVE_SERVER_CONTEXTS.add(nonce)
    try:
        yield nonce
    finally:
        _ACTIVE_SERVER_CONTEXTS.discard(nonce)
        os.environ.pop(NONCE_ENV_VAR, None)
        os.environ.pop(MARKER_ENV_VAR, None)
        _remove(directory, marker)


def _read_marker(marker: "Path") -> "dict[str, object] | None":
    try:
        info = marker.lstat()
    except OSError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    if os.name == "posix" and (info.st_uid != os.getuid() or info.st_mode & (stat.S_IRWXG | stat.S_IRWXO)):
        return None
    try:
        document = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(document, dict) or set(document) != _MARKER_FIELDS:
        return None
    return document


def server_context_active() -> "bool":
    """Return whether this process belongs to an invocation that owns a queue worker.

    Returns:
        True when both private variables are present and the marker file they
        name matches this invocation exactly.
    """
    nonce = os.environ.get(NONCE_ENV_VAR)
    path = os.environ.get(MARKER_ENV_VAR)
    if not nonce or not path:
        return False
    marker = Path(path)
    if not marker.is_absolute():
        return False
    document = _read_marker(marker)
    if document is None:
        return False
    owner_pid = document["owner_pid"]
    if document["version"] != MARKER_VERSION or not isinstance(owner_pid, int) or isinstance(owner_pid, bool):
        return False
    if document["nonce"] != nonce:
        return False
    # The owning process publishes the marker from its entered context; a
    # descendant inherits it. Only the former can be forged by setting the
    # environment by hand, so only the former is cross-checked in memory.
    return owner_pid != os.getpid() or nonce in _ACTIVE_SERVER_CONTEXTS
