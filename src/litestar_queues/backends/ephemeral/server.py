"""Private temporary-database lifecycle for the ephemeral queue backend.

This module is dormant: it creates and removes one private database for a
Litestar CLI server invocation and never starts a process. Chapter 2 enters it
from ``QueuePlugin.server_lifespan`` inside the server invocation context.
"""

import logging
import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from litestar_queues.backends.ephemeral.schema import initialize_database
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self

__all__ = ("DATABASE_NAME", "EphemeralServerContext")

logger = logging.getLogger(__name__)

DATABASE_NAME = "queue.sqlite3"
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_CLEANUP_DEADLINE = 5.0
_CLEANUP_RETRY = 0.05
_PREEXISTING_ERROR = (
    "The ephemeral queue environment is already set. Nested Litestar servers cannot share one ephemeral database."
)
_CLEANUP_WARNING = "Could not remove the private ephemeral queue directory; it is left for inspection."
_CLEANUP_FILE_WARNING = "Could not remove the private ephemeral queue files (%s); they are left on disk."


class EphemeralServerContext:
    """Own one private temporary SQLite database for a server invocation."""

    __slots__ = ("_directory", "_logger", "_nonce", "_nonce_env_var", "_path", "_path_env_var", "_resource_prefix")

    def __init__(self, *, nonce: "str", namespace: "QueueNamespace | None" = None) -> "None":
        names = namespace or QueueNamespace()
        self._nonce = nonce
        self._directory: "Path | None" = None
        self._path: "Path | None" = None
        self._path_env_var = names.environment("ephemeral", "path")
        self._nonce_env_var = names.environment("ephemeral", "nonce")
        self._resource_prefix = f"{names.resource()}-"
        self._logger = logging.getLogger(names.logger("backends", "ephemeral"))

    @property
    def path(self) -> "Path":
        """Absolute path of the prepared database.

        Returns:
            The database path.

        Raises:
            QueueConfigurationError: If the context has not been entered.
        """
        if self._path is None:
            msg = "The ephemeral queue database has not been created."
            raise QueueConfigurationError(msg)
        return self._path

    def __enter__(self) -> "Self":
        """Create the private directory, database, schema, and environment.

        Returns:
            The entered context.

        Raises:
            QueueConfigurationError: If the private environment already exists.
        """
        if os.environ.get(self._path_env_var) or os.environ.get(self._nonce_env_var):
            raise QueueConfigurationError(_PREEXISTING_ERROR)
        directory = Path(tempfile.mkdtemp(prefix=self._resource_prefix))
        try:
            directory.chmod(_DIRECTORY_MODE)
        except OSError:
            self._logger.debug("Could not restrict ephemeral queue directory permissions.")
        path = directory / DATABASE_NAME
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, _FILE_MODE)
        os.close(descriptor)
        try:
            initialize_database(path, nonce=self._nonce)
        except BaseException:
            self._remove(directory, path)
            raise
        self._directory = directory
        self._path = path
        os.environ[self._path_env_var] = str(path)
        os.environ[self._nonce_env_var] = self._nonce
        return self

    def __exit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_value: "BaseException | None",  # noqa: PYI036
        traceback: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        """Remove the environment, database files, and private directory."""
        del exc_type, exc_value, traceback
        os.environ.pop(self._path_env_var, None)
        os.environ.pop(self._nonce_env_var, None)
        directory, path = self._directory, self._path
        self._directory = self._path = None
        if directory is None or path is None:
            return
        self._remove(directory, path)

    def _remove(self, directory: "Path", path: "Path") -> "None":
        targets = (path, path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm"))
        survivors = [target.name for target in targets if not _unlink_within_deadline(target)]
        if survivors:
            self._logger.warning(_CLEANUP_FILE_WARNING, ", ".join(survivors))
        _remove_directory_within_deadline(directory, runtime_logger=self._logger)


def _unlink_within_deadline(target: "Path") -> "bool":
    """Delete one file, retrying while another process still holds it open.

    Each target carries its own deadline. Windows refuses to unlink a file with
    an open handle, so a single shared budget let the database consume all of it
    and leave the write-ahead log and shared-memory files unretried.

    Args:
        target: File to delete.

    Returns:
        ``True`` once the file is gone, ``False`` if it outlived the deadline.
    """
    deadline = time.monotonic() + _CLEANUP_DEADLINE
    while True:
        try:
            target.unlink(missing_ok=True)
        except OSError:
            if time.monotonic() >= deadline:
                return False
            time.sleep(_CLEANUP_RETRY)
            continue
        return True


def _remove_directory_within_deadline(directory: "Path", *, runtime_logger: "logging.Logger" = logger) -> "None":
    """Remove the private directory once it is empty, or warn and leave it."""
    deadline = time.monotonic() + _CLEANUP_DEADLINE
    while True:
        try:
            directory.rmdir()
        except FileNotFoundError:
            return
        except OSError:
            if time.monotonic() >= deadline:
                runtime_logger.warning(_CLEANUP_WARNING)
                return
            time.sleep(_CLEANUP_RETRY)
            continue
        return
