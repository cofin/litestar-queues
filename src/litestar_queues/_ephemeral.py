"""Private temporary-database lifecycle for the ephemeral queue backend.

This module is dormant: it creates and removes one private database for a
Litestar CLI server invocation and never starts a process. Chapter 2 enters it
from ``QueuePlugin.server_lifespan`` inside the server-proof context.
"""

import logging
import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from litestar_queues.backends.ephemeral.schema import NONCE_ENV_VAR, PATH_ENV_VAR, initialize_database
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self

__all__ = ("DATABASE_NAME", "EphemeralServerContext")

logger = logging.getLogger(__name__)

DATABASE_NAME = "queue.sqlite3"
_DIRECTORY_PREFIX = "litestar-queues-"
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_CLEANUP_DEADLINE = 5.0
_CLEANUP_RETRY = 0.05
_PREEXISTING_ERROR = (
    "The ephemeral queue environment is already set. Nested Litestar servers cannot share one ephemeral database."
)
_CLEANUP_WARNING = "Could not remove the private ephemeral queue directory; it is left for inspection."


class EphemeralServerContext:
    """Own one private temporary SQLite database for a server invocation."""

    __slots__ = ("_directory", "_nonce", "_path")

    def __init__(self, *, nonce: "str") -> "None":
        self._nonce = nonce
        self._directory: "Path | None" = None
        self._path: "Path | None" = None

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
        if os.environ.get(PATH_ENV_VAR) or os.environ.get(NONCE_ENV_VAR):
            raise QueueConfigurationError(_PREEXISTING_ERROR)
        directory = Path(tempfile.mkdtemp(prefix=_DIRECTORY_PREFIX))
        try:
            directory.chmod(_DIRECTORY_MODE)
        except OSError:
            logger.debug("Could not restrict ephemeral queue directory permissions.")
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
        os.environ[PATH_ENV_VAR] = str(path)
        os.environ[NONCE_ENV_VAR] = self._nonce
        return self

    def __exit__(
        self,
        exc_type: "type[BaseException] | None",  # noqa: PYI036
        exc_value: "BaseException | None",  # noqa: PYI036
        traceback: "TracebackType | None",  # noqa: PYI036
    ) -> "None":
        """Remove the environment, database files, and private directory."""
        del exc_type, exc_value, traceback
        os.environ.pop(PATH_ENV_VAR, None)
        os.environ.pop(NONCE_ENV_VAR, None)
        directory, path = self._directory, self._path
        self._directory = self._path = None
        if directory is None or path is None:
            return
        self._remove(directory, path)

    def _remove(self, directory: "Path", path: "Path") -> "None":
        targets = (path, path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm"))
        deadline = time.monotonic() + _CLEANUP_DEADLINE
        for target in targets:
            while True:
                try:
                    target.unlink(missing_ok=True)
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
