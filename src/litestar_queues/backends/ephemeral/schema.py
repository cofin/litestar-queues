"""Where the ephemeral database lives, how it is opened, and how it fails.

This module owns the private environment contract, connection setup, schema
initialization, and failure mapping. Every SQLite failure surfaced by this
package is translated into one typed :class:`EphemeralDatabaseError` carrying a
constant message. No message interpolates the database path, task arguments,
results, or metadata.
"""

import os
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from litestar_queues.exceptions import QueueError

if TYPE_CHECKING:
    from collections.abc import Generator

__all__ = (
    "NONCE_ENV_VAR",
    "PATH_ENV_VAR",
    "SCHEMA_VERSION",
    "EphemeralDatabaseError",
    "connect",
    "initialize_database",
    "is_private_directory",
    "read_environment",
    "read_runtime",
    "sqlite_errors",
)

SCHEMA_VERSION = 2
BUSY_TIMEOUT_MS = 5000

PATH_ENV_VAR = "LITESTAR_QUEUES_EPHEMERAL_PATH"
NONCE_ENV_VAR = "LITESTAR_QUEUES_EPHEMERAL_NONCE"

BUSY_ERROR = "The ephemeral queue database stayed busy past its timeout."
UNREADABLE_ERROR = "The ephemeral SQLite database contains an unreadable queue payload."
MISSING_ERROR = "The ephemeral queue database is no longer available to this server invocation."

_BUSY_MARKERS = ("locked", "busy")
_MISSING_MARKERS = ("unable to open database file", "no such table", "no such database")


class EphemeralDatabaseError(QueueError):
    """Raised when the private ephemeral database is busy, unreadable, or gone."""


def _classify(error: "sqlite3.DatabaseError") -> "str | None":
    if isinstance(error, sqlite3.IntegrityError):
        return None
    text = str(error).lower()
    if any(marker in text for marker in _BUSY_MARKERS):
        return BUSY_ERROR
    if any(marker in text for marker in _MISSING_MARKERS):
        return MISSING_ERROR
    return UNREADABLE_ERROR


def read_environment() -> "tuple[str, str] | None":
    """Return the active database path and invocation nonce.

    Returns:
        The ``(path, nonce)`` pair when both private variables are set and the
        path is absolute, otherwise ``None``.
    """
    path = os.environ.get(PATH_ENV_VAR)
    nonce = os.environ.get(NONCE_ENV_VAR)
    if not path or not nonce or not Path(path).is_absolute():
        return None
    return path, nonce


def is_private_directory(directory: "Path") -> "bool":
    """Return whether ``directory`` is a non-symlink directory restricted to its owner.

    Returns:
        True when the directory is safe to use.
    """
    try:
        info = directory.lstat()
    except OSError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        return False
    return not bool(info.st_mode & (stat.S_IRWXG | stat.S_IRWXO))


@contextmanager
def sqlite_errors() -> "Generator[None]":
    """Translate SQLite failures into one typed error with a constant message.

    Uniqueness violations are re-raised untouched because the backend uses them
    as an ordinary control-flow signal for keyed enqueue.

    Yields:
        None: with SQLite failures translated on exit.

    Raises:
        EphemeralDatabaseError: If the database is busy, unreadable, or missing.
    """
    try:
        yield
    except sqlite3.DatabaseError as error:
        message = _classify(error)
        if message is None:
            raise
        raise EphemeralDatabaseError(message) from None


_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS queue_task (
        id TEXT PRIMARY KEY,
        task_name TEXT NOT NULL,
        queue TEXT NOT NULL,
        execution_backend TEXT NOT NULL,
        status TEXT NOT NULL,
        priority INTEGER NOT NULL,
        retry_count INTEGER NOT NULL,
        scheduled_at TEXT,
        expires_at TEXT,
        created_at TEXT NOT NULL,
        completed_at TEXT,
        heartbeat_at TEXT,
        task_key TEXT,
        payload BLOB NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_task_claim
        ON queue_task(status, execution_backend, queue, scheduled_at, expires_at, priority, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_task_expiry
        ON queue_task(status, expires_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_task_completed
        ON queue_task(task_name, status, completed_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_queue_task_active_key
        ON queue_task(task_key)
        WHERE task_key IS NOT NULL AND status NOT IN ('completed', 'failed', 'cancelled', 'expired')
    """,
    """
    CREATE TABLE IF NOT EXISTS queue_reservation (
        identity_key TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        task_name TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS queue_maintenance (
        name TEXT PRIMARY KEY,
        token TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS queue_event (
        event_id TEXT PRIMARY KEY,
        task_id TEXT,
        task_name TEXT,
        stage TEXT,
        occurred_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        sequence INTEGER,
        payload BLOB NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_event_task ON queue_event(task_id, occurred_at, event_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_event_name ON queue_event(task_name, occurred_at, event_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS queue_runtime (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        schema_version INTEGER NOT NULL,
        invocation_nonce TEXT NOT NULL
    )
    """,
)


def connect(path: "str | Path", *, create: "bool" = False) -> "sqlite3.Connection":
    """Open one short-lived connection with the backend PRAGMAs applied.

    Unless ``create`` is set the database is opened read-write but never
    created, so a deleted file fails loudly instead of resurfacing as an empty
    database.

    Returns:
        A configured connection owned by the caller.
    """
    uri = f"{Path(path).absolute().as_uri()}?mode={'rwc' if create else 'rw'}"
    with sqlite_errors():
        connection = sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        with sqlite_errors():
            connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.execute("PRAGMA foreign_keys = ON")
    except BaseException:
        connection.close()
        raise
    return connection


def initialize_database(path: "str | Path", *, nonce: "str") -> "None":
    """Create the schema and record the invocation nonce exactly once."""
    connection = connect(path, create=True)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for statement in _STATEMENTS:
            connection.execute(statement)
        connection.execute(
            "INSERT OR REPLACE INTO queue_runtime (singleton, schema_version, invocation_nonce) VALUES (1, ?, ?)",
            (SCHEMA_VERSION, nonce),
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def read_runtime(path: "str | Path") -> "tuple[int, str] | None":
    """Return the stored schema version and invocation nonce.

    Returns:
        The ``(schema_version, invocation_nonce)`` pair, or ``None`` when the
        database is absent, unreadable, or not an ephemeral queue database.
    """
    try:
        connection = connect(path)
    except (EphemeralDatabaseError, OSError, ValueError):
        return None
    try:
        row = connection.execute(
            "SELECT schema_version, invocation_nonce FROM queue_runtime WHERE singleton = 1"
        ).fetchone()
    except sqlite3.DatabaseError:
        return None
    finally:
        connection.close()
    if row is None:
        return None
    return int(row["schema_version"]), str(row["invocation_nonce"])
