"""Schema and connection setup for the ephemeral SQLite backend."""

import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ("SCHEMA_VERSION", "connect", "initialize_database", "read_runtime")

SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 5000
CONNECT_TIMEOUT = 5.0

_PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA foreign_keys = ON",
    f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}",
)

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
        created_at TEXT NOT NULL,
        completed_at TEXT,
        heartbeat_at TEXT,
        task_key TEXT,
        payload BLOB NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_task_claim
        ON queue_task(status, execution_backend, queue, scheduled_at, priority, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_queue_task_completed
        ON queue_task(task_name, status, completed_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_queue_task_active_key
        ON queue_task(task_key)
        WHERE task_key IS NOT NULL AND status NOT IN ('completed', 'failed', 'cancelled')
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


def connect(path: "str | Path") -> "sqlite3.Connection":
    """Open one short-lived connection with the backend PRAGMAs applied.

    Returns:
        A configured connection owned by the caller.
    """
    connection = sqlite3.connect(str(path), timeout=CONNECT_TIMEOUT, isolation_level=None)
    connection.row_factory = sqlite3.Row
    for pragma in _PRAGMAS:
        connection.execute(pragma)
    return connection


def initialize_database(path: "str | Path", *, nonce: "str") -> "None":
    """Create the schema and record the invocation nonce exactly once."""
    connection = connect(path)
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
        The ``(schema_version, invocation_nonce)`` pair, or ``None`` when absent.
    """
    connection = connect(path)
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
