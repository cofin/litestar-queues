"""Lifecycle tests for the private ephemeral SQLite database.

The context is dormant in this chapter: it creates and removes exactly one
private database and never starts a process, opens a socket, or publishes an
endpoint.
"""

import os
import sqlite3
import stat
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from litestar_queues import QueueConfig
from litestar_queues.backends.ephemeral import EphemeralQueueBackend
from litestar_queues.backends.ephemeral import server as ephemeral_runtime
from litestar_queues.backends.ephemeral.schema import (
    NONCE_ENV_VAR,
    PATH_ENV_VAR,
    SCHEMA_VERSION,
    initialize_database,
    is_private_directory,
    read_environment,
    read_runtime,
)
from litestar_queues.backends.ephemeral.server import DATABASE_NAME, EphemeralServerContext
from litestar_queues.backends.factory import get_queue_backend
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.anyio

posix_only = pytest.mark.skipif(os.name != "posix", reason="File modes are only meaningful on POSIX.")


@pytest.fixture(autouse=True)
def _clean_environment() -> "Iterator[None]":
    for name in (PATH_ENV_VAR, NONCE_ENV_VAR):
        os.environ.pop(name, None)
    yield
    for name in (PATH_ENV_VAR, NONCE_ENV_VAR):
        os.environ.pop(name, None)


def test_entering_creates_a_private_database_and_environment() -> "None":
    with EphemeralServerContext(nonce="nonce-1") as context:
        path = context.path

        assert path.name == DATABASE_NAME
        assert path.is_absolute()
        assert path.exists()
        assert os.environ[PATH_ENV_VAR] == str(path)
        assert os.environ[NONCE_ENV_VAR] == "nonce-1"
        assert read_runtime(path) == (SCHEMA_VERSION, "nonce-1")

    assert not path.exists()
    assert not path.parent.exists()
    assert PATH_ENV_VAR not in os.environ
    assert NONCE_ENV_VAR not in os.environ


def test_entering_with_namespace_derives_private_environment_and_directory() -> "None":
    from litestar_queues import QueueNamespace

    names = QueueNamespace("myapp")
    path_env = names.environment("ephemeral", "path")
    nonce_env = names.environment("ephemeral", "nonce")

    with EphemeralServerContext(nonce="nonce-myapp", namespace=names) as context:
        path = context.path
        assert os.environ[path_env] == str(path)
        assert os.environ[nonce_env] == "nonce-myapp"
        assert path.parent.name.startswith("myapp-")

    assert path_env not in os.environ
    assert nonce_env not in os.environ


@posix_only
def test_the_private_directory_and_file_are_owner_only() -> "None":
    with EphemeralServerContext(nonce="nonce-modes") as context:
        directory_mode = stat.S_IMODE(context.path.parent.stat().st_mode)
        file_mode = stat.S_IMODE(context.path.stat().st_mode)

    assert directory_mode == 0o700
    assert file_mode == 0o600


def test_the_path_property_requires_entry() -> "None":
    context = EphemeralServerContext(nonce="nonce-unentered")

    with pytest.raises(QueueConfigurationError, match="has not been created"):
        _ = context.path


def test_entering_rejects_a_pre_existing_ephemeral_environment() -> "None":
    os.environ[PATH_ENV_VAR] = "/tmp/already-there/queue.sqlite3"

    with pytest.raises(QueueConfigurationError, match="Nested Litestar servers"), EphemeralServerContext(nonce="n"):
        pass


def test_entering_rejects_a_pre_existing_nonce() -> "None":
    os.environ[NONCE_ENV_VAR] = "another-invocation"

    with pytest.raises(QueueConfigurationError, match="Nested Litestar servers"), EphemeralServerContext(nonce="n"):
        pass


def test_the_environment_parser_requires_both_values_and_an_absolute_path() -> "None":
    assert read_environment() is None

    os.environ[PATH_ENV_VAR] = "/tmp/queue.sqlite3"
    assert read_environment() is None

    os.environ[NONCE_ENV_VAR] = "nonce-2"
    assert read_environment() == ("/tmp/queue.sqlite3", "nonce-2")

    os.environ[PATH_ENV_VAR] = "relative/queue.sqlite3"
    assert read_environment() is None


async def test_a_backend_built_before_entry_resolves_its_path_only_on_open() -> "None":
    backend = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))

    with pytest.raises(QueueConfigurationError, match="litestar run"):
        _ = backend.path

    with EphemeralServerContext(nonce="nonce-3") as context:
        assert await backend.open() is True
        assert backend.path == str(context.path)
        await backend.close()


async def test_the_factory_builds_a_backend_for_the_active_database() -> "None":
    with EphemeralServerContext(nonce="nonce-4") as context:
        backend = get_queue_backend("ephemeral", QueueConfig(queue_backend="ephemeral"))
        assert isinstance(backend, EphemeralQueueBackend)
        await backend.open()

        assert backend.path == str(context.path)
        await backend.close()


async def test_open_rejects_a_database_in_a_world_readable_directory(tmp_path: "Path") -> "None":
    exposed = tmp_path / "exposed"
    exposed.mkdir(mode=0o755)
    path = exposed / DATABASE_NAME
    path.touch()
    initialize_database(path, nonce="nonce-5")
    os.environ[PATH_ENV_VAR] = str(path)
    os.environ[NONCE_ENV_VAR] = "nonce-5"
    backend = EphemeralQueueBackend(QueueConfig(queue_backend="ephemeral"))

    with pytest.raises(QueueConfigurationError, match="does not belong to this server invocation"):
        await backend.open()


def test_is_private_directory_rejects_symlinks_and_shared_modes(tmp_path: "Path") -> "None":
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)
    link = tmp_path / "link"
    link.symlink_to(private, target_is_directory=True)

    assert is_private_directory(private, _platform="posix") is True
    assert is_private_directory(shared, _platform="posix") is False
    assert is_private_directory(link, _platform="posix") is False
    assert is_private_directory(tmp_path / "absent", _platform="posix") is False


def test_is_private_directory_uses_windows_directory_acl_boundary(tmp_path: "Path") -> "None":
    directory = tmp_path / "windows-private"
    directory.mkdir(mode=0o755)

    assert is_private_directory(directory, _platform="nt") is True


def test_a_schema_failure_leaves_no_environment_or_files(monkeypatch: "pytest.MonkeyPatch") -> "None":
    created: "list[Path]" = []

    def explode(path: "Path | str", *, nonce: "str") -> "None":
        del nonce
        created.append(Path(path))
        msg = "schema failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(ephemeral_runtime, "initialize_database", explode)

    with pytest.raises(RuntimeError, match="schema failed"), EphemeralServerContext(nonce="nonce-6"):
        pass

    assert len(created) == 1
    assert not created[0].exists()
    assert not created[0].parent.exists()
    assert PATH_ENV_VAR not in os.environ
    assert NONCE_ENV_VAR not in os.environ


def test_a_failure_inside_the_body_still_removes_the_database() -> "None":
    with pytest.raises(RuntimeError, match="body failed"), EphemeralServerContext(nonce="nonce-7") as context:
        path = context.path
        msg = "body failed"
        raise RuntimeError(msg)

    assert not path.exists()
    assert not path.parent.exists()
    assert PATH_ENV_VAR not in os.environ


def test_exit_removes_the_write_ahead_log_and_shared_memory_files() -> "None":
    with EphemeralServerContext(nonce="nonce-8") as context:
        path = context.path
        connection = sqlite3.connect(path, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("INSERT INTO queue_maintenance VALUES ('sweep', 'token', '2026-01-01T00:00:00+00:00')")
            connection.execute("COMMIT")
            sidecars = [path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm")]
            assert any(sidecar.exists() for sidecar in sidecars)
        finally:
            connection.close()

    assert not path.exists()
    assert not any(sidecar.exists() for sidecar in sidecars)
    assert not path.parent.exists()


def test_exit_is_idempotent() -> "None":
    context = EphemeralServerContext(nonce="nonce-9")
    context.__enter__()
    path = context.path
    context.__exit__(None, None, None)
    context.__exit__(None, None, None)

    assert not path.exists()
    assert not path.parent.exists()


def test_an_unexpected_extra_file_prevents_removal_and_is_never_deleted(
    monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture"
) -> "None":
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_DEADLINE", 0.05)
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_RETRY", 0.01)
    with caplog.at_level("WARNING", logger=ephemeral_runtime.__name__), EphemeralServerContext(nonce="nonce-10") as ctx:
        path = ctx.path
        intruder = path.parent / "unexpected.txt"
        intruder.write_text("keep me", encoding="utf-8")

    assert intruder.exists()
    assert intruder.read_text(encoding="utf-8") == "keep me"
    assert not path.exists()
    assert "left for inspection" in caplog.text
    intruder.unlink()
    path.parent.rmdir()


def test_a_sharing_violation_on_the_database_is_retried_and_bounded(
    monkeypatch: "pytest.MonkeyPatch", caplog: "pytest.LogCaptureFixture"
) -> "None":
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_DEADLINE", 0.05)
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_RETRY", 0.01)
    attempts = {"count": 0}
    original = Path.unlink

    def blocked(self: "Path", missing_ok: "bool" = False) -> "None":
        if self.name == DATABASE_NAME:
            attempts["count"] += 1
            msg = "The process cannot access the file because it is being used by another process"
            raise PermissionError(msg)
        original(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", blocked)
    with (
        caplog.at_level("WARNING", logger=ephemeral_runtime.__name__),
        EphemeralServerContext(nonce="nonce-11") as context,
    ):
        path = context.path

    monkeypatch.undo()
    assert attempts["count"] > 1
    assert path.exists()
    # A database that outlives its server is a leak on the host, so it has to be
    # reported by name. Windows refuses to unlink files with open handles, which
    # made this the silent failure mode there.
    assert DATABASE_NAME in caplog.text
    path.unlink()
    path.parent.rmdir()


def test_one_stuck_file_does_not_consume_the_retry_budget_of_the_others(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Each target gets its own deadline.

    A single shared budget let the database exhaust it, so the write-ahead log and
    shared-memory files were never retried and leaked alongside it.
    """
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_DEADLINE", 0.05)
    monkeypatch.setattr(ephemeral_runtime, "_CLEANUP_RETRY", 0.01)
    original = Path.unlink
    wal_attempts = {"count": 0}

    def blocked(self: "Path", missing_ok: "bool" = False) -> "None":
        if self.name == DATABASE_NAME:
            msg = "The process cannot access the file because it is being used by another process"
            raise PermissionError(msg)
        if self.name.endswith("-wal"):
            wal_attempts["count"] += 1
            if wal_attempts["count"] < 3:
                msg = "The process cannot access the file because it is being used by another process"
                raise PermissionError(msg)
        original(self, missing_ok=missing_ok)

    with EphemeralServerContext(nonce="nonce-12") as context:
        path = context.path
        wal = path.with_name(f"{path.name}-wal")
        wal.write_bytes(b"")
        monkeypatch.setattr(Path, "unlink", blocked)

    monkeypatch.undo()
    assert not wal.exists()
    path.unlink()
    path.parent.rmdir()


def test_the_lifecycle_opens_no_socket_listener_or_manager() -> "None":
    source = Path(ephemeral_runtime.__file__).read_text(encoding="utf-8")
    imports = [line.strip() for line in source.splitlines() if line.startswith(("import ", "from "))]

    banned = ("socket", "multiprocessing", "asyncio", "http", "ssl", "xmlrpc", "secrets")
    assert [line for line in imports if any(name in line for name in banned)] == []
    assert "listen(" not in source
    assert "bind(" not in source
