"""Adapter-to-store selection contract for the SQLSpec queue backend.

Each adapter keeps its own store module; these assertions pin the behavior that
module resolves to, so a change to a shared dialect base cannot silently alter
what a given adapter does.
"""

import sys
import types
from typing import Any

import pytest

from litestar_queues.backends.sqlspec.stores.factory import _adapter_store_type
from litestar_queues.exceptions import QueueConfigurationError

# adapter -> dialect, quote style, datetime binding, JSON column type
_ADAPTER_BEHAVIOR = {
    "aiomysql": ("mysql", "backtick", False, "JSON"),
    "aiosqlite": ("sqlite", "double", True, "TEXT"),
    "arrow_odbc": ("mssql", "double", True, "NVARCHAR(MAX)"),
    "asyncmy": ("mysql", "backtick", False, "JSON"),
    "asyncpg": ("postgres", "double", False, "JSONB"),
    "cockroach_asyncpg": ("cockroachdb", "double", False, "JSONB"),
    "cockroach_psycopg": ("cockroachdb", "double", False, "JSONB"),
    "duckdb": ("duckdb", "double", False, "JSON"),
    "mssql_python": ("mssql", "none", False, "NVARCHAR(MAX)"),
    "mysqlconnector": ("mysql", "backtick", False, "JSON"),
    "oracledb": ("oracle", "none", False, "JSON"),
    "psqlpy": ("postgres", "double", False, "JSONB"),
    "psycopg": ("postgres", "double", False, "JSONB"),
    "pymssql": ("mssql", "none", False, "NVARCHAR(MAX)"),
    "pymysql": ("mysql", "backtick", False, "JSON"),
    "spanner": ("spanner", "backtick", False, "JSON"),
    "sqlite": ("sqlite", "double", True, "TEXT"),
}

# Adapters whose dialect enables Postgres storage parameters and returning-claim.
_POSTGRES_NATIVE = {"asyncpg", "psqlpy", "psycopg"}
_RETURNING_CLAIM = {"asyncpg", "psqlpy", "psycopg"}


def _fake_config(adapter: "str", *, is_async: "bool" = True, dialect: "str | None" = None) -> "Any":
    """Build a stand-in SQLSpec adapter config that only carries its module path."""
    module_name = f"sqlspec.adapters.{adapter}.config"
    config_type = type(f"{'Async' if is_async else 'Sync'}Config", (), {"__module__": module_name})
    config = config_type()
    config.statement_config = types.SimpleNamespace(dialect=dialect)
    config.extension_config = {}
    return config


def test_a_stand_in_config_never_shadows_the_real_adapter_module(monkeypatch: "pytest.MonkeyPatch") -> "None":
    """Resolution reads ``__module__`` as text, so nothing here has to be importable.

    A stand-in left behind in ``sys.modules`` carries no ``__file__``, and every
    later import of that adapter resolves to it instead of the installed package.
    Which tests that breaks depends only on the order the workers happen to run in.
    """
    module_name = "sqlspec.adapters.sqlite.config"
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    _adapter_store_type(_fake_config("sqlite"))

    assert module_name not in sys.modules


@pytest.mark.parametrize("adapter", sorted(_ADAPTER_BEHAVIOR))
def test_adapter_resolves_to_its_dialect_behavior(adapter: "str") -> "None":
    dialect, quote_style, bind_datetime_as_text, json_type = _ADAPTER_BEHAVIOR[adapter]

    store_type = _adapter_store_type(_fake_config(adapter))

    assert store_type.data_dictionary_dialect == dialect
    assert store_type.identifier_quote_style == quote_style
    assert store_type.bind_datetime_as_text is bind_datetime_as_text
    assert store_type._json_type(store_type.__new__(store_type)) == json_type


@pytest.mark.parametrize("adapter", sorted(_ADAPTER_BEHAVIOR))
def test_postgres_capabilities_match_the_adapter(adapter: "str") -> "None":
    store_type = _adapter_store_type(_fake_config(adapter))

    assert getattr(store_type, "table_storage_parameters", False) is (adapter in _POSTGRES_NATIVE)
    assert store_type.supports_returning_claim is (adapter in _RETURNING_CLAIM)


@pytest.mark.parametrize("adapter", ["cockroach_psycopg", "mysqlconnector", "oracledb", "psycopg"])
def test_async_and_sync_configs_resolve_to_their_own_stores(adapter: "str") -> "None":
    """Each driver keeps a distinct store class for its sync and async config."""
    async_store = _adapter_store_type(_fake_config(adapter, is_async=True))
    sync_store = _adapter_store_type(_fake_config(adapter, is_async=False))

    assert async_store is not sync_store


def test_adbc_resolves_to_the_sqlite_store() -> "None":
    store_type = _adapter_store_type(_fake_config("adbc", dialect="sqlite"))

    assert store_type.data_dictionary_dialect == "sqlite"


def test_adbc_rejects_non_sqlite_dialects() -> "None":
    with pytest.raises(QueueConfigurationError, match="sqlite dialect"):
        _adapter_store_type(_fake_config("adbc", dialect="postgres"))


def test_unsupported_adapter_is_rejected() -> "None":
    with pytest.raises(QueueConfigurationError, match="not supported"):
        _adapter_store_type(_fake_config("nonexistent_driver"))


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(("version", "expected"), [("5.7.44", False), ("8.0.1", True)])
async def test_connected_locking_capability_uses_server_version(is_async: bool, version: str, expected: bool) -> None:
    """The real dictionary resolves version gates once on the session's thread."""
    import threading
    from contextlib import asynccontextmanager, contextmanager
    from datetime import datetime, timezone

    from sqlspec.adapters.aiomysql import AiomysqlConfig
    from sqlspec.adapters.aiomysql.data_dictionary import AiomysqlDataDictionary
    from sqlspec.adapters.pymysql import PyMysqlConfig
    from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    calls: list[int] = []

    def version_value(*args: Any) -> str:
        calls.append(threading.get_ident())
        return version

    async def async_version(*args: Any) -> str:
        return version_value()

    driver = types.SimpleNamespace(
        data_dictionary=AiomysqlDataDictionary() if is_async else PyMysqlDataDictionary(),
        select_value_or_none=async_version if is_async else version_value,
        rollback=lambda: None,
    )

    @asynccontextmanager
    async def async_session(config: Any) -> Any:
        yield driver

    @contextmanager
    def sync_session(config: Any) -> Any:
        calls.append(threading.get_ident())
        yield driver
        calls.append(threading.get_ident())

    config: Any = AiomysqlConfig() if is_async else PyMysqlConfig()
    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=config))
    backend._sqlspec = types.SimpleNamespace(  # type: ignore[assignment]
        provide_session=async_session if is_async else sync_session, close_all_pools=lambda: None
    )
    try:
        await backend.open()
        store = backend._get_store()
        assert store.supports_skip_locked is expected
        statement = store.select_claimable(now=datetime.now(timezone.utc), limit=1).build(dialect="mysql").sql
        assert ("SKIP LOCKED" in statement) is expected
        assert len(calls) == (1 if is_async else 3)
        assert len(set(calls)) == 1
        assert (calls[0] == threading.get_ident()) is is_async
    finally:
        await backend.close()


@pytest.mark.anyio
async def test_locking_capability_resets_on_reopen_and_gates_repair_reads() -> None:
    from contextlib import asynccontextmanager
    from datetime import datetime, timezone
    from typing import cast

    from sqlspec.adapters.aiomysql import AiomysqlConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    flags = {"supports_for_update": True, "supports_skip_locked": True}
    calls: list[str] = []

    async def feature(driver: Any, name: str) -> bool:
        calls.append(name)
        return flags[name]

    @asynccontextmanager
    async def session(config: Any) -> Any:
        yield types.SimpleNamespace(data_dictionary=types.SimpleNamespace(get_feature_flag=feature))

    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=AiomysqlConfig()))
    backend._owns_sqlspec = False
    backend._sqlspec = cast("Any", types.SimpleNamespace(provide_session=session))
    store = backend._get_store()

    def repair_sql() -> str:
        return (
            store
            .get_dispatch_repair_candidate(
                task_id="task", execution_backend="cloudtasks", now=datetime.now(timezone.utc)
            )
            .build(dialect="mysql")
            .sql
        )

    assert backend._get_store().supports_skip_locked is False
    assert "FOR UPDATE" not in repair_sql()
    try:
        await backend.open()
        assert backend._get_store().supports_skip_locked is True
        assert "FOR UPDATE" in repair_sql()
        await backend.open()
        assert len(calls) == 2
        await backend.close()
        assert backend._get_store().supports_skip_locked is False
        assert "FOR UPDATE" not in repair_sql()
        flags["supports_for_update"] = False
        await backend.open()
        assert backend._get_store().supports_skip_locked is False
        assert "FOR UPDATE" not in repair_sql()
        assert len(calls) == 4
    finally:
        await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_locking_probe_failure_cleans_owned_resources(cancelled: bool) -> None:
    import asyncio
    from contextlib import contextmanager
    from typing import cast

    from sqlspec.adapters.pymysql import PyMysqlConfig

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    failure = asyncio.CancelledError() if cancelled else RuntimeError("probe failed")
    closed: list[str] = []

    def feature(driver: Any, name: str) -> bool:
        raise failure

    @contextmanager
    def session(config: Any) -> Any:
        try:
            yield types.SimpleNamespace(
                data_dictionary=types.SimpleNamespace(get_feature_flag=feature), rollback=lambda: None
            )
        finally:
            closed.append("session")

    def close_pools() -> None:
        closed.append("pools")
        msg = "secondary cleanup failure"
        raise ValueError(msg)

    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=PyMysqlConfig()))
    backend._sqlspec = cast("Any", types.SimpleNamespace(provide_session=session, close_all_pools=close_pools))
    with pytest.raises(type(failure)) as caught:
        await backend.open()
    assert caught.value is failure
    assert closed == ["session", "pools"]
    assert backend._opened is False
    assert backend._sync_executor is None
    assert backend._sqlspec is None
    assert backend._get_store().supports_skip_locked is False


@pytest.mark.anyio
@pytest.mark.parametrize("major", [11, 23])
async def test_oracle_locking_capability_uses_actual_dictionary_flags(major: int) -> None:
    from contextlib import asynccontextmanager
    from typing import cast

    from sqlspec.adapters.oracledb import OracleAsyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import (
        OracledbAsyncDataDictionary,
        OracleVersionCache,
        OracleVersionInfo,
    )

    from litestar_queues.backends.sqlspec import SQLSpecBackendConfig, SQLSpecQueueBackend

    cache = OracleVersionCache()
    cache.resolved = True
    cache.version = OracleVersionInfo(major)
    driver = types.SimpleNamespace(data_dictionary=OracledbAsyncDataDictionary(), _oracle_version_cache=cache)

    @asynccontextmanager
    async def session(config: Any) -> Any:
        yield driver

    backend = SQLSpecQueueBackend(backend_config=SQLSpecBackendConfig(sqlspec_config=OracleAsyncConfig()))
    backend._sqlspec = cast("Any", types.SimpleNamespace(provide_session=session, close_all_pools=lambda: None))
    try:
        await backend.open()
        assert backend._get_store().supports_for_update is True
        assert backend._get_store().supports_skip_locked is True
    finally:
        await backend.close()
