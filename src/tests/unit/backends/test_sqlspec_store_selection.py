"""Adapter-to-store selection contract for the SQLSpec queue backend.

Each adapter keeps its own store module; these assertions pin the behavior that
module resolves to, so a change to a shared dialect base cannot silently alter
what a given adapter does.
"""

import importlib
import re
import sys
import types
from datetime import UTC
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.events.store import AsyncpgEventQueueStore
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.schema import SchemaTarget

from litestar_queues.backends.sqlspec import SQLSpecBackendConfig
from litestar_queues.backends.sqlspec.backend import _adapter_wakeup_transport, _events_queue_store, _match_store_class
from litestar_queues.backends.sqlspec.event_log import SQLSpecQueueEventLogStore
from litestar_queues.backends.sqlspec.extension import QUEUE_EXTENSION_NAME
from litestar_queues.backends.sqlspec.migrations.reconcile import (
    downgrade,
    reconcile,
    reconcile_stores_async,
    reconcile_stores_sync,
)
from litestar_queues.backends.sqlspec.schema import event_history_table_name_for, maintenance_table_name_for
from litestar_queues.backends.sqlspec.stores._dialects import MySQLQueueStore, PostgresQueueStore
from litestar_queues.backends.sqlspec.stores.asyncpg import AsyncpgQueueStore
from litestar_queues.backends.sqlspec.stores.base import SQLSpecQueueStore, _adapter_name
from litestar_queues.backends.sqlspec.stores.factory import _adapter_store_type, create_queue_store
from litestar_queues.config import QueueConfig
from litestar_queues.exceptions import QueueConfigurationError

_CockroachPsycopgSyncConfig: Any
try:
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig as _CockroachPsycopgSyncConfig
except ImportError:  # pragma: no cover
    _CockroachPsycopgSyncConfig = None

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

_POSTGRES_NATIVE = {"asyncpg", "psqlpy", "psycopg"}
_RETURNING_CLAIM = {"asyncpg", "psqlpy", "psycopg"}


def _fake_config(
    adapter: "str",
    *,
    is_async: "bool" = True,
    dialect: "str | None" = None,
    connection_config: "dict[str, object] | None" = None,
) -> "Any":
    """Build a stand-in SQLSpec adapter config that only carries its module path."""
    module_name = f"sqlspec.adapters.{adapter}.config"
    config_type = type(
        f"{'Async' if is_async else 'Sync'}Config", (), {"__module__": module_name, "is_async": is_async}
    )
    config = config_type()
    config.statement_config = types.SimpleNamespace(dialect=dialect)
    config.extension_config = {}
    config.connection_config = connection_config or {}
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
    from datetime import datetime

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
        statement = store.select_claimable(now=datetime.now(UTC), limit=1).build(dialect="mysql").sql
        assert ("SKIP LOCKED" in statement) is expected
        assert len(calls) == (1 if is_async else 3)
        assert len(set(calls)) == 1
        assert (calls[0] == threading.get_ident()) is is_async
    finally:
        await backend.close()


@pytest.mark.anyio
async def test_locking_capability_resets_on_reopen_and_gates_repair_reads() -> None:
    from contextlib import asynccontextmanager
    from datetime import datetime
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
            .get_dispatch_repair_candidate(task_id="task", execution_backend="cloudtasks", now=datetime.now(UTC))
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


@pytest.mark.parametrize("adapter", ["cockroach_psycopg", "mysqlconnector", "oracledb", "psycopg"])
@pytest.mark.parametrize("is_async", [True, False])
@pytest.mark.parametrize("subclass_name", ["ApplicationConfig", "MisleadingAsyncConfig"])
def test_config_subclass_store_selection_uses_declared_mode(
    adapter: "str", is_async: "bool", subclass_name: "str"
) -> "None":
    canonical = _fake_config(adapter, is_async=is_async)
    canonical.is_async = is_async
    application_type = type(subclass_name, (type(canonical),), {"__module__": "application", "is_async": is_async})

    assert _adapter_store_type(application_type()) is _adapter_store_type(canonical)


@pytest.mark.parametrize(
    ("adapter", "config_name", "store_name"),
    [
        ("duckdb", "DuckDBConfig", "DuckDBEventQueueStore"),
        ("asyncpg", "AsyncpgConfig", "AsyncpgEventQueueStore"),
        ("psycopg", "PsycopgAsyncConfig", "PsycopgAsyncEventQueueStore"),
        ("psycopg", "PsycopgSyncConfig", "PsycopgSyncEventQueueStore"),
    ],
)
def test_config_subclass_native_event_store_uses_canonical_ancestry(
    adapter: "str", config_name: "str", store_name: "str"
) -> "None":
    from importlib import import_module

    config_type = getattr(import_module(f"sqlspec.adapters.{adapter}"), config_name)
    application_type = type("ApplicationConfig", (config_type,), {"__module__": "application"})
    config = application_type(extension_config={"events": {"queue_table": "application_events"}})

    store = _events_queue_store(config)

    expected_type = getattr(import_module(f"sqlspec.adapters.{adapter}.events.store"), store_name)
    assert isinstance(store, expected_type)
    assert store.table_name == "application_events"


def test_unsupported_config_native_event_store_has_configuration_error() -> "None":
    with pytest.raises(QueueConfigurationError, match="no supported native events queue store"):
        _events_queue_store(_fake_config("nonexistent_driver"))


def test_adapter_name_resolution_explicit_classvar() -> "None":
    """Explicit adapter_name ClassVar on a class or instance takes precedence."""

    class ExternalConfig:
        adapter_name: "ClassVar[str]" = "asyncpg"

    assert _adapter_name(ExternalConfig) == "asyncpg"
    assert _adapter_name(ExternalConfig()) == "asyncpg"


def test_external_config_uses_dialect_queue_store_and_wakeup_transport() -> "None":
    """An external configuration subclass outside sqlspec.adapters resolves its dialect store."""

    class ExternalPostgresConfig:
        adapter_name: "ClassVar[str]" = "asyncpg"
        is_async: "ClassVar[bool]" = True
        statement_config: "Any" = None
        extension_config: "dict[str, Any]" = {}

    config = ExternalPostgresConfig()
    assert _adapter_store_type(config) is AsyncpgQueueStore
    assert _adapter_wakeup_transport(_adapter_name(config)) == "notify_queue"


def test_external_config_uses_native_events_queue_store() -> "None":
    """An external configuration subclass outside sqlspec.adapters resolves the driver's native event store."""

    class ExternalPostgresConfig:
        adapter_name: "ClassVar[str]" = "asyncpg"
        is_async: "ClassVar[bool]" = True
        extension_config: "dict[str, Any]" = {"events": {"queue_table": "external_events"}}

    config = ExternalPostgresConfig()
    store = _events_queue_store(cast("Any", config))
    assert isinstance(store, AsyncpgEventQueueStore)
    assert store.table_name == "external_events"


def test_external_config_prefers_explicit_events_store_class() -> "None":
    """Explicit events_store_class on an external config takes precedence over module lookup."""

    class CustomEventStore(AsyncpgEventQueueStore):
        pass

    class ExternalConfigWithCustomEventStore:
        adapter_name: "ClassVar[str]" = "asyncpg"
        is_async: "ClassVar[bool]" = True
        events_store_class: "ClassVar[type[AsyncpgEventQueueStore]]" = CustomEventStore
        extension_config: "dict[str, Any]" = {}

    config = ExternalConfigWithCustomEventStore()
    store = _events_queue_store(cast("Any", config))
    assert isinstance(store, CustomEventStore)


def test_external_config_prefers_explicit_queue_store_class() -> "None":
    """Explicit queue_store_class on an external config takes precedence over adapter_name lookup."""

    class CustomQueueStore(SQLSpecQueueStore):
        pass

    class ExternalConfigWithCustomQueueStore:
        queue_store_class: "ClassVar[type[SQLSpecQueueStore]]" = CustomQueueStore
        is_async: "ClassVar[bool]" = True
        statement_config: "Any" = None
        extension_config: "dict[str, Any]" = {}

    config = ExternalConfigWithCustomQueueStore()
    assert _adapter_store_type(config) is CustomQueueStore
    store = create_queue_store(config, table_name="custom_tasks")
    assert isinstance(store, CustomQueueStore)


def test_resolve_adapter_name_handles_mock_objects_safely() -> "None":
    """Mock instances without explicit adapter settings return empty string without recursing."""
    mock_config = MagicMock()
    assert _adapter_name(mock_config) == ""

    mock_store = MagicMock()
    inner = types.SimpleNamespace(adapter_name="asyncpg")
    mock_store._config = inner
    assert _adapter_name(mock_store) == "asyncpg"


@pytest.mark.parametrize(
    ("adapter_name", "expected_wakeup"),
    [
        ("asyncpg", "notify_queue"),
        ("psycopg", "notify_queue"),
        ("psqlpy", "notify_queue"),
        ("cockroach_asyncpg", "polling"),
        ("cockroach_psycopg", "polling"),
        ("duckdb", "poll_queue"),
        ("sqlite", "polling"),
        ("aiosqlite", "polling"),
        ("spanner", "polling"),
        ("mysqlconnector", "polling"),
    ],
)
def test_external_config_wakeup_transport_mapping(adapter_name: "str", expected_wakeup: "str") -> "None":
    """External configurations map to their corresponding wakeup transport via adapter_name."""

    class ExternalConfig:
        adapter_name: "ClassVar[str]" = ""

    ExternalConfig.adapter_name = adapter_name
    assert _adapter_wakeup_transport(_adapter_name(ExternalConfig)) == expected_wakeup


def test_match_store_class_rejects_ambiguous_unprefixed_event_stores() -> "None":
    """A module exporting multiple non-matching EventQueueStore classes returns None."""
    module = types.ModuleType("fake_events_store")

    class AlphaEventQueueStore:
        pass

    class BetaEventQueueStore:
        pass

    module.AlphaEventQueueStore = AlphaEventQueueStore  # type: ignore[attr-defined]
    module.BetaEventQueueStore = BetaEventQueueStore  # type: ignore[attr-defined]

    assert _match_store_class(module, ["CustomConfig"], is_async=True) is None


@pytest.mark.parametrize("invalid_ttl", [0.0, -5.0])
def test_sqlspec_backend_config_rejects_non_positive_terminal_ttl(invalid_ttl: "float") -> "None":
    """SQLSpecBackendConfig.terminal_ttl must be strictly positive when provided."""
    with pytest.raises(QueueConfigurationError, match="terminal_ttl must be greater than 0"):
        SQLSpecBackendConfig(terminal_ttl=invalid_ttl)


def test_cockroach_queue_store_emits_filtered_row_level_ttl_ddl() -> "None":
    """CockroachQueueStore embeds a filtered row-level ttl_expiration_expression when terminal_ttl is set."""
    config = _fake_config("cockroach_asyncpg", is_async=True, dialect="postgres")
    store_without_ttl = create_queue_store(config)
    assert "ttl_expiration_expression" not in store_without_ttl.create_statements()[0]

    store_with_ttl = create_queue_store(config, terminal_ttl=3600.0)
    create_table_sql = store_with_ttl.create_statements()[0]
    assert "WITH (ttl = 'on', ttl_expiration_expression = '" in create_table_sql
    assert "IN (''completed'', ''failed'', ''cancelled'', ''expired'')" in create_table_sql
    assert "INTERVAL ''3600 seconds''" in create_table_sql
    assert "ELSE NULL END')" in create_table_sql


def test_spanner_queue_store_emits_row_deletion_policy_ddl() -> "None":
    """SpannerQueueStore appends a native ROW DELETION POLICY on completed_at when terminal_ttl is set."""
    config = _fake_config("spanner", is_async=False, dialect="postgres")
    store_without_ttl = create_queue_store(config)
    assert "ROW DELETION POLICY" not in store_without_ttl.create_statements()[0]

    store_with_ttl = create_queue_store(config, terminal_ttl=90000.0)
    create_table_sql = store_with_ttl.create_statements()[0]
    assert "ROW DELETION POLICY (OLDER_THAN(`completed_at`, INTERVAL 2 DAY))" in create_table_sql


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter", "expected_clause"),
    [
        ("cockroach_asyncpg", "ttl_expiration_expression"),
        ("spanner", "ROW DELETION POLICY (OLDER_THAN(`completed_at`, INTERVAL 1 DAY))"),
    ],
)
async def test_initial_migration_applies_configured_terminal_ttl(adapter: "str", expected_clause: "str") -> "None":
    """The 0001_create_queue_tasks migration threads terminal_ttl into native DDL."""
    migration = importlib.import_module("litestar_queues.backends.sqlspec.migrations.0001_create_queue_tasks")
    sqlspec_config = _fake_config(adapter, is_async=(adapter != "spanner"), dialect="postgres")
    sqlspec_config.migration_config = {}
    sqlspec_config.set_migration_config = lambda cfg: setattr(sqlspec_config, "migration_config", cfg)
    sqlspec_config.add_extension_migrations = lambda *_args, **_kwargs: None
    sqlspec_config.remove_extension_migrations = lambda *_args: None

    backend_config = SQLSpecBackendConfig(sqlspec_config=sqlspec_config, terminal_ttl=3600.0)
    queue_config = QueueConfig(queue_backend=backend_config)
    backend_config.configure_migrations(queue_config)

    context = types.SimpleNamespace(config=sqlspec_config)
    statements = await migration.up(cast("Any", context))
    assert any(expected_clause in stmt for stmt in statements)


@pytest.mark.parametrize("adapter", [*sorted(_ADAPTER_BEHAVIOR), "adbc"])
@pytest.mark.parametrize("terminal_ttl", [None, 3600.0])
def test_create_queue_store_instantiates_every_adapter_with_terminal_ttl(
    adapter: "str", terminal_ttl: "float | None"
) -> "None":
    """Every adapter store accepts terminal_ttl through create_queue_store."""
    dialect = "sqlite" if adapter == "adbc" else ("tsql" if adapter == "arrow_odbc" else None)
    config = _fake_config(adapter, is_async=(adapter not in {"spanner", "sqlite", "adbc"}), dialect=dialect)
    store = create_queue_store(config, terminal_ttl=terminal_ttl)
    assert isinstance(store, SQLSpecQueueStore)
    assert store._terminal_ttl == terminal_ttl


class _RecordingSyncDriver:
    def __init__(self, row: "Any" = None) -> "None":
        self.row = row
        self.queries: "list[str]" = []
        self.executed: "list[str]" = []

    def select_one_or_none(self, sql_text: "str") -> "Any":
        self.queries.append(sql_text)
        return self.row

    def execute(self, sql_text: "str") -> "None":
        self.executed.append(sql_text)


class _RecordingAsyncDriver:
    def __init__(self, row: "Any" = None) -> "None":
        self.row = row
        self.queries: "list[str]" = []
        self.executed: "list[str]" = []

    async def select_one_or_none(self, sql_text: "str") -> "Any":
        self.queries.append(sql_text)
        return self.row

    async def execute(self, sql_text: "str") -> "None":
        self.executed.append(sql_text)


@pytest.mark.parametrize(
    ("adapter", "dialect", "expected_sqlglot_dialect"),
    [
        ("sqlite", "sqlite", "sqlite"),
        ("aiosqlite", "sqlite", "sqlite"),
        ("adbc", "sqlite", "sqlite"),
        ("asyncpg", "postgres", "postgres"),
        ("psycopg", "postgres", "postgres"),
        ("psqlpy", "postgres", "postgres"),
        ("cockroach_asyncpg", "cockroachdb", "postgres"),
        ("cockroach_psycopg", "cockroachdb", "postgres"),
        ("pymysql", "mysql", "mysql"),
        ("aiomysql", "mysql", "mysql"),
        ("asyncmy", "mysql", "mysql"),
        ("mysqlconnector", "mysql", "mysql"),
        ("pymssql", "mssql", "tsql"),
        ("mssql_python", "mssql", "tsql"),
        ("arrow_odbc", "tsql", "tsql"),
        ("duckdb", "duckdb", "duckdb"),
    ],
)
def test_queue_store_sqlglot_dialect_mapping(adapter: "str", dialect: "str", expected_sqlglot_dialect: "str") -> "None":
    """Queue stores expose SQLGlot-compatible dialect names across all supported adapters."""
    store = create_queue_store(
        _fake_config(adapter, is_async=(adapter not in {"sqlite", "adbc", "arrow_odbc"}), dialect=dialect)
    )
    assert store.sqlglot_dialect() == expected_sqlglot_dialect


@pytest.mark.parametrize(
    ("raw_dialect", "expected_sqlglot_dialect"),
    [
        ("cockroachdb", "postgres"),
        ("postgresql", "postgres"),
        ("mssql", "tsql"),
        ("mariadb", "mysql"),
        ("sqlite", "sqlite"),
        (None, None),
    ],
)
def test_base_queue_store_sqlglot_dialect_aliases(
    raw_dialect: "str | None", expected_sqlglot_dialect: "str | None"
) -> "None":
    """Base SQLSpecQueueStore normalizes SQLGlot dialect aliases."""
    store = SQLSpecQueueStore(_fake_config("custom", dialect=raw_dialect))
    assert store.sqlglot_dialect() == expected_sqlglot_dialect


@pytest.mark.parametrize(
    ("adapter", "dialect", "terminal_ttl"),
    [
        ("sqlite", "sqlite", None),
        ("asyncpg", "postgres", None),
        ("cockroach_asyncpg", "postgres", 3600.0),
        ("pymysql", "mysql", None),
        ("pymssql", "mssql", None),
        ("arrow_odbc", "tsql", None),
        ("duckdb", "duckdb", None),
    ],
)
@pytest.mark.parametrize("table_name", ["queue_task", "app.queue_task"])
def test_schema_target_parses_every_queue_store_table_ddl(
    adapter: "str", dialect: "str", terminal_ttl: "float | None", table_name: "str"
) -> "None":
    """SchemaTarget.from_ddl parses table-only DDL across every queue store and dialect."""
    store = create_queue_store(
        _fake_config(adapter, is_async=(adapter not in {"sqlite", "arrow_odbc"}), dialect=dialect),
        table_name=table_name,
        terminal_ttl=terminal_ttl,
    )
    table_ddl = store.create_table_statement()
    target = SchemaTarget.from_ddl(store.table_name, table_ddl, dialect=store.sqlglot_dialect())
    assert len(target.create_table.columns) == 25
    indexes = store.companion_indexes()
    assert indexes
    assert all(isinstance(name, str) and bool(name) and isinstance(stmt, str) and bool(stmt) for name, stmt in indexes)


def test_mysql_queue_store_separates_table_ddl_and_companion_indexes() -> "None":
    """MySQLQueueStore omits inline indexes from create_table_statement and keeps them in companion_indexes."""
    store = create_queue_store(_fake_config("pymysql", dialect="mysql"))
    assert isinstance(store, MySQLQueueStore)

    table_ddl = store.create_table_statement()
    assert "INDEX " not in table_ddl
    assert "UNIQUE" in table_ddl
    assert "ENGINE=InnoDB" in table_ddl

    indexes = store.companion_indexes()
    index_map = dict(indexes)
    assert tuple(index_map) == (
        "ix_queue_task_pending",
        "ix_queue_task_dispatch_repair",
        "ix_queue_task_heartbeat",
        "ix_queue_task_created",
        "ix_queue_task_list",
    )
    assert index_map["ix_queue_task_pending"] == (
        "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_pending` "
        "(`status`(32), `queue`(191), `execution_backend`(191), `scheduled_at`, `priority`, `queued_at`, `created_at`)"
    )
    assert index_map["ix_queue_task_dispatch_repair"] == (
        "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_dispatch_repair` "
        "(`execution_backend`, `status`, `dispatch_checked_at`, `created_at`, `id`)"
    )
    assert (
        index_map["ix_queue_task_heartbeat"]
        == "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_heartbeat` (`status`(32), `heartbeat_at`)"
    )
    assert (
        index_map["ix_queue_task_created"]
        == "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_created` (`created_at`, `id`)"
    )
    assert (
        index_map["ix_queue_task_list"]
        == "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_list` (`status`(32), `queue`(191), `created_at`, `id`)"
    )

    versioned = store.create_statements()
    assert len(versioned) == 1
    assert "INDEX `ix_queue_task_created` (`created_at`, `id`)" in versioned[0]
    assert "INDEX `ix_queue_task_list` (`status`(32), `queue`(191), `created_at`, `id`)" in versioned[0]


@pytest.mark.parametrize("adapter", ["pymssql", "mssql_python", "arrow_odbc"])
def test_mssql_and_arrow_odbc_preserve_filtered_task_key_companion_index(adapter: "str") -> "None":
    """SQL Server stores preserve filtered task_key uniqueness in companion_indexes without duplication."""
    store = create_queue_store(_fake_config(adapter, is_async=False, dialect="tsql"))
    indexes = store.companion_indexes()
    index_map = dict(indexes)
    assert len(index_map) == len(indexes)
    assert "ix_queue_task_task_key" in index_map
    assert "WHERE [task_key] IS NOT NULL" in index_map["ix_queue_task_task_key"]
    assert store.create_statements() == [store.create_table_statement(), *(stmt for _, stmt in indexes)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter", "expect_autovacuum"),
    [("asyncpg", True), ("psycopg", True), ("psqlpy", True), ("plain_postgres", False)],
)
async def test_postgres_reconcile_storage_options_applies_autovacuum(
    adapter: "str", expect_autovacuum: "bool"
) -> "None":
    """PostgresQueueStore applies autovacuum storage parameters only when table_storage_parameters is enabled."""
    store: SQLSpecQueueStore
    if adapter == "plain_postgres":
        store = PostgresQueueStore(_fake_config("postgres", dialect="postgres"))
    else:
        store = create_queue_store(_fake_config(adapter, dialect="postgres"))

    sync_driver = _RecordingSyncDriver()
    async_driver = _RecordingAsyncDriver()
    store.prepare_schema_sync(sync_driver)
    await store.prepare_schema_async(async_driver)
    store.reconcile_storage_options_sync(sync_driver)
    await store.reconcile_storage_options_async(async_driver)

    expected = (
        ['ALTER TABLE "queue_task" SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02)']
        if expect_autovacuum
        else []
    )
    assert sync_driver.executed == expected
    assert async_driver.executed == expected
    assert "ALTER TABLE" not in store.create_table_statement()
    assert ("WITH (fillfactor = 80)" in store.create_table_statement()) is expect_autovacuum
    assert store.create_statements() == [
        store.create_table_statement(),
        *(stmt for _, stmt in store.companion_indexes()),
        *expected,
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("row", "terminal_ttl", "column_map", "expected_action"),
    [
        (None, 3600.0, None, "set"),
        ({}, None, None, "noop"),
        ({"create_statement": None}, None, None, "noop"),
        (
            types.SimpleNamespace(
                create_statement=(
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                    "''01:00:00''::INTERVAL ELSE NULL END')"
                )
            ),
            3600.0,
            None,
            "noop",
        ),
        (
            {
                "CREATE_STATEMENT": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN (status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'')) AND (completed_at IS NOT NULL) THEN completed_at + "
                    "''01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            3600.0,
            None,
            "noop",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN \"status\" IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND \"completed_at\" IS NOT NULL THEN \"completed_at\" + "
                    "''3600s''::INTERVAL ELSE NULL END')"
                )
            },
            3600.0,
            None,
            "noop",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                    "''1 day 01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            90000.0,
            None,
            "noop",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                    "INTERVAL ''60 seconds'' ELSE NULL END')"
                )
            },
            120.0,
            None,
            "set",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                    "''01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            None,
            None,
            "reset",
        ),
        ({"create_statement": 'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'off\')'}, None, None, "noop"),
        (
            {"create_statement": 'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'off\')'},
            3600.0,
            None,
            "set",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN state IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND done_at IS NOT NULL THEN done_at + "
                    "''01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            3600.0,
            {"status": "state", "completed_at": "done_at"},
            "noop",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                    "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                    "''01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            3600.0,
            {"status": "state", "completed_at": "done_at"},
            "set",
        ),
        (
            {
                "create_statement": (
                    'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                    "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'') "
                    "AND completed_at IS NOT NULL THEN completed_at + ''01:00:00''::INTERVAL ELSE NULL END')"
                )
            },
            3600.0,
            None,
            "set",
        ),
    ],
)
@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
async def test_cockroach_reconcile_storage_options_converges_ttl(
    adapter: "str",
    row: "Any",
    terminal_ttl: "float | None",
    column_map: "dict[str, str] | None",
    expected_action: "str",
) -> "None":
    """CockroachQueueStore converges row-level TTL across canonicalized SHOW CREATE TABLE responses."""
    is_async = adapter == "cockroach_asyncpg"
    store = create_queue_store(
        _fake_config(adapter, is_async=is_async, dialect="postgres"), terminal_ttl=terminal_ttl, column_map=column_map
    )
    if is_async:
        driver = _RecordingAsyncDriver(row=row)
        await store.reconcile_storage_options_async(driver)
        queries, executed = driver.queries, driver.executed
    else:
        sync_driver = _RecordingSyncDriver(row=row)
        store.reconcile_storage_options_sync(sync_driver)
        queries, executed = sync_driver.queries, sync_driver.executed

    assert queries == ['SHOW CREATE TABLE "queue_task"']
    if expected_action == "noop":
        assert executed == []
    elif expected_action == "reset":
        assert executed == ['ALTER TABLE "queue_task" RESET (ttl)']
    else:
        assert len(executed) == 1
        assert executed[0].startswith("ALTER TABLE \"queue_task\" SET (ttl = 'on', ttl_expiration_expression = ")
        if column_map:
            assert '"state"' in executed[0]
            assert '"done_at"' in executed[0]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter", "dialect", "terminal_ttl"),
    [
        ("sqlite", "sqlite", None),
        ("asyncpg", "postgres", None),
        ("cockroach_asyncpg", "postgres", 3600.0),
        ("pymysql", "mysql", None),
        ("pymssql", "mssql", None),
        ("arrow_odbc", "tsql", None),
        ("duckdb", "duckdb", None),
    ],
)
async def test_manage_schema_false_suppresses_ddl_indexes_and_storage_options(
    adapter: "str", dialect: "str", terminal_ttl: "float | None"
) -> "None":
    """manage_schema=False emits empty DDL/indexes and skips storage-option queries/writes."""
    store = create_queue_store(
        _fake_config(adapter, is_async=(adapter not in {"sqlite", "arrow_odbc"}), dialect=dialect),
        manage_schema=False,
        terminal_ttl=terminal_ttl,
    )
    sync_driver = _RecordingSyncDriver(row={"create_statement": "WITH (ttl = 'on')"})
    async_driver = _RecordingAsyncDriver(row={"create_statement": "WITH (ttl = 'on')"})

    assert store.create_table_statement() == ""
    assert store.companion_indexes() == ()
    assert store.create_statements() == []

    store.reconcile_storage_options_sync(sync_driver)
    await store.reconcile_storage_options_async(async_driver)
    assert sync_driver.queries == []
    assert sync_driver.executed == []
    assert async_driver.queries == []
    assert async_driver.executed == []


@pytest.mark.parametrize(
    ("adapter", "dialect", "connection_config"),
    [
        ("spanner", "postgres", None),
        ("db2", "db2", None),
        ("bigquery", "bigquery", None),
        ("adbc", "flightsql", None),
        ("adbc", "postgresql", None),
        ("adbc", "postgresql", {"driver_name": "adbc_driver_postgresql"}),
        ("arrow_odbc", "db2", None),
    ],
)
@pytest.mark.parametrize("action", ["reconcile", "downgrade"])
def test_reconcile_and_downgrade_reject_unsupported_builtin_adapters(
    adapter: "str", dialect: "str", connection_config: "dict[str, object] | None", action: "str"
) -> "None":
    """Unsupported built-in adapters fail closed with MigrationError before executing DDL."""
    config = _fake_config(adapter, is_async=False, dialect=dialect, connection_config=connection_config)
    driver = MagicMock()
    context = types.SimpleNamespace(config=config, driver=driver, is_async_driver=False, extension_config={})
    runner = reconcile if action == "reconcile" else downgrade

    with pytest.raises(MigrationError):
        runner(cast("Any", context))

    driver.execute_script.assert_not_called()


def test_reconcile_honors_explicit_third_party_queue_store_class() -> "None":
    """Explicit queue_store_class outside sqlspec.adapters bypasses built-in adapter rejection."""
    call_order: list[str] = []

    class CustomQueueStore(SQLSpecQueueStore):
        data_dictionary_dialect: "ClassVar[str | None]" = "sqlite"

        def prepare_schema_sync(self, driver: "Any") -> "None":
            call_order.append("prepare")

        def create_table_statement(self) -> "str":
            call_order.append("create_table")
            return super().create_table_statement()

    class ExternalCustomConfig:
        queue_store_class: "ClassVar[type[SQLSpecQueueStore]]" = CustomQueueStore
        is_async: "ClassVar[bool]" = False
        statement_config: "Any" = types.SimpleNamespace(dialect="sqlite")
        extension_config: "dict[str, Any]" = {}

    catalog = _SpyIndexCatalog(dialect="sqlite")
    driver = _SpySyncReconcileDriver(catalog=catalog)
    context = types.SimpleNamespace(
        config=ExternalCustomConfig(), driver=driver, is_async_driver=False, extension_config={}
    )

    assert reconcile(cast("Any", context)) is None
    assert call_order[:2] == ["prepare", "create_table"]
    table_scripts = [stmt for stmt in driver.scripts if "CREATE TABLE" in stmt]
    assert len(table_scripts) == 3
    assert any("queue_task" in stmt for stmt in table_scripts)
    assert any("queue_maintenance" in stmt for stmt in table_scripts)
    assert any("queue_task_reservation" in stmt for stmt in table_scripts)
    assert any("ix_queue_task_pending" in stmt for stmt in driver.scripts)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_reconcile_stores_validate_adapter_rejects_spanner(is_async: "bool") -> "None":
    """Direct store reconciliation helpers enforce adapter validation when requested."""
    config = _fake_config("spanner", is_async=is_async, dialect="postgres")
    store = create_queue_store(config)
    driver = MagicMock()

    with pytest.raises(MigrationError):
        if is_async:
            await reconcile_stores_async(driver, [store], config=config, validate_adapter=True)
        else:
            reconcile_stores_sync(driver, [store], config=config, validate_adapter=True)


@pytest.mark.anyio
async def test_reconcile_and_downgrade_dispatch_async_drivers() -> "None":
    """Reconcile and downgrade return awaitables when context.is_async_driver is truthy."""
    config = _fake_config("aiosqlite", is_async=True, dialect="sqlite")
    catalog = _SpyIndexCatalog(dialect="sqlite")
    driver = _SpyAsyncReconcileDriver(catalog=catalog)
    context = types.SimpleNamespace(config=config, driver=driver, is_async_driver=True, extension_config={})

    awaitable = reconcile(cast("Any", context))
    assert awaitable is not None
    await awaitable
    table_scripts = [stmt for stmt in driver.scripts if "CREATE TABLE" in stmt]
    assert len(table_scripts) == 3
    assert any("ix_queue_task_pending" in stmt for stmt in driver.scripts)

    driver.scripts.clear()
    down_awaitable = downgrade(cast("Any", context))
    assert down_awaitable is not None
    await down_awaitable
    assert any("queue_task_reservation" in stmt for stmt in driver.scripts)
    assert any("queue_maintenance" in stmt for stmt in driver.scripts)
    assert any("queue_task" in stmt for stmt in driver.scripts)


def test_reconcile_passes_schema_qualified_table_name_to_schema_target() -> "None":
    """Schema-qualified queue_table_name values preserve schema and unqualified table identity."""
    config = _fake_config("sqlite", is_async=False, dialect="sqlite")
    catalog = _SpyIndexCatalog(dialect="sqlite")
    driver = _SpySyncReconcileDriver(catalog=catalog)
    context = types.SimpleNamespace(
        config=config, driver=driver, is_async_driver=False, extension_config={"queue_table_name": "app.queue_task"}
    )

    assert reconcile(cast("Any", context)) is None
    assert catalog.table_queries == ["app", "app", "app"]
    assert catalog.index_queries == [("queue_task", "app"), ("queue_task", "app")]
    assert any('"app"."queue_task"' in stmt for stmt in driver.scripts)
    assert any('"app"."queue_maintenance"' in stmt for stmt in driver.scripts)
    assert any('"app"."queue_task_reservation"' in stmt for stmt in driver.scripts)


_CREATE_INDEX_RE = re.compile(
    r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([^"\s(]+)"?\s+ON\s+'
    r'(?:"?([^".\s]+)"?\.)?"?([^"\s(]+)"?',
    re.IGNORECASE,
)


class _SpyIndexCatalog:
    def __init__(
        self,
        *,
        dialect: "str | None" = None,
        default_schema: "str | None" = None,
        initial_rows: "list[dict[str, Any]] | None" = None,
    ) -> "None":
        self.dialect = dialect
        self._default_schema = default_schema
        self.table_queries: "list[str | None]" = []
        self.index_queries: "list[tuple[str | None, str | None]]" = []
        self.rows: "list[dict[str, Any]]" = list(initial_rows or [])

    def resolve_schema(self, schema: "str | None") -> "str | None":
        return schema if schema is not None else self._default_schema

    def record_script(self, script: "str") -> "None":
        match = _CREATE_INDEX_RE.search(script)
        if match is None:
            return
        index_name, schema_name, table_name = match.groups()
        self.rows.append({
            "index_name": index_name,
            "table_name": table_name,
            "schema_name": schema_name or self._default_schema,
        })

    def lookup_indexes(self, table: "str | None" = None, schema: "str | None" = None) -> "list[dict[str, Any]]":
        self.index_queries.append((table, schema))
        if self.dialect == "cockroachdb" and schema is None:
            return []
        if table is None:
            return list(self.rows)
        return [row for row in self.rows if row["table_name"] == table or (row.get("extra_return_for") == table)]


class _SpySyncDataDictionary:
    def __init__(self, catalog: "_SpyIndexCatalog") -> "None":
        self._catalog = catalog
        self.dialect = catalog.dialect

    def resolve_schema(self, schema: "str | None") -> "str | None":
        return self._catalog.resolve_schema(schema)

    def get_tables(self, _driver: "Any", *, schema: "str | None" = None) -> "list[Any]":
        self._catalog.table_queries.append(schema)
        return []

    def get_indexes(
        self, _driver: "Any", *, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        return self._catalog.lookup_indexes(table=table, schema=schema)


class _SpyAsyncDataDictionary:
    def __init__(self, catalog: "_SpyIndexCatalog") -> "None":
        self._catalog = catalog
        self.dialect = catalog.dialect

    def resolve_schema(self, schema: "str | None") -> "str | None":
        return self._catalog.resolve_schema(schema)

    async def get_tables(self, _driver: "Any", *, schema: "str | None" = None) -> "list[Any]":
        self._catalog.table_queries.append(schema)
        return []

    async def get_indexes(
        self, _driver: "Any", *, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        return self._catalog.lookup_indexes(table=table, schema=schema)


class _SpySyncReconcileDriver:
    def __init__(self, *, catalog: "_SpyIndexCatalog", create_statement_sql: "str | None" = None) -> "None":
        self.catalog = catalog
        self.data_dictionary = _SpySyncDataDictionary(catalog)
        self.create_statement_sql = create_statement_sql
        self.queries: "list[str]" = []
        self.executed: "list[str]" = []
        self.scripts: "list[str]" = []
        self.commit = MagicMock(side_effect=AssertionError("commit must stay under runner control"))

    def select_one_or_none(self, sql_text: "str") -> "Any":
        self.queries.append(sql_text)
        if self.create_statement_sql is None:
            return None
        return {"create_statement": self.create_statement_sql}

    def execute(self, sql_text: "str") -> "None":
        self.executed.append(sql_text)

    def execute_script(self, sql_text: "str") -> "None":
        self.scripts.append(sql_text)
        self.catalog.record_script(sql_text)


class _SpyAsyncReconcileDriver:
    def __init__(self, *, catalog: "_SpyIndexCatalog", create_statement_sql: "str | None" = None) -> "None":
        self.catalog = catalog
        self.data_dictionary = _SpyAsyncDataDictionary(catalog)
        self.create_statement_sql = create_statement_sql
        self.queries: "list[str]" = []
        self.executed: "list[str]" = []
        self.scripts: "list[str]" = []
        self.commit = MagicMock(side_effect=AssertionError("commit must stay under runner control"))

    async def select_one_or_none(self, sql_text: "str") -> "Any":
        self.queries.append(sql_text)
        if self.create_statement_sql is None:
            return None
        return {"create_statement": self.create_statement_sql}

    async def execute(self, sql_text: "str") -> "None":
        self.executed.append(sql_text)

    async def execute_script(self, sql_text: "str") -> "None":
        self.scripts.append(sql_text)
        self.catalog.record_script(sql_text)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("migration_config", "expected_schema"), [({}, "public"), ({"default_schema": "analytics"}, "analytics")]
)
@pytest.mark.parametrize("dictionary_dialect", ["cockroachdb", None])
async def test_reconcile_cockroach_event_log_get_indexes_resolves_schema(
    migration_config: "dict[str, Any]", expected_schema: "str", dictionary_dialect: "str | None"
) -> "None":
    """CockroachDB SQLSpecQueueEventLogStore resolves explicit schema for get_indexes and preserves schema_name=target.schema."""
    assert SQLSpecQueueEventLogStore.data_dictionary_dialect is None
    event_table = event_history_table_name_for("queue_task")
    catalog = _SpyIndexCatalog(
        dialect=dictionary_dialect,
        default_schema=expected_schema,
        initial_rows=[
            {
                "index_name": f"ix_{event_table}_occurred_at",
                "table_name": "other_table",
                "schema_name": expected_schema,
                "extra_return_for": event_table,
            }
        ],
    )
    driver = _SpyAsyncReconcileDriver(catalog=catalog)
    config = CockroachAsyncpgConfig(
        extension_config={QUEUE_EXTENSION_NAME: {"event_history_enabled": True}}, migration_config=migration_config
    )
    context = types.SimpleNamespace(config=config, driver=driver, is_async_driver=True, extension_config={})

    awaitable = reconcile(cast("Any", context))
    assert awaitable is not None
    await awaitable

    assert ("queue_task", expected_schema) in catalog.index_queries
    assert (event_table, expected_schema) in catalog.index_queries
    assert maintenance_table_name_for("queue_task") not in {tbl for tbl, _ in catalog.index_queries}
    assert "queue_task_reservation" not in {tbl for tbl, _ in catalog.index_queries}
    for suffix in ("task_id", "task_name", "actor_id", "occurred_at", "scope_key", "entity"):
        assert any(f"ix_{event_table}_{suffix}" in stmt for stmt in driver.scripts)


@pytest.mark.anyio
async def test_reconcile_cockroach_event_log_detects_driver_dictionary_dialect() -> "None":
    """driver.data_dictionary.dialect == 'cockroachdb' triggers default-schema index queries even without cockroach config name."""
    event_table = event_history_table_name_for("queue_task")
    catalog = _SpyIndexCatalog(dialect="cockroachdb", default_schema=None)
    driver = _SpyAsyncReconcileDriver(catalog=catalog)
    config = AsyncpgConfig(extension_config={QUEUE_EXTENSION_NAME: {"event_history_enabled": True}})
    context = types.SimpleNamespace(config=config, driver=driver, is_async_driver=True, extension_config={})

    awaitable = reconcile(cast("Any", context))
    assert awaitable is not None
    await awaitable

    assert (event_table, "public") in catalog.index_queries


@pytest.mark.anyio
@pytest.mark.parametrize("adapter", ["cockroach_asyncpg", "cockroach_psycopg"])
@pytest.mark.parametrize(
    ("existing_create_sql", "terminal_ttl", "expected_action"),
    [
        ('CREATE TABLE "queue_task" ("id" VARCHAR(64))', 3600.0, "set"),
        (
            (
                'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                "''01:00:00''::INTERVAL ELSE NULL END')"
            ),
            3600.0,
            "noop",
        ),
        (
            (
                'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                "INTERVAL ''60 seconds'' ELSE NULL END')"
            ),
            120.0,
            "set",
        ),
        (
            (
                'CREATE TABLE "queue_task" ("id" VARCHAR(64)) WITH (ttl = \'on\', '
                "ttl_expiration_expression = 'CASE WHEN status IN (''completed'', ''failed'', "
                "''cancelled'', ''expired'') AND completed_at IS NOT NULL THEN completed_at + "
                "''01:00:00''::INTERVAL ELSE NULL END')"
            ),
            None,
            "reset",
        ),
    ],
)
async def test_reconcile_cockroach_ttl_reconcile_delegates_and_isolates_maintenance(
    adapter: "str", existing_create_sql: "str", terminal_ttl: "float | None", expected_action: "str"
) -> "None":
    """reconcile() delegates TTL convergence to CockroachQueueStore and isolates queue_maintenance."""
    if adapter == "cockroach_psycopg" and _CockroachPsycopgSyncConfig is None:
        pytest.skip("psycopg not installed")
    catalog = _SpyIndexCatalog(dialect="cockroachdb", default_schema="public")
    if adapter == "cockroach_asyncpg":
        config: Any = CockroachAsyncpgConfig()
        driver: Any = _SpyAsyncReconcileDriver(catalog=catalog, create_statement_sql=existing_create_sql)
        is_async = True
    else:
        config = _CockroachPsycopgSyncConfig()
        driver = _SpySyncReconcileDriver(catalog=catalog, create_statement_sql=existing_create_sql)
        is_async = False

    context = types.SimpleNamespace(
        config=config, driver=driver, is_async_driver=is_async, extension_config={"terminal_ttl": terminal_ttl}
    )
    result = reconcile(cast("Any", context))
    if is_async:
        assert result is not None
        await result
    else:
        assert result is None

    assert driver.queries == ['SHOW CREATE TABLE "queue_task"']
    assert not any("queue_maintenance" in stmt for stmt in [*driver.queries, *driver.executed])
    assert ("queue_task", "public") in catalog.index_queries
    assert maintenance_table_name_for("queue_task") not in {tbl for tbl, _ in catalog.index_queries}
    if expected_action == "noop":
        assert driver.executed == []
    elif expected_action == "reset":
        assert driver.executed == ['ALTER TABLE "queue_task" RESET (ttl)']
    else:
        assert len(driver.executed) == 1
        assert driver.executed[0].startswith("ALTER TABLE \"queue_task\" SET (ttl = 'on', ttl_expiration_expression = ")
        assert terminal_ttl is not None
        assert f"{int(terminal_ttl)} seconds" in driver.executed[0]


@pytest.mark.anyio
async def test_reconcile_postgres_delegates_autovacuum_and_isolates_maintenance() -> "None":
    """reconcile() delegates Postgres autovacuum options to PostgresQueueStore and skips queue_maintenance."""
    catalog = _SpyIndexCatalog(dialect="postgres", default_schema="public")
    driver = _SpyAsyncReconcileDriver(catalog=catalog)
    context = types.SimpleNamespace(config=AsyncpgConfig(), driver=driver, is_async_driver=True, extension_config={})

    awaitable = reconcile(cast("Any", context))
    assert awaitable is not None
    await awaitable

    assert driver.executed == [
        'ALTER TABLE "queue_task" SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.02)'
    ]


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg", "psqlpy", "cockroach_asyncpg", "cockroach_psycopg"])
@pytest.mark.parametrize("table_name", ["queue_task", "app.queue_task"])
def test_postgres_and_cockroach_emit_and_drop_listing_indexes(adapter: "str", table_name: "str") -> "None":
    """Postgres and Cockroach stores emit and drop created and list listing indexes."""
    store = create_queue_store(_fake_config(adapter, dialect="postgres"), table_name=table_name)
    created_name = f"ix_{table_name.replace('.', '_')}_created"
    list_name = f"ix_{table_name.replace('.', '_')}_list"
    quoted_table = '"app"."queue_task"' if "." in table_name else '"queue_task"'
    expected_created = f'CREATE INDEX IF NOT EXISTS "{created_name}" ON {quoted_table} ("created_at" DESC, "id" DESC)'
    expected_list = (
        f'CREATE INDEX IF NOT EXISTS "{list_name}" ON {quoted_table} ("status", "queue", "created_at" DESC, "id" DESC)'
    )

    companion = dict(store.companion_indexes())
    assert companion[created_name] == expected_created
    assert companion[list_name] == expected_list

    created = store.create_statements()
    assert expected_created in created
    assert expected_list in created

    drops = store.drop_statements()
    drop_table_pos = next(index for index, stmt in enumerate(drops) if "DROP TABLE" in stmt)
    list_drop_pos = next(index for index, stmt in enumerate(drops) if list_name in stmt)
    created_drop_pos = next(index for index, stmt in enumerate(drops) if created_name in stmt)
    assert list_drop_pos < drop_table_pos
    assert created_drop_pos < drop_table_pos


@pytest.mark.parametrize("adapter", ["asyncpg", "cockroach_asyncpg"])
def test_postgres_and_cockroach_cap_listing_index_names_at_63_bytes(adapter: "str") -> "None":
    """Postgres and Cockroach cap created and list index names at 63 UTF-8 bytes."""
    long_table = "q" * 62
    store = create_queue_store(_fake_config(adapter, dialect="postgres"), table_name=long_table)
    created_name = store._index_name("created")
    list_name = store._index_name("list")
    pending_name = store._index_name("pending")
    scheduled_name = store._index_name("scheduled")
    heartbeat_name = store._index_name("heartbeat")
    repair_name = store._index_name("dispatch_repair")

    assert len(created_name.encode("utf-8")) <= 63
    assert len(list_name.encode("utf-8")) <= 63
    assert len({created_name, list_name, pending_name, scheduled_name, heartbeat_name, repair_name}) == 6
    if adapter.startswith("cockroach"):
        assert len(pending_name.encode("utf-8")) > 63
        assert len(scheduled_name.encode("utf-8")) > 63
        assert len(heartbeat_name.encode("utf-8")) > 63

    companion = dict(store.companion_indexes())
    assert created_name in companion
    assert list_name in companion
    assert any(created_name in stmt for stmt in store.create_statements())
    assert any(list_name in stmt for stmt in store.create_statements())
    assert any(created_name in stmt for stmt in store.drop_statements())
    assert any(list_name in stmt for stmt in store.drop_statements())


@pytest.mark.parametrize("adapter", ["aiomysql", "asyncmy", "pymysql", "mysqlconnector"])
def test_mysql_stores_emit_inline_and_companion_listing_indexes(adapter: "str") -> "None":
    """MySQL stores emit prefixed created and list indexes inline and in companion_indexes without DESC."""
    store = create_queue_store(_fake_config(adapter, dialect="mysql"), table_name="queue_task")

    assert "INDEX " not in store.create_table_statement()

    companion = dict(store.companion_indexes())
    assert (
        companion["ix_queue_task_created"]
        == "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_created` (`created_at`, `id`)"
    )
    assert companion["ix_queue_task_list"] == (
        "ALTER TABLE `queue_task` ADD INDEX `ix_queue_task_list` (`status`(32), `queue`(191), `created_at`, `id`)"
    )

    statements = store.create_statements()
    assert len(statements) == 1
    assert "INDEX `ix_queue_task_created` (`created_at`, `id`)" in statements[0]
    assert "INDEX `ix_queue_task_list` (`status`(32), `queue`(191), `created_at`, `id`)" in statements[0]
    assert "DESC" not in statements[0]
    assert any("DROP TABLE" in stmt for stmt in store.drop_statements())


@pytest.mark.parametrize("adapter", ["pymssql", "mssql_python"])
def test_mssql_stores_emit_guarded_listing_indexes_and_drops(adapter: "str") -> "None":
    """MSSQL stores emit and drop sys.indexes-guarded created and list indexes."""
    store = create_queue_store(_fake_config(adapter, is_async=False, dialect="mssql"), table_name="queue_task")
    expected_created = (
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_queue_task_created' "
        "AND object_id = OBJECT_ID(N'queue_task')) "
        "CREATE INDEX [ix_queue_task_created] ON [queue_task] ([created_at] DESC, [id] DESC);"
    )
    expected_list = (
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_queue_task_list' "
        "AND object_id = OBJECT_ID(N'queue_task')) "
        "CREATE INDEX [ix_queue_task_list] ON [queue_task] ([status], [queue], [created_at] DESC, [id] DESC);"
    )

    companion = dict(store.companion_indexes())
    assert companion["ix_queue_task_created"] == expected_created
    assert companion["ix_queue_task_list"] == expected_list

    created = store.create_statements()
    assert expected_created in created
    assert expected_list in created

    drops = store.drop_statements()
    expected_drop_list = (
        "IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_queue_task_list' "
        "AND object_id = OBJECT_ID(N'queue_task')) "
        "DROP INDEX [ix_queue_task_list] ON [queue_task];"
    )
    expected_drop_created = (
        "IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_queue_task_created' "
        "AND object_id = OBJECT_ID(N'queue_task')) "
        "DROP INDEX [ix_queue_task_created] ON [queue_task];"
    )
    drop_table_pos = next(index for index, stmt in enumerate(drops) if "DROP TABLE [queue_task];" in stmt)
    assert drops.index(expected_drop_list) < drop_table_pos
    assert drops.index(expected_drop_created) < drop_table_pos


def test_spanner_store_emits_and_drops_listing_indexes() -> "None":
    """SpannerQueueStore creates listing indexes without IF NOT EXISTS and drops them before DROP TABLE."""
    store = create_queue_store(_fake_config("spanner", is_async=False, dialect="spanner"), table_name="queue_task")
    expected_created = "CREATE INDEX `ix_queue_task_created` ON `queue_task` (`created_at` DESC, `id` DESC)"
    expected_list = (
        "CREATE INDEX `ix_queue_task_list` ON `queue_task` (`status`, `queue`, `created_at` DESC, `id` DESC)"
    )

    created = store.create_statements()
    assert expected_created in created
    assert expected_list in created
    assert not any("IF NOT EXISTS" in stmt for stmt in created)

    drops = store.drop_statements()
    drop_table_pos = drops.index("DROP TABLE `queue_task`")
    assert drops.index("DROP INDEX `ix_queue_task_list`") < drop_table_pos
    assert drops.index("DROP INDEX `ix_queue_task_created`") < drop_table_pos


@pytest.mark.parametrize(
    ("adapter", "dialect", "expected_created_cols", "expected_list_cols", "has_companion"),
    [
        (
            "asyncpg",
            "postgres",
            '("created" DESC, "task_id" DESC)',
            '("state", "lane", "created" DESC, "task_id" DESC)',
            True,
        ),
        (
            "cockroach_asyncpg",
            "postgres",
            '("created" DESC, "task_id" DESC)',
            '("state", "lane", "created" DESC, "task_id" DESC)',
            True,
        ),
        ("aiomysql", "mysql", "(`created`, `task_id`)", "(`state`(32), `lane`(191), `created`, `task_id`)", True),
        (
            "pymssql",
            "mssql",
            "([created] DESC, [task_id] DESC)",
            "([state], [lane], [created] DESC, [task_id] DESC)",
            True,
        ),
        (
            "spanner",
            "spanner",
            "(`created` DESC, `task_id` DESC)",
            "(`state`, `lane`, `created` DESC, `task_id` DESC)",
            False,
        ),
    ],
)
def test_dialect_listing_indexes_honor_custom_column_map(
    adapter: "str", dialect: "str", expected_created_cols: "str", expected_list_cols: "str", has_companion: "bool"
) -> "None":
    """Dialect created and list index DDL propagates custom column_map overrides."""
    column_map = {"status": "state", "queue": "lane", "created_at": "created", "id": "task_id"}
    store = create_queue_store(
        _fake_config(adapter, is_async=(adapter not in {"pymssql", "spanner"}), dialect=dialect), column_map=column_map
    )

    statements_sql = "\n".join(store.create_statements())
    assert expected_created_cols in statements_sql
    assert expected_list_cols in statements_sql
    if has_companion:
        companion = dict(store.companion_indexes())
        assert expected_created_cols in companion["ix_queue_task_created"]
        assert expected_list_cols in companion["ix_queue_task_list"]


@pytest.mark.parametrize(
    ("adapter", "dialect"),
    [
        ("asyncpg", "postgres"),
        ("cockroach_asyncpg", "postgres"),
        ("pymysql", "mysql"),
        ("pymssql", "mssql"),
        ("spanner", "spanner"),
    ],
)
def test_dialect_manage_schema_false_suppresses_drop_statements(adapter: "str", dialect: "str") -> "None":
    """manage_schema=False suppresses drop_statements alongside create_statements and companion_indexes."""
    store = create_queue_store(
        _fake_config(adapter, is_async=(adapter not in {"pymssql", "spanner"}), dialect=dialect), manage_schema=False
    )

    assert store.create_table_statement() == ""
    assert store.companion_indexes() == ()
    assert store.create_statements() == []
    assert store.drop_statements() == []
