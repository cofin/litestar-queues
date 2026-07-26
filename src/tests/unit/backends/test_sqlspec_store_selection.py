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
    sys.modules.setdefault(module_name, types.ModuleType(module_name))
    config_type = type(f"{'Async' if is_async else 'Sync'}Config", (), {"__module__": module_name})
    config = config_type()
    config.statement_config = types.SimpleNamespace(dialect=dialect)  # type: ignore[attr-defined]
    config.extension_config = {}  # type: ignore[attr-defined]
    return config


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
