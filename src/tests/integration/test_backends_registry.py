"""Registry-level checks for the integration backend matrix."""

from tests.integration._backends import QUEUE_BACKENDS


def test_sqlspec_backend_registry_includes_mysql_pymysql_case() -> "None":
    case = next((case for case in QUEUE_BACKENDS if case.name == "mysql-pymysql"), None)

    assert case is not None
    assert case.extras == frozenset({"pymysql", "sqlspec"})
    assert case.service_attr == "mysql_service"
    assert case.capabilities == frozenset({"polling-only", "json-column", "sync-driver"})
