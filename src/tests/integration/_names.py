"""Helpers for integration test object names."""

from hashlib import blake2s

__all__ = ("table_name_for_test",)


def table_name_for_test(prefix: "str", case_name: "str", nodeid: "str", *, max_length: "int" = 63) -> "str":
    """Build a deterministic database table name for one parametrized test.

    Returns:
        A database identifier bounded by ``max_length``.
    """
    normalized_case = "".join(char if char.isalnum() else "_" for char in case_name.lower()).strip("_")
    node_hash = blake2s(nodeid.encode(), digest_size=5).hexdigest()
    table_name = f"{prefix}_{normalized_case}_{node_hash}"
    return table_name[:max_length].rstrip("_")
