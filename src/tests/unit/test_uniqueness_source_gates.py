"""Source gates enforcing the task-uniqueness owner policies.

These assert structural invariants that unit behavior alone cannot guarantee:
identity never uses pickle or ``repr()``, keeps no global argument-digest cache,
and the enqueue fast paths never bind or serialize arguments (all argument
inspection lives in the identity module).
"""

import ast
import inspect
from pathlib import Path

from litestar_queues import _identity, service


def _source(module: "object") -> "str":
    return Path(inspect.getfile(module)).read_text()  # type: ignore[arg-type]


def _tree(module: "object") -> "ast.Module":
    return ast.parse(_source(module))


def test_identity_module_forbids_pickle_import_and_repr_calls() -> "None":
    tree = _tree(_identity)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] != "pickle" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] != "pickle"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id != "repr", "identity must not derive keys from repr()"


def test_identity_module_keeps_no_global_argument_digest_cache() -> "None":
    tree = _tree(_identity)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                name = decorator.attr if isinstance(decorator, ast.Attribute) else getattr(decorator, "id", "")
                assert name not in {"cache", "lru_cache"}, "no global args-to-digest cache is allowed"


def test_identity_module_uses_versioned_sha256_json() -> "None":
    source = _source(_identity)
    assert "hashlib.sha256" in source
    assert "json.dumps" in source
    assert "sort_keys=True" in source
    assert "allow_nan=False" in source


def test_service_fast_paths_never_bind_or_serialize_arguments() -> "None":
    source = _source(service)
    # Signature binding / default application belong to the identity module only.
    assert ".bind(" not in source
    assert "apply_defaults" not in source
    # ``arguments_identity`` is invoked exactly once: the unique_by="arguments" branch.
    assert source.count("arguments_identity(") == 1
