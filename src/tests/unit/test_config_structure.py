import ast
import contextlib
import io
import re
import tokenize
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from litestar_queues import QueueConfig
from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


PACKAGE_ROOT = Path("src/litestar_queues")
_DIRECTIVE_COMMENT_RE = re.compile(
    r"^#\s*(?:noqa(?:\b|:)|ruff:\s*noqa\b|type:\s*ignore\b|pyright:\s*ignore\b|pragma:\s*no\s*cover\b)", re.IGNORECASE
)

_APPROVED_NESTED_IMPORTS = {
    "_cli.py": {"litestar.cli._utils", "litestar_queues.worker.runtime"},
    "worker/supervisor.py": {"litestar.cli._utils", "litestar_queues.plugin", "multiprocessing.connection"},
    "backends/advanced_alchemy/event_log.py": {"litestar.pagination", "litestar_queues.events"},
    "backends/advanced_alchemy/service.py": {"litestar_queues.events", "litestar_queues.exceptions"},
    "backends/redis/backend.py": {"redis"},
    "backends/redis/event_log.py": {"litestar_queues.events.history", "litestar_queues.events.query"},
    "backends/sqlspec/backend.py": {"duckdb", "sqlspec.adapters.aiosqlite"},
    "backends/sqlspec/config.py": {
        "litestar_queues.backends.sqlspec.backend",
        "litestar_queues.backends.sqlspec.extension",
        "litestar_queues.backends.sqlspec.schema",
    },
    "backends/sqlspec/event_log.py": {"litestar_queues.events.query", "litestar_queues.events.typing"},
    "backends/sqlspec/event_sink.py": {"sqlspec", "sqlspec.adapters.aiosqlite"},
    "backends/sqlspec/maintenance.py": {"litestar_queues.backends.sqlspec.stores.spanner"},
    "backends/sqlspec/reservation.py": {"litestar_queues.backends.sqlspec.stores.spanner"},
    "backends/sqlspec/stores/spanner.py": {"google.api_core.exceptions", "sqlspec.adapters.spanner"},
    "backends/valkey/backend.py": {"valkey"},
    "config.py": {
        "litestar.di",
        "litestar_queues.backends",
        "litestar_queues.events",
        "litestar_queues.execution",
        "litestar_queues.service",
    },
    "events/__init__.py": {"litestar_queues.events.channels_sink"},
    "execution/__init__.py": {"litestar_queues.execution"},
    "execution/cloudtasks/oidc.py": {"google.auth.transport.requests", "google.oauth2.id_token"},
    "plugin.py": {
        "litestar_queues._cli",
        "litestar_queues.backends.ephemeral.server",
        "litestar_queues.events.streaming",
        "litestar_queues.execution.cloudtasks.routes",
        "litestar_queues.maintenance",
        "litestar_queues.observability",
        "litestar_queues.worker",
        "litestar_queues.worker.invocation",
    },
    "service.py": {
        "litestar_queues.backends.base",
        "litestar_queues.execution.cloudtasks",
        "litestar_queues.observability",
    },
    "task.py": {"litestar_queues.config", "litestar_queues.service"},
}


def _is_dataclass_config(node: ast.ClassDef) -> bool:
    if not node.name.endswith("Config"):
        return False
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Name) and target.id == "dataclass":
            return True
    return False


def _is_class_var(annotation: ast.expr) -> bool:
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        return "ClassVar" in annotation.value
    return any(isinstance(node, ast.Name) and node.id == "ClassVar" for node in ast.walk(annotation))


def test_public_config_fields_have_immediate_attribute_docstrings() -> None:
    """Every public dataclass config field documents its active runtime purpose."""
    missing: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for class_node in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
            if not _is_dataclass_config(class_node):
                continue
            for index, node in enumerate(class_node.body):
                if (
                    not isinstance(node, ast.AnnAssign)
                    or not isinstance(node.target, ast.Name)
                    or node.target.id.startswith("_")
                    or _is_class_var(node.annotation)
                ):
                    continue
                following = class_node.body[index + 1] if index + 1 < len(class_node.body) else None
                documented = (
                    isinstance(following, ast.Expr)
                    and isinstance(following.value, ast.Constant)
                    and isinstance(following.value.value, str)
                )
                if not documented:
                    missing.append(f"{path}:{class_node.name}.{node.target.id}")

    assert missing == []


def test_public_config_fields_are_read_by_runtime_code() -> None:
    """A public config field cannot remain as an unused declaration."""
    trees = {path: ast.parse(path.read_text()) for path in sorted(PACKAGE_ROOT.rglob("*.py"))}
    loaded_attributes = {
        node.attr
        for tree in trees.values()
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load)
    }
    unused: list[str] = []
    for path, tree in trees.items():
        for class_node in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
            if not _is_dataclass_config(class_node):
                continue
            unused.extend(
                f"{path}:{class_node.name}.{node.target.id}"
                for node in class_node.body
                if isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and not node.target.id.startswith("_")
                and not _is_class_var(node.annotation)
                and node.target.id not in loaded_attributes
            )

    assert unused == []


def test_retired_pre_release_identifiers_are_absent() -> None:
    """The unreleased API break retains no aliases or migration-era prose."""
    retired = {
        "Enqueue" + "Spec",
        "Event" + "Config",
        "EventLog" + "Config",
        "Maintenance" + "Lease",
        "TaskPayload" + "TooLargeError",
        "Uniqueness" + "Tombstone",
        "allow_" + "unauthenticated",
        "in_app_" + "worker",
        "maintenance_" + "lease",
        "max_task_" + "payload_bytes",
        "notify_" + "transport",
        "quiet_" + "success",
        "worker_" + "batch_size",
        "worker_" + "max_concurrency",
        "worker_" + "poll_interval",
        "worker_" + "queues",
    }
    roots = (Path("README.md"), Path("docs"), Path("examples"), Path("tools"), PACKAGE_ROOT)
    matches: list[str] = []
    for root in roots:
        paths = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
        for path in paths:
            if path.suffix not in {".md", ".py", ".rst", ".toml"}:
                continue
            text = path.read_text()
            matches.extend(f"{path}:{identifier}" for identifier in retired if identifier in text)

    assert matches == []


def test_runtime_imports_stay_within_reviewed_lazy_boundaries() -> None:
    """Nested imports are limited to cycles, optional adapters, DI, CLI, and observability boundaries."""
    unexpected: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        relative_path = str(path.relative_to(PACKAGE_ROOT))
        approved = _APPROVED_NESTED_IMPORTS.get(relative_path, set())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            parent = parents.get(node)
            while parent is not None and not isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
                parent = parents.get(parent)
            if parent is None:
                continue
            modules = [node.module] if isinstance(node, ast.ImportFrom) else [alias.name for alias in node.names]
            unexpected.extend(f"{path}:{node.lineno}:{module}" for module in modules if module not in approved)

    assert unexpected == []


def test_modules_declare_imports_and_exports_before_constants() -> None:
    """Module layout is docstring, imports, TYPE_CHECKING, ``__all__``, then constants."""
    out_of_order: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        type_checking_line: int | None = None
        dunder_all_line: int | None = None
        first_constant: tuple[int, str] | None = None
        for node in tree.body:
            if isinstance(node, ast.If) and ast.unparse(node.test).strip() == "TYPE_CHECKING":
                type_checking_line = node.lineno
                continue
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = [target.id for target in targets if isinstance(target, ast.Name)]
            if "__all__" in names:
                dunder_all_line = node.lineno
            elif first_constant is None:
                first_constant = (node.lineno, names[0] if names else "<unnamed>")
        if dunder_all_line is None:
            out_of_order.append(f"{path}: missing __all__")
        if first_constant is None:
            continue
        line, name = first_constant
        if type_checking_line is not None and line < type_checking_line:
            out_of_order.append(f"{path}:{line}: {name} precedes the TYPE_CHECKING block")
        if dunder_all_line is not None and line < dunder_all_line:
            out_of_order.append(f"{path}:{line}: {name} precedes __all__")

    assert out_of_order == []


def test_python_files_contain_no_inline_comments() -> None:
    """Only shebangs, PEP 723 script blocks, and linter/typechecker directives may use #."""
    violations: list[str] = []
    roots = (Path("src"), Path("tools"), Path("docs/examples"), Path("examples"))
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            in_pep723 = False
            for tok in tokenize.generate_tokens(io.StringIO(source).readline):
                if tok.type != tokenize.COMMENT:
                    if tok.type not in (
                        tokenize.NL,
                        tokenize.NEWLINE,
                        tokenize.ENCODING,
                        tokenize.INDENT,
                        tokenize.DEDENT,
                    ):
                        in_pep723 = False
                    continue
                lineno, _ = tok.start
                stripped = tok.string.strip()
                if lineno == 1 and stripped.startswith("#!"):
                    continue
                if stripped == "# /// script":
                    in_pep723 = True
                    continue
                if in_pep723:
                    if stripped == "# ///":
                        in_pep723 = False
                    continue
                if _DIRECTIVE_COMMENT_RE.match(stripped):
                    continue
                violations.append(f"{path}:{lineno}:{stripped}")

    assert violations == []


def test_queue_config_rejects_resolver_and_provider_together() -> "None":
    """Two dependency mappings for one call have no defined precedence."""

    async def resolver(task: "object", record: "object", context: "object") -> "dict[str, object]":
        return {}

    @contextlib.asynccontextmanager
    async def provider(task: "object", record: "object", context: "object") -> "AsyncIterator[dict[str, object]]":
        yield {}

    with pytest.raises(QueueConfigurationError, match="task_dependency_provider"):
        QueueConfig(task_dependency_resolver=resolver, task_dependency_provider=provider)

    assert QueueConfig(task_dependency_resolver=resolver).task_dependency_provider is None
    assert QueueConfig(task_dependency_provider=provider).task_dependency_resolver is None
