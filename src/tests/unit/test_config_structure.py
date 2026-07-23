import ast
from pathlib import Path

PACKAGE_ROOT = Path("src/litestar_queues")

_APPROVED_NESTED_IMPORTS = {
    "_cli.py": {"litestar.cli._utils"},
    "backends/redis/backend.py": {"redis"},
    "backends/sqlspec/backend.py": {"sqlspec.adapters.aiosqlite", "sqlspec.utils.module_loader"},
    "backends/sqlspec/maintenance.py": {"litestar_queues.backends.sqlspec.stores.spanner.store"},
    "backends/sqlspec/reservation.py": {"litestar_queues.backends.sqlspec.stores.spanner.store"},
    "backends/sqlspec/stores/spanner/store.py": {"google.api_core.exceptions", "sqlspec.adapters.spanner"},
    "backends/valkey/backend.py": {"valkey"},
    "config.py": {
        "litestar.di",
        "litestar_queues.backends",
        "litestar_queues.backends.advanced_alchemy",
        "litestar_queues.backends.redis",
        "litestar_queues.backends.sqlspec",
        "litestar_queues.backends.valkey",
        "litestar_queues.events",
        "litestar_queues.exceptions",
        "litestar_queues.execution",
        "litestar_queues.maintenance",
        "litestar_queues.models",
        "litestar_queues.observability",
        "litestar_queues.service",
        "litestar_queues.task",
        "litestar_queues.worker",
    },
    "events/__init__.py": {"litestar_queues.events.litestar"},
    "events/sqlspec.py": {"sqlspec", "sqlspec.adapters.aiosqlite"},
    "plugin.py": {
        "litestar_queues._cli",
        "litestar_queues.backends.sqlspec",
        "litestar_queues.backends.sqlspec.backend",
        "litestar_queues.backends.sqlspec.extension",
        "litestar_queues.backends.sqlspec.schema",
        "litestar_queues.events.streaming",
        "litestar_queues.observability",
    },
    "service.py": {"litestar_queues.observability"},
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
