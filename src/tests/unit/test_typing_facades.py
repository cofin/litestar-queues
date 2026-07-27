"""Every ``_typing`` module is reached through the facade beside it.

The rule is one facade per level: ``<package>/typing.py`` re-exports
``<package>/_typing.py`` and nothing else. A parent never re-exports a child's
types, so there is exactly one supported import path for each name and the
private modules stay free to move.
"""

import ast
from pathlib import Path

PACKAGE_ROOT = Path("src/litestar_queues")
TESTS_ROOT = Path("src/tests")


def _private_typing_modules() -> "list[Path]":
    return sorted(PACKAGE_ROOT.rglob("_typing.py"))


def _imported_modules(tree: "ast.AST") -> "list[tuple[int, str]]":
    found: "list[tuple[int, str]]" = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            found.append((node.lineno, node.module))
        elif isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
    return found


def test_every_private_typing_module_has_a_facade_beside_it() -> "None":
    missing = [str(path) for path in _private_typing_modules() if not (path.parent / "typing.py").is_file()]

    assert missing == []


def test_each_facade_re_exports_only_its_own_private_module() -> "None":
    """A facade that reaches into another package gives a name two import paths."""
    offenders: "list[str]" = []
    for facade in sorted(PACKAGE_ROOT.rglob("typing.py")):
        own = f"{'.'.join(('litestar_queues', *facade.parent.relative_to(PACKAGE_ROOT).parts))}._typing"
        tree = ast.parse(facade.read_text(encoding="utf-8"))
        offenders.extend(
            f"{facade}:{lineno}:{module}"
            for lineno, module in _imported_modules(tree)
            if module.endswith("._typing") and module != own
        )

    assert offenders == []


def test_no_facade_re_exports_a_nested_facade() -> "None":
    offenders: "list[str]" = []
    for facade in sorted(PACKAGE_ROOT.rglob("typing.py")):
        tree = ast.parse(facade.read_text(encoding="utf-8"))
        offenders.extend(
            f"{facade}:{lineno}:{module}"
            for lineno, module in _imported_modules(tree)
            if module.startswith("litestar_queues.") and module.endswith(".typing")
        )

    assert offenders == []


def test_nothing_outside_the_package_imports_a_private_typing_module() -> "None":
    """Tests and downstream code use the facade, never the private sibling."""
    offenders: "list[str]" = []
    for path in sorted(TESTS_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        offenders.extend(
            f"{path}:{lineno}:{module}"
            for lineno, module in _imported_modules(tree)
            if module.startswith("litestar_queues.") and (module.endswith("._typing") or "._typing." in module)
        )

    assert offenders == []


def test_importing_a_facade_never_loads_an_optional_adapter() -> "None":
    """The top-level facade is stdlib-only, so it cannot pull in an extra."""
    import subprocess
    import sys

    code = (
        "import sys\n"
        "import litestar_queues.typing\n"
        "loaded = sorted(m for m in sys.modules if m.split('.')[0] in "
        "{'sqlspec', 'google', 'redis', 'valkey', 'advanced_alchemy'})\n"
        "assert not loaded, loaded\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, timeout=20)

    assert result.returncode == 0, result.stderr.decode()
