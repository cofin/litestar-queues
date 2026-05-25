"""Tests for the ``discover_tasks`` walker."""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio

_FIXTURE_PACKAGE = "tests.support.discover_tasks_pkg"
_EXPECTED_TASKS = (
    "discover.bar.notify",
    "discover.baz.inner.run",
    "discover.foo.send",
)


def _drop_fixture_modules() -> None:
    """Evict the fixture tree from ``sys.modules`` so reload semantics are testable."""
    for module_name in list(sys.modules):
        if module_name == _FIXTURE_PACKAGE or module_name.startswith(f"{_FIXTURE_PACKAGE}."):
            del sys.modules[module_name]


@pytest.fixture(autouse=True)
def _clean_discover_state() -> None:
    """Force a clean slate between tests; ``clean_task_registry`` already clears _task_registry."""
    _drop_fixture_modules()


def test_discover_tasks_walks_jobs_subpackages_at_every_depth() -> None:
    """``discover_tasks`` imports shallow, sibling, and deeply-nested ``.jobs.`` modules."""
    from litestar_queues import discover_tasks, get_task_registry

    discovered = discover_tasks(_FIXTURE_PACKAGE)

    for task_name in _EXPECTED_TASKS:
        assert task_name in discovered, f"{task_name!r} missing from walker output: {discovered!r}"
        assert task_name in get_task_registry()


def test_discover_tasks_returns_sorted_deduplicated_names() -> None:
    """The return tuple is sorted, deduplicated, and idempotent across repeated calls."""
    from litestar_queues import discover_tasks

    first = discover_tasks(_FIXTURE_PACKAGE)
    second = discover_tasks(_FIXTURE_PACKAGE)

    assert first == tuple(sorted(set(first)))
    assert first == second


def test_discover_tasks_respects_subpackage_argument() -> None:
    """When ``subpackage`` matches no modules, the walker returns the empty tuple."""
    from litestar_queues import discover_tasks

    discovered = discover_tasks(_FIXTURE_PACKAGE, subpackage="not-a-real-subpackage")

    assert discovered == ()


def test_discover_tasks_force_reload_reimports() -> None:
    """``force_reload=True`` re-imports modules even when already in ``sys.modules``."""
    from litestar_queues import discover_tasks
    from litestar_queues.task import clear_task_registry

    discover_tasks(_FIXTURE_PACKAGE)
    clear_task_registry()

    discovered = discover_tasks(_FIXTURE_PACKAGE, force_reload=True)

    for task_name in _EXPECTED_TASKS:
        assert task_name in discovered


def test_discover_tasks_rejects_non_package_module() -> None:
    """Passing a plain module instead of a package raises ``ModuleNotFoundError``."""
    from litestar_queues import discover_tasks

    with pytest.raises(ModuleNotFoundError):
        discover_tasks(f"{_FIXTURE_PACKAGE}.foo.jobs.send")


def test_discover_tasks_skips_non_jobs_siblings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Modules outside any ``.jobs.`` subpackage are not imported by the walker."""
    from litestar_queues import discover_tasks

    pkg_root = tmp_path / "filter_pkg"
    (pkg_root / "things").mkdir(parents=True)
    (pkg_root / "things" / "jobs").mkdir(parents=True)
    (pkg_root / "things" / "helpers").mkdir(parents=True)

    (pkg_root / "__init__.py").write_text("")
    (pkg_root / "things" / "__init__.py").write_text("")
    (pkg_root / "things" / "jobs" / "__init__.py").write_text("")
    (pkg_root / "things" / "jobs" / "real.py").write_text(
        "from litestar_queues import task\n\n@task('filter.real')\nasync def real(): return 'real'\n"
    )
    (pkg_root / "things" / "helpers" / "__init__.py").write_text("")
    (pkg_root / "things" / "helpers" / "side.py").write_text(
        "raise RuntimeError('should not be imported by discover_tasks')\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))

    discovered = discover_tasks("filter_pkg")

    assert "filter.real" in discovered
    assert "filter_pkg.things.helpers.side" not in sys.modules
