import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.docs_audit import (  # noqa: E402
    CANONICAL_SQLSPEC_TRANSPORTS,
    SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS,
    capability_matrix_errors,
    retired_transport_errors,
)


def _quickstart_python_block() -> str:
    source = (DOCS / "getting_started" / "quickstart.rst").read_text()
    match = re.search(r"\.\. code-block:: python\n(?:\s+:\w+:.*\n)*\n(?P<body>(?:   .*\n|\n)+)", source)
    assert match is not None
    return "\n".join(line[3:] for line in match.group("body").splitlines())


def test_quickstart_is_complete_and_beginner_focused() -> None:
    block = _quickstart_python_block()
    ast.parse(block)

    for marker in ("@task(", "QueuePlugin", "QueueConfig", "QueueService", ".enqueue(", "Litestar("):
        assert marker in block
    for advanced_topic in ("SQLSpec", "Redis", "Valkey", "CloudRun", "EventDeliveryConfig", "task_modules"):
        assert advanced_topic not in block


def test_configured_navigation_targets_exist() -> None:
    source = (DOCS / "conf.py").read_text()
    tree = ast.parse(source)
    nav_links = next(
        node.value
        for node in tree.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "html_theme_options"
    )
    assert nav_links is not None
    options = ast.literal_eval(nav_links)
    targets = [
        child["url"]
        for group in options["nav_links"]
        for child in group.get("children", ())
        if not child["url"].startswith(("http://", "https://"))
    ]

    assert targets
    for target in targets:
        assert (DOCS / f"{target}.rst").is_file(), target


def test_learning_path_and_canonical_terms_are_exposed() -> None:
    conf = (DOCS / "conf.py").read_text()
    homepage = (DOCS / "index.rst").read_text()
    concepts = (DOCS / "usage" / "concepts.rst").read_text()

    for label in ("Start here", "Concepts", "How-to guides", "Examples", "Reference"):
        assert label in conf
        assert label in homepage
    for term in ("queue backend", "execution backend", "worker wakeup", "task event"):
        assert term in concepts.lower()
    assert "source of truth" in concepts.lower()
    assert "worker discovery" in concepts.lower()


def test_required_documentation_pages_exist() -> None:
    pages = (
        "examples/index.rst",
        "usage/concepts.rst",
        "usage/task-options.rst",
        "usage/results.rst",
        "usage/background-tasks.rst",
        "usage/failures-and-cancellation.rst",
        "usage/maintenance.rst",
        "usage/backends/sqlspec.rst",
        "usage/backends/advanced-alchemy.rst",
        "usage/backends/redis-valkey.rst",
        "usage/event-streams.rst",
        "usage/event-history.rst",
        "usage/event-testing.rst",
        "contributing/documentation.rst",
    )

    for page in pages:
        assert (DOCS / page).is_file(), page


def test_event_config_modules_have_one_canonical_reference_location() -> None:
    """Each event configuration module is exposed by one Sphinx automodule directive."""
    reference_source = "\n".join(path.read_text() for path in sorted((DOCS / "reference").glob("*.rst")))
    for module in (
        "litestar_queues.events.config",
        "litestar_queues.events.history",
        "litestar_queues.events.stream_config",
    ):
        assert reference_source.count(f".. automodule:: {module}\n") == 1


def test_transport_capability_matrix_matches_runtime_sources() -> None:
    from litestar_queues.backends.advanced_alchemy._notifications import SUPPORTED_NOTIFY_DRIVERS
    from litestar_queues.backends.sqlspec.backend import _adapter_wakeup_transport
    from litestar_queues.backends.sqlspec.config import WAKEUP_TRANSPORTS

    assert frozenset(WAKEUP_TRANSPORTS) - {"polling"} == CANONICAL_SQLSPEC_TRANSPORTS
    assert frozenset({"postgresql+asyncpg", "postgresql+psycopg"}) == SUPPORTED_NOTIFY_DRIVERS
    assert {
        adapter: _adapter_wakeup_transport(adapter) for adapter in SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS
    } == SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS
    assert capability_matrix_errors(DOCS) == []


def test_retired_transport_names_are_limited_to_migration_paragraph(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    usage = docs / "usage"
    usage.mkdir(parents=True)
    migration = usage / "migration.rst"
    migration.write_text(
        """
.. retired-sqlspec-transport-names-start

Map ``listen_notify`` to ``notify``, ``listen_notify_durable`` to
``notify_queue``, and ``table_queue`` to ``poll_queue``.

.. retired-sqlspec-transport-names-end
""",
        encoding="utf-8",
    )
    page = usage / "worker-wakeups.rst"
    page.write_text("Canonical transport names only.\n", encoding="utf-8")

    assert retired_transport_errors(docs) == []

    for term in ("listen_notify", "listen_notify_durable", "table_queue"):
        page.write_text(f"Retired outside migration: ``{term}``.\n", encoding="utf-8")
        errors = retired_transport_errors(docs)
        assert len(errors) == 1
        assert term in errors[0]
        page.write_text("Canonical transport names only.\n", encoding="utf-8")

        migration.write_text(
            migration.read_text(encoding="utf-8") + f"\nRetired outside the allowed paragraph: ``{term}``.\n",
            encoding="utf-8",
        )
        errors = retired_transport_errors(docs)
        assert len(errors) == 1
        assert term in errors[0]
        migration.write_text(migration.read_text(encoding="utf-8").rsplit("\n", 2)[0] + "\n", encoding="utf-8")
