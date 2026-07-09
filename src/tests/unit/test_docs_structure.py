import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"


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
    for advanced_topic in ("SQLSpec", "Redis", "Valkey", "CloudRun", "EventConfig", "task_modules"):
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
        "usage/worker-wakeups.rst",
        "usage/worker-recovery.rst",
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
