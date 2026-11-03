"""Structural and contract verification tests for the Markdown documentation suite."""

import ast
import re
import tomllib
from pathlib import Path
from typing import Any

import pytest

from litestar_queues.backends.advanced_alchemy._notifications import SUPPORTED_NOTIFY_DRIVERS
from litestar_queues.backends.sqlspec.backend import _adapter_wakeup_transport
from litestar_queues.backends.sqlspec.config import WAKEUP_TRANSPORTS

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
INCLUDES = ROOT / "tools" / "docs" / "includes"
ZENSICAL_TOML = ROOT / "zensical.toml"

CANONICAL_SQLSPEC_TRANSPORTS: frozenset[str] = frozenset({"aq", "notify", "notify_queue", "poll_queue", "txeventq"})
SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS: dict[str, str] = {
    "asyncpg": "notify_queue",
    "psycopg": "notify_queue",
    "psqlpy": "notify_queue",
    "duckdb": "poll_queue",
    "aiosqlite": "polling",
    "asyncmy": "polling",
    "oracledb": "polling",
}
RETIRED_TRANSPORT_TERMS: tuple[str, ...] = ("listen_notify", "listen_notify_durable", "table_queue")

REQUIRED_DOCS_PAGES: tuple[str, ...] = (
    "index.md",
    "getting-started/index.md",
    "getting-started/installation.md",
    "getting-started/quickstart.md",
    "getting-started/concepts.md",
    "learn/index.md",
    "learn/tasks.md",
    "learn/task-options.md",
    "learn/results.md",
    "learn/failures-and-cancellation.md",
    "learn/schedules.md",
    "learn/workers.md",
    "learn/dependency-injection.md",
    "learn/background-responses.md",
    "learn/configuration.md",
    "learn/runtime-namespace.md",
    "learn/testing.md",
    "backends/index.md",
    "backends/sqlspec.md",
    "backends/advanced-alchemy.md",
    "backends/redis-valkey.md",
    "backends/migration.md",
    "execution/index.md",
    "execution/cloud-tasks.md",
    "execution/cloud-run.md",
    "execution/broker-transports.md",
    "execution/pubsub.md",
    "execution/sqs.md",
    "execution/rabbitmq.md",
    "execution/kafka.md",
    "execution/maintenance.md",
    "execution/cli.md",
    "events/index.md",
    "events/streams.md",
    "events/history.md",
    "events/history-extending.md",
    "events/standalone.md",
    "events/testing.md",
    "events/observability.md",
    "architecture/index.md",
    "architecture/examples.md",
    "reference/index.md",
    "reference/plugin-and-config.md",
    "reference/tasks-and-models.md",
    "reference/backends.md",
    "reference/execution-and-workers.md",
    "reference/events.md",
    "reference/observability.md",
    "reference/cli.md",
    "reference/contributing.md",
    "changelog.md",
)

REQUIRED_DIAGRAM_INCLUDES: tuple[str, ...] = (
    "diagram-storage-vs-execution.html",
    "diagram-worker-placements.html",
    "diagram-task-lifecycle.html",
    "diagram-serverless-scale-to-zero.html",
    "diagram-events-and-streams.html",
    "diagram-wakeup-transports.html",
)


def _collect_nav_paths(entries: list[Any]) -> list[str]:
    paths: list[str] = []
    for entry in entries:
        if isinstance(entry, str):
            paths.append(entry)
        elif isinstance(entry, dict):
            for value in entry.values():
                if isinstance(value, str):
                    paths.append(value)
                elif isinstance(value, list):
                    paths.extend(_collect_nav_paths(value))
    return paths


def test_quickstart_is_complete_and_beginner_focused() -> None:
    """The quickstart example parses, covers the core workflow, and matches README.md."""
    block = (DOCS / "examples" / "getting_started" / "quickstart_app.py").read_text(encoding="utf-8")
    quickstart_page = (DOCS / "getting-started" / "quickstart.md").read_text(encoding="utf-8")
    readme_source = (ROOT / "README.md").read_text(encoding="utf-8")
    ast.parse(block)

    assert '--8<-- "docs/examples/getting_started/quickstart_app.py"' in quickstart_page
    for marker in ("@task(", "QueuePlugin", "QueueConfig", "QueueService", ".enqueue(", "Litestar("):
        assert marker in block
        assert marker in readme_source
    for advanced_topic in ("SQLSpec", "Redis", "Valkey", "CloudRun", "EventDeliveryConfig", "task_modules"):
        assert advanced_topic not in block


def test_configured_navigation_targets_exist() -> None:
    """Every navigation entry in zensical.toml points to an existing Markdown page and covers all docs."""
    config = tomllib.loads(ZENSICAL_TOML.read_text(encoding="utf-8"))
    nav_targets = _collect_nav_paths(config["project"]["nav"])
    assert nav_targets

    for target in nav_targets:
        assert (DOCS / target).is_file(), target

    actual_md_files = {path.relative_to(DOCS).as_posix() for path in DOCS.rglob("*.md")}
    assert actual_md_files == set(nav_targets)


def test_learning_path_and_canonical_terms_are_exposed() -> None:
    """Top-level navigation and the concepts guide expose the canonical architectural vocabulary."""
    config_text = ZENSICAL_TOML.read_text(encoding="utf-8")
    homepage = (DOCS / "index.md").read_text(encoding="utf-8")
    concepts = (DOCS / "getting-started" / "concepts.md").read_text(encoding="utf-8")

    for label in ("Getting Started", "Core Concepts", "Storage Backends", "Architecture", "Reference"):
        assert label in config_text
    assert "Quickstart" in homepage
    for term in ("queue backend", "execution backend", "worker wakeup", "task event"):
        assert term in concepts.lower()
    assert "source of truth" in concepts.lower()
    assert "worker discovery" in concepts.lower()


@pytest.mark.parametrize("page", REQUIRED_DOCS_PAGES)
def test_required_documentation_pages_exist(page: str) -> None:
    """Every required Markdown guide and reference page exists on disk."""
    assert (DOCS / page).is_file(), page


@pytest.mark.parametrize("diagram_name", REQUIRED_DIAGRAM_INCLUDES)
def test_interactive_diagrams_are_embedded_in_guides(diagram_name: str) -> None:
    """Every interactive HTML+SVG architecture diagram exists and is embedded in the guides."""
    diagram_path = INCLUDES / diagram_name
    assert diagram_path.is_file(), diagram_name
    corpus = "\n".join(path.read_text(encoding="utf-8") for path in sorted(DOCS.rglob("*.md")))
    assert f'--8<-- "tools/docs/includes/{diagram_name}"' in corpus


def test_event_config_modules_have_one_canonical_reference_location() -> None:
    """Each event configuration module is referenced once across the reference pages."""
    reference_source = "\n".join(path.read_text(encoding="utf-8") for path in sorted((DOCS / "reference").glob("*.md")))
    for module in (
        "litestar_queues.events.config",
        "litestar_queues.events.history",
        "litestar_queues.events.stream_config",
    ):
        assert reference_source.count(module) == 1


def test_transport_capability_matrix_matches_runtime_sources() -> None:
    """The storage backends wakeup capability matrix matches runtime wakeup transport constants."""
    assert frozenset(WAKEUP_TRANSPORTS) - {"polling"} == CANONICAL_SQLSPEC_TRANSPORTS
    assert frozenset({"postgresql+asyncpg", "postgresql+psycopg"}) == SUPPORTED_NOTIFY_DRIVERS
    assert {
        adapter: _adapter_wakeup_transport(adapter) for adapter in SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS
    } == SQLSPEC_DEFAULT_WAKEUP_TRANSPORTS

    backends_index = (DOCS / "backends" / "index.md").read_text(encoding="utf-8")
    for transport in ("notify_queue", "poll_queue", "aq", "txeventq", "postgres-listen-notify"):
        assert transport in backends_index


@pytest.mark.parametrize("retired_term", RETIRED_TRANSPORT_TERMS)
def test_no_retired_transport_names_in_public_docs(retired_term: str) -> None:
    """Retired pre-release SQLSpec transport names do not appear anywhere in the public docs."""
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(retired_term)}(?![A-Za-z0-9_])")
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in sorted(DOCS.rglob("*.md"))
        if pattern.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_task_dependency_docs_cover_both_hooks() -> None:
    """The dependency injection guide covers both task dependency hooks and their lifecycle rules."""
    page = (DOCS / "learn" / "dependency-injection.md").read_text(encoding="utf-8")

    for marker in (
        "task_dependency_provider",
        "task_dependency_resolver",
        "QueueConfigurationError",
        "asyncio.CancelledError",
        "exactly once",
    ):
        assert marker in page
    assert "signature_namespace" not in page
    assert "Dishka" not in page


def test_scale_to_zero_and_runtime_hardening_docs_coverage() -> None:
    """Serverless execution, maintenance, stream rollover, and v0.14.0 features are documented."""
    cloud_tasks = (DOCS / "execution" / "cloud-tasks.md").read_text(encoding="utf-8")
    cloud_run = (DOCS / "execution" / "cloud-run.md").read_text(encoding="utf-8")
    maintenance = (DOCS / "execution" / "maintenance.md").read_text(encoding="utf-8")
    event_streams = (DOCS / "events" / "streams.md").read_text(encoding="utf-8")
    events = (DOCS / "events" / "index.md").read_text(encoding="utf-8")
    workers = (DOCS / "learn" / "workers.md").read_text(encoding="utf-8")
    sqlspec_guide = (DOCS / "backends" / "sqlspec.md").read_text(encoding="utf-8")
    redis_guide = (DOCS / "backends" / "redis-valkey.md").read_text(encoding="utf-8")

    for marker in (
        "mount_route",
        "verify_oidc",
        "CloudTasksOIDCGuard",
        "execution_backends",
        "--cpu-throttling",
        "dispatch_deadline",
    ):
        assert marker in cloud_tasks, f"missing {marker!r} in cloud-tasks.md"

    for marker in ("schedules_on_enqueue", "execution_backends"):
        assert marker in cloud_run, f"missing {marker!r} in cloud-run.md"

    for marker in ("expire", "external", "stale", "schedules", "terminal", "events", "mount_route"):
        assert marker in maintenance, f"missing {marker!r} in maintenance.md"

    for marker in ("max_stream_duration", "retry_ms"):
        assert marker in event_streams, f"missing {marker!r} in streams.md"

    assert "strict" in events
    for marker in ("run_app_lifespan", "litestar_queues.cli", "terminal_retention", "terminal_check_interval"):
        assert marker in workers, f"missing {marker!r} in workers.md"

    for marker in ("terminal_ttl", "adapter_name", "queue_store_class", "events_store_class"):
        assert marker in sqlspec_guide, f"missing {marker!r} in sqlspec.md"

    assert "terminal_ttl" in redis_guide
