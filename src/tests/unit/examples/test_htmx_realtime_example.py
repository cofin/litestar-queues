import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES_ROOT = ROOT / "examples"

EXAMPLE_VARIANTS = {
    "htmx_realtime": {
        "package": "litestar-queues-htmx-realtime-memory",
        "backend_markers": ('BACKEND_NAME = "memory"',),
    },
    "htmx_realtime_sqlspec": {
        "package": "litestar-queues-htmx-realtime-sqlspec",
        "backend_markers": (
            "SQLSpecBackendConfig",
            "AiosqliteConfig",
            "LITESTAR_QUEUES_EXAMPLE_SQLSPEC_DB",
            "create_schema=False",
            "run_migrations=True",
        ),
    },
    "htmx_realtime_advanced_alchemy": {
        "package": "litestar-queues-htmx-realtime-advanced-alchemy",
        "backend_markers": (
            "AdvancedAlchemyBackendConfig",
            "SQLAlchemyAsyncConfig",
            "create_all=True",
            "sqlite+aiosqlite",
            "LITESTAR_QUEUES_EXAMPLE_ADVANCED_ALCHEMY_DB",
        ),
    },
    "htmx_realtime_redis": {
        "package": "litestar-queues-htmx-realtime-redis",
        "backend_markers": (
            "RedisBackendConfig",
            "LITESTAR_QUEUES_EXAMPLE_REDIS_URL",
            "litestar_queues:examples:htmx_realtime_redis",
        ),
    },
    "htmx_realtime_valkey": {
        "package": "litestar-queues-htmx-realtime-valkey",
        "backend_markers": (
            "ValkeyBackendConfig",
            "LITESTAR_QUEUES_EXAMPLE_VALKEY_URL",
            "litestar_queues:examples:htmx_realtime_valkey",
        ),
    },
}


def test_htmx_realtime_example_variants_have_expected_files() -> None:
    expected_files = {
        "app.py",
        "README.md",
        "package.json",
        "vite.config.ts",
        "resources/main.ts",
        "resources/styles.css",
        "templates/base.html",
        "templates/index.html",
        "templates/partials/job_status.html",
        "scripts/external_publisher.py",
    }

    for name in EXAMPLE_VARIANTS:
        example_root = EXAMPLES_ROOT / name
        for relative_path in expected_files:
            assert (example_root / relative_path).is_file(), f"{name}/{relative_path}"


def test_htmx_realtime_example_variants_keep_simple_queue_and_vite_config() -> None:
    for name, config in EXAMPLE_VARIANTS.items():
        app_source = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert "HTMXPlugin()" in app_source
        assert 'mode="htmx"' in app_source
        assert 'PathConfig(root=EXAMPLE_ROOT, resource_dir="resources")' in app_source
        assert 'signature_namespace={"NamedDependency": NamedDependency}' in app_source
        assert "RuntimeConfig(" not in app_source
        assert 'executor="bun"' not in app_source
        assert "sync_to_thread=False" not in app_source
        assert "from __future__ import annotations" not in app_source
        assert "ruff: noqa" not in app_source
        assert "# noqa: TC002" not in app_source
        assert "execution_backend=" not in app_source
        assert "in_app_worker=" not in app_source
        assert "worker_poll_interval=" not in app_source
        assert 'buffer=EventBufferConfig(buffer_size=8, flush_interval=0.2, overflow="drop_oldest")' in app_source
        assert "EventStreamConfig(" in app_source
        assert 'scopes={"task", "custom"}' in app_source
        assert "status_json" in app_source
        assert "HTMXTemplate(" in app_source
        assert "trigger_event=" in app_source

        for marker in config["backend_markers"]:
            assert marker in app_source

    memory_app = (EXAMPLES_ROOT / "htmx_realtime" / "app.py").read_text()
    assert "queue_backend=" not in memory_app


def test_htmx_realtime_examples_use_litestar_vite_htmx_features() -> None:
    for name in EXAMPLE_VARIANTS:
        example_root = EXAMPLES_ROOT / name
        frontend_source = (example_root / "resources" / "main.ts").read_text()
        script_source = (example_root / "scripts" / "external_publisher.py").read_text()
        assert "from __future__ import annotations" not in script_source
        assert 'from "litestar-vite-plugin/helpers"' in frontend_source
        assert "registerHtmxExtension()" in frontend_source
        assert "connectWebSocket" in frontend_source
        assert "connectSse" in frontend_source
        assert "task.completed" in frontend_source
        assert '"ping"' in frontend_source
        assert "queue-demo:started" in frontend_source
        assert "demo:mission-control" in frontend_source

        template_source = (example_root / "templates" / "index.html").read_text()
        assert 'hx-swap="json"' in template_source
        assert 'ls-if="backend"' in template_source
        assert 'hx-disabled-elt="find button"' in template_source
        assert 'hx-sync="this:replace"' in template_source
        assert "transition:true" in template_source
        assert 'hx-indicator="#launch-indicator"' in template_source
        assert "hx-on:htmx:after-request" in template_source


def test_htmx_realtime_examples_use_litestar_asset_commands_and_current_packages() -> None:
    for name, config in EXAMPLE_VARIANTS.items():
        example_root = EXAMPLES_ROOT / name
        readme_source = (example_root / "README.md").read_text()
        assert "uv run litestar assets install" in readme_source
        assert "npm install" not in readme_source
        assert "bun install" not in readme_source

        package = json.loads((example_root / "package.json").read_text())
        assert package["name"] == config["package"]
        assert package["dependencies"]["htmx.org"] == "^2.0.10"
        assert package["dependencies"]["htmx-ext-sse"] == "^2.2.4"
        assert package["dependencies"]["htmx-ext-ws"] == "^2.0.4"
        assert package["devDependencies"]["litestar-vite-plugin"] == "^0.26.1"
        assert package["devDependencies"]["vite"] == "^8.1.3"
        assert package["devDependencies"]["typescript"] == "^6.0.3"

    gitignore_source = (ROOT / ".gitignore").read_text()
    assert "examples/**/node_modules/" in gitignore_source
    assert "examples/**/package-lock.json" in gitignore_source
    assert "examples/**/public/" in gitignore_source
    assert "examples/**/hot" in gitignore_source


def test_htmx_realtime_docs_import_from_runnable_example() -> None:
    docs_source = (ROOT / "docs" / "usage" / "events.rst").read_text()
    assert "examples/htmx_realtime/app.py" in docs_source
    assert "examples/htmx_realtime/resources/main.ts" in docs_source
    assert "examples/htmx_realtime/templates/index.html" in docs_source
    assert "examples/htmx_realtime_sqlspec" in docs_source
    assert "examples/htmx_realtime_advanced_alchemy" in docs_source
    assert "examples/htmx_realtime_redis" in docs_source
    assert "examples/htmx_realtime_valkey" in docs_source
