import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES_ROOT = ROOT / "examples"

BACKEND_VARIANTS = {
    "memory": {"suffix": "", "backend_markers": ('BACKEND_NAME = "memory"',)},
    "sqlspec": {
        "suffix": "_sqlspec",
        "backend_markers": (
            "SQLSpecBackendConfig",
            "AiosqliteConfig",
            "LITESTAR_QUEUES_EXAMPLE_SQLSPEC_DB",
            "create_schema=False",
            "run_migrations=True",
        ),
    },
    "advanced-alchemy": {
        "suffix": "_advanced_alchemy",
        "backend_markers": (
            "AdvancedAlchemyBackendConfig",
            "SQLAlchemyAsyncConfig",
            "create_all=True",
            "sqlite+aiosqlite",
            "LITESTAR_QUEUES_EXAMPLE_ADVANCED_ALCHEMY_DB",
        ),
    },
    "redis": {
        "suffix": "_redis",
        "backend_markers": (
            "RedisBackendConfig",
            "LITESTAR_QUEUES_EXAMPLE_REDIS_URL",
            'key_prefix="litestar_queues:examples:',
        ),
    },
    "valkey": {
        "suffix": "_valkey",
        "backend_markers": (
            "ValkeyBackendConfig",
            "LITESTAR_QUEUES_EXAMPLE_VALKEY_URL",
            'key_prefix="litestar_queues:examples:',
        ),
    },
}

TRANSPORT_VARIANTS = {
    "websocket": {
        "expected_markers": (
            "connectWebSocket",
            "new WebSocket",
            "wsUrl(",
            "/queues/events/tasks/",
            "/queues/events/custom/",
            "htmx-ext-ws",
        ),
        "forbidden_markers": (
            "connectSse",
            "EventSource",
            "/queues/events/sse",
            "task_sse_url",
            "mission_sse_url",
            "htmx-ext-sse",
            "data-transport-toggle",
        ),
    },
    "sse": {
        "expected_markers": (
            "connectSse",
            "EventSource",
            "/queues/events/sse/tasks/",
            "/queues/events/sse/custom/",
            "htmx-ext-sse",
        ),
        "forbidden_markers": (
            "connectWebSocket",
            "new WebSocket",
            "/queues/events/tasks/",
            "task_ws_url",
            "mission_ws_url",
            "htmx-ext-ws",
            "data-transport-toggle",
        ),
    },
}

EXAMPLE_VARIANTS = {
    f"htmx_realtime_{transport}{backend['suffix']}": {
        "package": f"litestar-queues-htmx-realtime-{transport}-{backend_name}",
        "transport": transport,
        "backend_name": backend_name,
        "backend_markers": backend["backend_markers"],
    }
    for transport in TRANSPORT_VARIANTS
    for backend_name, backend in BACKEND_VARIANTS.items()
}

EXAMPLE_VARIANTS["htmx_realtime_websocket"].update({
    "package": "litestar-queues-htmx-realtime-websocket-memory",
    "backend_markers": ('BACKEND_NAME = "memory"',),
})
EXAMPLE_VARIANTS["htmx_realtime_sse"].update({
    "package": "litestar-queues-htmx-realtime-sse-memory",
    "backend_markers": ('BACKEND_NAME = "memory"',),
})


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


def test_htmx_realtime_examples_keep_simple_queue_and_vite_config() -> None:
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

    memory_app = (EXAMPLES_ROOT / "htmx_realtime_websocket" / "app.py").read_text()
    assert "queue_backend=" not in memory_app
    sse_memory_app = (EXAMPLES_ROOT / "htmx_realtime_sse" / "app.py").read_text()
    assert "queue_backend=" not in sse_memory_app


def test_htmx_realtime_examples_use_transport_specific_frontend_features() -> None:
    for name, config in EXAMPLE_VARIANTS.items():
        example_root = EXAMPLES_ROOT / name
        frontend_source = (example_root / "resources" / "main.ts").read_text()
        script_source = (example_root / "scripts" / "external_publisher.py").read_text()
        assert "from __future__ import annotations" not in script_source
        assert 'from "litestar-vite-plugin/helpers"' in frontend_source
        assert "registerHtmxExtension()" in frontend_source
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

        job_status_source = (example_root / "templates" / "partials" / "job_status.html").read_text()
        package_source = (example_root / "package.json").read_text()
        app_source = (example_root / "app.py").read_text()
        combined_source = f"{frontend_source}\n{template_source}\n{job_status_source}\n{package_source}\n{app_source}"
        transport_config = TRANSPORT_VARIANTS[config["transport"]]
        for marker in transport_config["expected_markers"]:
            assert marker in combined_source, f"{name} missing {marker}"
        for marker in transport_config["forbidden_markers"]:
            assert marker not in combined_source, f"{name} should not include {marker}"


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
        if config["transport"] == "sse":
            assert package["dependencies"]["htmx-ext-sse"] == "^2.2.4"
            assert "htmx-ext-ws" not in package["dependencies"]
        else:
            assert package["dependencies"]["htmx-ext-ws"] == "^2.0.4"
            assert "htmx-ext-sse" not in package["dependencies"]
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
    assert "examples/htmx_realtime_websocket/app.py" in docs_source
    assert "examples/htmx_realtime_websocket/resources/main.ts" in docs_source
    assert "examples/htmx_realtime_websocket/templates/index.html" in docs_source
    assert "examples/htmx_realtime_sse/app.py" in docs_source
    assert "examples/htmx_realtime_sse/resources/main.ts" in docs_source
    assert "examples/htmx_realtime_sse/templates/index.html" in docs_source
    for transport in TRANSPORT_VARIANTS:
        assert f"examples/htmx_realtime_{transport}_sqlspec" in docs_source
        assert f"examples/htmx_realtime_{transport}_advanced_alchemy" in docs_source
        assert f"examples/htmx_realtime_{transport}_redis" in docs_source
        assert f"examples/htmx_realtime_{transport}_valkey" in docs_source
