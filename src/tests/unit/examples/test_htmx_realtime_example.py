import json
import subprocess
import sys
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES_ROOT = ROOT / "examples"
SHARED_ROOT = EXAMPLES_ROOT / "shared"

# All ten examples use the same simple frontend and declarative htmx stream
# connection; only the queue backend and transport vary.
BACKEND_VARIANTS = {
    "memory": {"suffix": "", "backend_markers": ('BACKEND_NAME = "memory"',)},
    "sqlspec": {
        "suffix": "_sqlspec",
        "backend_markers": (
            "SQLSpecBackendConfig",
            "AiosqliteConfig",
            'QUEUE_DB_PATH = EXAMPLE_ROOT / "queue-sqlspec.db"',
        ),
    },
    "advanced-alchemy": {
        "suffix": "_advanced_alchemy",
        "backend_markers": (
            "SQLAlchemyBackendConfig",
            "SQLAlchemyAsyncConfig",
            "create_all=True",
            "sqlite+aiosqlite",
            'QUEUE_DB_PATH = EXAMPLE_ROOT / "queue-advanced-alchemy.db"',
        ),
    },
    "redis": {
        "suffix": "_redis",
        "backend_markers": ("RedisBackendConfig", "LITESTAR_QUEUES_EXAMPLE_REDIS_URL", "REDIS_KEY_PREFIX"),
    },
    "valkey": {
        "suffix": "_valkey",
        "backend_markers": ("ValkeyBackendConfig", "LITESTAR_QUEUES_EXAMPLE_VALKEY_URL", "VALKEY_KEY_PREFIX"),
    },
}

# Transport-specific markers: the connection is a declarative htmx extension
# element and the JS adapter cancels the default swap.
TRANSPORT_MARKERS = {
    "websocket": {
        "expected_markers": (
            "htmx-ext-ws",
            'hx-ext="ws"',
            "ws-connect",
            "/queues/events/tasks/",
            "htmx:wsBeforeMessage",
        ),
        "forbidden_markers": (
            "htmx-ext-sse",
            "sse-connect",
            "/queues/events/sse",
            "EventSource",
            "htmx:sseBeforeMessage",
            "task_sse_url",
        ),
    },
    "sse": {
        "expected_markers": (
            "htmx-ext-sse",
            'hx-ext="sse"',
            "sse-connect",
            "/queues/events/sse/tasks/",
            "htmx:sseBeforeMessage",
        ),
        "forbidden_markers": ("htmx-ext-ws", "ws-connect", "htmx:wsBeforeMessage", "task_ws_url", "new WebSocket"),
    },
}

EXAMPLE_VARIANTS = {
    f"htmx_realtime_{transport}{backend['suffix']}": {
        "package": f"litestar-queues-htmx-realtime-{transport}-{backend_name}",
        "transport": transport,
        "backend_name": backend_name,
        "backend_markers": backend["backend_markers"],
    }
    for transport in TRANSPORT_MARKERS
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


def test_each_example_owns_only_its_application_and_readme() -> None:
    """An example is its ``app.py`` and its README; the frontend is shared.

    Every asset below was byte-identical across all ten examples, or varied
    only by transport, so each copy was one more place to forget an edit.
    """
    for name in EXAMPLE_VARIANTS:
        example_root = EXAMPLES_ROOT / name
        owned = {path.name for path in example_root.iterdir() if path.is_file()}

        assert owned == {"app.py", "README.md", "__init__.py"}, name


def test_the_shared_frontend_holds_one_copy_of_every_asset() -> None:
    shared_files = {
        "package.json",
        "vite.config.ts",
        "resources/main-sse.ts",
        "resources/main-ws.ts",
        "resources/styles.css",
        "resources/vite-env.d.ts",
        "templates/base.html",
        "templates/index.html",
        "templates/partials/stream_mount_sse.html",
        "templates/partials/stream_mount_ws.html",
        "scripts/external_publisher.py",
    }

    for relative_path in shared_files:
        assert (SHARED_ROOT / relative_path).is_file(), relative_path


def test_htmx_realtime_examples_keep_simple_queue_and_vite_config() -> None:
    for name, config in EXAMPLE_VARIANTS.items():
        app_source = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert "HTMXPlugin()" in app_source
        assert 'mode="htmx"' in app_source
        assert 'PathConfig(root=SHARED_ROOT, resource_dir="resources")' in app_source
        assert 'signature_namespace={"NamedDependency": NamedDependency}' in app_source
        assert "RuntimeConfig(" not in app_source
        assert 'executor="bun"' not in app_source
        assert "sync_to_thread=False" not in app_source
        assert "from __future__ import annotations" not in app_source
        assert "ruff: noqa" not in app_source
        assert "# noqa: TC002" not in app_source
        assert "execution_backend=" not in app_source
        assert "in_app_" + "worker=" not in app_source
        assert "worker_" + "poll_interval=" not in app_source
        assert "buffer=EventBufferConfig(batch_size=8, flush_interval=0.2)" in app_source
        assert "EventStreamConfig(" in app_source
        assert "status_json" in app_source
        assert "HTMXTemplate(" in app_source
        assert "trigger_event=" in app_source
        assert 'scopes={"task"}' in app_source
        assert "mission-control" not in app_source
        assert "MISSION_CONTROL" not in app_source
        assert "publish_mission_control" not in app_source

        # Heartbeat timestamps are automatic and interval-driven; the canonical
        # task never calls ctx.beat() or duplicates progress through a generic
        # "task.event". A single demonstrative domain event is allowed only at
        # one real transition, and the task does not publish its own "finished"
        # log/event -- completion is automatic (task.completed).
        assert "ctx.beat(" not in app_source
        assert 'ctx.event("task.event"' not in app_source
        assert "crawl.page_discovered" in app_source
        assert app_source.count("publish_task_log(") == 1

        for marker in config["backend_markers"]:
            assert marker in app_source

    # The single-process demos must be explicit about being single-process: a
    # process-local queue and a process-local Channels backend only work when
    # the worker shares the process, which the server-owned default does not.
    for name in ("htmx_realtime_websocket", "htmx_realtime_sse"):
        single_process_app = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert 'queue_backend="memory"' in single_process_app, name
        assert 'placement="asgi"' in single_process_app, name


def test_realtime_examples_and_guides_lock_task_context_lifecycle_semantics() -> None:
    examples_readme = (EXAMPLES_ROOT / "README.md").read_text()
    events_guide = (ROOT / "docs" / "usage" / "events.rst").read_text()
    tasks_guide = (ROOT / "docs" / "usage" / "tasks.rst").read_text()

    for name in EXAMPLE_VARIANTS:
        app_source = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert "ctx.beat(" not in app_source, name
        assert (
            'ctx.progress(current=step, total=DEMO_STEPS, message=message, payload={"line": message})' in app_source
        ), name
        assert '"crawl.page_discovered",' in app_source, name
        assert 'payload={"url": DISCOVERED_PAGE_URL},' in app_source, name
        assert 'return {"task_id": ctx.task_id, "status": "complete"}' in app_source, name
        assert 'ctx.event("task.completed"' not in app_source, name
        assert 'ctx.event("task.failed"' not in app_source, name

    for marker in (
        "Automatic heartbeats keep active jobs live",
        "ctx.beat(detail)",
        "ctx.progress(current=..., total=..., message=..., payload=...)",
        "ctx.event(name, message=..., payload=..., immediate=False)",
        "Returning completes the task",
        "Raising follows retry policy",
    ):
        assert marker in examples_readme
    for marker in (
        "ctx.progress(",
        "current=3,",
        "total=6,",
        'message="3/6 pages",',
        'payload={"page": 3},',
        "ctx.event(",
        '"crawl.page_discovered",',
        "immediate=False,",
    ):
        assert marker in events_guide
    assert "does not update progress or terminal task state" in " ".join(events_guide.split())
    assert "A return value becomes the completed result" in tasks_guide
    assert "A raised exception follows the configured retry policy" in tasks_guide


def test_htmx_realtime_examples_keep_live_channels_process_local_by_default() -> None:
    for name in EXAMPLE_VARIANTS:
        app_source = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert "MemoryChannelsBackend" in app_source, name
        assert "ChannelsPlugin(" in app_source, name


def test_redis_and_valkey_examples_offer_explicit_shared_channels_mode() -> None:
    common_source = (EXAMPLES_ROOT / "htmx_realtime_common.py").read_text()
    assert 'os.getenv("LITESTAR_QUEUES_EXAMPLE_PLACEMENT", "asgi")' in common_source
    assert '{"server", "asgi", "external"}' in common_source
    assert "graceful_shutdown_timeout=5" in common_source

    for name in EXAMPLE_VARIANTS:
        if str(EXAMPLE_VARIANTS[name]["backend_name"]) not in {"redis", "valkey"}:
            continue
        app_source = (EXAMPLES_ROOT / name / "app.py").read_text()
        assert "LITESTAR_QUEUES_EXAMPLE_SHARED_CHANNELS" in app_source, name
        assert "example_worker_config" in app_source, name
        assert "RedisChannelsStreamBackend" in app_source, name
        assert "decode_responses=False" in app_source, name
        assert "CHANNELS_KEY_PREFIX" in app_source, name
        if str(EXAMPLE_VARIANTS[name]["backend_name"]) == "valkey":
            assert "from redis" not in app_source, name


def _assert_canonical_frontend(example_root: Path, transport: str) -> str:
    entry = "main-ws.ts" if transport == "websocket" else "main-sse.ts"
    frontend_source = (SHARED_ROOT / "resources" / entry).read_text()
    assert 'from "litestar-vite-plugin/helpers"' in frontend_source
    assert 'import htmx from "htmx.org"' in frontend_source
    assert "registerHtmxExtension()" in frontend_source
    assert "window as unknown" in frontend_source
    assert "task.completed" in frontend_source
    assert '"ping"' in frontend_source
    assert "queue-demo:started" in frontend_source
    assert "mission-control" not in frontend_source

    template_source = (SHARED_ROOT / "templates" / "index.html").read_text()
    assert 'hx-swap="json"' in template_source
    assert 'ls-if="backend"' in template_source
    assert 'hx-post="/demo/restart"' in template_source
    assert 'hx-target="#stream-mount"' in template_source
    assert 'hx-disabled-elt="this"' in template_source
    assert 'hx-sync="this:replace"' in template_source
    assert "<form" not in template_source

    mount = "stream_mount_ws.html" if transport == "websocket" else "stream_mount_sse.html"
    partial_source = (SHARED_ROOT / "templates" / "partials" / mount).read_text()
    app_source = (example_root / "app.py").read_text()
    # package.json is deliberately excluded: one shared manifest carries both
    # htmx transport extensions, so it cannot satisfy a per-transport check.
    return f"{frontend_source}\n{template_source}\n{partial_source}\n{app_source}"


def test_htmx_realtime_examples_use_transport_specific_frontend_features() -> None:
    for name, config in EXAMPLE_VARIANTS.items():
        example_root = EXAMPLES_ROOT / name
        combined_source = _assert_canonical_frontend(example_root, str(config["transport"]))
        transport_config = cast("dict[str, tuple[str, ...]]", TRANSPORT_MARKERS[str(config["transport"])])

        for marker in transport_config["expected_markers"]:
            assert marker in combined_source, f"{name} missing {marker}"
        for marker in transport_config["forbidden_markers"]:
            assert marker not in combined_source, f"{name} should not include {marker}"


def test_htmx_realtime_examples_use_litestar_asset_commands_and_current_packages() -> None:
    for name in EXAMPLE_VARIANTS:
        example_root = EXAMPLES_ROOT / name
        readme_source = (example_root / "README.md").read_text()
        assert "uv run litestar assets install" in readme_source
        assert "uv run litestar assets build" in readme_source
        assert "npm install" not in readme_source
        assert "bun install" not in readme_source

    # One manifest, one install. It carries both htmx transport extensions
    # because the single bundle has an entry point for each.
    package = json.loads((SHARED_ROOT / "package.json").read_text())
    assert package["name"] == "litestar-queues-htmx-realtime-examples"
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


def test_htmx_realtime_examples_discover_routes_and_active_vite_integration() -> None:
    forbidden_output = (
        "Queue event streams have no configured authorization",
        "VitePlugin inert; skipping asset and route wiring",
    )

    for name, config in EXAMPLE_VARIANTS.items():
        app_path = f"examples.{name}.app:app"
        commands = (("routes",), ("assets", "status"))
        for command in commands:
            completed = subprocess.run(
                (sys.executable, "-m", "litestar", f"--app={app_path}", *command),
                cwd=ROOT,
                capture_output=True,
                check=False,
                text=True,
                timeout=30,
            )
            output = f"{completed.stdout}\n{completed.stderr}"
            assert completed.returncode == 0, f"{name} {' '.join(command)} failed:\n{output}"
            assert all(message not in output for message in forbidden_output), output

            if command == ("routes",):
                expected_route = (
                    "/queues/events/sse/tasks/" if config["transport"] == "sse" else "/queues/events/tasks/"
                )
                assert expected_route in output, output
            else:
                assert "Vite Integration Status" in output, output
                assert "Assets URL: /static/" in output, output


def test_htmx_realtime_docs_import_from_runnable_example() -> None:
    docs_source = (ROOT / "docs" / "usage" / "event-streams.rst").read_text()
    assert "examples/htmx_realtime_websocket/app.py" in docs_source
    assert "examples/shared/resources/main-ws.ts" in docs_source
    assert "examples/shared/templates/partials/stream_mount_ws.html" in docs_source
    assert "examples/htmx_realtime_sse/app.py" in docs_source
    assert "examples/shared/resources/main-sse.ts" in docs_source
    assert "examples/shared/templates/partials/stream_mount_sse.html" in docs_source
    assert "examples/shared/templates/index.html" in docs_source
    for transport in TRANSPORT_MARKERS:
        assert f"examples/htmx_realtime_{transport}_sqlspec" in docs_source
        assert f"examples/htmx_realtime_{transport}_advanced_alchemy" in docs_source
        assert f"examples/htmx_realtime_{transport}_redis" in docs_source
        assert f"examples/htmx_realtime_{transport}_valkey" in docs_source
