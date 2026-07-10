"""Cross-process queue and Channels topology tests."""

import os
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from .server_manager import ExampleServer, QueueWorker
from .test_realtime_browser import (
    _assert_clean_browser,
    _assert_task_event,
    _events,
    _frames,
    _install_event_collector,
    _start_mission,
    _wait_for_completion,
)

if TYPE_CHECKING:
    from .conftest import BrowserDiagnostics

pytestmark = [pytest.mark.e2e, pytest.mark.topology]


def test_queue_worker_uses_the_litestar_queue_cli() -> None:
    worker = QueueWorker("sse", environment={})
    assert worker.command[-6:] == ["queues", "run", "--queue", "demo", "--drain-timeout", "30"]


@pytest.mark.parametrize(
    ("backend", "url_env", "default_url", "key_env"),
    [
        ("redis", "LITESTAR_QUEUES_EXAMPLE_REDIS_URL", "redis://127.0.0.1:16379/0", "REDIS"),
        ("valkey", "LITESTAR_QUEUES_EXAMPLE_VALKEY_URL", "redis://127.0.0.1:16380/0", "VALKEY"),
    ],
)
@pytest.mark.parametrize("transport", ["sse", "websocket"])
def test_shared_channels_deliver_from_a_standalone_worker(
    backend: str,
    url_env: str,
    default_url: str,
    key_env: str,
    transport: str,
    browser_page: Any,
    browser_diagnostics: "BrowserDiagnostics",
) -> None:
    """A separate queue worker publishes live events through shared Channels."""
    url = os.getenv(url_env, default_url)
    client = _connect_or_skip(backend, url)
    suffix = uuid4().hex
    queue_prefix = f"litestar_queues:e2e:{suffix}:queue"
    environment = {
        url_env: url,
        "LITESTAR_QUEUES_EXAMPLE_SHARED_CHANNELS": "1",
        "LITESTAR_QUEUES_EXAMPLE_IN_APP_WORKER": "0",
        f"LITESTAR_QUEUES_EXAMPLE_{key_env}_KEY_PREFIX": queue_prefix,
        "LITESTAR_QUEUES_EXAMPLE_CHANNELS_KEY_PREFIX": f"litestar_queues:e2e:{suffix}:channels",
    }
    example_name = f"{transport}_{backend}"
    server = ExampleServer(example_name, mode="production", environment=environment)
    worker = QueueWorker(example_name, environment=environment)
    try:
        server.start()
        server.wait_until_ready(timeout=90.0)
        worker.start()
        worker.wait_until_running()

        page = browser_page
        page.goto(server.base_url, wait_until="domcontentloaded")
        _install_event_collector(page)
        task_id = _start_mission(page, server.base_url)
        mount = page.locator("#stream-mount")
        if transport == "sse":
            assert mount.get_attribute("sse-connect") == f"/queues/events/sse/tasks/{task_id}"
            page.wait_for_function("() => (window.__queueE2EEvents ?? []).includes('htmx:sseOpen')")
        else:
            assert mount.get_attribute("ws-connect") == f"/queues/events/tasks/{task_id}"
            page.wait_for_function("() => (window.__queueE2EEvents ?? []).includes('htmx:wsOpen')")
        page.set_default_timeout(15_000)
        try:
            _wait_for_completion(page)
        except Exception as exc:
            message = (
                f"browser events: {_events(page)}\n"
                f"worker output: {worker._recent_logs()}\n"
                f"server output: {server._recent_logs()}\n"
                f"queue keys: {list(client.scan_iter(match=f'{queue_prefix}*'))}"
            )
            raise AssertionError(message) from exc
        _assert_task_event(_frames(page))
        assert "htmx:sseError" not in _events(page)
        assert "htmx:wsError" not in _events(page)
        _assert_clean_browser(browser_diagnostics, server_mode="production")
    finally:
        worker.stop()
        server.stop()
        _delete_prefix(client, f"litestar_queues:e2e:{suffix}:")
        client.close()


def _connect_or_skip(backend: str, url: str) -> Any:
    try:
        if backend == "redis":
            from redis import Redis

            client: Any = Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1)
        else:
            from valkey import Valkey

            client = Valkey.from_url(url, socket_connect_timeout=1, socket_timeout=1)
        client.ping()
    except Exception as exc:  # noqa: BLE001 - vendor client errors vary by backend
        pytest.skip(f"{backend} service unavailable at {url}: {exc}")
    return client


def _delete_prefix(client: object, prefix: str) -> None:
    keys = list(client.scan_iter(match=f"{prefix}*"))  # type: ignore[attr-defined]
    if keys:
        client.delete(*keys)  # type: ignore[attr-defined]
