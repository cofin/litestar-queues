"""Browser contracts for the HTMX SSE and WebSocket examples."""

import json
from typing import Any, cast

import pytest

from .conftest import BrowserDiagnostics

pytestmark = pytest.mark.e2e

_ERROR_EVENTS = {"htmx:sseError", "htmx:wsError", "htmx:responseError", "htmx:sendError"}


def _install_event_collector(page: Any) -> None:
    page.evaluate(
        """
        () => {
          const events = [];
          for (const name of [
            "htmx:sseOpen", "htmx:sseError", "htmx:sseClose",
            "htmx:wsOpen", "htmx:wsError", "htmx:wsClose",
            "htmx:responseError", "htmx:sendError",
            "htmx:sseBeforeMessage", "htmx:wsBeforeMessage",
          ]) {
            document.body.addEventListener(name, (event) => {
              events.push(name);
              if (name === "htmx:sseBeforeMessage") {
                window.__queueE2EFrames.push(event.detail?.data ?? null);
              }
              if (name === "htmx:wsBeforeMessage") {
                window.__queueE2EFrames.push(event.detail?.message ?? null);
              }
            });
          }
          window.__queueE2EEvents = events;
          window.__queueE2EFrames = [];
        }
        """
    )


def _events(page: Any) -> list[str]:
    return cast("list[str]", page.evaluate("() => window.__queueE2EEvents ?? []"))


def _frames(page: Any) -> list[str]:
    return [frame for frame in page.evaluate("() => window.__queueE2EFrames ?? []") if isinstance(frame, str)]


def _assert_custom_event(frames: list[str]) -> None:
    payloads = [json.loads(frame) for frame in frames]
    assert any(payload.get("type") == "crawl.page_discovered" for payload in payloads), frames


def _assert_clean_browser(
    diagnostics: BrowserDiagnostics, *, server_mode: str, allow_stream_abort: bool = False
) -> None:
    allowed_console = []
    if server_mode == "dev":
        allowed_console = [
            message
            for message in diagnostics.console_errors
            if "vite" in message.lower() and "websocket" in message.lower()
        ]
    unexpected_console = [message for message in diagnostics.console_errors if message not in allowed_console]
    assert not unexpected_console, f"unexpected browser console errors: {unexpected_console}"
    assert not diagnostics.page_errors, f"browser page errors: {diagnostics.page_errors}"
    request_failures = diagnostics.request_failures
    if allow_stream_abort:
        request_failures = [
            failure
            for failure in request_failures
            if "net::ERR_ABORTED" not in failure or "/queues/events/" not in failure
        ]
    assert not request_failures, f"failed browser requests: {request_failures}"
    assert not diagnostics.failed_responses, f"failed browser responses: {diagnostics.failed_responses}"


def _assert_common_page(page: Any, base_url: str) -> None:
    assert page.url == f"{base_url}/"
    assert page.locator("#stream-mount").count() == 1
    assert page.locator('button[hx-post="/demo/restart"]').count() == 1


def _start_mission(page: Any, base_url: str) -> str:
    with page.expect_response(
        lambda response: response.request.method == "POST" and response.url == f"{base_url}/demo/restart"
    ) as response_info:
        page.get_by_role("button", name="Restart the demo queue task").click()
    response = response_info.value
    assert response.status == 200, response.status_text

    mount = page.locator("#stream-mount")
    mount.wait_for(state="attached")
    task_id = cast("str | None", mount.get_attribute("data-task-id"))
    assert task_id
    return task_id


def _wait_for_completion(page: Any, *, require_first_line: bool = True) -> None:
    page.locator("#job-readout").filter(has_text="Task complete").wait_for(state="visible")
    if require_first_line:
        assert "1/6 -" in page.locator("#crawl-lines").inner_text()
    assert "Backend message received" in page.locator("#delivery-status").inner_text()


@pytest.mark.parametrize("example_name", ["sse"], indirect=True)
def test_sse_browser_contract(example_server: Any, browser_page: Any, browser_diagnostics: BrowserDiagnostics) -> None:
    """SSE opens on the Litestar origin and renders live terminal state."""
    page = browser_page
    page.goto(example_server.base_url, wait_until="domcontentloaded")
    _install_event_collector(page)
    _assert_common_page(page, example_server.base_url)
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode)

    task_id = _start_mission(page, example_server.base_url)
    mount = page.locator("#stream-mount")
    assert mount.get_attribute("hx-ext") == "sse"
    stream_url = mount.get_attribute("sse-connect")
    assert stream_url == f"/queues/events/sse/tasks/{task_id}"

    page.wait_for_function("() => (window.__queueE2EEvents ?? []).includes('htmx:sseOpen')")
    _wait_for_completion(page)

    events = _events(page)
    assert "htmx:sseOpen" in events
    assert "htmx:sseBeforeMessage" in events
    _assert_custom_event(_frames(page))
    assert not _ERROR_EVENTS.intersection(events), events
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode)


@pytest.mark.parametrize("example_name", ["websocket"], indirect=True)
def test_websocket_browser_contract(
    example_server: Any, browser_page: Any, browser_diagnostics: BrowserDiagnostics
) -> None:
    """WebSocket opens on the Litestar origin and renders live terminal state."""
    page = browser_page
    page.goto(example_server.base_url, wait_until="domcontentloaded")
    _install_event_collector(page)
    _assert_common_page(page, example_server.base_url)
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode)

    task_id = _start_mission(page, example_server.base_url)
    mount = page.locator("#stream-mount")
    assert mount.get_attribute("hx-ext") == "ws"
    stream_url = mount.get_attribute("ws-connect")
    assert stream_url == f"/queues/events/tasks/{task_id}"

    page.wait_for_function("() => (window.__queueE2EEvents ?? []).includes('htmx:wsOpen')")
    _wait_for_completion(page)

    events = _events(page)
    assert "htmx:wsOpen" in events
    assert "htmx:wsBeforeMessage" in events
    _assert_custom_event(_frames(page))
    assert not _ERROR_EVENTS.intersection(events), events
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode)


@pytest.mark.parametrize(
    ("transport", "example_name"), [("sse", "sse"), ("websocket", "websocket")], indirect=["example_name"]
)
def test_replacing_stream_mount_reconnects_cleanly(
    example_server: Any, browser_page: Any, browser_diagnostics: BrowserDiagnostics, transport: str
) -> None:
    """Replacing the HTMX mount closes the old stream before the next run."""
    page = browser_page
    page.goto(example_server.base_url, wait_until="domcontentloaded")
    _install_event_collector(page)
    websocket_states: dict[str, bool] = {}

    if transport == "websocket":

        def record_websocket(websocket: Any) -> None:
            websocket_states[websocket.url] = False
            websocket.on("close", lambda: websocket_states.__setitem__(websocket.url, True))

        page.on("websocket", record_websocket)

    first_task_id = _start_mission(page, example_server.base_url)
    _wait_for_completion(page)
    first_events = _events(page)
    event_prefix = "sse" if transport == "sse" else "ws"
    assert f"htmx:{event_prefix}Open" in first_events

    second_task_id = _start_mission(page, example_server.base_url)
    assert second_task_id != first_task_id
    mount = page.locator("#stream-mount")
    assert mount.get_attribute("data-task-id") == second_task_id
    _wait_for_completion(page)
    _assert_custom_event(_frames(page))

    events = _events(page)
    close_event = f"htmx:{event_prefix}Close"
    open_event = f"htmx:{event_prefix}Open"
    if transport == "sse":
        assert close_event in events, events
        assert events.index(close_event) < len(events) - 1
        assert events.index(close_event) < max(index for index, event in enumerate(events) if event == open_event)
    else:
        first_socket = next((url for url in websocket_states if first_task_id in url), None)
        second_socket = next((url for url in websocket_states if second_task_id in url), None)
        assert first_socket is not None, websocket_states
        assert second_socket is not None, websocket_states
        assert websocket_states[first_socket], websocket_states
    assert not _ERROR_EVENTS.intersection(events), events
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode, allow_stream_abort=True)


@pytest.mark.parametrize("example_name", ["sse"], indirect=True)
def test_repeated_start_reuses_active_task(
    example_server: Any, browser_page: Any, browser_diagnostics: BrowserDiagnostics
) -> None:
    """A second click while the demo is active follows the same queue record."""
    page = browser_page
    page.goto(example_server.base_url, wait_until="domcontentloaded")
    _install_event_collector(page)

    first_task_id = _start_mission(page, example_server.base_url)
    second_task_id = _start_mission(page, example_server.base_url)

    assert second_task_id == first_task_id
    page.locator("#job-readout").filter(has_text="already running").wait_for(state="visible")
    _wait_for_completion(page, require_first_line=False)
    _assert_clean_browser(browser_diagnostics, server_mode=example_server.mode, allow_stream_abort=True)
