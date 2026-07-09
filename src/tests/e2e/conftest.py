"""Fixtures for queue example browser tests."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import pytest

from .server_manager import ExampleMode, ExampleServer

E2E_TEST_TIMEOUT = float(os.getenv("LITESTAR_QUEUES_E2E_TIMEOUT", "90"))


@dataclass
class BrowserDiagnostics:
    """Browser failures collected for a failure message."""

    console_errors: list[str] = field(default_factory=list)
    page_errors: list[str] = field(default_factory=list)
    request_failures: list[str] = field(default_factory=list)
    failed_responses: list[str] = field(default_factory=list)


@pytest.fixture(scope="session", params=["sse", "websocket"])
def example_name(request: pytest.FixtureRequest) -> str:
    """Select a canonical browser example alias.

    Returns:
        The example alias selected by the parametrized fixture.
    """
    return str(request.param)


@pytest.fixture(scope="session", params=["dev", "production"])
def server_mode(request: pytest.FixtureRequest) -> ExampleMode:
    """Select the asset mode under test.

    Returns:
        The selected Litestar/Vite asset mode.
    """
    return request.param


@pytest.fixture(scope="session")
def example_server(example_name: str, server_mode: ExampleMode) -> ExampleServer:
    """Start one example per example/mode pair and clean it up at session end.

    Yields:
        The ready example process manager.
    """
    server = ExampleServer(example_name, mode=server_mode)
    server.start()
    try:
        server.wait_until_ready(timeout=E2E_TEST_TIMEOUT)
        yield server
    finally:
        server.stop()


@pytest.fixture
def e2e_timeout() -> float:
    """Return the configurable browser timeout in seconds."""
    return E2E_TEST_TIMEOUT


@pytest.fixture
def browser_diagnostics() -> BrowserDiagnostics:
    """Collect browser console, page, request, and response failures.

    Returns:
        A mutable diagnostics collection for the current browser test.
    """
    return BrowserDiagnostics()


@pytest.fixture
def browser_page(page: Any, browser_diagnostics: BrowserDiagnostics, e2e_timeout: float) -> Any:
    """Return a fresh Playwright page with actionable diagnostics attached.

    Yields:
        A fresh Playwright page configured with the E2E timeout.
    """
    page.set_default_timeout(e2e_timeout * 1000)
    page.on(
        "console",
        lambda message: browser_diagnostics.console_errors.append(message.text) if message.type == "error" else None,
    )
    page.on("pageerror", lambda error: browser_diagnostics.page_errors.append(str(error)))
    page.on(
        "requestfailed",
        lambda request: browser_diagnostics.request_failures.append(
            f"{request.method} {request.url}: {request.failure}"
        ),
    )
    page.on(
        "response",
        lambda response: (
            browser_diagnostics.failed_responses.append(f"{response.status} {response.request.method} {response.url}")
            if response.status >= 400
            else None
        ),
    )
    yield page
