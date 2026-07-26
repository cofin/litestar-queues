"""Smoke tests for the queue example process harness."""

import pytest

from .server_manager import ExampleServer


@pytest.mark.e2e
@pytest.mark.parametrize("example_name", ["sse", "websocket"])
@pytest.mark.parametrize("mode", ["dev", "production"])
def test_example_server_starts(example_name: str, mode: str) -> None:
    """Both canonical browser examples start through the Litestar CLI."""
    server = ExampleServer(example_name, mode=mode)  # type: ignore[arg-type]
    server.start()
    try:
        server.wait_until_ready(timeout=90.0)
        assert server.base_url.startswith("http://127.0.0.1:")
    finally:
        server.stop()
