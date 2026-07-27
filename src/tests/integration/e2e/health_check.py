"""HTTP readiness checks for example-process E2E tests."""

import time
from collections.abc import Iterable

import httpx


def wait_for_paths(base_url: str, paths: Iterable[str], *, timeout: float) -> dict[str, httpx.Response]:
    """Wait until every path returns HTTP 200 on the Litestar origin.

    Returns:
        The successful responses keyed by request path.
    """
    paths = tuple(paths)
    deadline = time.monotonic() + timeout
    last_status: dict[str, int | None] = dict.fromkeys(paths)
    last_error: dict[str, str] = {}

    with httpx.Client(base_url=base_url, follow_redirects=True) as client:
        while time.monotonic() < deadline:
            responses: dict[str, httpx.Response] = {}
            ready = True
            for path in paths:
                try:
                    response = client.get(path, timeout=5.0)
                except httpx.HTTPError as exc:
                    last_error[path] = str(exc)
                    ready = False
                    continue

                responses[path] = response
                last_status[path] = response.status_code
                if response.status_code != 200:
                    ready = False

            if ready:
                return responses
            time.sleep(0.25)

    details = []
    for path in paths:
        status = last_status[path]
        error = last_error.get(path)
        details.append(f"{path}: status={status!r} error={error!r}")
    message = f"Example did not become ready at {base_url}: {'; '.join(details)}"
    raise TimeoutError(message)
