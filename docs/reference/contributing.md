# Contributing & Testing

This guide covers the test suite tiers, the integration backend registry, and the documentation verification pipeline for `litestar-queues`.

## Test Suite Architecture

The test suite lives under `src/tests/` and separates verification into three distinct tiers:

- `src/tests/unit/`: Pure-Python unit tests covering task registration, configuration validation, worker lifecycle, event buffering, docstrings, and documentation examples without requiring Docker or external services.
- `src/tests/integration/`: Integration contract tests against real databases, Redis, Valkey, and message broker containers/emulators via `pytest-databases`.
- `src/tests/integration/e2e/`: Real-browser Playwright end-to-end tests exercising the runnable HTMX WebSocket and SSE applications (`examples/htmx_realtime_*`).

### Running Tests

Install development dependencies and shared frontend example assets:

```bash
make install
```

Run the unit test suite (no Docker required):

```bash
make test-unit
```

Run the integration test suite (autoskips unavailable container services):

```bash
make install-test-adapters
make test-integration
```

Run the Playwright browser E2E suite:

```bash
make install-e2e
uv run playwright install chromium
make test-examples-e2e
```

### Browser Topology Boundaries

| Test Slice | Processes & Services | What It Verifies |
| :--- | :--- | :--- |
| Memory SSE / WebSocket browser tests | Single Litestar process + Chromium | HTMX boot, enqueue request, stream lifecycle, DOM progress updates, and terminal events. |
| Redis / Valkey topology tests | Web process + standalone `litestar queues run` worker + Chromium + Redis/Valkey | Shared queue persistence and cross-process Channels fan-out (`RedisChannelsStreamBackend`). |
| Unit stream tests | Test process only | Route content types, `channel_authorizer` hooks, camelCase wire envelopes, keepalives, and sink behavior. |

### Integration Backend Registry

`src/tests/integration/_backends.py` defines the queue-backend registry. Each `BackendCase` declares:

- `name`: The pytest parameter ID.
- `extras`: Import names checked with `pytest.importorskip` before requesting a container service.
- `service_attr`: The `pytest-databases` fixture name for real database/broker services or emulators.
- `build`: Async builder returning an unopened `BaseQueueBackend` instance.
- `capabilities`: Capability tags used to filter contract tests.

## Documentation Authoring & Verification

Documentation source files live in `docs/`, runnable code snippets live in `docs/examples/`, and documentation build/verification scripts live in `tools/docs/`.

### Authoring Rules

1. **Runnable Code Snippets**: All multi-line guide examples live as standalone Python modules under `docs/examples/<domain>/<example_name>.py` with an async `main()` verification function and are embedded into Markdown guides via PyMdown Snippets directives.
2. **Strict Python Standards**: Modules under `docs/examples/` and `tools/docs/` follow the same rules as `src/litestar_queues/`—no `from __future__ import annotations`, zero inline `#` comments, top-level imports, and full `ruff`, `mypy`, and `pyright` compliance.
3. **Public Docstrings**: Every public class, method, function, and `@dataclass` config field in `src/litestar_queues/` must have a Google-style docstring (or immediate attribute docstring) verified by `tools/docs/scan_public_docstrings.py`.
4. **CLI Reference Sync**: `docs/reference/cli.md` is generated directly from the live Click command tree via `tools/docs/generate_cli_docs.py`.

### Documentation Verification Commands

```bash
# Audit public docstrings, CLI reference drift, and documentation structure tests
make docs-audit

# Run runnable documentation example tests
uv run pytest src/tests/unit/test_docs_examples.py -q

# Build HTML site, generate objects.inv / llms.txt / legacy redirects, and verify artifacts
make docs

# Preview documentation locally with live reload
make docs-serve
```
