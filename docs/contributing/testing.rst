Testing
=======

The test suite lives under ``src/tests``. Keep these roles separate:

* ``src/tests/unit`` covers pure-Python behavior.
* ``src/tests/integration`` covers queue drivers, service containers, vendor
  emulators, and execution backends.
* ``src/tests/_factories`` contains shared test factories.

The root ``src/tests/conftest.py`` provides fixtures that every tier can use:
``anyio_backend``, task-registry cleanup, and default Litestar app/plugin
fixtures. Unit tests must not import optional queue drivers. Integration tests
may import drivers, request pytest-databases services, and use Docker-backed
or emulator-backed fixtures.

Running Tests
-------------

Run the unit tier when changing pure task, worker, plugin, model, or event
behavior:

.. code-block:: bash

   uv run pytest src/tests/unit

Install project dependencies for local development before running tests:

.. code-block:: bash

   make install

This installs package/test dependencies and provisions frontend assets for the shipped
example apps.

Install test dependencies before running the full integration matrix:

.. code-block:: bash

   make install-test-adapters
   uv run pytest src/tests/integration

Browser E2E tests are intentionally separate from the unit and integration
tiers. Install their Python dependencies and Chromium, then run:

.. code-block:: bash

   make install-e2e
   uv run playwright install chromium
   make test-examples-e2e

The ``e2e`` dependency group installs Playwright and pytest-playwright. The E2E
target installs that group and Chromium so CI and a new workstation use one
self-contained command. It starts real Litestar/Vite examples with
``litestar run`` and controls them through Chromium. ``make test`` and the
normal integration workflow do not include these browser tests.

Browser topology boundaries
---------------------------

.. list-table::
   :header-rows: 1

   * - Test slice
     - Processes and services
     - What it proves
   * - Memory SSE/WebSocket browser tests
     - One Litestar process plus Chromium
     - HTMX boot, enqueue request, stream lifecycle, DOM progress, terminal event.
   * - Redis/Valkey topology tests
     - Web process, standalone ``litestar queues run`` worker, Chromium, real service
     - Shared queue persistence and explicitly shared Channels delivery.
   * - Unit stream tests
     - Test process only
     - Route content type, authorization hooks, envelopes, keepalives, and sink behavior.

The memory browser cases run in one process. They cannot prove behavior with a
separate worker or multiple web replicas. Redis/Valkey cases enable shared
Channels and use unique queue and Channels prefixes. Selecting a Redis/Valkey
queue backend alone is not enough. Browser tests use demo-only stream
authorization. Production routes must check tenant and user ownership,
authenticate service connections, and enforce origin and proxy policy.

CI keeps unit/integration and browser jobs separate because Chromium and real
Redis/Valkey topology have different setup, runtime, and failure diagnostics.

If you want to prebuild all example frontend assets in one step:

.. code-block:: bash

   make build-examples-assets

Integration tests rely on pytest-databases to skip unavailable services. A
test should request a fixture such as ``postgres_service``, ``mysql_service``,
``oracle_service``, ``redis_service``, or ``valkey_service``. Let the fixture
skip when Docker or an emulator is unavailable. Do not add custom "Docker is
available" assertions.

Backend Registry
----------------

``src/tests/integration/_backends.py`` is the queue-backend registry. Each
``BackendCase`` declares:

* ``name``: the pytest param id.
* ``extras``: import names checked with ``pytest.importorskip`` before the case
  requests a service.
* ``service_attr``: the pytest fixture name for real services or emulators.
* ``build``: the async builder that returns an unopened queue backend.
* ``capabilities``: behavior tags used by contract tests.

``src/tests/integration/conftest.py`` runs each test that requests
``queue_backend`` against the registry. It creates a ``FixtureCtx`` with
``tmp_path`` and any requested service. It then opens the backend, gives it to
the test, and removes queue artifacts during teardown for service-backed
adapters.

Adding a Backend
----------------

1. Add the driver or emulator dependency to the ``tests`` group when it
   is not already installed by another test dependency.
2. Make the service fixture available to the integration tier. Prefer an
   upstream pytest-databases plugin; use a narrow wrapper fixture only when the
   upstream plugin imports optional clients too early for local autoskip.
3. Add a builder function in ``src/tests/integration/_backends.py`` that
   constructs the backend from ``FixtureCtx``.
4. Add a ``BackendCase`` with import-skip gates for the adapter and emulator
   client packages.
5. Run a focused collect or registry check, then the relevant integration
   contract tests.

Cloud Run
---------

Google Cloud Run Jobs has no public local emulator. Tests under
``src/tests/integration/execution/cloudrun`` therefore inject fake
``JobsClient`` and ``ExecutionsClient`` implementations. They cover request
construction, dispatch ownership, status checks, entrypoint behavior, and the
optional import boundary without calling GCP.
