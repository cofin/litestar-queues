Testing
=======

The project test suite lives under ``src/tests``. Keep that layout intact:
``src/tests/unit`` is for pure-Python behavior, ``src/tests/integration`` is
for driver-backed queues, service containers, vendor emulators, and execution
backend coverage, and ``src/tests/_factories`` holds shared test factories.

The root ``src/tests/conftest.py`` owns fixtures that every tier can use:
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

Install test dependencies before running the full integration matrix:

.. code-block:: bash

   make install-test-adapters
   uv run pytest src/tests/integration

The integration tier intentionally relies on pytest-databases autoskip
behavior. A test should request a service fixture such as ``postgres_service``,
``mysql_service``, ``oracle_service``, ``redis_service``, or ``valkey_service`` and let the
fixture skip when Docker or an emulator dependency is unavailable. Do not add
custom "Docker is available" assertions to tests.

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

``src/tests/integration/conftest.py`` parametrizes any test that asks for the
``queue_backend`` fixture across the registry. It builds a ``FixtureCtx`` with
``tmp_path`` and any requested service handle, opens the backend, yields it to
the test, and drops queue artifacts on teardown for service-backed adapters.

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

Google Cloud Run Jobs does not provide a public local emulator. The Cloud Run
execution suite therefore lives under
``src/tests/integration/execution/cloudrun`` and uses injected fake
``JobsClient`` and ``ExecutionsClient`` implementations to exercise request
construction, dispatch ownership, reconciliation, entrypoint behavior, and the
optional import boundary without calling GCP.
