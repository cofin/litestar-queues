"""Temporary socket-enabled Floci-GCP provider for real Managed Kafka tests.

pytest-databases 0.19 cannot pass Docker volumes through ``DockerService.run``.
This narrow fixture therefore uses its retained Docker client directly. Remove
it when the released Floci-GCP provider exposes the non-mock Kafka capability.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from docker.errors import APIError  # type: ignore[import-untyped]
from pytest_databases.types import ServiceContainer

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases._service import DockerService

_IMAGE = "floci/floci-gcp:0.6.0"
_PORT = 4588


@dataclass
class FlociManagedKafkaService(ServiceContainer):
    project_id: "str"
    resource_prefix: "str"
    docker_host: "str"
    capabilities: "frozenset[str]"
    location: "str"
    docker_client: "Any"

    @property
    def grpc_endpoint(self) -> "str":
        return f"{self.host}:{self.port}"

    @property
    def rest_endpoint(self) -> "str":
        return f"http://{self.host}:{self.port}"


@pytest.fixture(scope="session")
def floci_managed_kafka_service(docker_service: "DockerService") -> "Generator[FlociManagedKafkaService, None, None]":
    socket_path = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock").removeprefix("unix://")
    if not Path(socket_path).exists():
        pytest.skip(f"Floci Managed Kafka requires a local Docker socket; {socket_path!r} is unavailable")
    client: "Any" = docker_service._client  # pyright: ignore[reportPrivateUsage]  # temporary 0.19 shim
    try:
        client.images.get(_IMAGE)
    except Exception:  # noqa: BLE001 -- Docker SDK exposes registry-specific subclasses
        client.images.pull(*_IMAGE.rsplit(":", maxsplit=1))
    container = client.containers.run(
        _IMAGE,
        detach=True,
        remove=True,
        ports={f"{_PORT}/tcp": None},
        volumes={socket_path: {"bind": "/var/run/docker.sock", "mode": "rw"}},
        environment={"FLOCI_GCP_SERVICES_KAFKA_MOCK": "false"},
        labels=["pytest_databases"],
        name=f"pytest_databases_floci_managed_kafka_{os.getpid()}",
    )
    try:
        for _ in range(20):
            container.reload()
            bindings = container.ports.get(f"{_PORT}/tcp")
            if bindings:
                break
            time.sleep(0.25)
        else:
            msg = "Floci-GCP did not publish its REST port"
            raise RuntimeError(msg)
        host_port = int(bindings[0]["HostPort"])
        endpoint = f"http://127.0.0.1:{host_port}"
        for _ in range(180):
            try:
                with urlopen(f"{endpoint}/health", timeout=0.5) as response:
                    if response.status == 200:
                        break
            except (OSError, URLError):
                pass
            time.sleep(0.5)
        else:
            logs = container.logs(tail=100).decode(errors="replace")
            msg = f"Floci-GCP did not become ready: {logs}"
            raise RuntimeError(msg)
        yield FlociManagedKafkaService(
            container=container,
            host="127.0.0.1",
            port=host_port,
            project_id="litestar-queues-test",
            resource_prefix=f"litestar-queues-{os.getpid()}-",
            docker_host="127.0.0.1",
            capabilities=frozenset({"managed-kafka", "managed-kafka-redpanda"}),
            location="us-central1",
            docker_client=client,
        )
    finally:
        try:
            container.stop(timeout=5)
        except APIError as exc:
            if exc.status_code not in {404, 409}:
                raise
