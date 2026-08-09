"""Temporary Floci-GCP fixture pending its pytest-databases release."""

import os
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases._service import DockerService

_FLOCI_GCP_IMAGE = "floci/floci-gcp:0.6.0"
_FLOCI_GCP_PORT = 4588


@dataclass
class FlociGcpService(ServiceContainer):
    """Coordinates and supported data planes for one Floci-GCP instance."""

    project_id: str
    resource_prefix: str
    docker_host: str
    capabilities: frozenset[str]

    @property
    def grpc_endpoint(self) -> str:
        """Return the unified insecure gRPC endpoint."""
        return f"{self.host}:{self.port}"

    @property
    def rest_endpoint(self) -> str:
        """Return the unified REST endpoint."""
        return f"http://{self.host}:{self.port}"


@pytest.fixture(scope="session")
def floci_gcp_service(docker_service: "DockerService") -> "Generator[FlociGcpService, None, None]":
    """Start the pinned Floci-GCP emulator image.

    Yields:
        Connection details for the running emulator.
    """
    worker_num = get_xdist_worker_num()
    suffix = worker_num or "main"
    project_id = os.getenv("FLOCI_GCP_PROJECT", "litestar-queues-test")
    prefix = os.getenv("FLOCI_GCP_RESOURCE_PREFIX", f"litestar-queues-{suffix}-")

    def check(service: ServiceContainer) -> bool:
        try:
            with urlopen(f"http://{service.host}:{service.port}/health", timeout=0.5) as response:
                return int(response.status) == 200
        except (OSError, URLError):
            return False

    with docker_service.run(
        image=_FLOCI_GCP_IMAGE,
        name=f"floci_gcp_{suffix}",
        container_port=_FLOCI_GCP_PORT,
        check=check,
        timeout=90,
        pause=0.5,
        transient=worker_num is not None,
    ) as service:
        yield FlociGcpService(
            host=service.host,
            port=service.port,
            container=service.container,
            project_id=project_id,
            resource_prefix=prefix,
            docker_host=os.getenv("FLOCI_GCP_RECEIVER_HOST", socket.gethostbyname(socket.gethostname())),
            capabilities=frozenset({"eventarc-standard", "pubsub"}),
        )
