"""Official Google Cloud Pub/Sub emulator fixture."""

import os
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_databases._service import DockerService

_PUBSUB_PORT = 8085


@dataclass
class PubSubEmulatorService(ServiceContainer):
    """Connection details for the official Pub/Sub emulator."""

    project_id: "str"
    resource_prefix: "str"

    @property
    def endpoint(self) -> "str":
        return f"{self.host}:{self.port}"


@pytest.fixture(scope="session")
def pubsub_emulator_service(docker_service: "DockerService") -> "Generator[PubSubEmulatorService, None, None]":
    """Start Google's official Pub/Sub emulator container.

    Yields:
        Connection details for the running emulator.
    """
    worker_num = get_xdist_worker_num()
    suffix = worker_num or "main"
    project_id = os.getenv("PUBSUB_EMULATOR_PROJECT", "litestar-queues-test")
    prefix = os.getenv("PUBSUB_RESOURCE_PREFIX", f"litestar-queues-{suffix}-")

    def check(service: "ServiceContainer") -> "bool":
        try:
            with socket.create_connection((service.host, service.port), timeout=0.25):
                return True
        except OSError:
            return False

    command = f"gcloud beta emulators pubsub start --project={project_id} --host-port=0.0.0.0:{_PUBSUB_PORT} --quiet"
    with docker_service.run(
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
        name=f"pubsub_{suffix}",
        container_port=_PUBSUB_PORT,
        command=command,
        check=check,
        timeout=90,
        pause=0.5,
        transient=worker_num is not None,
    ) as service:
        yield PubSubEmulatorService(
            host=service.host,
            port=service.port,
            container=service.container,
            project_id=project_id,
            resource_prefix=prefix,
        )
