"""Temporary Redpanda provider derived from pytest-databases PR 155.

Remove this module once ``pytest_databases.docker.redpanda`` is released.
"""

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

_INTERNAL_PORT = 9092
_EXTERNAL_PORT = 19092


@dataclass
class RedpandaService(ServiceContainer):
    bootstrap_servers: "str"
    topic_prefix: "str"


@pytest.fixture(scope="session")
def redpanda_service(docker_service: "DockerService") -> "Generator[RedpandaService, None, None]":
    worker_num = get_xdist_worker_num()
    suffix = worker_num or "main"
    with socket.socket() as port_socket:
        port_socket.bind(("127.0.0.1", 0))
        host_port = port_socket.getsockname()[1]
    command = " ".join([
        "redpanda start --mode dev-container --smp 1",
        f"--kafka-addr internal://0.0.0.0:{_INTERNAL_PORT},external://0.0.0.0:{_EXTERNAL_PORT}",
        f"--advertise-kafka-addr internal://127.0.0.1:{_INTERNAL_PORT},external://127.0.0.1:{host_port}",
    ])

    def check(service: "ServiceContainer") -> "bool":
        result = service.container.exec_run([
            "sh",
            "-c",
            f"rpk cluster health --exit-when-healthy -X brokers=127.0.0.1:{_INTERNAL_PORT}",
        ])
        if result.exit_code != 0:
            return False
        try:
            with socket.create_connection((service.host, service.port), timeout=0.25):
                return True
        except OSError:
            return False

    with docker_service.run(
        image=os.environ.get("REDPANDA_IMAGE", "docker.redpanda.com/redpandadata/redpanda:v26.1.13"),
        container_port=_EXTERNAL_PORT,
        name=f"redpanda_{suffix}",
        command=command,
        check=check,
        timeout=120,
        pause=0.5,
        transient=worker_num is not None,
        host_port=host_port,
    ) as service:
        yield RedpandaService(
            container=service.container,
            host=service.host,
            port=service.port,
            bootstrap_servers=f"{service.host}:{service.port}",
            topic_prefix=f"litestar_queues_{suffix}_",
        )
