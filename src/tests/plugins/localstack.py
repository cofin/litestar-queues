"""Temporary LocalStack provider adapted from pytest-databases PR 156.

Copied from upstream commit b8b036e and adapted only from the unreleased
ContainerService/container_service names to pytest-databases 0.19.0's released
DockerService/docker_service API. Remove this module once the official
pytest_databases.docker.localstack provider is released.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from pytest_databases._service import DockerService

_LOCALSTACK_GATEWAY_PORT = 4566
_LOCALSTACK_CLI = """\
if command -v awslocal >/dev/null 2>&1; then
    exec awslocal --region "$AWS_DEFAULT_REGION" "$@"
fi
if command -v aws >/dev/null 2>&1; then
    exec aws --endpoint-url=http://localhost:4566 --region "$AWS_DEFAULT_REGION" "$@"
fi
exit 127
"""


def _exec_aws_cli(container: "Any", arguments: "Sequence[str]") -> "tuple[int, str]":
    result = container.exec_run(["sh", "-c", _LOCALSTACK_CLI, "litestar-queues", *arguments])
    output_bytes = result.output if isinstance(result.output, bytes) else b"".join(result.output)
    output = output_bytes.decode("utf-8", errors="replace")
    if result.exit_code is None:
        msg = "LocalStack AWS CLI command did not return an exit code"
        raise RuntimeError(msg)
    if result.exit_code == 127:
        msg = "LocalStack image must provide the awslocal or aws CLI"
        raise RuntimeError(msg)
    return result.exit_code, output


def _health_is_ready(container: "Any") -> "bool":
    result = container.exec_run([
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--dump-header",
        "-",
        "--output",
        "/dev/null",
        "http://localhost:4566/_localstack/health",
    ])
    output = result.output if isinstance(result.output, bytes) else b"".join(result.output)
    return result.exit_code == 0 and b"x-localstack:" in output.lower()


@dataclass
class LocalStackService(ServiceContainer):
    endpoint_url: "str"
    region: "str"
    access_key: "str"
    secret_key: "str"
    services: "tuple[str, ...] | None"
    resource_prefix: "str"

    def exec_aws_cli(self, *arguments: "str") -> "str":
        exit_code, output = _exec_aws_cli(self.container, arguments)
        if exit_code != 0:
            msg = f"LocalStack AWS CLI command failed: {output.strip()}"
            raise RuntimeError(msg)
        return output


@pytest.fixture(scope="session")
def localstack_service(docker_service: "DockerService") -> "Generator[LocalStackService, None, None]":
    region = os.getenv("LOCALSTACK_REGION", "us-east-1")
    access_key = os.getenv("LOCALSTACK_ACCESS_KEY", "test")
    secret_key = os.getenv("LOCALSTACK_SECRET_KEY", "test")
    worker_num = get_xdist_worker_num()
    prefix = os.getenv("LOCALSTACK_RESOURCE_PREFIX", f"litestar-queues-{worker_num or 'main'}-")
    environment = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_DEFAULT_REGION": region,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "SERVICES": "sqs",
    }

    def check(service: "ServiceContainer") -> "bool":
        if not _health_is_ready(service.container):
            return False
        exit_code, _ = _exec_aws_cli(service.container, ("sqs", "list-queues", "--output", "json"))
        return exit_code == 0

    with docker_service.run(
        image="localstack/localstack:4.14.0",
        name=f"localstack_{worker_num}" if worker_num is not None else "localstack",
        container_port=_LOCALSTACK_GATEWAY_PORT,
        env=environment,
        check=check,
        timeout=60,
        pause=0.5,
        transient=worker_num is not None,
    ) as service:
        yield LocalStackService(
            host=service.host,
            port=service.port,
            container=service.container,
            endpoint_url=f"http://{service.host}:{service.port}",
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            services=("sqs",),
            resource_prefix=prefix,
        )
