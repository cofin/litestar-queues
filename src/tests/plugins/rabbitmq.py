"""Temporary RabbitMQ provider derived from pytest-databases PR 158.

Derived from upstream commit 633edd7. Compatibility adaptations are limited
to removing future annotations, ContainerService -> DockerService,
container_service -> docker_service, a local isolation Literal because 0.19.0
lacks XdistIsolationLevel, and the released DockerService.run() call shape.
Remove this module once pytest_databases.docker.rabbitmq is released.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote

import pytest
from docker.errors import APIError  # type: ignore[import-untyped]
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from pytest_databases._service import DockerService

RabbitMQIsolationLevel = Literal["database", "server"]

_RABBITMQ_COMMAND = (
    "mkdir -p /run/pytest-databases-rabbitmq && "
    "chown rabbitmq:rabbitmq /run/pytest-databases-rabbitmq && exec gosu rabbitmq rabbitmq-server"
)


def _output_to_bytes(output: "bytes | str | Iterator[bytes]") -> "bytes":
    if isinstance(output, bytes):
        return output
    if isinstance(output, str):
        return output.encode()
    return b"".join(output)


def _exec(container: "Any", *args: "str") -> "tuple[int, bytes]":
    result = container.exec_run(list(args))
    return result.exit_code if result.exit_code is not None else -1, _output_to_bytes(result.output)


def _rabbitmq_responsive(service: "ServiceContainer") -> "bool":
    try:
        running_code, _ = _exec(service.container, "rabbitmq-diagnostics", "-q", "check_running")
        listener_code, _ = _exec(service.container, "rabbitmq-diagnostics", "-q", "check_port_listener", "5672")
    except APIError:
        return False
    return running_code == 0 and listener_code == 0


def _ensure_vhost(container: "Any", *, username: "str", vhost: "str") -> "None":
    list_code, output = _exec(container, "rabbitmqctl", "-q", "list_vhosts", "name", "--no-table-headers")
    if list_code != 0:
        raise RuntimeError(output.decode(errors="replace"))
    if vhost not in output.decode().splitlines():
        add_code, add_output = _exec(container, "rabbitmqctl", "-q", "add_vhost", vhost)
        if add_code != 0:
            raise RuntimeError(add_output.decode(errors="replace"))
    permission_code, permission_output = _exec(
        container, "rabbitmqctl", "-q", "set_permissions", "-p", vhost, username, ".*", ".*", ".*"
    )
    if permission_code != 0:
        raise RuntimeError(permission_output.decode(errors="replace"))


@dataclass
class RabbitMQService(ServiceContainer):
    username: "str"
    password: "str"
    vhost: "str"

    @property
    def amqp_url(self) -> "str":
        username = quote(self.username, safe="")
        password = quote(self.password, safe="")
        vhost = quote(self.vhost, safe="")
        return f"amqp://{username}:{password}@{self.host}:{self.port}/{vhost}"


@pytest.fixture(scope="session")
def xdist_rabbitmq_isolation_level() -> "RabbitMQIsolationLevel":
    return "database"


@pytest.fixture(scope="session")
def rabbitmq_image() -> "str":
    return "rabbitmq:4.3-management"


@pytest.fixture(scope="session")
def rabbitmq_username() -> "str":
    return "pytest-databases"


@pytest.fixture(scope="session")
def rabbitmq_password() -> "str":
    return "pytest-databases-secret"


@pytest.fixture(scope="session")
def rabbitmq_vhost(xdist_rabbitmq_isolation_level: "RabbitMQIsolationLevel") -> "str":
    worker_num = get_xdist_worker_num()
    if worker_num is not None and xdist_rabbitmq_isolation_level == "database":
        return f"pytest_databases_{worker_num}"
    return "pytest_databases"


@pytest.fixture(scope="session")
def rabbitmq_service(  # noqa: PLR0917
    docker_service: "DockerService",
    rabbitmq_image: "str",
    rabbitmq_username: "str",
    rabbitmq_password: "str",
    rabbitmq_vhost: "str",
    xdist_rabbitmq_isolation_level: "RabbitMQIsolationLevel",
) -> "Generator[RabbitMQService, None, None]":
    worker_num = get_xdist_worker_num()
    name = (
        f"rabbitmq_43_{worker_num + 1}"
        if worker_num is not None and xdist_rabbitmq_isolation_level == "server"
        else "rabbitmq_43"
    )
    with docker_service.run(
        image=rabbitmq_image,
        container_port=5672,
        name=name,
        command=f"bash -c '{_RABBITMQ_COMMAND}'",
        check=_rabbitmq_responsive,
        wait_for_log="Server startup complete",
        env={
            "RABBITMQ_DEFAULT_USER": rabbitmq_username,
            "RABBITMQ_DEFAULT_PASS": rabbitmq_password,
            "RABBITMQ_DEFAULT_VHOST": "pytest_databases",
            "RABBITMQ_ERLANG_COOKIE": "pytest-databases-erlang-cookie",
            "RABBITMQ_MNESIA_BASE": "/run/pytest-databases-rabbitmq/mnesia",
            "HOME": "/run/pytest-databases-rabbitmq",
        },
        timeout=60,
        transient=xdist_rabbitmq_isolation_level == "server",
    ) as service:
        _ensure_vhost(service.container, username=rabbitmq_username, vhost=rabbitmq_vhost)
        yield RabbitMQService(
            container=service.container,
            host=service.host,
            port=service.port,
            username=rabbitmq_username,
            password=rabbitmq_password,
            vhost=rabbitmq_vhost,
        )
