"""Local Redis and Valkey container lifecycle for development."""

import argparse
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


CONTAINER_PORT = 6379
DEFAULT_REDIS_PORT = 16379
DEFAULT_VALKEY_PORT = 16380
HEALTH_INTERVAL_SECONDS = 1.0
HEALTH_RETRIES = 60
RUNTIME_ENV = "LITESTAR_QUEUES_CONTAINER_RUNTIME"
INSPECT_NAME_INDEX = 0
INSPECT_STATUS_INDEX = 1
INSPECT_HEALTH_INDEX = 2
INSPECT_IMAGE_INDEX = 3
INSPECT_ID_INDEX = 4
SHORT_CONTAINER_ID_LENGTH = 12


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    label: str
    key: str
    container_name: str
    image: str
    host_port: int
    volume_name: str
    cli_name: str

    @property
    def url(self) -> str:
        return f"redis://127.0.0.1:{self.host_port}/0"

    @property
    def health_command(self) -> str:
        return f"{self.cli_name} -h 127.0.0.1 -p {CONTAINER_PORT} ping"


@dataclass(frozen=True, slots=True)
class ContainerStatus:
    name: str
    status: str
    health: str
    image: str
    container_id: str
    ports: str


class ContainerRuntime:
    def __init__(self, command: str | None = None) -> None:
        self.command = command or _detect_runtime()

    def run(
        self,
        args: "Sequence[str]",
        *,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = 30,
    ) -> "subprocess.CompletedProcess[str]":
        command = [self.command, *args]
        result = subprocess.run(command, capture_output=capture_output, check=False, text=True, timeout=timeout)
        if check and result.returncode != 0:
            raise ContainerCommandError(command, result)
        return result

    def container_exists(self, container_name: str) -> bool:
        result = self.run(
            ["ps", "-a", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"], check=False
        )
        return container_name in result.stdout.splitlines()

    def container_running(self, container_name: str) -> bool:
        result = self.run(["ps", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"], check=False)
        return container_name in result.stdout.splitlines()

    def volume_exists(self, volume_name: str) -> bool:
        result = self.run(["volume", "ls", "--filter", f"name=^{volume_name}$", "--format", "{{.Name}}"], check=False)
        return volume_name in result.stdout.splitlines()

    def status(self, container_name: str) -> ContainerStatus | None:
        if not self.container_exists(container_name):
            return None

        template = "{{.Name}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.Config.Image}}|{{.Id}}"
        result = self.run(["inspect", "--format", template, container_name], check=False)
        if result.returncode != 0:
            return None

        parts = result.stdout.strip().split("|")
        ports = self.run(["port", container_name], check=False).stdout.strip() or "none"
        name = _inspect_part(parts, INSPECT_NAME_INDEX, container_name)
        return ContainerStatus(
            name=name.lstrip("/") if name else container_name,
            status=_inspect_part(parts, INSPECT_STATUS_INDEX, "unknown"),
            health=_inspect_part(parts, INSPECT_HEALTH_INDEX, "unknown"),
            image=_inspect_part(parts, INSPECT_IMAGE_INDEX, ""),
            container_id=_inspect_part(parts, INSPECT_ID_INDEX, "")[:SHORT_CONTAINER_ID_LENGTH],
            ports=ports,
        )

    def health_status(self, container_name: str) -> str:
        template = "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}"
        result = self.run(["inspect", "--format", template, container_name], check=False)
        return result.stdout.strip() if result.returncode == 0 else "unknown"


class InfraManager:
    def __init__(self, runtime: ContainerRuntime, services: "Sequence[ServiceConfig]") -> None:
        self.runtime = runtime
        self.services = list(services)

    def start(self, *, pull: bool = False, recreate: bool = False) -> None:
        print(f"Using container runtime: {self.runtime.command}")
        for service in self.services:
            self._start_service(service, pull=pull, recreate=recreate)

    def stop(self) -> None:
        for service in self.services:
            if not self.runtime.container_running(service.container_name):
                print(f"{service.label}: not running")
                continue
            print(f"{service.label}: stopping {service.container_name}")
            self.runtime.run(["stop", service.container_name])

    def restart(self, *, pull: bool = False) -> None:
        self.stop()
        self.start(pull=pull)

    def status(self) -> None:
        print(f"{'service':<8} {'container':<28} {'status':<12} {'health':<10} {'ports'}")
        print("-" * 82)
        for service in self.services:
            status = self.runtime.status(service.container_name)
            if status is None:
                print(f"{service.key:<8} {service.container_name:<28} {'missing':<12} {'n/a':<10} -")
                continue
            health = status.health if status.status == "running" else "n/a"
            print(f"{service.key:<8} {status.name:<28} {status.status:<12} {health:<10} {status.ports}")

    def logs(self, *, follow: bool = False, tail: int = 50) -> None:
        if follow and len(self.services) != 1:
            message = "following logs requires --service redis or --service valkey"
            raise InfraError(message)

        for service in self.services:
            if not self.runtime.container_exists(service.container_name):
                print(f"{service.label}: container does not exist")
                continue
            print(f"==> {service.label} ({service.container_name})")
            args = ["logs", "--tail", str(tail)]
            if follow:
                args.append("-f")
            args.append(service.container_name)
            self.runtime.run(args, capture_output=False, timeout=None)

    def wipe(self) -> None:
        for service in self.services:
            if self.runtime.container_exists(service.container_name):
                print(f"{service.label}: removing container {service.container_name}")
                self.runtime.run(["rm", "-f", service.container_name])
            if self.runtime.volume_exists(service.volume_name):
                print(f"{service.label}: removing volume {service.volume_name}")
                self.runtime.run(["volume", "rm", service.volume_name])

    def _start_service(self, service: ServiceConfig, *, pull: bool, recreate: bool) -> None:
        if self.runtime.container_running(service.container_name):
            if not recreate:
                print(f"{service.label}: already running at {service.url}")
                return
            print(f"{service.label}: recreating running container")
            self.runtime.run(["rm", "-f", service.container_name])

        if self.runtime.container_exists(service.container_name):
            if recreate:
                print(f"{service.label}: removing existing container")
                self.runtime.run(["rm", "-f", service.container_name])
            else:
                print(f"{service.label}: starting existing container")
                self.runtime.run(["start", service.container_name])
                self._wait_for_health(service)
                print(f"{service.label}: ready at {service.url}")
                return

        if pull:
            print(f"{service.label}: pulling {service.image}")
            self.runtime.run(["pull", service.image], timeout=600)

        if not self.runtime.volume_exists(service.volume_name):
            print(f"{service.label}: creating volume {service.volume_name}")
            self.runtime.run(["volume", "create", service.volume_name])

        print(f"{service.label}: creating container {service.container_name}")
        self.runtime.run(_run_args(service), timeout=120)
        self._wait_for_health(service)
        print(f"{service.label}: ready at {service.url}")

    def _wait_for_health(self, service: ServiceConfig) -> None:
        for _ in range(HEALTH_RETRIES):
            health = self.runtime.health_status(service.container_name)
            if health == "healthy":
                return
            if health == "unhealthy":
                _print_recent_logs(self.runtime, service.container_name)
                message = f"{service.label} container became unhealthy"
                raise InfraError(message)
            time.sleep(HEALTH_INTERVAL_SECONDS)

        _print_recent_logs(self.runtime, service.container_name)
        message = f"{service.label} did not become healthy within {HEALTH_RETRIES} seconds"
        raise InfraError(message)


class InfraError(Exception):
    """Base error for local infrastructure failures."""


class ContainerCommandError(InfraError):
    def __init__(self, command: "Sequence[str]", result: "subprocess.CompletedProcess[str]") -> None:
        self.command = list(command)
        self.result = result
        stderr = result.stderr.strip()
        detail = f": {stderr}" if stderr else ""
        super().__init__(f"{' '.join(command)} failed with exit code {result.returncode}{detail}")


def _run_args(service: ServiceConfig) -> list[str]:
    return [
        "run",
        "-d",
        "--name",
        service.container_name,
        "--hostname",
        service.key,
        "-p",
        f"127.0.0.1:{service.host_port}:{CONTAINER_PORT}",
        "-v",
        f"{service.volume_name}:/data",
        "--restart",
        "unless-stopped",
        "--health-cmd",
        service.health_command,
        "--health-interval",
        "5s",
        "--health-timeout",
        "3s",
        "--health-retries",
        "20",
        service.image,
    ]


def _inspect_part(parts: "Sequence[str]", index: int, default: str) -> str:
    return parts[index] if len(parts) > index else default


def _service_configs() -> list[ServiceConfig]:
    return [
        ServiceConfig(
            label="Redis",
            key="redis",
            container_name=os.getenv("LITESTAR_QUEUES_REDIS_CONTAINER", "litestar-queues-redis"),
            image=os.getenv("LITESTAR_QUEUES_REDIS_IMAGE", "redis:7-alpine"),
            host_port=_env_int("LITESTAR_QUEUES_REDIS_PORT", DEFAULT_REDIS_PORT),
            volume_name=os.getenv("LITESTAR_QUEUES_REDIS_VOLUME", "litestar-queues-redis-data"),
            cli_name="redis-cli",
        ),
        ServiceConfig(
            label="Valkey",
            key="valkey",
            container_name=os.getenv("LITESTAR_QUEUES_VALKEY_CONTAINER", "litestar-queues-valkey"),
            image=os.getenv("LITESTAR_QUEUES_VALKEY_IMAGE", "valkey/valkey:8-alpine"),
            host_port=_env_int("LITESTAR_QUEUES_VALKEY_PORT", DEFAULT_VALKEY_PORT),
            volume_name=os.getenv("LITESTAR_QUEUES_VALKEY_VOLUME", "litestar-queues-valkey-data"),
            cli_name="valkey-cli",
        ),
    ]


def _select_services(selection: str) -> list[ServiceConfig]:
    services = _service_configs()
    if selection == "all":
        return services
    return [service for service in services if service.key == selection]


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        message = f"{name} must be an integer, got {value!r}"
        raise InfraError(message) from exc


def _detect_runtime() -> str:
    configured = os.getenv(RUNTIME_ENV)
    if configured:
        if configured not in {"docker", "podman"}:
            message = f"{RUNTIME_ENV} must be 'docker' or 'podman'"
            raise InfraError(message)
        if _runtime_responds(configured):
            return configured
        message = f"configured runtime {configured!r} is not available"
        raise InfraError(message)

    for command in ("docker", "podman"):
        if _runtime_responds(command):
            return command

    message = "No container runtime available. Install Docker or Podman."
    raise InfraError(message)


def _runtime_responds(command: str) -> bool:
    if shutil.which(command) is None:
        return False
    try:
        result = subprocess.run([command, "--version"], capture_output=True, check=False, text=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _print_recent_logs(runtime: ContainerRuntime, container_name: str) -> None:
    result = runtime.run(["logs", "--tail", "20", container_name], check=False)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="start Redis and Valkey containers")
    _add_service_arg(start)
    start.add_argument("--pull", action="store_true", help="pull images before starting")
    start.add_argument("--recreate", action="store_true", help="remove and recreate existing containers")

    stop = subparsers.add_parser("stop", help="stop containers")
    _add_service_arg(stop)

    restart = subparsers.add_parser("restart", help="restart containers")
    _add_service_arg(restart)
    restart.add_argument("--pull", action="store_true", help="pull images before starting")

    status = subparsers.add_parser("status", help="show container status")
    _add_service_arg(status)

    logs = subparsers.add_parser("logs", help="show container logs")
    _add_service_arg(logs)
    logs.add_argument("-f", "--follow", action="store_true", help="follow logs for one selected service")
    logs.add_argument("--tail", type=int, default=50, help="number of log lines to show")

    wipe = subparsers.add_parser("wipe", help="remove containers and volumes")
    _add_service_arg(wipe)
    wipe.add_argument("--yes", action="store_true", help="confirm destructive removal")

    return parser


def _add_service_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--service", choices=["all", "redis", "valkey"], default="all", help="service to manage")


def main(argv: "Sequence[str] | None" = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    services = _select_services(args.service)
    manager = InfraManager(ContainerRuntime(), services)

    try:
        if args.command == "start":
            manager.start(pull=args.pull, recreate=args.recreate)
        elif args.command == "stop":
            manager.stop()
        elif args.command == "restart":
            manager.restart(pull=args.pull)
        elif args.command == "status":
            manager.status()
        elif args.command == "logs":
            manager.logs(follow=args.follow, tail=args.tail)
        elif args.command == "wipe":
            if not args.yes:
                parser.error("wipe requires --yes")
            manager.wipe()
        else:
            parser.error(f"unknown command: {args.command}")
    except InfraError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
