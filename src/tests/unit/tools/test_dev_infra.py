import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType


def _load_dev_infra() -> ModuleType:
    path = Path(__file__).resolve().parents[4] / "tools" / "dev_infra.py"
    spec = importlib.util.spec_from_file_location("dev_infra", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dev_infra = _load_dev_infra()


def test_run_args_builds_localhost_bound_redis_container() -> None:
    service = dev_infra.ServiceConfig(
        label="Redis",
        key="redis",
        container_name="litestar-queues-redis",
        image="redis:7-alpine",
        host_port=16379,
        volume_name="litestar-queues-redis-data",
        cli_name="redis-cli",
    )

    args = dev_infra._run_args(service)

    assert args[:3] == ["run", "-d", "--name"]
    assert "litestar-queues-redis" in args
    assert "127.0.0.1:16379:6379" in args
    assert "litestar-queues-redis-data:/data" in args
    assert "redis-cli -h 127.0.0.1 -p 6379 ping" in args
    assert args[-1] == "redis:7-alpine"


def test_start_creates_redis_and_valkey_containers_without_compose() -> None:
    runtime = _RecordingRuntime()
    services = [
        dev_infra.ServiceConfig(
            label="Redis",
            key="redis",
            container_name="litestar-queues-redis",
            image="redis:7-alpine",
            host_port=16379,
            volume_name="litestar-queues-redis-data",
            cli_name="redis-cli",
        ),
        dev_infra.ServiceConfig(
            label="Valkey",
            key="valkey",
            container_name="litestar-queues-valkey",
            image="valkey/valkey:8-alpine",
            host_port=16380,
            volume_name="litestar-queues-valkey-data",
            cli_name="valkey-cli",
        ),
    ]

    dev_infra.InfraManager(runtime, services).start()

    assert ["volume", "create", "litestar-queues-redis-data"] in runtime.commands
    assert ["volume", "create", "litestar-queues-valkey-data"] in runtime.commands
    run_commands = [command for command in runtime.commands if command[:2] == ["run", "-d"]]
    assert len(run_commands) == 2
    assert all("compose" not in part for command in run_commands for part in command)
    assert any("redis:7-alpine" in command for command in run_commands)
    assert any("valkey/valkey:8-alpine" in command for command in run_commands)


def test_root_conftest_uses_pytest_databases_for_redis_and_valkey() -> None:
    from tests import conftest

    assert "pytest_databases.docker.redis" in conftest.pytest_plugins
    assert "pytest_databases.docker.valkey" in conftest.pytest_plugins


class _RecordingRuntime:
    command = "docker"

    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = 30,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(args)
        return subprocess.CompletedProcess(["docker", *args], 0, stdout="", stderr="")

    def container_exists(self, container_name: str) -> bool:
        return False

    def container_running(self, container_name: str) -> bool:
        return False

    def volume_exists(self, volume_name: str) -> bool:
        return False

    def health_status(self, container_name: str) -> str:
        return "healthy"
