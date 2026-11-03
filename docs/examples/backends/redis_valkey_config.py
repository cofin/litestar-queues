"""Configuring RedisBackendConfig and ValkeyBackendConfig with key_prefix and terminal_ttl."""

from litestar_queues import QueueConfig, WorkerConfig
from litestar_queues.backends.redis import RedisBackendConfig
from litestar_queues.backends.valkey import ValkeyBackendConfig

TERMINAL_TTL_SECONDS = 86_400.0


def build_redis_queue_config() -> QueueConfig:
    """Configure Redis queue storage with Pub/Sub worker wakeups and native terminal TTL."""
    return QueueConfig(
        queue_backend=RedisBackendConfig(
            url="redis://localhost:6379/0",
            key_prefix="billing_queues",
            worker_wakeups=True,
            terminal_ttl=TERMINAL_TTL_SECONDS,
        ),
        execution_backend="local",
        worker=WorkerConfig(placement="external", max_concurrency=16),
    )


def build_valkey_queue_config() -> QueueConfig:
    """Configure Valkey queue storage with Pub/Sub worker wakeups and native terminal TTL."""
    return QueueConfig(
        queue_backend=ValkeyBackendConfig(
            url="valkey://localhost:6379/0",
            key_prefix="billing_queues",
            worker_wakeups=True,
            terminal_ttl=TERMINAL_TTL_SECONDS,
        ),
        execution_backend="local",
        worker=WorkerConfig(placement="external", max_concurrency=16),
    )


async def main() -> None:
    """Verify RedisBackendConfig and ValkeyBackendConfig initialization and inheritance."""
    redis_config = build_redis_queue_config()
    valkey_config = build_valkey_queue_config()
    assert isinstance(redis_config.queue_backend, RedisBackendConfig)
    assert isinstance(valkey_config.queue_backend, ValkeyBackendConfig)
    assert redis_config.queue_backend.terminal_ttl == TERMINAL_TTL_SECONDS
    assert valkey_config.queue_backend.terminal_ttl == TERMINAL_TTL_SECONDS
