import subprocess
import sys


def test_valkey_backend_import_does_not_require_redis() -> "None":
    """Importing ``ValkeyQueueBackend`` must not require the redis client package."""
    code = """
import builtins
import sys
import types

valkey = types.ModuleType("valkey")
valkey.__path__ = []
valkey_asyncio = types.ModuleType("valkey.asyncio")

def from_url(url, decode_responses=True):
    return object()

valkey_asyncio.from_url = from_url
valkey.asyncio = valkey_asyncio
sys.modules["valkey"] = valkey
sys.modules["valkey.asyncio"] = valkey_asyncio

original_import = builtins.__import__

def blocked_import(name, *args, **kwargs):
    if name == "redis" or name.startswith("redis."):
        raise ModuleNotFoundError(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = blocked_import

from litestar_queues.backends.valkey import ValkeyBackendConfig, ValkeyQueueBackend

assert "redis" not in sys.modules
assert ValkeyBackendConfig(url="redis://example").url == "redis://example"
assert ValkeyQueueBackend.__name__ == "ValkeyQueueBackend"
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, check=False, text=True)

    assert result.returncode == 0, result.stderr
