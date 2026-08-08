import subprocess
import sys


def test_pubsub_is_a_lazy_builtin_execution_backend() -> "None":
    code = """
import sys
import litestar_queues
from litestar_queues.execution import get_execution_backend_class, list_execution_backends
assert 'google.cloud.pubsub_v1' not in sys.modules
assert 'pubsub' in list_execution_backends()
backend = get_execution_backend_class('pubsub')
assert backend.__name__ == 'PubSubExecutionBackend'
assert 'google.cloud.pubsub_v1' not in sys.modules
assert litestar_queues.PubSubExecutionConfig.backend_name == 'pubsub'
"""

    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
