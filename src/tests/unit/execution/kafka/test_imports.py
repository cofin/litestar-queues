import subprocess
import sys


def test_kafka_is_a_lazy_builtin_execution_backend() -> "None":
    code = """
import sys
import litestar_queues
from litestar_queues.execution import get_execution_backend_class, list_execution_backends
assert 'aiokafka' not in sys.modules
assert 'kafka' in list_execution_backends()
backend = get_execution_backend_class('kafka')
assert backend.__name__ == 'KafkaExecutionBackend'
assert 'aiokafka' not in sys.modules
assert litestar_queues.KafkaExecutionConfig.backend_name == 'kafka'
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
