import asyncio
import json
import socket
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import pytest

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.kafka import KafkaExecutionBackend, KafkaExecutionConfig

if TYPE_CHECKING:
    from tests.plugins.floci_managed_kafka import FlociManagedKafkaService

pytestmark = pytest.mark.anyio


def _request(url: "str", *, method: "str" = "GET", payload: "dict[str, Any] | None" = None) -> "dict[str, Any]":
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read()
    except HTTPError as exc:
        body = exc.read().decode(errors="replace")
        msg = f"Floci request failed with {exc.code}: {body}"
        raise RuntimeError(msg) from exc
    return json.loads(body) if body else {}


def _probe(address: "tuple[str, int]") -> "None":
    with socket.create_connection(address, timeout=1):
        pass


async def test_kafka_floci_gcp_dispatch_consume_and_commit(
    floci_managed_kafka_service: "FlociManagedKafkaService",
) -> "None":
    pytest.importorskip("aiokafka")
    service_fixture = floci_managed_kafka_service
    cluster_id = f"queues-{uuid4().hex[:12]}"
    base = (
        f"{service_fixture.rest_endpoint}/v1/projects/{service_fixture.project_id}"
        f"/locations/{service_fixture.location}/clusters"
    )
    cluster = await asyncio.to_thread(
        _request,
        f"{base}?clusterId={cluster_id}",
        method="POST",
        payload={
            "clusterId": cluster_id,
            "cluster": {
                "capacityConfig": {"vcpuCount": 3, "memoryBytes": 3_221_225_472},
                "gcpConfig": {
                    "accessConfig": {
                        "networkConfigs": [
                            {
                                "subnet": (
                                    f"projects/{service_fixture.project_id}/regions/{service_fixture.location}"
                                    "/subnetworks/default"
                                )
                            }
                        ]
                    }
                },
            },
        },
    )
    cluster_url = f"{base}/{cluster_id}"
    try:
        for _ in range(240):
            cluster = await asyncio.to_thread(_request, cluster_url)
            if cluster.get("state") == "ACTIVE" and cluster.get("bootstrapAddress"):
                break
            await asyncio.sleep(0.5)
        else:
            pytest.fail(f"Floci Managed Kafka cluster did not become ACTIVE: {cluster}")
        bootstrap_host, bootstrap_port = cluster["bootstrapAddress"].rsplit(":", 1)
        try:
            await asyncio.to_thread(_probe, (bootstrap_host, int(bootstrap_port)))
        except OSError as exc:
            pytest.skip(
                "Floci provisioned a real Managed Kafka cluster, but its Docker-bridge bootstrap address "
                f"{cluster['bootstrapAddress']} is unreachable from this test process: {exc}"
            )
        topic = f"dispatch-{uuid4().hex}"
        await asyncio.to_thread(
            _request,
            f"{cluster_url}/topics?topicId={topic}",
            method="POST",
            payload={"topicId": topic, "topic": {"partitionCount": 1, "replicationFactor": 1}},
        )

        @task("tests.kafka.floci.delivered")
        async def delivered() -> "str":
            return "done"

        execution_config = KafkaExecutionConfig(
            bootstrap_servers=cluster["bootstrapAddress"], topic=topic, consumer_group=f"{topic}-workers"
        )
        config = QueueConfig(
            queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
        )
        queue_backend = InMemoryQueueBackend()
        backend = KafkaExecutionBackend(config, execution_config=execution_config)
        try:
            async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as queue_service:
                result = await queue_service.enqueue(delivered.using(execution_backend="kafka"))
                record = await queue_backend.get_task(result.id)
                assert record is not None
                await backend.dispatch(queue_service, record)
                consumer = asyncio.create_task(backend.run_consumer(queue_service, max_concurrency=1, drain_timeout=1))
                try:
                    for _ in range(200):
                        stored = await queue_backend.get_task(result.id)
                        if stored is not None and stored.status == "completed":
                            break
                        await asyncio.sleep(0.05)
                    assert stored is not None
                    assert stored.status == "completed"
                finally:
                    consumer.cancel()
                    await asyncio.gather(consumer, return_exceptions=True)
        finally:
            await backend.close()
    finally:
        await asyncio.to_thread(_request, cluster_url, method="DELETE")
