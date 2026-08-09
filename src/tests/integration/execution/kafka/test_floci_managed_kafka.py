import asyncio
import json
from contextlib import suppress
from http.client import RemoteDisconnected
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import pytest

from tests.integration.execution.kafka._contract import assert_kafka_transport_contract

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


async def _probe(address: "str") -> "None":
    aiokafka = pytest.importorskip("aiokafka")
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=address, request_timeout_ms=1_000)
    try:
        try:
            await asyncio.wait_for(producer.start(), timeout=3)
        except Exception as exc:
            msg = f"Kafka metadata probe failed for {address}"
            raise OSError(msg) from exc
    finally:
        with suppress(Exception):
            await producer.stop()


def _container_for_ip(service: "FlociManagedKafkaService", address: "str") -> "Any | None":
    for container in service.docker_client.containers.list(all=True):
        container.reload()
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        if any(network.get("IPAddress") == address for network in networks.values()):
            return container
    return None


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
    child: "Any | None" = None
    try:
        for _ in range(240):
            cluster = await asyncio.to_thread(_request, cluster_url)
            if cluster.get("state") == "ACTIVE" and cluster.get("bootstrapAddress"):
                break
            await asyncio.sleep(0.5)
        else:
            pytest.fail(f"Floci Managed Kafka cluster did not become ACTIVE: {cluster}")
        bootstrap_host, _ = cluster["bootstrapAddress"].rsplit(":", 1)
        child = await asyncio.to_thread(_container_for_ip, service_fixture, bootstrap_host)
        assert child is not None
        try:
            await _probe(cluster["bootstrapAddress"])
        except (OSError, TimeoutError) as exc:
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

        await assert_kafka_transport_contract(bootstrap_servers=cluster["bootstrapAddress"], topic=topic)
    finally:
        with suppress(RemoteDisconnected, URLError):
            await asyncio.to_thread(_request, cluster_url, method="DELETE")
        for _ in range(40):
            try:
                await asyncio.to_thread(_request, cluster_url)
            except RuntimeError as exc:
                if "with 404" in str(exc):
                    break
                raise
            except (RemoteDisconnected, URLError):
                await asyncio.sleep(0.25)
                continue
            await asyncio.sleep(0.25)
        else:
            pytest.fail("Floci Managed Kafka cluster remained after DELETE")
        if child is not None:
            for _ in range(40):
                try:
                    await asyncio.to_thread(child.reload)
                except Exception:  # noqa: BLE001 -- Docker SDK not-found subclasses vary by daemon
                    break
                await asyncio.sleep(0.25)
            else:
                pytest.fail(f"Floci Managed Kafka child container {child.id} remained after cluster deletion")
