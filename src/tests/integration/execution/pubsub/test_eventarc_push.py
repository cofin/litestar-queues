"""Joined Floci-GCP Pub/Sub to Eventarc Standard delivery contract."""

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import grpc  # type: ignore[import-untyped]
import httpx
import pytest
from litestar.testing import AsyncTestClient

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task
from litestar_queues.backends import InMemoryQueueBackend
from litestar_queues.execution.pubsub import PubSubExecutionBackend, PubSubExecutionConfig
from tests.helpers.eventarc_receiver import EventarcDelivery, create_eventarc_receiver

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from litestar import Litestar

    from tests.plugins.floci_gcp import FlociGcpService

pytestmark = pytest.mark.anyio


async def test_floci_pubsub_dispatch_reaches_eventarc_http_receiver(  # noqa: PLR0915
    floci_gcp_service: "FlociGcpService",
) -> "None":
    if "eventarc-standard" not in floci_gcp_service.capabilities:
        pytest.skip("Floci-GCP image does not declare Eventarc Standard delivery")
    publisher_module = pytest.importorskip("google.pubsub_v1.services.publisher")
    transport_module = pytest.importorskip("google.pubsub_v1.services.publisher.transports.grpc_asyncio")
    channel = grpc.aio.insecure_channel(floci_gcp_service.grpc_endpoint)
    publisher = publisher_module.PublisherAsyncClient(
        transport=transport_module.PublisherGrpcAsyncIOTransport(channel=channel)
    )
    topic_id = f"{floci_gcp_service.resource_prefix}eventarc-dispatch"
    trigger_id = f"{floci_gcp_service.resource_prefix}eventarc-push"
    topic_path = publisher.topic_path(floci_gcp_service.project_id, topic_id)
    trigger_path = f"/v1/projects/{floci_gcp_service.project_id}/locations/us-central1/triggers/{trigger_id}"
    execution_config = PubSubExecutionConfig(
        project_id=floci_gcp_service.project_id,
        topic_id=topic_id,
        subscription_id="unused-eventarc-push",
        api_endpoint=floci_gcp_service.grpc_endpoint,
        api_insecure=True,
    )
    config = QueueConfig(
        queue_backend="memory", execution_backend=execution_config, worker=WorkerConfig(placement="external")
    )
    queue_backend = InMemoryQueueBackend()
    backend = PubSubExecutionBackend(config, execution_config=execution_config, publisher=publisher)

    @task("tests.eventarc.floci.delivered")
    async def delivered() -> str:
        return "done"

    try:
        await publisher.create_topic(request={"name": topic_path})
        async with QueueService(config, queue_backend=queue_backend, execution_backend=backend) as service:
            receiver = create_eventarc_receiver(queue_service=service, topic=topic_path)
            async with _serve(receiver) as receiver_port:
                receiver_uri = f"http://{floci_gcp_service.docker_host}:{receiver_port}/eventarc/pubsub"
                async with httpx.AsyncClient(base_url=floci_gcp_service.rest_endpoint) as client:
                    trigger_created = False
                    try:
                        response = await client.post(
                            trigger_path.rsplit("/", maxsplit=1)[0],
                            params={"triggerId": trigger_id},
                            json={
                                "eventFilters": [
                                    {"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"},
                                    {"attribute": "topic", "value": topic_path},
                                ],
                                "destination": {"httpEndpoint": {"uri": receiver_uri}},
                            },
                        )
                        response.raise_for_status()
                        trigger_created = True
                        result = await service.enqueue(delivered.using(execution_backend="pubsub"))
                        record = await queue_backend.get_task(result.id)
                        assert record is not None
                        attempt = await backend.dispatch(service, record)
                        assert attempt is not None
                        stored: Any | None = None
                        for _ in range(100):
                            stored = await queue_backend.get_task(result.id)
                            if stored is not None and stored.status == "completed":
                                break
                            await asyncio.sleep(0.05)
                        assert stored is not None
                        assert stored.status == "completed", receiver.state.eventarc_receiver_probe.rejections
                        assert stored.result == "done"
                        assert receiver.state.eventarc_receiver_probe.deliveries == [
                            EventarcDelivery(task_id=result.id, attempt=attempt)
                        ]
                    finally:
                        if trigger_created:
                            delete_response = await client.delete(trigger_path)
                            delete_response.raise_for_status()
    finally:
        try:
            await publisher.delete_topic(request={"topic": topic_path})
        finally:
            await publisher.transport.close()
            await channel.close()


@asynccontextmanager
async def _serve(app: "Litestar") -> "AsyncGenerator[int, None]":
    """Expose the ASGI receiver while explicitly declining Floci's h2c upgrade.

    Floci's Java HTTP client asks to upgrade cleartext requests to HTTP/2. The
    tiny HTTP/1 bridge keeps the original POST body intact and passes the
    request into Litestar's normal ASGI test transport.

    Yields:
        The Docker-reachable receiver port.
    """
    async with AsyncTestClient(app=app) as client:

        async def forward(reader: "asyncio.StreamReader", writer: "asyncio.StreamWriter") -> "None":
            try:
                head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
                lines = head.decode("latin-1").split("\r\n")
                method, path, _protocol = lines[0].split(" ", maxsplit=2)
                headers = dict(line.split(":", maxsplit=1) for line in lines[1:] if line)
                headers = {name.strip(): value.strip() for name, value in headers.items()}
                length = int(headers.get("Content-Length", "0"))
                body = await reader.readexactly(length)
                response = await client.request(method, path, headers=headers, content=body)
                writer.write(
                    f"HTTP/1.1 {response.status_code} Eventarc\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(forward, "0.0.0.0", 0)
        port = server.sockets[0].getsockname()[1]
        async with server:
            yield port
