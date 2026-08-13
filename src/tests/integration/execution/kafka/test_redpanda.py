from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tests.integration.execution.kafka._contract import assert_kafka_transport_contract

if TYPE_CHECKING:
    from tests.plugins.redpanda import RedpandaService

pytestmark = pytest.mark.anyio


async def test_kafka_redpanda_dispatch_consume_and_commit(redpanda_service: "RedpandaService") -> "None":
    topic = f"{redpanda_service.topic_prefix}dispatch_{uuid4().hex}"
    await assert_kafka_transport_contract(bootstrap_servers=redpanda_service.bootstrap_servers, topic=topic)
