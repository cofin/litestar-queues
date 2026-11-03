"""Defining tasks with @task and enqueuing work through QueueService."""

from litestar_queues import QueueConfig, QueueService, WorkerConfig, task


@task("emails.send_welcome", queue="emails")
async def send_welcome_email(user_id: int, email: str) -> dict[str, object]:
    """Send a welcome email and return delivery metadata."""
    return {"user_id": user_id, "email": email, "delivered": True}


async def main() -> None:
    """Enqueue a task by callable and by registered name."""
    config = QueueConfig(
        queue_backend="memory", execution_backend="immediate", worker=WorkerConfig(placement="external")
    )
    async with QueueService(config=config) as service:
        first = await service.enqueue(send_welcome_email, 42, email="ada@example.com")
        await first.refresh()
        assert first.status == "completed"
        assert first.result == {"user_id": 42, "email": "ada@example.com", "delivered": True}

        second = await service.enqueue("emails.send_welcome", 99, email="grace@example.com")
        await second.refresh()
        assert second.status == "completed"
        assert second.result == {"user_id": 99, "email": "grace@example.com", "delivered": True}
