import asyncio
import contextlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar_queues.service import QueueService

__all__ = ("Worker",)


class Worker:
    """Local in-process queue worker."""

    __slots__ = ("_batch_size", "_is_running", "_poll_interval", "_service", "_stop_event")

    def __init__(self, service: "QueueService", *, batch_size: int = 10, poll_interval: float = 0.1) -> None:
        """Initialize the worker."""
        self._service = service
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._stop_event = asyncio.Event()
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """Return whether the worker loop is active."""
        return self._is_running

    async def start(self) -> None:
        """Run the worker loop until stopped or cancelled."""
        self._is_running = True
        self._stop_event.clear()
        try:
            while not self._stop_event.is_set():
                processed = await self.run_once()
                if processed == 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval)
        finally:
            self._is_running = False

    async def stop(self) -> None:
        """Stop the worker loop."""
        self._stop_event.set()

    async def run_once(self) -> int:
        """Process one batch of due tasks.

        Returns:
            Number of claimed task records.
        """
        storage = self._service.get_storage_backend()
        records = await storage.list_pending(limit=self._batch_size)
        processed = 0
        for record in records:
            claimed = await storage.claim_task(record.id)
            if claimed is None:
                continue
            await self._service.get_execution_backend().execute(self._service, claimed)
            processed += 1
        return processed
