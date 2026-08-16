import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from litestar_queues.service import QueueService


class RunJobEnv(TypedDict):
    name: "str"
    value: "str"


class RunJobContainerOverride(TypedDict):
    env: "list[RunJobEnv]"


class RunJobOverrides(TypedDict):
    container_overrides: "list[RunJobContainerOverride]"
    timeout: "str"


class RunJobRequest(TypedDict):
    name: "str"
    overrides: "RunJobOverrides"


@dataclass(slots=True)
class FakeCloudRunExecution:
    name: "str" = "projects/test/locations/us-central1/jobs/worker/executions/run-1"
    succeeded_count: "int" = 0
    failed_count: "int" = 0
    cancelled_count: "int" = 0
    conditions: "list[object] | None" = None


class FakeOperation:
    def __init__(
        self, execution: "FakeCloudRunExecution", *, block_result: "bool" = False, metadata: "object | None" = None
    ) -> "None":
        self.execution = execution
        self.metadata = execution if metadata is None else metadata
        self.result_called = False
        self._result_ready = asyncio.Event() if block_result else None

    async def result(self) -> "FakeCloudRunExecution":
        self.result_called = True
        if self._result_ready is not None:
            await self._result_ready.wait()
        return self.execution


class FakeJobsClient:
    def __init__(
        self,
        execution: "FakeCloudRunExecution | None" = None,
        *,
        error: "Exception | None" = None,
        block_result: "bool" = False,
        metadata: "object | None" = None,
    ) -> "None":
        self.execution = execution or FakeCloudRunExecution()
        self.error = error
        self.block_result = block_result
        self.metadata = metadata
        self.requests: "list[RunJobRequest]" = []
        self.operations: "list[FakeOperation]" = []

    async def run_job(self, *, request: "RunJobRequest") -> "FakeOperation":
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        operation = FakeOperation(self.execution, block_result=self.block_result, metadata=self.metadata)
        self.operations.append(operation)
        return operation


class FakeExecutionsClient:
    def __init__(
        self, execution: "FakeCloudRunExecution | Exception", *, cancel_error: "Exception | None" = None
    ) -> "None":
        self.execution = execution
        self.names: "list[str]" = []
        self.cancelled_names: "list[str]" = []
        self.cancel_error = cancel_error

    async def get_execution(self, *, name: "str") -> "FakeCloudRunExecution":
        self.names.append(name)
        if isinstance(self.execution, Exception):
            raise self.execution
        return self.execution

    async def cancel_execution(self, *, name: "str") -> "object":
        self.cancelled_names.append(name)
        if self.cancel_error is not None:
            raise self.cancel_error
        # We need to return a FakeOperation, which requires a FakeCloudRunExecution.
        # If self.execution is an Exception, we can just return a dummy execution.
        execution = self.execution if isinstance(self.execution, FakeCloudRunExecution) else FakeCloudRunExecution()
        return FakeOperation(execution)


class NoopServiceContext:
    def __init__(self, service: "QueueService") -> "None":
        self.service = service

    async def __aenter__(self) -> "QueueService":
        return self.service

    async def __aexit__(self, *_exc_info: object) -> "None":
        return None


NotFoundError = type("NotFound", (Exception,), {})


def env_map(request: "RunJobRequest") -> "dict[str, str]":
    env = request["overrides"]["container_overrides"][0]["env"]
    return {item["name"]: item["value"] for item in env}


def task_id_from_request(request: "RunJobRequest") -> "str":
    """Return the task id carried by a captured run-job request.

    Returns:
        The queued record id the request dispatches.
    """
    return env_map(request)["QUEUES_TASK_ID"]
