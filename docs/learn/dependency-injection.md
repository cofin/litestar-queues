# Task Dependency Injection

Background tasks execute outside the scope of an incoming HTTP request. `litestar-queues` provides two complementary hooks on [`QueueConfig`](../reference/plugin-and-config.md#litestar_queues.config.QueueConfig) for injecting database sessions, API clients, and domain services into task executions:

- **`task_dependency_provider`**: Attempt-scoped async context manager that sets up and tears down resources around each task execution attempt.
- **`task_dependency_resolver`**: Stateless async callable that returns a dictionary of extra keyword arguments before the task body runs.

!!! warning "Mutually Exclusive Configuration"
    Configuring both `task_dependency_provider` and `task_dependency_resolver` on the same `QueueConfig` raises `QueueConfigurationError` during initialization.

## Runnable Example

```python title="docs/examples/learn/dependency_injection.py"
--8<-- "docs/examples/learn/dependency_injection.py"
```

## Attempt-Scoped Providers vs Stateless Resolvers

| Capability | `task_dependency_provider` | `task_dependency_resolver` |
| :--- | :--- | :--- |
| **Callable Shape** | `(task, record, context) -> AbstractAsyncContextManager[Mapping[str, object]]` | `async (task, record, context) -> Mapping[str, object]` |
| **Per-Attempt Teardown** | Yes — `__aexit__` is awaited `exactly once` after every attempt outcome | No — returns a mapping with no post-attempt teardown hook |
| **Service-Level Lifecycle** | Optional `open()` and `close()` hooks managed by `QueueService` | None |
| **Attempt Timeout Coverage** | Both `__aenter__`, task body, and `__aexit__` run inside `task.timeout` | Resolution and task body run inside `task.timeout` |
| **Recommended Use Case** | Database transactions, unit-of-work sessions, leased connections | Stateless configuration objects, pre-built singletons, request metadata |

## Automatic Signature Filtering

Both `task_dependency_provider` and `task_dependency_resolver` can yield a shared mapping of application dependencies without coupling every task signature to the full dictionary:

1. **Standard Signatures**: When a task function does not declare a `**kwargs` (`VAR_KEYWORD`) parameter, `litestar-queues` filters the yielded mapping to inject only keys matching parameters declared in the task's `inspect.Signature` that were not already supplied explicitly at `enqueue()` time.
2. **Variadic Signatures (`**kwargs`)**: When a task declares `**kwargs`, all non-overlapping keys from the provider or resolver mapping are forwarded.
3. **Explicit Enqueue Precedence**: Arguments passed explicitly to `await queue_service.enqueue(task_fn, ...)` always take precedence over keys yielded by a provider or resolver.

## Lifecycle & Teardown Guarantees

When `task_dependency_provider` is configured, `QueueService` enforces strict resource safety:

- **Guaranteed Exit**: Once `__aenter__` succeeds, `__aexit__` is awaited `exactly once` for every attempt outcome: completion, retryable exception, `non_retryable()` failure, `job_cancelled()`, attempt timeout, external cancellation, heartbeat claim loss, and worker shutdown interruption.
- **Coroutine Cancellation Semantics**: In `__aexit__(exc_type, exc_val, exc_tb)`, `exc_type` receives `asyncio.CancelledError` for attempt timeout, external cancellation, heartbeat claim loss, and worker shutdown alike because all four interrupt the running coroutine via cancellation. Inspect `record` and `context` (for example `context.is_cancelled`) rather than branching on `exc_type` alone.
- **No Exception Suppression**: Returning a truthy value from `__aexit__` is ignored by `QueueService`; the provider is strictly a resource lifecycle scope, never an exception filter.
- **Process-Level `open()` / `close()`**: If the provider object defines `open()` and/or `close()` methods (sync or async), `QueueService.open()` invokes `provider.open()` before opening queue backends and `QueueService.close()` invokes `provider.close()` during shutdown (including partial-open rollback).
