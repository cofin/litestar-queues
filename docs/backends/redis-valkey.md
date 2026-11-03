# Redis & Valkey Storage Backends

The Redis ([`RedisQueueBackend`](../reference/backends.md#litestar_queues.backends.redis.RedisQueueBackend)) and Valkey ([`ValkeyQueueBackend`](../reference/backends.md#litestar_queues.backends.valkey.ValkeyQueueBackend)) backends deliver sub-5ms task pickup using atomic server-side Lua scripts, sorted-set priority scheduling, and native Pub/Sub wakeups.

## Runnable Example

```python title="docs/examples/backends/redis_valkey_config.py"
--8<-- "docs/examples/backends/redis_valkey_config.py"
```

!!! tip "Install the C Parser for Maximum Throughput"
    Install `hiredis` alongside `litestar-queues[redis]` (or `libvalkey` with `litestar-queues[valkey]`) so the async client uses the native C protocol parser automatically for multi-record Lua claim and heartbeat pipelines.

## Key Prefix Isolation

Both [`RedisBackendConfig`](../reference/backends.md#litestar_queues.backends.redis.RedisBackendConfig) and [`ValkeyBackendConfig`](../reference/backends.md#litestar_queues.backends.valkey.ValkeyBackendConfig) namespace every Redis/Valkey data structure under `key_prefix` (defaulting to `"litestar_queues"`):

| Key Pattern | Data Structure | Purpose |
| :--- | :--- | :--- |
| `{prefix}:task:{task_id}` | Hash | Serialized `QueuedTaskRecord` fields, retry counters, and metadata |
| `{prefix}:pending:{queue}` | Sorted Set (`ZSET`) | Due tasks ordered by composite priority and `queued_at` score |
| `{prefix}:scheduled` | Sorted Set (`ZSET`) | Delayed and recurring tasks ordered by `scheduled_at` epoch milliseconds |
| `{prefix}:running` | Sorted Set (`ZSET`) | Active claims scored by `heartbeat_at` epoch milliseconds |
| `{prefix}:running_external` | Set / ZSET | Active records executing on external serverless transports (`cloudtasks`, `cloudrun`) |
| `{prefix}:terminal` | Sorted Set (`ZSET`) | Terminal task IDs scored by `completed_at` for retention cleanup |
| `{prefix}:terminal_meta` | Hash | Packed `{status}\x1f{queue}` metadata per terminal task ID for TTL drift reconciliation |
| `{prefix}:unique:{key}` | String | Active `unique_until="terminal"` deduplication lock |
| `{prefix}:task_reservations` | Hash | Permanent `unique_until="forever"` `TaskReservation` payloads |

## Native Terminal TTL & Secondary Index Pruning

Setting `terminal_ttl` (in seconds, for example `terminal_ttl=86_400.0`) on `RedisBackendConfig` or `ValkeyBackendConfig` expires finished task hashes natively without leaving orphan entries in secondary sorted sets or status counters:

1. **Atomic `PEXPIRE` on Terminal Transition**: When a task reaches `completed`, `failed`, `cancelled`, or `expired`, the Lua transition script applies `PEXPIRE` (`int(ceil(terminal_ttl * 1000))` milliseconds) to `{prefix}:task:{task_id}`, indexes the completion timestamp in `{prefix}:terminal`, and records `{status}\x1f{queue}` in `{prefix}:terminal_meta`.
2. **Opportunistic Secondary-Index Pruning**: As Redis or Valkey evicts expired `{prefix}:task:{task_id}` keys in the background, read and maintenance paths (`get_statistics`, `list_tasks`, `cleanup_completed`) inspect expired members in `{prefix}:terminal`, decrement the corresponding status and queue counters using `{prefix}:terminal_meta`, and remove the stale entries from `{prefix}:terminal` and `{prefix}:terminal_meta`.
3. **Never Call Raw `EXPIRE` Externally**: Do not call raw Redis `EXPIRE` or `DEL` on `{prefix}:task:{task_id}` keys outside `litestar-queues`; always configure `terminal_ttl` or run `cleanup_completed` so secondary indexes and statistics counters stay consistent.

When `WorkerConfig.terminal_retention` is `None`, workers also inherit `terminal_ttl` for periodic `cleanup_terminal()` sweeps.

## Rebuilding Maintenance and External-Dispatch Indexes

To rebuild secondary maintenance and external-dispatch indexes online across existing task hashes in Redis or Valkey, call `await backend.rebuild_maintenance_indexes(batch_size=500)`:

```python
from litestar_queues.backends.redis import RedisQueueBackend

assert isinstance(backend, RedisQueueBackend)
reindexed_count = await backend.rebuild_maintenance_indexes(batch_size=500)
```

## Pub/Sub Worker Wakeups vs Browser Event Streams

Setting `worker_wakeups=True` (the default) uses Redis/Valkey Pub/Sub internally so workers wake immediately on `enqueue()` and `TaskResult.wait()` resolves without polling delay.

If you also expose live SSE or WebSocket event streams to browsers (`EventStreamConfig(mount_routes=True)`), register a Litestar `ChannelsPlugin` backed by `RedisChannelsBackend` on your `Litestar` application so browser stream fan-out shares the same Redis/Valkey instance across web workers. See [Realtime SSE & WebSocket Streams](../events/streams.md) for the complete wiring.
