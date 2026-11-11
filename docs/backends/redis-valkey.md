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
| `{prefix}:tasks` | Set | Registered task ID index across all queues and statuses |
| `{prefix}:status:{status}` | Set | Task IDs grouped by `TaskStatus` (`pending`, `scheduled`, `running`, `completed`, `failed`, `cancelled`, `expired`) |
| `{prefix}:queue:{sha256}` | Set | Task IDs grouped by SHA-256 hashed logical queue name |
| `{prefix}:ready` | Sorted Set (`ZSET`) | Due pending tasks ordered by composite priority and `queued_at` score |
| `{prefix}:scheduled` | Sorted Set (`ZSET`) | Future-scheduled and retry-delayed tasks scored by `scheduled_at` epoch milliseconds |
| `{prefix}:keys` | Hash | Active deduplication `key` to `task_id` mapping for `unique_until="terminal"` |
| `{prefix}:task_reservations` | Hash | Permanent `unique_until="forever"` `TaskReservation` payloads |
| `{prefix}:maintenance:running` / `external` / `expiry` / `terminal` | Sorted Sets (`ZSET`) | Timestamp-indexed maintenance sweeps (`cleanup_terminal`, stale recovery, external dispatch repair, queue expiry) |
| `{prefix}:terminal_meta` | Hash | JSON `{"status","queue_index_key","key"}` metadata per terminal task ID for TTL drift reconciliation |

## Native Terminal TTL & Secondary Index Pruning

Setting `terminal_ttl` (in seconds, for example `terminal_ttl=86_400.0`) on `RedisBackendConfig` or `ValkeyBackendConfig` expires finished task hashes natively without leaving orphan entries in secondary sets or status counters:

1. **Atomic `PEXPIRE` on Terminal Transition**: When a task reaches `completed`, `failed`, `cancelled`, or `expired`, the Lua transition script applies `PEXPIRE` (`max(1, int(terminal_ttl * 1000))` milliseconds) to `{prefix}:task:{task_id}`, records the completion score in `{prefix}:maintenance:terminal`, and writes JSON `{"status","queue_index_key","key"}` into `{prefix}:terminal_meta`.
2. **Opportunistic Secondary-Index Pruning & Live-Hash Listing**: Lua terminal transitions sweep up to `16` expired entries (`LIMIT 0 16`) from `{prefix}:maintenance:terminal`, `get_statistics()` prunes up to `64` entries via `_prune_expired_terminal()` using `{prefix}:terminal_meta`, and `cleanup_terminal` sweeps expired terminal sets bounded by cutoff and limit. For paginated inspection, `list_tasks` intersects `{prefix}:status:{status}`, `{prefix}:queue:{sha256}`, and `{prefix}:tasks` and validates live `{prefix}:task:{task_id}` hashes via batched `HMGET` calls so records evicted asynchronously by `PEXPIRE` never inflate `total` or produce short pages.
3. **Never Call Raw `EXPIRE` Externally**: Do not call raw Redis `EXPIRE` or `DEL` on `{prefix}:task:{task_id}` keys outside `litestar-queues`; always configure `terminal_ttl` or run `cleanup_terminal` so secondary indexes and statistics counters stay consistent.

When `WorkerConfig.terminal_retention` is `None`, workers also inherit `terminal_ttl` for periodic `cleanup_terminal()` sweeps.

## Rebuilding Maintenance and External-Dispatch Indexes

To rebuild secondary maintenance and external-dispatch indexes online across existing task hashes in Redis or Valkey, call `await backend.rebuild_maintenance_indexes()`:

```python
from litestar_queues.backends.redis import RedisQueueBackend

assert isinstance(backend, RedisQueueBackend)
reindexed_count = await backend.rebuild_maintenance_indexes()
```

## Pub/Sub Worker Wakeups vs Browser Event Streams

Setting `worker_wakeups=True` (the default) uses Redis/Valkey Pub/Sub internally so workers wake immediately on `enqueue()` and `TaskResult.wait()` resolves without polling delay.

If you also expose live SSE or WebSocket event streams to browsers (`EventStreamConfig(mount_routes=True)`), register a Litestar `ChannelsPlugin` backed by `RedisChannelsBackend` on your `Litestar` application so browser stream fan-out shares the same Redis/Valkey instance across web workers. See [Realtime SSE & WebSocket Streams](../events/streams.md) for the complete wiring.
