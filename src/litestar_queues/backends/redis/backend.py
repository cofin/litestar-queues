"""Redis queue backend.

Stores queued task records in a Redis-protocol key-value server. The
implementation lives directly on ``RedisQueueBackend``; the Valkey
backend inherits from this class and only swaps the client factory and
``_backend_name`` ClassVar.
"""

import asyncio
import inspect
import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast
from uuid import UUID

from litestar_queues.backends._notification_wait import PendingNativeRead
from litestar_queues.backends.base import (
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    record_matches_filters,
    stale_requeue_error,
    stale_requeue_priority,
)
from litestar_queues.backends.redis.config import RedisBackendConfig as _RedisBackendConfig
from litestar_queues.backends.redis.event_log import RedisQueueEventLog, hashed_index_value
from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskReservation,
    TaskStatus,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from litestar_queues.backends.redis._typing import RedisClientLike, RedisPipelineLike, RedisPubSubLike
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch, TaskRequest

__all__ = ("RedisQueueBackend",)

_DUE_STATUSES = {"pending", "scheduled"}
_STATUS_VALUES = {"cancelled", "completed", "failed", "pending", "running", "scheduled"}
_TERMINAL_STATUSES = {"cancelled", "completed", "failed"}
_MAINTENANCE_INDEX_VERSION = "1"
_TOUCH_HEARTBEAT_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'running' then
    return 0
end

local expected_retry_count = ARGV[1]
if expected_retry_count ~= '' then
    local retry_count = redis.call('HGET', KEYS[1], 'retry_count')
    if retry_count ~= expected_retry_count then
        return 0
    end
end

local heartbeat_at = ARGV[2]
local metadata_patch_json = ARGV[3]
local heartbeat_score = ARGV[4]
local prefix = ARGV[5]
local task_id = ARGV[6]
if metadata_patch_json ~= '' then
    local metadata_json = redis.call('HGET', KEYS[1], 'metadata')
    local metadata = {}
    if metadata_json and metadata_json ~= '' then
        local ok, decoded = pcall(cjson.decode, metadata_json)
        if ok and type(decoded) == 'table' then
            metadata = decoded
        end
    end

    local ok_patch, metadata_patch = pcall(cjson.decode, metadata_patch_json)
    if ok_patch and type(metadata_patch) == 'table' then
        for key, value in pairs(metadata_patch) do
            metadata[key] = value
        end
    end
    redis.call('HSET', KEYS[1], 'heartbeat_at', heartbeat_at, 'heartbeat_score', heartbeat_score,
        'metadata', cjson.encode(metadata))
else
    redis.call('HSET', KEYS[1], 'heartbeat_at', heartbeat_at, 'heartbeat_score', heartbeat_score)
end
redis.call('ZADD', prefix .. ':maintenance:running', heartbeat_score, task_id)

return 1
"""
_CLAIM_SCRIPT = """
local ready = KEYS[1]
local scheduled = KEYS[2]
local prefix = ARGV[1]
local now_ms = tonumber(ARGV[2])
local now_iso = ARGV[3]
local limit = tonumber(ARGV[4])
local eb_filter = ARGV[5]
local window = tonumber(ARGV[6])

local queue_filter = {}
local has_queue_filter = false
for i = 7, #ARGV do
    queue_filter[ARGV[i]] = true
    has_queue_filter = true
end

local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now_ms)
for _, id in ipairs(due) do
    local hkey = prefix .. ':task:' .. id
    local status = redis.call('HGET', hkey, 'status')
    if status == 'scheduled' or status == 'pending' then
        local ready_score = redis.call('HGET', hkey, 'ready_score')
        if ready_score then
            redis.call('ZADD', ready, ready_score, id)
        end
        if status == 'scheduled' then
            redis.call('SREM', prefix .. ':status:scheduled', id)
            redis.call('SADD', prefix .. ':status:pending', id)
            redis.call('HSET', hkey, 'status', 'pending')
        end
    end
    redis.call('ZREM', scheduled, id)
end

local claimed = {}
local candidates = redis.call('ZRANGE', ready, 0, window)
for _, id in ipairs(candidates) do
    if #claimed >= limit then break end
    local hkey = prefix .. ':task:' .. id
    local status = redis.call('HGET', hkey, 'status')
    if status ~= 'pending' then
        redis.call('ZREM', ready, id)
    else
        local eb = redis.call('HGET', hkey, 'execution_backend')
        local q = redis.call('HGET', hkey, 'queue')
        local eb_ok = (eb_filter == '' or eb == eb_filter)
        local q_ok = (not has_queue_filter or queue_filter[q] == true)
        if eb_ok and q_ok then
            redis.call('HSET', hkey, 'status', 'running', 'started_at', now_iso, 'heartbeat_at', now_iso,
                'started_score', now_ms, 'heartbeat_score', now_ms)
            redis.call('SREM', prefix .. ':status:pending', id)
            redis.call('SADD', prefix .. ':status:running', id)
            redis.call('ZREM', ready, id)
            redis.call('ZADD', prefix .. ':maintenance:running', now_ms, id)
            redis.call('ZREM', prefix .. ':maintenance:terminal', id)
            local execution_ref = redis.call('HGET', hkey, 'execution_ref')
            if execution_ref and execution_ref ~= '' then
                redis.call('ZADD', prefix .. ':maintenance:external', now_ms, id)
            else
                redis.call('ZREM', prefix .. ':maintenance:external', id)
            end
            claimed[#claimed + 1] = id
        end
    end
end
return claimed
"""
_COMPLETE_SCRIPT = """
local hkey = KEYS[1]
local prefix = ARGV[1]
local task_id = ARGV[2]
local expected = ARGV[3]
local completed_at = ARGV[4]
local result_json = ARGV[5]
local channel = ARGV[6]
local completed_score = ARGV[7]

local status = redis.call('HGET', hkey, 'status')
if status ~= 'running' then
    return {0}
end
if expected ~= '' then
    local retry_count = redis.call('HGET', hkey, 'retry_count')
    if retry_count ~= expected then
        return {0}
    end
end
redis.call('HSET', hkey, 'status', 'completed', 'completed_at', completed_at,
    'completed_score', completed_score, 'heartbeat_at', '', 'heartbeat_score', '0',
    'result', result_json, 'error', '')
redis.call('SREM', prefix .. ':status:running', task_id)
redis.call('SADD', prefix .. ':status:completed', task_id)
redis.call('ZREM', prefix .. ':maintenance:running', task_id)
redis.call('ZREM', prefix .. ':maintenance:external', task_id)
redis.call('ZADD', prefix .. ':maintenance:terminal', completed_score, task_id)
redis.call('PUBLISH', channel, task_id)
return {1}
"""
_FAIL_SCRIPT = """
local hkey = KEYS[1]
local ready = KEYS[2]
local prefix = ARGV[1]
local task_id = ARGV[2]
local expected = ARGV[3]
local error = ARGV[4]
local retry = ARGV[5]
local completed_at = ARGV[6]
local channel = ARGV[7]
local completed_score = ARGV[8]

local status = redis.call('HGET', hkey, 'status')
if status ~= 'running' then
    return {0, ''}
end
local retry_count = tonumber(redis.call('HGET', hkey, 'retry_count')) or 0
if expected ~= '' and tostring(retry_count) ~= expected then
    return {0, ''}
end
redis.call('HSET', hkey, 'error', error)
local max_retries = tonumber(redis.call('HGET', hkey, 'max_retries')) or 0
if retry == '1' and retry_count < max_retries then
    local new_retry_count = retry_count + 1
    redis.call('HSET', hkey, 'status', 'pending', 'retry_count', new_retry_count,
        'started_at', '', 'started_score', '0', 'heartbeat_at', '', 'heartbeat_score', '0')
    redis.call('SREM', prefix .. ':status:running', task_id)
    redis.call('SADD', prefix .. ':status:pending', task_id)
    redis.call('ZREM', prefix .. ':maintenance:running', task_id)
    redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
    local execution_ref = redis.call('HGET', hkey, 'execution_ref')
    if execution_ref and execution_ref ~= '' then
        local created_score = redis.call('HGET', hkey, 'created_score') or '0'
        redis.call('ZADD', prefix .. ':maintenance:external', created_score, task_id)
    else
        redis.call('ZREM', prefix .. ':maintenance:external', task_id)
    end
    local ready_score = redis.call('HGET', hkey, 'ready_score')
    if ready_score then
        redis.call('ZADD', ready, ready_score, task_id)
    end
    return {1, 'pending'}
end
redis.call('HSET', hkey, 'status', 'failed', 'completed_at', completed_at,
    'completed_score', completed_score, 'heartbeat_at', '', 'heartbeat_score', '0')
redis.call('SREM', prefix .. ':status:running', task_id)
redis.call('SADD', prefix .. ':status:failed', task_id)
redis.call('ZREM', prefix .. ':maintenance:running', task_id)
redis.call('ZREM', prefix .. ':maintenance:external', task_id)
redis.call('ZADD', prefix .. ':maintenance:terminal', completed_score, task_id)
redis.call('PUBLISH', channel, task_id)
return {1, 'failed'}
"""
_ENQUEUE_SCRIPT = """
local ready = KEYS[1]
local scheduled = KEYS[2]
local prefix = ARGV[1]
local task_id = ARGV[2]
local status = ARGV[3]
local due = ARGV[4]
local score = ARGV[5]
local channel = ARGV[6]
local notify_payload = ARGV[7]
local publish = ARGV[8]
local maintenance_version = ARGV[9]
local hkey = prefix .. ':task:' .. task_id
local maintenance_version_key = prefix .. ':maintenance:index-version'
if not redis.call('GET', maintenance_version_key) and redis.call('SCARD', prefix .. ':tasks') == 0 then
    redis.call('SET', maintenance_version_key, maintenance_version)
end
redis.call('HSET', hkey, unpack(ARGV, 10))
redis.call('SADD', prefix .. ':tasks', task_id)
redis.call('SADD', prefix .. ':status:' .. status, task_id)
if due == '1' then
    redis.call('ZADD', ready, score, task_id)
    if publish == '1' then
        redis.call('PUBLISH', channel, notify_payload)
    end
else
    redis.call('ZADD', scheduled, score, task_id)
end
return {1}
"""
_ENQUEUE_KEYED_SCRIPT = """
local ready = KEYS[1]
local scheduled = KEYS[2]
local prefix = ARGV[1]
local task_id = ARGV[2]
local status = ARGV[3]
local due = ARGV[4]
local score = ARGV[5]
local channel = ARGV[6]
local notify_payload = ARGV[7]
local publish = ARGV[8]
local dedup_key = ARGV[9]
local maintenance_version = ARGV[10]
local keys_hash = prefix .. ':keys'
local existing_id = redis.call('HGET', keys_hash, dedup_key)
if existing_id then
    local existing_status = redis.call('HGET', prefix .. ':task:' .. existing_id, 'status')
    if existing_status == 'pending' or existing_status == 'scheduled' or existing_status == 'running' then
        return {0, existing_id}
    end
end
redis.call('HSET', keys_hash, dedup_key, task_id)
local hkey = prefix .. ':task:' .. task_id
local maintenance_version_key = prefix .. ':maintenance:index-version'
if not redis.call('GET', maintenance_version_key) and redis.call('SCARD', prefix .. ':tasks') == 0 then
    redis.call('SET', maintenance_version_key, maintenance_version)
end
redis.call('HSET', hkey, unpack(ARGV, 11))
redis.call('SADD', prefix .. ':tasks', task_id)
redis.call('SADD', prefix .. ':status:' .. status, task_id)
if due == '1' then
    redis.call('ZADD', ready, score, task_id)
    if publish == '1' then
        redis.call('PUBLISH', channel, notify_payload)
    end
else
    redis.call('ZADD', scheduled, score, task_id)
end
return {1, task_id}
"""
_TRANSITION_SCRIPT = """
local hkey = KEYS[1]
local ready = KEYS[2]
local scheduled = KEYS[3]
local prefix = ARGV[1]
local task_id = ARGV[2]
local expected_status = ARGV[3]
local expected_retry = ARGV[4]
local new_status = ARGV[5]
local zset_action = ARGV[6]
local score = ARGV[7]
local channel = ARGV[8]
local payload = ARGV[9]

local status = redis.call('HGET', hkey, 'status')
if not status then
    return {0}
end
if expected_status ~= '' and status ~= expected_status then
    return {0}
end
if expected_retry ~= '' then
    local retry_count = redis.call('HGET', hkey, 'retry_count')
    if retry_count ~= expected_retry then
        return {0}
    end
end
if new_status ~= '' then
    redis.call('SREM', prefix .. ':status:' .. status, task_id)
    redis.call('SADD', prefix .. ':status:' .. new_status, task_id)
    redis.call('HSET', hkey, 'status', new_status)
end
if #ARGV >= 10 then
    redis.call('HSET', hkey, unpack(ARGV, 10))
end
if zset_action == 'ready' then
    redis.call('ZADD', ready, score, task_id)
    redis.call('ZREM', scheduled, task_id)
elseif zset_action == 'scheduled' then
    redis.call('ZADD', scheduled, score, task_id)
    redis.call('ZREM', ready, task_id)
elseif zset_action == 'remove' then
    redis.call('ZREM', ready, task_id)
    redis.call('ZREM', scheduled, task_id)
end
if channel ~= '' then
    redis.call('PUBLISH', channel, payload)
end
local final_status = new_status ~= '' and new_status or status
if final_status == 'running' then
    local heartbeat_score = redis.call('HGET', hkey, 'heartbeat_score')
        or redis.call('HGET', hkey, 'started_score') or '0'
    redis.call('ZADD', prefix .. ':maintenance:running', heartbeat_score, task_id)
else
    redis.call('ZREM', prefix .. ':maintenance:running', task_id)
end
if final_status == 'completed' or final_status == 'failed' or final_status == 'cancelled' then
    local completed_score = redis.call('HGET', hkey, 'completed_score') or '0'
    redis.call('ZADD', prefix .. ':maintenance:terminal', completed_score, task_id)
else
    redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
end
local execution_ref = redis.call('HGET', hkey, 'execution_ref')
if execution_ref and execution_ref ~= ''
        and (final_status == 'pending' or final_status == 'scheduled' or final_status == 'running') then
    local external_score
    if final_status == 'running' then
        external_score = redis.call('HGET', hkey, 'started_score') or redis.call('HGET', hkey, 'created_score') or '0'
    else
        external_score = redis.call('HGET', hkey, 'created_score') or '0'
    end
    redis.call('ZADD', prefix .. ':maintenance:external', external_score, task_id)
else
    redis.call('ZREM', prefix .. ':maintenance:external', task_id)
end
return {1}
"""
_DELETE_TERMINAL_SCRIPT = """
local hkey = KEYS[1]
local prefix = ARGV[1]
local task_id = ARGV[2]
local status = redis.call('HGET', hkey, 'status')
if status ~= 'completed' and status ~= 'failed' and status ~= 'cancelled' then
    return {0}
end
local dedup_key = redis.call('HGET', hkey, 'key')
redis.call('DEL', hkey)
redis.call('SREM', prefix .. ':tasks', task_id)
redis.call('ZREM', prefix .. ':ready', task_id)
redis.call('ZREM', prefix .. ':scheduled', task_id)
redis.call('SREM', prefix .. ':status:' .. status, task_id)
redis.call('ZREM', prefix .. ':maintenance:running', task_id)
redis.call('ZREM', prefix .. ':maintenance:external', task_id)
redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
if dedup_key and dedup_key ~= '' then
    if redis.call('HGET', prefix .. ':keys', dedup_key) == task_id then
        redis.call('HDEL', prefix .. ':keys', dedup_key)
    end
end
return {1}
"""
_RESERVE_IDENTITY_SCRIPT = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    return existing
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return false
"""


_RESET_IDENTITY_SCRIPT = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if not existing then
    return {0}
end
if ARGV[2] ~= '' then
    local ok, owner = pcall(cjson.decode, existing)
    if not ok or tostring(owner.task_id) ~= ARGV[2] then
        return {0}
    end
end
return {redis.call('HDEL', KEYS[1], ARGV[1])}
"""


_RELEASE_MAINTENANCE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return {redis.call('DEL', KEYS[1])}
end
return {0}
"""


_CHECK_MAINTENANCE_INDEX_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current then
    return {current}
end
if redis.call('SCARD', KEYS[2]) == 0 then
    redis.call('SET', KEYS[1], ARGV[1])
    return {ARGV[1]}
end
return {''}
"""


class RedisQueueBackend(BaseQueueBackend):
    """Queue backend that stores records in a Redis-protocol key-value server.

    Ready work lives in one global ``{prefix}:ready`` sorted set scored
    priority-major / created_at-minor, so the claim pops the globally-correct
    next task with one ordered ``ZRANGE`` instead of a Python-side sort over an
    ``HGETALL`` of every due candidate. A separate ``{prefix}:scheduled`` sorted
    set scored by ``scheduled_at`` preserves exact delayed-promotion due-gating:
    the claim script promotes now-due scheduled ids into ``ready`` before
    scanning, so future-scheduled tasks are never claimable early. Keeping one
    global ``ready`` set rather than per-queue sets makes the claim a single
    ``EVAL`` with no queue enumeration; the queue and execution_backend filters
    skip non-matching top entries inside the script.

    Ready scores are IEEE-754 doubles, exact for integers up to 2^53. With
    stride ``1e13`` and ``created_ms`` near ``1.7e12`` the priority band
    ``(-priority) * 1e13`` stays exact for ``abs(priority) <= 450``, far beyond
    realistic priorities; ties break on ``created_ms`` ascending at millisecond
    resolution.

    All Lua scripts build their keys from a ``key_prefix`` ARG via string
    concatenation, which is single-node/replica only. Redis Cluster is out of
    scope: multi-key scripts on a cluster require same-slot hash-tagged keys and
    no hash-tag support is added. The composite-score ``ready``/``scheduled``
    layout replaces the old ``{prefix}:pending`` zset outright with no data
    migration; records enqueued under the old layout are stranded (benchmark
    namespaces are ephemeral).
    """

    _backend_name: "ClassVar[str]" = "redis"

    __slots__ = (
        "_client",
        "_event_log",
        "_key_prefix",
        "_notifications",
        "_owns_client",
        "_pending_read",
        "_pubsub",
        "_url",
        "_wakeup_channel",
    )

    def __init__(
        self, config: "QueueConfig | None" = None, *, backend_config: "_RedisBackendConfig | None" = None
    ) -> "None":
        super().__init__(config=config)
        backend_config = backend_config or _RedisBackendConfig()
        self._client: "RedisClientLike | None" = cast("RedisClientLike | None", backend_config.client)
        self._owns_client = self._client is None
        self._url = backend_config.url
        self._key_prefix = backend_config.key_prefix.rstrip(":")
        self._notifications = backend_config.worker_wakeups
        self._wakeup_channel = backend_config.wakeup_channel
        self._pubsub: "RedisPubSubLike | None" = None
        self._pending_read = PendingNativeRead()
        self._event_log: "RedisQueueEventLog | None" = None

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_worker_wakeups=self._notifications,
            wakeup_backend=f"{self._backend_name}-pubsub" if self._notifications else None,
            wakeups_durable=False,
            supports_completion_events=self._notifications,
            supports_maintenance=True,
        )

    async def open(self) -> "bool":
        """Open Redis-protocol client resources.

        Returns:
            True when the client is ready.
        """
        if self._client is None:
            self._client = self._create_client(self._url)
            self._owns_client = True
        return True

    async def close(self) -> "None":
        """Close owned Redis-protocol client resources."""
        if self._event_log is not None:
            await self._event_log.flush_events()
        await self._pending_read.aclose()
        if self._pubsub is not None:
            await _close_pubsub(self._pubsub, self._wakeup_channel)
            self._pubsub = None
        if self._owns_client and self._client is not None:
            close = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result
            self._client = None

    def get_event_log(self, config: "EventHistoryConfig") -> "QueueEventLog | None":
        """Return Redis-protocol queue event history when enabled."""
        if self._event_log is None:
            self._event_log = RedisQueueEventLog(backend=self, config=config)
        return self._event_log

    async def enqueue(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]" = (),
        kwargs: "dict[str, Any] | None" = None,
        queue: "str" = "default",
        priority: "int" = 0,
        max_retries: "int" = 0,
        scheduled_at: "datetime | None" = None,
        key: "str | None" = None,
        execution_backend: "str" = "local",
        execution_profile: "str | None" = None,
        metadata: "dict[str, Any] | None" = None,
        id: "UUID | None" = None,  # noqa: A002
    ) -> "QueuedTaskRecord":
        """Persist a queued task.

        Returns:
            The created or deduplicated queued task record.
        """
        record = self._create_record(
            task_name,
            args=args,
            kwargs=kwargs,
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            key=key,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            metadata=metadata,
        )
        if id is not None:
            record.id = id
        if key is not None:
            return await self._enqueue_keyed(record, key, publish=True)
        await self._save_new_record(record, publish=True)
        return record

    async def enqueue_many(self, requests: "Sequence[TaskRequest]") -> "list[QueuedTaskRecord]":
        """Persist a batch of Redis-backed tasks and coalesce worker wakeups.

        Returns:
            Queue task records in the same order as ``requests``.
        """
        if not requests:
            return []

        results: "list[QueuedTaskRecord]" = []
        unkeyed_records: "list[QueuedTaskRecord]" = []
        for request in requests:
            if request.key is not None:
                record = self._create_record(
                    request.task_name,
                    args=request.args,
                    kwargs=request.kwargs,
                    queue=request.queue,
                    priority=request.priority,
                    max_retries=request.max_retries,
                    scheduled_at=request.scheduled_at,
                    key=request.key,
                    execution_backend=request.execution_backend,
                    execution_profile=request.execution_profile,
                    metadata=request.metadata,
                )
                results.append(await self._enqueue_keyed(record, request.key, publish=False))
                continue

            record = self._create_record(
                request.task_name,
                args=request.args,
                kwargs=request.kwargs,
                queue=request.queue,
                priority=request.priority,
                max_retries=request.max_retries,
                scheduled_at=request.scheduled_at,
                key=None,
                execution_backend=request.execution_backend,
                execution_profile=request.execution_profile,
                metadata=request.metadata,
            )
            unkeyed_records.append(record)
            results.append(record)

        if unkeyed_records:
            await self._save_new_records(unkeyed_records, publish=False)
        await self.notify_new_tasks(results)
        return results

    async def get_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Return a queued task by ID."""
        mapping = await self._client_hgetall(self._task_key(task_id))
        if not mapping:
            return None
        return self._record_from_mapping(mapping)

    async def get_task_by_key(self, key: "str") -> "QueuedTaskRecord | None":
        """Return a queued task by deduplication key."""
        task_id = await self._client_hget(self._keys_key, key)
        if task_id is None:
            return None
        return await self.get_task(UUID(str(_decode(task_id))))

    async def list_pending(
        self, *, limit: "int" = 1, queue: "str | None" = None, execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Return due pending or scheduled tasks ordered for execution."""
        client = await self._get_client()
        now_ms = _utc_now().timestamp() * 1000.0
        ready_ids = await client.zrange(self._ready_key, 0, -1)
        scheduled_ids = await client.zrangebyscore(self._scheduled_key, "-inf", now_ms)
        candidate_ids = [*ready_ids, *scheduled_ids]
        due_records = [
            record
            for record in await self._records_from_ids(candidate_ids)
            if record.status in _DUE_STATUSES
            and record.is_due
            and (queue is None or record.queue == queue)
            and (execution_backend is None or record.execution_backend == execution_backend)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.created_at))
        return due_records[:limit]

    async def claim_task(self, task_id: "UUID") -> "QueuedTaskRecord | None":
        """Atomically claim a pending task via a single fenced script.

        Returns:
            The claimed record, if it was still due and claimable.
        """
        record = await self.get_task(task_id)
        if record is None or record.status not in _DUE_STATUSES or not record.is_due:
            return None
        now = _utc_now()
        maintenance_score = repr(_maintenance_score(now))
        committed = await self._commit_transition(
            task_id,
            expected_status=record.status,
            new_status="running",
            patch={
                "started_at": _serialize_datetime(now),
                "started_score": maintenance_score,
                "heartbeat_at": _serialize_datetime(now),
                "heartbeat_score": maintenance_score,
            },
            zset_action="remove",
        )
        if not committed:
            return None
        record.status = "running"
        record.started_at = now
        record.heartbeat_at = now
        return record

    async def claim_many(
        self, *, limit: "int", queues: "tuple[str, ...]" = (), execution_backend: "str | None" = None
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks in a single fenced ``EVAL``.

        Returns:
            Claimed task records in claim order.
        """
        if limit <= 0:
            return []
        client = await self._get_client()
        now = _utc_now()
        window = max(limit * 2, limit + 10)
        args = [
            self._key_prefix,
            repr(now.timestamp() * 1000.0),
            _serialize_datetime(now),
            str(limit),
            execution_backend or "",
            str(window),
            *queues,
        ]
        claimed_ids = await _eval_script(client, _CLAIM_SCRIPT, [self._ready_key, self._scheduled_key], args)
        if not claimed_ids:
            return []
        return await self._records_from_ids([_decode(value) for value in claimed_ids])

    async def complete_task(
        self, task_id: "UUID", *, result: "Any" = None, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        """Mark a task as completed via a single fenced script.

        Returns:
            The completed record, if it exists.
        """
        client = await self._get_client()
        now = _utc_now()
        outcome = await _eval_script(
            client,
            _COMPLETE_SCRIPT,
            [self._task_key(task_id)],
            [
                self._key_prefix,
                str(task_id),
                "" if expected_retry_count is None else str(expected_retry_count),
                _serialize_datetime(now),
                _json_dumps(result),
                self._completion_channel,
                repr(_maintenance_score(now)),
            ],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        return await self.get_task(task_id)

    async def fail_task(
        self, task_id: "UUID", error: "str", *, retry: "bool" = True, expected_retry_count: "int | None" = None
    ) -> "QueuedTaskRecord | None":
        """Mark a task as failed or retry it via a single fenced script.

        Returns:
            The updated record, if it exists.
        """
        client = await self._get_client()
        now = _utc_now()
        outcome = await _eval_script(
            client,
            _FAIL_SCRIPT,
            [self._task_key(task_id), self._ready_key],
            [
                self._key_prefix,
                str(task_id),
                "" if expected_retry_count is None else str(expected_retry_count),
                error,
                "1" if retry else "0",
                _serialize_datetime(now),
                self._completion_channel,
                repr(_maintenance_score(now)),
            ],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        record = await self.get_task(task_id)
        if record is not None and _decode(outcome[1]) == "pending":
            await self.notify_new_task(record)
        return record

    async def cancel_task(self, task_id: "UUID", *, include_running: "bool" = False) -> "bool":
        """Cancel a task via a single fenced script.

        Returns:
            True when the task was cancelled.
        """
        record = await self.get_task(task_id)
        cancellable_statuses = (*_DUE_STATUSES, "running") if include_running else _DUE_STATUSES
        if record is None or record.status not in cancellable_statuses:
            return False
        return await self._commit_cancel(record)

    async def cancel_tasks(
        self,
        *,
        task_name: "str | None" = None,
        queue: "str | None" = None,
        kwargs: "Mapping[str, Any] | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
        include_running: "bool" = False,
    ) -> "int":
        """Cancel tasks matching a domain predicate.

        Returns:
            Number of records cancelled.
        """
        statuses = tuple(sorted((*_DUE_STATUSES, "running") if include_running else _DUE_STATUSES))
        cancelled = 0
        for record in await self._list_records_by_statuses(statuses):
            if not record_matches_filters(record, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata):
                continue
            latest = await self.get_task(record.id)
            if latest is None or latest.status not in statuses:
                continue
            if not record_matches_filters(latest, task_name=task_name, queue=queue, kwargs=kwargs, metadata=metadata):
                continue
            if await self._commit_cancel(latest):
                cancelled += 1
        return cancelled

    async def _commit_cancel(self, record: "QueuedTaskRecord") -> "bool":
        now = _utc_now()
        return await self._commit_transition(
            record.id,
            expected_status=record.status,
            new_status="cancelled",
            patch={
                "completed_at": _serialize_datetime(now),
                "completed_score": repr(_maintenance_score(now)),
                "heartbeat_at": "",
                "heartbeat_score": "0",
            },
            zset_action="remove",
            publish_channel=self._completion_channel if self._notifications else "",
            publish_payload=str(record.id),
        )

    async def touch_heartbeats(self, touches: "Sequence[HeartbeatTouch]") -> "HeartbeatTouchResult":
        """Update heartbeat timestamps for running tasks.

        Returns:
            The task IDs confirmed touched or missed by the backend.
        """
        result = HeartbeatTouchResult()
        if not touches:
            return result
        client = await self._get_client()
        pipeline = _create_pipeline(client)
        now = _utc_now()
        heartbeat_at = _serialize_datetime(now)
        heartbeat_score = repr(_maintenance_score(now))
        for touch in touches:
            expected_retry_count = "" if touch.expected_retry_count is None else str(touch.expected_retry_count)
            metadata_patch = _json_dumps(touch.metadata_patch) if touch.metadata_patch else ""
            pipeline.eval(
                _TOUCH_HEARTBEAT_SCRIPT,
                1,
                self._task_key(touch.task_id),
                expected_retry_count,
                heartbeat_at,
                metadata_patch,
                heartbeat_score,
                self._key_prefix,
                str(touch.task_id),
            )
        outcomes = await _execute_pipeline(pipeline)
        for touch, outcome in zip(touches, outcomes, strict=True):
            if int(outcome) == 1:
                result.touched_task_ids.add(touch.task_id)
            else:
                result.missed_task_ids.add(touch.task_id)
        return result

    async def null_heartbeats(self, task_ids: "list[UUID]", *, expected_retry_count: "int | None" = None) -> "None":
        """Clear heartbeat timestamps for task IDs via a fenced script."""
        for task_id in task_ids:
            await self._commit_transition(
                task_id,
                expected_status="",
                expected_retry_count=expected_retry_count,
                patch={"heartbeat_at": "", "heartbeat_score": "0"},
            )

    async def requeue_stale_running(
        self, *, stale_after: "timedelta", limit: "int | None" = None
    ) -> "StaleTaskRecoveryResult":
        """Requeue running tasks with stale heartbeats.

        Candidates are ordered oldest-heartbeat-first (then by id) and capped at
        ``limit`` before any mutation so one maintenance batch is bounded.

        Returns:
            Summary of recovered records.
        """
        cutoff = _utc_now() - stale_after
        result = StaleTaskRecoveryResult()
        if limit is not None and limit <= 0:
            return result
        if limit is None:
            records = await self._list_records_by_statuses(("running",))
        else:
            await self._require_maintenance_indexes()
            client = await self._get_client()
            task_ids = await client.zrangebyscore(
                self._maintenance_running_key, "-inf", f"({_maintenance_score(cutoff)}", start=0, num=limit
            )
            records = await self._records_from_ids(task_ids)
        candidates = [
            record
            for record in records
            if record.status == "running" and (record.heartbeat_at is None or record.heartbeat_at < cutoff)
        ]
        candidates.sort(key=_stale_sort_key)
        if limit is not None:
            candidates = candidates[:limit]
        for record in candidates:
            latest = await self.get_task(record.id)
            if latest is None or latest.status != "running":
                result.skipped += 1
                continue
            if latest.heartbeat_at is not None and latest.heartbeat_at >= cutoff:
                result.skipped += 1
                continue
            requeue_on_stale = latest.metadata.get("requeue_on_stale", True) is not False
            if requeue_on_stale and latest.retry_count < latest.max_retries:
                if await self._commit_stale_requeue(latest):
                    result.requeued += 1
                else:
                    result.skipped += 1
            elif await self._commit_stale_failure(latest):
                result.failed += 1
                result.failed_task_ids.append(latest.id)
                if not requeue_on_stale:
                    result.handler_needed += 1
                    result.handler_needed_task_ids.append(latest.id)
            else:
                result.skipped += 1
        return result

    async def _commit_stale_requeue(self, record: "QueuedTaskRecord") -> "bool":
        expected_retry = record.retry_count
        record.status = "pending"
        record.priority = stale_requeue_priority(record.priority)
        record.started_at = None
        record.heartbeat_at = None
        record.error = stale_requeue_error(record.error)
        record.retry_count += 1
        zset_action, score = self._index_action(record)
        return await self._commit_transition(
            record.id,
            expected_status="running",
            expected_retry_count=expected_retry,
            new_status="pending",
            patch={
                "priority": str(record.priority),
                "started_at": "",
                "started_score": "0",
                "heartbeat_at": "",
                "heartbeat_score": "0",
                "error": record.error or "",
                "retry_count": str(record.retry_count),
                "ready_score": repr(_ready_score(record)),
            },
            zset_action=zset_action,
            score=score,
            publish_channel=self._wakeup_channel if (self._notifications and zset_action == "ready") else "",
            publish_payload=_json_dumps({"event": "task_available"}),
        )

    async def _commit_stale_failure(self, record: "QueuedTaskRecord") -> "bool":
        now = _utc_now()
        record.status = "failed"
        record.completed_at = now
        record.heartbeat_at = None
        record.error = STALE_HEARTBEAT_ERROR
        return await self._commit_transition(
            record.id,
            expected_status="running",
            new_status="failed",
            patch={
                "completed_at": _serialize_datetime(now),
                "completed_score": repr(_maintenance_score(now)),
                "heartbeat_at": "",
                "heartbeat_score": "0",
                "error": STALE_HEARTBEAT_ERROR,
            },
            zset_action="remove",
        )

    async def set_execution_ref(
        self, task_id: "UUID", execution_backend: "str", execution_ref: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an external execution reference for a running task via a fenced script.

        Returns:
            The updated record, if it exists.
        """
        record = await self.get_task(task_id)
        if record is None:
            return None
        committed = await self._commit_transition(
            task_id,
            expected_status="",
            patch={
                "execution_backend": execution_backend,
                "execution_profile": execution_profile or "",
                "execution_ref": execution_ref or "",
            },
        )
        if not committed:
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = execution_ref
        return record

    async def set_execution_backend(
        self, task_id: "UUID", execution_backend: "str", *, execution_profile: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        """Persist an execution backend/profile change for a queued task via a fenced script.

        Returns:
            The updated record, if it exists.
        """
        record = await self.get_task(task_id)
        if record is None:
            return None
        record.execution_backend = execution_backend
        record.execution_profile = execution_profile
        record.execution_ref = None
        due = record.status in _DUE_STATUSES and record.is_due
        await self._commit_transition(
            task_id,
            expected_status="",
            patch={
                "execution_backend": execution_backend,
                "execution_profile": execution_profile or "",
                "execution_ref": "",
            },
            publish_channel=self._wakeup_channel if (self._notifications and due) else "",
            publish_payload=_json_dumps({"event": "task_available"}),
        )
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Return externally dispatched tasks with references to reconcile."""
        if limit is not None and limit <= 0:
            return []
        if limit is None:
            candidate_records = await self._list_records_by_statuses(("pending", "scheduled", "running"))
        else:
            await self._require_maintenance_indexes()
            client = await self._get_client()
            task_ids = await client.zrange(self._maintenance_external_key, 0, limit - 1)
            candidate_records = await self._records_from_ids(task_ids)
        records = [
            record
            for record in candidate_records
            if record.status in {"pending", "scheduled", "running"} and record.execution_ref is not None
        ]
        records.sort(key=lambda record: (record.started_at or record.created_at, str(record.id)))
        return records[:limit] if limit is not None else records

    async def get_statistics(self) -> "QueueStatistics":
        """Return queue status counts."""
        client = await self._get_client()
        statistics = QueueStatistics()
        statuses = sorted(_STATUS_VALUES)
        counts = await _pipeline_scard(client, [self._status_key(status) for status in statuses])
        for status, count in zip(statuses, counts, strict=True):
            setattr(statistics, status, int(count))
        return statistics

    async def list_completed_by_task(
        self, task_name: "str", *, since: "datetime | None" = None, limit: "int" = 10
    ) -> "list[QueuedTaskRecord]":
        """Return recent completed records for a task name."""
        records = [
            record
            for record in await self._list_records_by_statuses(("completed",))
            if record.task_name == task_name
            and record.status == "completed"
            and record.completed_at is not None
            and (since is None or record.completed_at >= since)
        ]
        records.sort(key=lambda record: record.completed_at or record.created_at, reverse=True)
        return records[:limit]

    async def cleanup_terminal(self, before: "datetime", *, limit: "int | None" = None) -> "int":
        """Delete terminal records completed before a cutoff.

        Candidates are ordered oldest-completion-first (then by id) and capped at
        ``limit`` before any deletion so one maintenance batch is bounded.

        Returns:
            Number of deleted records.
        """
        client = await self._get_client()
        if limit is not None and limit <= 0:
            return 0
        if limit is None:
            records = await self._list_records_by_statuses(tuple(sorted(_TERMINAL_STATUSES)))
        else:
            await self._require_maintenance_indexes()
            task_ids = await client.zrangebyscore(
                self._maintenance_terminal_key, "-inf", f"({_maintenance_score(before)}", start=0, num=limit
            )
            records = await self._records_from_ids(task_ids)
        candidates = [
            record
            for record in records
            if record.status in _TERMINAL_STATUSES and record.completed_at is not None and record.completed_at < before
        ]
        candidates.sort(key=lambda record: (cast("datetime", record.completed_at), str(record.id)))
        if limit is not None:
            candidates = candidates[:limit]
        count = 0
        for record in candidates:
            outcome = await _eval_script(
                client, _DELETE_TERMINAL_SCRIPT, [self._task_key(record.id)], [self._key_prefix, str(record.id)]
            )
            if outcome and int(outcome[0]) == 1:
                count += 1
        return count

    async def rebuild_maintenance_indexes(self) -> "int":
        """Rebuild ordered maintenance indexes for a populated pre-index namespace.

        This is an intentionally unbounded, one-time upgrade operation. Stop all
        queue writers using this Redis/Valkey namespace before calling it. Interrupted calls are safe
        to retry because the version marker is written only after every task has
        been reindexed.

        Returns:
            Number of queue records indexed.
        """
        client = await self._get_client()
        records = await self._list_records_by_statuses(tuple(sorted(_STATUS_VALUES)))
        pipeline = _create_pipeline(client)
        pipeline.delete(self._maintenance_running_key, self._maintenance_external_key, self._maintenance_terminal_key)
        for record in records:
            task_id = str(record.id)
            timestamp_scores = {
                "created_score": repr(_maintenance_score(record.created_at)),
                "started_score": repr(_maintenance_score(record.started_at)),
                "completed_score": repr(_maintenance_score(record.completed_at)),
                "heartbeat_score": repr(_maintenance_score(record.heartbeat_at)),
            }
            pipeline.hset(self._task_key(record.id), mapping=timestamp_scores)
            if record.status == "running":
                pipeline.zadd(self._maintenance_running_key, {task_id: _maintenance_score(record.heartbeat_at)})
            if record.execution_ref is not None and record.status in {"pending", "scheduled", "running"}:
                pipeline.zadd(
                    self._maintenance_external_key,
                    {task_id: _maintenance_score(record.started_at or record.created_at)},
                )
            if record.status in _TERMINAL_STATUSES and record.completed_at is not None:
                pipeline.zadd(self._maintenance_terminal_key, {task_id: _maintenance_score(record.completed_at)})
        await _execute_pipeline(pipeline)
        marked = client.set(self._maintenance_index_version_key, _MAINTENANCE_INDEX_VERSION)
        if inspect.isawaitable(marked):
            await marked
        return len(records)

    async def acquire_maintenance(self, name: "str", token: "str", *, ttl: "timedelta") -> "bool":
        """Acquire namespaced ``SET NX PX`` maintenance ownership.

        Returns:
            True when ownership was set for ``token``.
        """
        client = await self._get_client()
        ttl_ms = max(1, int(ttl.total_seconds() * 1000))
        result = client.set(self._maintenance_key(name), token, nx=True, px=ttl_ms)
        if inspect.isawaitable(result):
            result = await result
        return bool(result)

    async def release_maintenance(self, name: "str", token: "str") -> "bool":
        """Release maintenance ownership via token-checked Lua compare-and-delete.

        Returns:
            True when ownership held under ``token`` was deleted.
        """
        client = await self._get_client()
        outcome = await _eval_script(client, _RELEASE_MAINTENANCE_SCRIPT, [self._maintenance_key(name)], [token])
        return bool(outcome and int(outcome[0]) == 1)

    async def reserve_identity(self, key: "str", *, task_id: "UUID", task_name: "str") -> "TaskReservation | None":
        """Reserve a forever identity via an atomic HGET-or-HSET script.

        The task-reservation hash is separate from ``:task:``/``:keys`` and is never
        touched by terminal cleanup.

        Returns:
            ``None`` when this caller won the reservation; otherwise the existing
            owner reservation.
        """
        client = await self._get_client()
        created_at = _utc_now()
        payload = _json_dumps({
            "key": key,
            "task_id": str(task_id),
            "task_name": task_name,
            "created_at": _serialize_datetime(created_at),
        })
        result = client.eval(_RESERVE_IDENTITY_SCRIPT, 1, self._task_reservation_key, key, payload)
        if inspect.isawaitable(result):
            result = await result
        if result is None or result is False:
            return None
        return _reservation_from_payload(_decode(result))

    async def has_identity(self, key: "str") -> "TaskReservation | None":
        """Return the reservation owning a reserved forever identity, if any."""
        raw = await self._client_hget(self._task_reservation_key, key)
        if raw is None:
            return None
        return _reservation_from_payload(_decode(raw))

    async def reset_identity(self, key: "str", *, expected_task_id: "UUID | None" = None) -> "bool":
        """Delete a forever identity reservation via atomic compare-and-delete.

        Args:
            key: The exact effective identity key.
            expected_task_id: Optional task owner required for deletion.

        Returns:
            ``True`` when a reservation was removed.
        """
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _RESET_IDENTITY_SCRIPT,
            [self._task_reservation_key],
            [key, str(expected_task_id) if expected_task_id is not None else ""],
        )
        return bool(outcome and int(outcome[0]) == 1)

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Publish a Redis-protocol pub/sub message when work is available."""
        if self._notifications and record.status in _DUE_STATUSES and record.is_due:
            payload = _json_dumps({"event": "task_available"})
            client = await self._get_client()
            await client.publish(self._wakeup_channel, payload)

    async def wait_for_wakeups(self, timeout: "float | None" = None) -> "bool":
        """Wait for a Redis-protocol pub/sub message when notifications are enabled.

        A single pub/sub receive is retained across worker poll timeouts; only
        a real message, a read failure, or backend close ends it.

        Returns:
            True when a notification was observed.
        """
        if not self._notifications:
            return await super().wait_for_wakeups(timeout=timeout)
        pubsub = await self._get_pubsub()
        task = await self._pending_read.race(lambda: _receive_pubsub_message(pubsub), timeout)
        if task is None:
            return False
        exc = task.exception()
        if exc is not None:
            await self._reset_pubsub()
            raise exc
        return bool(task.result())

    async def time_until_next_due(self, *, queues: "tuple[str, ...]" = ()) -> "float | None":
        """Return seconds until the earliest not-yet-due scheduled record.

        Reads the lowest-scored member of the global ``scheduled`` sorted set
        (scored by ``scheduled_at``): an O(1) lookup independent of queue
        size. ``queues`` is not applied because the sorted set is not
        queue-scoped; an unfiltered bound is always safe here (it can only
        wake the worker sooner than strictly necessary, never later).

        Returns:
            Seconds until the next due record, or ``None`` when there is no
            upcoming scheduled work.
        """
        del queues
        client = await self._get_client()
        member_ids = await client.zrange(self._scheduled_key, 0, 0)
        if not member_ids:
            return None
        records = await self._records_from_ids(member_ids)
        if not records or records[0].scheduled_at is None:
            return None
        return max((records[0].scheduled_at - _utc_now()).total_seconds(), 0.0)

    async def _reset_pubsub(self) -> "None":
        """Drop the pub/sub subscription so the next wait re-establishes it."""
        await self._pending_read.aclose()
        pubsub = self._pubsub
        self._pubsub = None
        if pubsub is not None:
            await _close_pubsub(pubsub, self._wakeup_channel)

    async def wait_for_completion(self, task_id: "UUID", *, timeout: "float | None" = None) -> "bool":
        """Wait for a terminal completion message naming ``task_id``.

        Returns:
            True when a completion signal for ``task_id`` arrived before the deadline.
        """
        if not self._notifications:
            return False
        client = await self._get_client()
        pubsub = client.pubsub()
        subscribe = pubsub.subscribe(self._completion_channel)
        if inspect.isawaitable(subscribe):
            await subscribe
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout if timeout is not None else None
            target = str(task_id)
            while True:
                remaining = None if deadline is None else max(0.0, deadline - loop.time())
                if remaining is not None and remaining <= 0.0:
                    return False
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining)
                if message is not None and str(_decode(message.get("data"))) == target:
                    return True
                if deadline is None and message is None:
                    return False
        finally:
            await _close_pubsub(pubsub, self._completion_channel)

    def _create_client(self, url: "str") -> "RedisClientLike":
        from redis import asyncio as redis_asyncio

        return cast("RedisClientLike", redis_asyncio.from_url(url, decode_responses=True))

    async def _get_client(self) -> "RedisClientLike":
        if self._client is None:
            await self.open()
        return cast("RedisClientLike", self._client)

    async def _get_pubsub(self) -> "RedisPubSubLike":
        if self._pubsub is None:
            client = await self._get_client()
            self._pubsub = client.pubsub()
            subscribe = self._pubsub.subscribe(self._wakeup_channel)
            if inspect.isawaitable(subscribe):
                await subscribe
        return self._pubsub

    async def _commit_transition(
        self,
        task_id: "UUID",
        *,
        expected_status: "str",
        new_status: "str" = "",
        patch: "Mapping[str, str] | None" = None,
        zset_action: "str" = "none",
        score: "str" = "",
        expected_retry_count: "int | None" = None,
        publish_channel: "str" = "",
        publish_payload: "str" = "",
    ) -> "bool":
        client = await self._get_client()
        args = [
            self._key_prefix,
            str(task_id),
            expected_status,
            "" if expected_retry_count is None else str(expected_retry_count),
            new_status,
            zset_action,
            score,
            publish_channel,
            publish_payload,
        ]
        if patch:
            for field, value in patch.items():
                args.append(field)
                args.append(value)
        outcome = await _eval_script(
            client, _TRANSITION_SCRIPT, [self._task_key(task_id), self._ready_key, self._scheduled_key], args
        )
        return bool(outcome and int(outcome[0]) == 1)

    async def _enqueue_keyed(self, record: "QueuedTaskRecord", key: "str", *, publish: "bool") -> "QueuedTaskRecord":
        client = await self._get_client()
        args = self._enqueue_args(record, publish=publish)
        args = [*args[:8], key, *args[8:]]
        outcome = await _eval_script(client, _ENQUEUE_KEYED_SCRIPT, [self._ready_key, self._scheduled_key], args)
        if int(outcome[0]) == 1:
            return record
        existing = await self.get_task(UUID(str(_decode(outcome[1]))))
        return existing if existing is not None else record

    def _index_action(self, record: "QueuedTaskRecord") -> "tuple[str, str]":
        if record.status == "pending" and record.is_due:
            return "ready", repr(_ready_score(record))
        if record.status in _DUE_STATUSES:
            return "scheduled", repr(_scheduled_score(record.scheduled_at))
        return "remove", ""

    def _create_record(
        self,
        task_name: "str",
        *,
        args: "tuple[Any, ...]",
        kwargs: "dict[str, Any] | None",
        queue: "str",
        priority: "int",
        max_retries: "int",
        scheduled_at: "datetime | None",
        key: "str | None",
        execution_backend: "str",
        execution_profile: "str | None",
        metadata: "dict[str, Any] | None",
    ) -> "QueuedTaskRecord":
        return QueuedTaskRecord(
            task_name=task_name,
            args=args,
            kwargs=dict(kwargs or {}),
            queue=queue,
            execution_backend=execution_backend,
            execution_profile=execution_profile,
            status="scheduled" if scheduled_at is not None and scheduled_at > _utc_now() else "pending",
            priority=priority,
            max_retries=max_retries,
            scheduled_at=scheduled_at,
            key=key,
            metadata=dict(metadata or {}),
        )

    async def _save_new_record(self, record: "QueuedTaskRecord", *, publish: "bool") -> "None":
        await self._save_new_records([record], publish=publish)

    async def _save_new_records(self, records: "Sequence[QueuedTaskRecord]", *, publish: "bool") -> "None":
        if not records:
            return
        client = await self._get_client()
        keys = [self._ready_key, self._scheduled_key]
        pipeline = _create_pipeline(client)
        for record in records:
            pipeline.eval(_ENQUEUE_SCRIPT, len(keys), *keys, *self._enqueue_args(record, publish=publish))
        await _execute_pipeline(pipeline)

    def _enqueue_args(self, record: "QueuedTaskRecord", *, publish: "bool") -> "list[str]":
        due = record.status == "pending" and record.is_due
        score = _ready_score(record) if due else _scheduled_score(record.scheduled_at)
        args = [
            self._key_prefix,
            str(record.id),
            record.status,
            "1" if due else "0",
            repr(score),
            self._wakeup_channel,
            _json_dumps({"event": "task_available"}),
            "1" if publish and self._notifications else "0",
            _MAINTENANCE_INDEX_VERSION,
        ]
        for field, value in self._record_to_mapping(record).items():
            args.append(field)
            args.append(value)
        return args

    async def _list_records_by_statuses(self, statuses: "tuple[str, ...]") -> "list[QueuedTaskRecord]":
        client = await self._get_client()
        member_sets = await _pipeline_smembers(client, [self._status_key(status) for status in statuses])
        task_ids = {value for member_set in member_sets for value in member_set}
        return await self._records_from_ids(tuple(task_ids))

    async def _require_maintenance_indexes(self) -> "None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _CHECK_MAINTENANCE_INDEX_SCRIPT,
            [self._maintenance_index_version_key, f"{self._key_prefix}:tasks"],
            [_MAINTENANCE_INDEX_VERSION],
        )
        version = str(_decode(outcome[0])) if outcome else ""
        if version == _MAINTENANCE_INDEX_VERSION:
            return
        msg = (
            f"{self._backend_name} maintenance indexes are missing for populated key prefix "
            f"{self._key_prefix!r}. Stop all queue writers using this namespace and run "
            "`await backend.rebuild_maintenance_indexes()` once before bounded maintenance."
        )
        raise QueueConfigurationError(msg)

    async def _records_from_ids(self, task_ids: "Iterable[Any]") -> "list[QueuedTaskRecord]":
        task_keys = [self._task_key(UUID(str(_decode(value)))) for value in task_ids]
        mappings = await _pipeline_hgetall(await self._get_client(), task_keys)
        records: "list[QueuedTaskRecord]" = []
        for mapping in mappings:
            decoded = _decode_mapping(mapping)
            if decoded:
                records.append(self._record_from_mapping(decoded))
        return records

    async def _client_hget(self, name: "str", key: "str") -> "Any":
        client = await self._get_client()
        return await client.hget(name, key)

    async def _client_hgetall(self, name: "str") -> "dict[str, Any]":
        client = await self._get_client()
        return _decode_mapping(await client.hgetall(name))

    @property
    def _keys_key(self) -> "str":
        return f"{self._key_prefix}:keys"

    @property
    def _task_reservation_key(self) -> "str":
        return f"{self._key_prefix}:task_reservations"

    @property
    def _ready_key(self) -> "str":
        return f"{self._key_prefix}:ready"

    @property
    def _scheduled_key(self) -> "str":
        return f"{self._key_prefix}:scheduled"

    @property
    def _maintenance_running_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:running"

    @property
    def _maintenance_index_version_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:index-version"

    @property
    def _maintenance_external_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:external"

    @property
    def _maintenance_terminal_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:terminal"

    @property
    def _completion_channel(self) -> "str":
        return f"{self._key_prefix}:completions"

    def _status_key(self, status: "str") -> "str":
        return f"{self._key_prefix}:status:{status}"

    def _task_key(self, task_id: "UUID") -> "str":
        return f"{self._key_prefix}:task:{task_id}"

    def _maintenance_key(self, name: "str") -> "str":
        return f"{self._key_prefix}:maintenance:{name}"

    def _event_log_global_key(self) -> "str":
        return f"{self._key_prefix}:events"

    def _event_log_event_key(self, event_id: "str") -> "str":
        return f"{self._key_prefix}:events:record:{event_id}"

    def _event_log_task_key(self, task_id: "str") -> "str":
        return f"{self._key_prefix}:events:task:{hashed_index_value(task_id)}"

    def _event_log_task_name_key(self, task_name: "str") -> "str":
        return f"{self._key_prefix}:events:task_name:{hashed_index_value(task_name)}"

    def _event_log_event_type_key(self, event_type: "str") -> "str":
        return f"{self._key_prefix}:events:event_type:{hashed_index_value(event_type)}"

    def _record_to_mapping(self, record: "QueuedTaskRecord") -> "dict[str, str]":
        return {
            "id": str(record.id),
            "task_name": record.task_name,
            "args": _json_dumps(list(record.args)),
            "kwargs": _json_dumps(record.kwargs),
            "queue": record.queue,
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile or "",
            "execution_ref": record.execution_ref or "",
            "status": record.status,
            "priority": str(record.priority),
            "max_retries": str(record.max_retries),
            "retry_count": str(record.retry_count),
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "created_at": _serialize_datetime(record.created_at),
            "created_score": repr(_maintenance_score(record.created_at)),
            "started_at": _serialize_datetime(record.started_at),
            "started_score": repr(_maintenance_score(record.started_at)),
            "completed_at": _serialize_datetime(record.completed_at),
            "completed_score": repr(_maintenance_score(record.completed_at)),
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
            "heartbeat_score": repr(_maintenance_score(record.heartbeat_at)),
            "result": _json_dumps(record.result),
            "error": record.error or "",
            "key": record.key or "",
            "metadata": _json_dumps(record.metadata),
            "ready_score": repr(_ready_score(record)),
        }

    def _record_from_mapping(self, mapping: "dict[str, Any]") -> "QueuedTaskRecord":
        return QueuedTaskRecord(
            id=UUID(str(mapping["id"])),
            task_name=str(mapping["task_name"]),
            args=tuple(_json_loads(mapping.get("args"), [])),
            kwargs=dict(_json_loads(mapping.get("kwargs"), {})),
            queue=str(mapping.get("queue") or "default"),
            execution_backend=str(mapping.get("execution_backend") or "local"),
            execution_profile=str(mapping["execution_profile"]) if mapping.get("execution_profile") else None,
            execution_ref=str(mapping["execution_ref"]) if mapping.get("execution_ref") else None,
            status=_coerce_status(mapping.get("status")),
            priority=int(str(mapping.get("priority") or 0)),
            max_retries=int(str(mapping.get("max_retries") or 0)),
            retry_count=int(str(mapping.get("retry_count") or 0)),
            scheduled_at=_deserialize_datetime(mapping.get("scheduled_at")),
            created_at=_deserialize_datetime(mapping.get("created_at")) or _utc_now(),
            started_at=_deserialize_datetime(mapping.get("started_at")),
            completed_at=_deserialize_datetime(mapping.get("completed_at")),
            heartbeat_at=_deserialize_datetime(mapping.get("heartbeat_at")),
            result=_json_loads(mapping.get("result"), None),
            error=str(mapping["error"]) if mapping.get("error") else None,
            key=str(mapping["key"]) if mapping.get("key") else None,
            metadata=dict(_json_loads(mapping.get("metadata"), {})),
        )


def _create_pipeline(client: "RedisClientLike") -> "RedisPipelineLike":
    try:
        return client.pipeline(transaction=False)
    except TypeError:
        return client.pipeline()


async def _execute_pipeline(pipeline: "RedisPipelineLike") -> "list[Any]":
    result = pipeline.execute()
    if inspect.isawaitable(result):
        return list(await result)
    return list(cast("list[Any]", result))


async def _eval_script(client: "RedisClientLike", script: "str", keys: "list[str]", args: "list[str]") -> "list[Any]":
    result = client.eval(script, len(keys), *keys, *args)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    return list(cast("list[Any]", result))


async def _pipeline_hgetall(client: "RedisClientLike", keys: "list[str]") -> "list[dict[Any, Any]]":
    if not keys:
        return []
    pipeline = _create_pipeline(client)
    for key in keys:
        pipeline.hgetall(key)
    return cast("list[dict[Any, Any]]", await _execute_pipeline(pipeline))


async def _pipeline_smembers(client: "RedisClientLike", keys: "list[str]") -> "list[set[Any]]":
    if not keys:
        return []
    pipeline = _create_pipeline(client)
    for key in keys:
        pipeline.smembers(key)
    return [set(result) for result in await _execute_pipeline(pipeline)]


async def _pipeline_scard(client: "RedisClientLike", keys: "list[str]") -> "list[int]":
    if not keys:
        return []
    pipeline = _create_pipeline(client)
    for key in keys:
        pipeline.scard(key)
    return [int(result) for result in await _execute_pipeline(pipeline)]


def _utc_now() -> "datetime":
    return datetime.now(timezone.utc)


_MIN_DATETIME = datetime(1, 1, 1, tzinfo=timezone.utc)


def _stale_sort_key(record: "QueuedTaskRecord") -> "tuple[datetime, str]":
    """Order stale candidates oldest-heartbeat-first, then by record id.

    Returns:
        A sort key of (effective heartbeat, record id).
    """
    return (record.heartbeat_at or _MIN_DATETIME, str(record.id))


def _serialize_datetime(value: "datetime | None") -> "str":
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _deserialize_datetime(value: "Any") -> "datetime | None":
    value = _decode(value)
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


_PRIORITY_STRIDE = 1e13


def _ready_score(record: "QueuedTaskRecord") -> "float":
    created = record.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    created_ms = created.astimezone(timezone.utc).timestamp() * 1000.0
    return (-record.priority) * _PRIORITY_STRIDE + created_ms


def _scheduled_score(value: "datetime | None") -> "float":
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp() * 1000.0


def _maintenance_score(value: "datetime | None") -> "float":
    """Return an ordered-set score for a maintenance timestamp."""
    return _scheduled_score(value)


def _decode(value: "Any") -> "Any":
    if isinstance(value, bytes):
        return value.decode()
    return value


def _decode_mapping(mapping: "dict[Any, Any]") -> "dict[str, Any]":
    return {str(_decode(key)): _decode(value) for key, value in mapping.items()}


def _json_default(value: "Any") -> "Any":
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


def _json_dumps(value: "Any") -> "str":
    return json.dumps(value, default=_json_default, separators=(",", ":"), sort_keys=True)


def _json_loads(value: "Any", default: "Any") -> "Any":
    value = _decode(value)
    if value in {None, ""}:
        return default
    return json.loads(str(value))


def _reservation_from_payload(raw: "Any") -> "TaskReservation":
    data = json.loads(str(raw))
    return TaskReservation(
        key=str(data["key"]),
        task_id=UUID(str(data["task_id"])),
        task_name=str(data["task_name"]),
        created_at=_deserialize_datetime(data.get("created_at")) or _utc_now(),
    )


def _coerce_status(value: "Any") -> "TaskStatus":
    status = str(_decode(value))
    if status not in _STATUS_VALUES:
        msg = f"Unknown queued task status from Redis-protocol queue backend: {status!r}"
        raise ValueError(msg)
    return cast("TaskStatus", status)


async def _receive_pubsub_message(pubsub: "RedisPubSubLike") -> "bool":
    """Block until a real published ``message`` arrives on the subscription.

    ``get_message(timeout=None)`` blocks indefinitely; subscribe/unsubscribe
    confirmations are filtered to ``None`` by ``ignore_subscribe_messages``, so
    they are skipped without ending the retained read. This coroutine carries
    no deadline of its own — worker poll timeouts race it via
    :class:`PendingNativeRead`, leaving it pending until a message lands.

    Returns:
        True once a real published message is observed.
    """
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None:
            return True


async def _close_pubsub(pubsub: "RedisPubSubLike", channel: "str") -> "None":
    """Best-effort unsubscribe + close on a pubsub connection."""
    unsubscribe = getattr(pubsub, "unsubscribe", None)
    if unsubscribe is not None:
        result = unsubscribe(channel)
        if inspect.isawaitable(result):
            with suppress(Exception):
                await result
    close = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
    if close is not None:
        result = close()
        if inspect.isawaitable(result):
            with suppress(Exception):
                await result
