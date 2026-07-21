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
from litestar_queues.models import (
    HeartbeatTouchResult,
    QueueBackendCapabilities,
    QueuedTaskRecord,
    QueueStatistics,
    StaleTaskRecoveryResult,
    TaskStatus,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from litestar_queues.backends.redis._typing import RedisClientLike, RedisPipelineLike, RedisPubSubLike
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventLogConfig, QueueEventLog
    from litestar_queues.models import EnqueueSpec, HeartbeatTouch

__all__ = ("RedisQueueBackend",)

_DUE_STATUSES = {"pending", "scheduled"}
_STATUS_VALUES = {"cancelled", "completed", "failed", "pending", "running", "scheduled"}
_TERMINAL_STATUSES = {"cancelled", "completed", "failed"}
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
    redis.call('HSET', KEYS[1], 'heartbeat_at', heartbeat_at, 'metadata', cjson.encode(metadata))
else
    redis.call('HSET', KEYS[1], 'heartbeat_at', heartbeat_at)
end

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
            redis.call('HSET', hkey, 'status', 'running', 'started_at', now_iso, 'heartbeat_at', now_iso)
            redis.call('SREM', prefix .. ':status:pending', id)
            redis.call('SADD', prefix .. ':status:running', id)
            redis.call('ZREM', ready, id)
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
    'heartbeat_at', '', 'result', result_json, 'error', '')
redis.call('SREM', prefix .. ':status:running', task_id)
redis.call('SADD', prefix .. ':status:completed', task_id)
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
        'started_at', '', 'heartbeat_at', '')
    redis.call('SREM', prefix .. ':status:running', task_id)
    redis.call('SADD', prefix .. ':status:pending', task_id)
    local ready_score = redis.call('HGET', hkey, 'ready_score')
    if ready_score then
        redis.call('ZADD', ready, ready_score, task_id)
    end
    return {1, 'pending'}
end
redis.call('HSET', hkey, 'status', 'failed', 'completed_at', completed_at, 'heartbeat_at', '')
redis.call('SREM', prefix .. ':status:running', task_id)
redis.call('SADD', prefix .. ':status:failed', task_id)
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
local hkey = prefix .. ':task:' .. task_id
redis.call('HSET', hkey, unpack(ARGV, 9))
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
if dedup_key and dedup_key ~= '' then
    if redis.call('HGET', prefix .. ':keys', dedup_key) == task_id then
        redis.call('HDEL', prefix .. ':keys', dedup_key)
    end
end
return {1}
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
        "_notification_channel",
        "_notifications",
        "_owns_client",
        "_pending_read",
        "_pubsub",
        "_url",
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
        self._notifications = backend_config.notifications
        self._notification_channel = backend_config.notification_channel
        self._pubsub: "RedisPubSubLike | None" = None
        self._pending_read = PendingNativeRead()
        self._event_log: "RedisQueueEventLog | None" = None

    @property
    def capabilities(self) -> "QueueBackendCapabilities":
        """Backend behavior capabilities."""
        return QueueBackendCapabilities(
            supports_notifications=self._notifications,
            notification_backend=f"{self._backend_name}-pubsub" if self._notifications else None,
            notifications_durable=False,
            supports_completion_events=self._notifications,
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
            await _close_pubsub(self._pubsub, self._notification_channel)
            self._pubsub = None
        if self._owns_client and self._client is not None:
            close = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
            if close is not None:
                result = close()
                if inspect.isawaitable(result):
                    await result
            self._client = None

    def get_event_log(self, config: "EventLogConfig") -> "QueueEventLog | None":
        """Return Redis-protocol queue event history when enabled."""
        if not config.enabled:
            return None
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
        if key is not None:
            return await self._enqueue_keyed(record, key, publish=True)
        await self._save_new_record(record, publish=True)
        return record

    async def enqueue_many(self, specs: "Sequence[EnqueueSpec]") -> "list[QueuedTaskRecord]":
        """Persist a batch of Redis-backed tasks and coalesce worker wakeups.

        Returns:
            Queue task records in the same order as ``specs``.
        """
        if not specs:
            return []

        results: "list[QueuedTaskRecord]" = []
        unkeyed_records: "list[QueuedTaskRecord]" = []
        for spec in specs:
            if spec.key is not None:
                record = self._create_record(
                    spec.task_name,
                    args=spec.args,
                    kwargs=spec.kwargs,
                    queue=spec.queue,
                    priority=spec.priority,
                    max_retries=spec.max_retries,
                    scheduled_at=spec.scheduled_at,
                    key=spec.key,
                    execution_backend=spec.execution_backend,
                    execution_profile=spec.execution_profile,
                    metadata=spec.metadata,
                )
                results.append(await self._enqueue_keyed(record, spec.key, publish=False))
                continue

            record = self._create_record(
                spec.task_name,
                args=spec.args,
                kwargs=spec.kwargs,
                queue=spec.queue,
                priority=spec.priority,
                max_retries=spec.max_retries,
                scheduled_at=spec.scheduled_at,
                key=None,
                execution_backend=spec.execution_backend,
                execution_profile=spec.execution_profile,
                metadata=spec.metadata,
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
        committed = await self._commit_transition(
            task_id,
            expected_status=record.status,
            new_status="running",
            patch={"started_at": _serialize_datetime(now), "heartbeat_at": _serialize_datetime(now)},
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
            patch={"completed_at": _serialize_datetime(now), "heartbeat_at": ""},
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
        heartbeat_at = _serialize_datetime(_utc_now())
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
                task_id, expected_status="", expected_retry_count=expected_retry_count, patch={"heartbeat_at": ""}
            )

    async def requeue_stale_running(self, *, stale_after: "timedelta") -> "StaleTaskRecoveryResult":
        """Requeue running tasks with stale heartbeats.

        Returns:
            Summary of recovered records.
        """
        cutoff = _utc_now() - stale_after
        result = StaleTaskRecoveryResult()
        for record in await self._list_records_by_statuses(("running",)):
            if record.status != "running":
                continue
            if record.heartbeat_at is not None and record.heartbeat_at >= cutoff:
                result.skipped += 1
                continue
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
                "heartbeat_at": "",
                "error": record.error or "",
                "retry_count": str(record.retry_count),
                "ready_score": repr(_ready_score(record)),
            },
            zset_action=zset_action,
            score=score,
            publish_channel=self._notification_channel if (self._notifications and zset_action == "ready") else "",
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
            patch={"completed_at": _serialize_datetime(now), "heartbeat_at": "", "error": STALE_HEARTBEAT_ERROR},
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
            publish_channel=self._notification_channel if (self._notifications and due) else "",
            publish_payload=_json_dumps({"event": "task_available"}),
        )
        return record

    async def list_running_external(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Return externally dispatched tasks with references to reconcile."""
        records = [
            record
            for record in await self._list_records_by_statuses(("pending", "scheduled", "running"))
            if record.status in {"pending", "scheduled", "running"} and record.execution_ref is not None
        ]
        records.sort(key=lambda record: (record.started_at or record.created_at, record.created_at))
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

    async def cleanup_terminal(self, before: "datetime") -> "int":
        """Delete terminal records completed before a cutoff.

        Returns:
            Number of deleted records.
        """
        client = await self._get_client()
        count = 0
        for record in await self._list_records_by_statuses(tuple(sorted(_TERMINAL_STATUSES))):
            if record.status not in _TERMINAL_STATUSES or record.completed_at is None or record.completed_at >= before:
                continue
            outcome = await _eval_script(
                client, _DELETE_TERMINAL_SCRIPT, [self._task_key(record.id)], [self._key_prefix, str(record.id)]
            )
            if outcome and int(outcome[0]) == 1:
                count += 1
        return count

    async def notify_new_task(self, record: "QueuedTaskRecord") -> "None":
        """Publish a Redis-protocol pub/sub message when work is available."""
        if self._notifications and record.status in _DUE_STATUSES and record.is_due:
            payload = _json_dumps({"event": "task_available"})
            client = await self._get_client()
            await client.publish(self._notification_channel, payload)

    async def wait_for_notifications(self, timeout: "float | None" = None) -> "bool":
        """Wait for a Redis-protocol pub/sub message when notifications are enabled.

        A single pub/sub receive is retained across worker poll timeouts; only
        a real message, a read failure, or backend close ends it.

        Returns:
            True when a notification was observed.
        """
        if not self._notifications:
            return await super().wait_for_notifications(timeout=timeout)
        pubsub = await self._get_pubsub()
        task = await self._pending_read.race(lambda: _receive_pubsub_message(pubsub), timeout)
        if task is None:
            return False
        exc = task.exception()
        if exc is not None:
            await self._reset_pubsub()
            raise exc
        return bool(task.result())

    async def _reset_pubsub(self) -> "None":
        """Drop the pub/sub subscription so the next wait re-establishes it."""
        await self._pending_read.aclose()
        pubsub = self._pubsub
        self._pubsub = None
        if pubsub is not None:
            await _close_pubsub(pubsub, self._notification_channel)

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
            subscribe = self._pubsub.subscribe(self._notification_channel)
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
            self._notification_channel,
            _json_dumps({"event": "task_available"}),
            "1" if publish and self._notifications else "0",
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
    def _ready_key(self) -> "str":
        return f"{self._key_prefix}:ready"

    @property
    def _scheduled_key(self) -> "str":
        return f"{self._key_prefix}:scheduled"

    @property
    def _completion_channel(self) -> "str":
        return f"{self._key_prefix}:completions"

    def _status_key(self, status: "str") -> "str":
        return f"{self._key_prefix}:status:{status}"

    def _task_key(self, task_id: "UUID") -> "str":
        return f"{self._key_prefix}:task:{task_id}"

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
            "started_at": _serialize_datetime(record.started_at),
            "completed_at": _serialize_datetime(record.completed_at),
            "heartbeat_at": _serialize_datetime(record.heartbeat_at),
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
