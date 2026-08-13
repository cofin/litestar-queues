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
    EXTERNAL_DISPATCH_RESERVATION_PREFIX,
    STALE_HEARTBEAT_ERROR,
    BaseQueueBackend,
    attempts_consumed,
    interruption_count,
    is_external_dispatch_reservation,
    record_matches_filters,
    retry_schedule,
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

    from litestar_queues.backends._protocol import ClientLike, PipelineLike, PubSubLike
    from litestar_queues.config import QueueConfig
    from litestar_queues.events import EventHistoryConfig, QueueEventLog
    from litestar_queues.models import HeartbeatTouch, TaskRequest

__all__ = ("RedisQueueBackend",)

_DUE_STATUSES = {"pending", "scheduled"}
_STATUS_VALUES = {"cancelled", "completed", "expired", "failed", "pending", "running", "scheduled"}
_TERMINAL_STATUSES = {"cancelled", "completed", "expired", "failed"}
_MAINTENANCE_INDEX_VERSION = "2"
_CLAIMED_OUTCOME = 1
_EXPIRED_OUTCOME = 2
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

local reservation_prefix = ARGV[7]
local queue_filter = {}
local has_queue_filter = false
for i = 8, #ARGV do
    queue_filter[ARGV[i]] = true
    has_queue_filter = true
end

local expired = {}
local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now_ms)
for _, id in ipairs(due) do
    local hkey = prefix .. ':task:' .. id
    local status = redis.call('HGET', hkey, 'status')
    local eb = redis.call('HGET', hkey, 'execution_backend')
    local q = redis.call('HGET', hkey, 'queue')
    local execution_ref = redis.call('HGET', hkey, 'execution_ref')
    local eb_ok = (eb_filter == '' or eb == eb_filter)
    local q_ok = (not has_queue_filter or queue_filter[q] == true)
    local reservation_active = execution_ref and string.sub(execution_ref, 1, string.len(reservation_prefix)) == reservation_prefix
    local unreserved = not reservation_active
    if (status == 'scheduled' or status == 'pending') and eb_ok and q_ok and unreserved then
        local expires_score = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
        if expires_score > 0 and expires_score <= now_ms then
            redis.call('HSET', hkey, 'status', 'expired', 'completed_at', now_iso,
                'completed_score', now_ms, 'heartbeat_at', '', 'heartbeat_score', '0')
            redis.call('SREM', prefix .. ':status:' .. status, id)
            redis.call('SADD', prefix .. ':status:expired', id)
            redis.call('ZREM', ready, id)
            redis.call('ZREM', prefix .. ':maintenance:running', id)
            redis.call('ZREM', prefix .. ':maintenance:external', id)
            redis.call('ZREM', prefix .. ':maintenance:expiry', id)
            redis.call('ZADD', prefix .. ':maintenance:terminal', now_ms, id)
            redis.call('PUBLISH', prefix .. ':completions', id)
            expired[#expired + 1] = id
        else
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
    elseif status ~= 'scheduled' and status ~= 'pending' then
        redis.call('ZREM', scheduled, id)
    end
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
        local execution_ref = redis.call('HGET', hkey, 'execution_ref')
        local eb_ok = (eb_filter == '' or eb == eb_filter)
        local q_ok = (not has_queue_filter or queue_filter[q] == true)
        local reservation_active = execution_ref and string.sub(execution_ref, 1, string.len(reservation_prefix)) == reservation_prefix
        local unreserved = not reservation_active
        if eb_ok and q_ok and unreserved then
            local expires_score = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
            if expires_score > 0 and expires_score <= now_ms then
                redis.call('HSET', hkey, 'status', 'expired', 'completed_at', now_iso,
                    'completed_score', now_ms, 'heartbeat_at', '', 'heartbeat_score', '0')
                redis.call('SREM', prefix .. ':status:pending', id)
                redis.call('SADD', prefix .. ':status:expired', id)
                redis.call('ZREM', ready, id)
                redis.call('ZREM', scheduled, id)
                redis.call('ZREM', prefix .. ':maintenance:running', id)
                redis.call('ZREM', prefix .. ':maintenance:external', id)
                redis.call('ZREM', prefix .. ':maintenance:expiry', id)
                redis.call('ZADD', prefix .. ':maintenance:terminal', now_ms, id)
                redis.call('PUBLISH', prefix .. ':completions', id)
                expired[#expired + 1] = id
            else
                redis.call('HSET', hkey, 'status', 'running', 'started_at', now_iso, 'heartbeat_at', now_iso,
                    'started_score', now_ms, 'heartbeat_score', now_ms)
                redis.call('SREM', prefix .. ':status:pending', id)
                redis.call('SADD', prefix .. ':status:running', id)
                redis.call('ZREM', ready, id)
                redis.call('ZADD', prefix .. ':maintenance:running', now_ms, id)
                redis.call('ZREM', prefix .. ':maintenance:terminal', id)
                redis.call('ZREM', prefix .. ':maintenance:expiry', id)
                redis.call('ZREM', prefix .. ':maintenance:external', id)
                claimed[#claimed + 1] = id
            end
        end
    end
end
local outcome = {}
for _, id in ipairs(claimed) do
    outcome[#outcome + 1] = 'claimed:' .. id
end
for _, id in ipairs(expired) do
    outcome[#outcome + 1] = 'expired:' .. id
end
return outcome
"""
_CLAIM_TASK_SCRIPT = """
local hkey = KEYS[1]
local ready = KEYS[2]
local scheduled = KEYS[3]
local prefix = ARGV[1]
local task_id = ARGV[2]
local now_ms = tonumber(ARGV[3])
local now_iso = ARGV[4]
local reservation_prefix = ARGV[5]
local expected_retry = ARGV[6]
local expected_ref = ARGV[7]

local status = redis.call('HGET', hkey, 'status')
if status ~= 'pending' and status ~= 'scheduled' then
    return {0}
end
local execution_ref = redis.call('HGET', hkey, 'execution_ref')
if expected_retry ~= '' and redis.call('HGET', hkey, 'retry_count') ~= expected_retry then return {0} end
if expected_ref ~= '' and execution_ref ~= expected_ref then return {0} end
if execution_ref and string.sub(execution_ref, 1, string.len(reservation_prefix)) == reservation_prefix then
    return {0}
end
local scheduled_score = redis.call('ZSCORE', scheduled, task_id)
if scheduled_score and tonumber(scheduled_score) > now_ms then
    return {0}
end
local expires_score = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
if (not execution_ref or execution_ref == '') and expires_score > 0 and expires_score <= now_ms then
    redis.call('HSET', hkey, 'status', 'expired', 'completed_at', now_iso,
        'completed_score', now_ms, 'heartbeat_at', '', 'heartbeat_score', '0')
    redis.call('SREM', prefix .. ':status:' .. status, task_id)
    redis.call('SADD', prefix .. ':status:expired', task_id)
    redis.call('ZREM', ready, task_id)
    redis.call('ZREM', scheduled, task_id)
    redis.call('ZREM', prefix .. ':maintenance:running', task_id)
    redis.call('ZREM', prefix .. ':maintenance:external', task_id)
    redis.call('ZREM', prefix .. ':maintenance:expiry', task_id)
    redis.call('ZADD', prefix .. ':maintenance:terminal', now_ms, task_id)
    redis.call('PUBLISH', prefix .. ':completions', task_id)
    return {2}
end

redis.call('HSET', hkey, 'status', 'running', 'started_at', now_iso, 'heartbeat_at', now_iso,
    'started_score', now_ms, 'heartbeat_score', now_ms)
redis.call('SREM', prefix .. ':status:' .. status, task_id)
redis.call('SADD', prefix .. ':status:running', task_id)
redis.call('ZREM', ready, task_id)
redis.call('ZREM', scheduled, task_id)
redis.call('ZADD', prefix .. ':maintenance:running', now_ms, task_id)
redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
redis.call('ZREM', prefix .. ':maintenance:expiry', task_id)
local execution_ref = redis.call('HGET', hkey, 'execution_ref')
if execution_ref and execution_ref ~= '' then
    redis.call('ZADD', prefix .. ':maintenance:external', now_ms, task_id)
else
    redis.call('ZREM', prefix .. ':maintenance:external', task_id)
end
return {1}
"""

_CLEAR_EXECUTION_REF_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'pending' and status ~= 'scheduled' then return {0} end
if redis.call('HGET', KEYS[1], 'retry_count') ~= ARGV[1] then return {0} end
if redis.call('HGET', KEYS[1], 'execution_ref') ~= ARGV[2] then return {0} end
redis.call('HSET', KEYS[1], 'execution_ref', '')
return {1}
"""

_REPLACE_EXECUTION_REF_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'pending' and status ~= 'scheduled' then return {0} end
if redis.call('HGET', KEYS[1], 'retry_count') ~= ARGV[1] then return {0} end
if redis.call('HGET', KEYS[1], 'execution_ref') ~= ARGV[2] then return {0} end
redis.call('HSET', KEYS[1], 'execution_ref', ARGV[3])
return {1}
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
local retry_at = ARGV[9]
local retry_score = ARGV[10]
local queued_at = ARGV[11]
local ready_score = ARGV[12]

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
local interruptions = 0
local metadata_json = redis.call('HGET', hkey, 'metadata')
if metadata_json and metadata_json ~= '' then
    local decoded_ok, decoded = pcall(cjson.decode, metadata_json)
    if decoded_ok and type(decoded) == 'table' and type(decoded.interruptions) == 'number' then
        interruptions = decoded.interruptions
    end
end
if retry == '1' and (retry_count - interruptions) < max_retries then
    local new_retry_count = retry_count + 1
    local retry_status = 'pending'
    if retry_at ~= '' then retry_status = 'scheduled' end
    redis.call('HSET', hkey, 'status', retry_status, 'retry_count', new_retry_count,
        'queued_at', queued_at, 'ready_score', ready_score, 'scheduled_at', retry_at,
        'started_at', '', 'started_score', '0', 'heartbeat_at', '', 'heartbeat_score', '0')
    redis.call('SREM', prefix .. ':status:running', task_id)
    redis.call('SADD', prefix .. ':status:' .. retry_status, task_id)
    redis.call('ZREM', prefix .. ':maintenance:running', task_id)
    redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
    local execution_ref = redis.call('HGET', hkey, 'execution_ref')
    if execution_ref and execution_ref ~= '' then
        local created_score = redis.call('HGET', hkey, 'created_score') or '0'
        redis.call('ZADD', prefix .. ':maintenance:external', created_score, task_id)
    else
        redis.call('ZREM', prefix .. ':maintenance:external', task_id)
    end
    if retry_at ~= '' then
        redis.call('ZREM', ready, task_id)
        redis.call('ZADD', KEYS[3], retry_score, task_id)
    else
        redis.call('ZREM', KEYS[3], task_id)
        redis.call('ZADD', ready, ready_score, task_id)
    end
    return {1, retry_status}
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
local expires_score = tonumber(ARGV[9]) or 0
local hkey = prefix .. ':task:' .. task_id
redis.call('HSET', hkey, unpack(ARGV, 10))
redis.call('SADD', prefix .. ':tasks', task_id)
redis.call('SADD', prefix .. ':status:' .. status, task_id)
redis.call('SADD', KEYS[3], task_id)
if expires_score > 0 then
    redis.call('ZADD', prefix .. ':maintenance:expiry', expires_score, task_id)
end
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
local expires_score = tonumber(ARGV[10]) or 0
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
redis.call('HSET', hkey, unpack(ARGV, 11))
redis.call('SADD', prefix .. ':tasks', task_id)
redis.call('SADD', prefix .. ':status:' .. status, task_id)
redis.call('SADD', KEYS[3], task_id)
if expires_score > 0 then
    redis.call('ZADD', prefix .. ':maintenance:expiry', expires_score, task_id)
end
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
local expected_worker = ARGV[10]

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
if expected_worker ~= '' then
    local worker_id = redis.call('HGET', hkey, 'worker_id')
    if worker_id ~= expected_worker then
        return {0}
    end
end
if new_status ~= '' then
    redis.call('SREM', prefix .. ':status:' .. status, task_id)
    redis.call('SADD', prefix .. ':status:' .. new_status, task_id)
    redis.call('HSET', hkey, 'status', new_status)
end
if #ARGV >= 11 then
    redis.call('HSET', hkey, unpack(ARGV, 11))
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
if final_status == 'completed' or final_status == 'failed' or final_status == 'cancelled'
        or final_status == 'expired' then
    local completed_score = redis.call('HGET', hkey, 'completed_score') or '0'
    redis.call('ZADD', prefix .. ':maintenance:terminal', completed_score, task_id)
else
    redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
end
if final_status == 'pending' or final_status == 'scheduled' then
    local expires_score = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
    if expires_score > 0 then
        redis.call('ZADD', prefix .. ':maintenance:expiry', expires_score, task_id)
    else
        redis.call('ZREM', prefix .. ':maintenance:expiry', task_id)
    end
else
    redis.call('ZREM', prefix .. ':maintenance:expiry', task_id)
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
if status ~= 'completed' and status ~= 'failed' and status ~= 'cancelled' and status ~= 'expired' then
    return {0}
end
local dedup_key = redis.call('HGET', hkey, 'key')
local queue_index_key = redis.call('HGET', hkey, 'queue_index_key')
redis.call('DEL', hkey)
redis.call('SREM', prefix .. ':tasks', task_id)
redis.call('ZREM', prefix .. ':ready', task_id)
redis.call('ZREM', prefix .. ':scheduled', task_id)
redis.call('SREM', prefix .. ':status:' .. status, task_id)
redis.call('ZREM', prefix .. ':maintenance:running', task_id)
redis.call('ZREM', prefix .. ':maintenance:external', task_id)
redis.call('ZREM', prefix .. ':maintenance:terminal', task_id)
redis.call('ZREM', prefix .. ':maintenance:expiry', task_id)
if queue_index_key and queue_index_key ~= '' then
    redis.call('SREM', queue_index_key, task_id)
end
if dedup_key and dedup_key ~= '' then
    if redis.call('HGET', prefix .. ':keys', dedup_key) == task_id then
        redis.call('HDEL', prefix .. ':keys', dedup_key)
    end
end
return {1}
"""
_QUEUE_STATISTICS_SCRIPT = """
local counts = {}
for index = 2, #KEYS do
    counts[index - 1] = redis.call('SINTERCARD', 2, KEYS[1], KEYS[index])
end
return counts
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


_RESERVE_EXTERNAL_DISPATCH_SCRIPT = """
local hkey = KEYS[1]
local status = redis.call('HGET', hkey, 'status')
if status ~= 'pending' and status ~= 'scheduled' then
    return {0}
end
local now = tonumber(ARGV[2])
local scheduled = tonumber(redis.call('HGET', hkey, 'scheduled_score')) or 0
local expires = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
local execution_ref = redis.call('HGET', hkey, 'execution_ref')
if ARGV[6] ~= '' and redis.call('HGET', hkey, 'retry_count') ~= ARGV[6] then
    return {0}
end
if scheduled > now or (expires > 0 and expires <= now)
        or (execution_ref and execution_ref ~= '') then
    return {0}
end
redis.call(
    'HSET',
    hkey,
    'execution_backend', ARGV[3],
    'execution_profile', ARGV[4],
    'execution_ref', ARGV[5]
)
redis.call('ZREM', KEYS[2], ARGV[1])
local created = tonumber(redis.call('HGET', hkey, 'created_score')) or 0
redis.call('ZADD', KEYS[3], created, ARGV[1])
return {1}
"""


_RELEASE_EXTERNAL_DISPATCH_SCRIPT = """
local hkey = KEYS[1]
if redis.call('HGET', hkey, 'execution_ref') ~= ARGV[2] then
    return {0}
end
redis.call(
    'HSET',
    hkey,
    'execution_backend', ARGV[3],
    'execution_profile', ARGV[4],
    'execution_ref', ''
)
redis.call('ZREM', KEYS[3], ARGV[1])
local status = redis.call('HGET', hkey, 'status')
local expires = tonumber(redis.call('HGET', hkey, 'expires_score')) or 0
if (status == 'pending' or status == 'scheduled') and expires > 0 then
    redis.call('ZADD', KEYS[2], expires, ARGV[1])
end
return {1}
"""

_FINALIZE_EXTERNAL_DISPATCH_SCRIPT = """
local hkey = KEYS[1]
if redis.call('HGET', hkey, 'execution_ref') ~= ARGV[1] then
    return {0}
end
redis.call(
    'HSET',
    hkey,
    'execution_backend', ARGV[2],
    'execution_profile', ARGV[3],
    'execution_ref', ARGV[4]
)
return {1}
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
        "_completion_lock",
        "_completion_pubsub",
        "_completion_reader_task",
        "_completion_waiters",
        "_control_channel",
        "_control_pending_read",
        "_control_pubsub",
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
        self._client: "ClientLike | None" = cast("ClientLike | None", backend_config.client)
        self._owns_client = self._client is None
        self._url = backend_config.url
        key_prefix = (
            backend_config.key_prefix
            if backend_config.key_prefix is not None
            else config.names.root
            if config is not None
            else "litestar_queues"
        )
        self._key_prefix = key_prefix.rstrip(":")
        self._notifications = backend_config.worker_wakeups
        self._wakeup_channel = (
            backend_config.wakeup_channel
            if backend_config.wakeup_channel is not None
            else config.names.channel("worker_wakeups")
            if config is not None
            else "litestar_queues:worker_wakeups"
        )
        self._control_channel = (
            config.names.channel("worker_control") if config is not None else "litestar_queues:worker_control"
        )
        self._pubsub: "PubSubLike | None" = None
        self._pending_read = PendingNativeRead()
        self._control_pubsub: "PubSubLike | None" = None
        self._control_pending_read = PendingNativeRead()
        self._completion_lock = asyncio.Lock()
        self._completion_pubsub: "PubSubLike | None" = None
        self._completion_reader_task: "asyncio.Task[None] | None" = None
        self._completion_waiters: "dict[str, set[asyncio.Future[bool]]]" = {}
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
        await self._require_maintenance_indexes()
        return True

    async def close(self) -> "None":
        """Close owned Redis-protocol client resources."""
        if self._event_log is not None:
            await self._event_log.flush_events()
        await self._pending_read.aclose()
        await self._control_pending_read.aclose()
        await self._close_completion_subscriber()
        if self._pubsub is not None:
            await _close_pubsub(self._pubsub, self._wakeup_channel)
            self._pubsub = None
        if self._control_pubsub is not None:
            await _close_pubsub(self._control_pubsub, self._control_channel)
            self._control_pubsub = None
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
        expires_at: "datetime | None" = None,
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
            expires_at=expires_at,
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
                    expires_at=request.expires_at,
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
                expires_at=request.expires_at,
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
        self._record_enqueue_batch(len(requests))
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
            and not record.is_expired
            and not is_external_dispatch_reservation(record.execution_ref)
            and (queue is None or record.queue == queue)
            and (execution_backend is None or record.execution_backend == execution_backend)
        ]
        due_records.sort(key=lambda record: (-record.priority, record.created_at))
        return due_records[:limit]

    async def claim_task(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "QueuedTaskRecord | None":
        claimed, _ = await self.claim_task_with_expired(
            task_id, expected_retry_count=expected_retry_count, expected_execution_ref=expected_execution_ref
        )
        return claimed

    async def claim_task_with_expired(
        self, task_id: "UUID", *, expected_retry_count: "int | None" = None, expected_execution_ref: "str | None" = None
    ) -> "tuple[QueuedTaskRecord | None, QueuedTaskRecord | None]":
        """Atomically claim a pending task via a single fenced script.

        Returns:
            The claimed record and the expired record, at most one of which is set.
        """
        client = await self._get_client()
        now = _utc_now()
        outcome = await _eval_script(
            client,
            _CLAIM_TASK_SCRIPT,
            [self._task_key(task_id), self._ready_key, self._scheduled_key],
            [
                self._key_prefix,
                str(task_id),
                repr(_maintenance_score(now)),
                _serialize_datetime(now),
                EXTERNAL_DISPATCH_RESERVATION_PREFIX,
                "" if expected_retry_count is None else str(expected_retry_count),
                expected_execution_ref or "",
            ],
        )
        if not outcome:
            return None, None
        result = int(outcome[0])
        if result == _CLAIMED_OUTCOME:
            return await self.get_task(task_id), None
        if result == _EXPIRED_OUTCOME:
            return None, await self.get_task(task_id)
        return None, None

    async def claim_many(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "list[QueuedTaskRecord]":
        """Claim up to ``limit`` due tasks in a single fenced ``EVAL``.

        Returns:
            Claimed task records in claim order.
        """
        claimed, _ = await self.claim_many_with_expired(
            limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
        )
        return claimed

    async def claim_many_with_expired(
        self,
        *,
        limit: "int",
        queues: "tuple[str, ...]" = (),
        execution_backend: "str | None" = None,
        queue_limits: "Mapping[str, int] | None" = None,
    ) -> "tuple[list[QueuedTaskRecord], list[QueuedTaskRecord]]":
        """Claim records and report expirations owned by the same Lua script."""
        if queue_limits is not None:
            return await super().claim_many_with_expired(
                limit=limit, queues=queues, execution_backend=execution_backend, queue_limits=queue_limits
            )
        if limit <= 0:
            return [], []
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
            EXTERNAL_DISPATCH_RESERVATION_PREFIX,
            *queues,
        ]
        outcome = await _eval_script(client, _CLAIM_SCRIPT, [self._ready_key, self._scheduled_key], args)
        claimed_ids: "list[str]" = []
        expired_ids: "list[str]" = []
        for value in outcome or ():
            kind, _, task_id = _decode(value).partition(":")
            if kind == "claimed":
                claimed_ids.append(task_id)
            elif kind == "expired":
                expired_ids.append(task_id)
        claimed = await self._records_from_ids(claimed_ids) if claimed_ids else []
        expired = await self._records_from_ids(expired_ids) if expired_ids else []
        return claimed, expired

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
        self,
        task_id: "UUID",
        error: "str",
        *,
        retry: "bool" = True,
        expected_retry_count: "int | None" = None,
        retry_at: "datetime | None" = None,
        queued_at: "datetime | None" = None,
    ) -> "QueuedTaskRecord | None":
        """Mark a task as failed or retry it via a single fenced script.

        Returns:
            The updated record, if it exists.
        """
        client = await self._get_client()
        now = _utc_now()
        queue_time = queued_at or now
        score_record = await self.get_task(task_id)
        if score_record is not None:
            score_record.queued_at = queue_time
        outcome = await _eval_script(
            client,
            _FAIL_SCRIPT,
            [self._task_key(task_id), self._ready_key, self._scheduled_key],
            [
                self._key_prefix,
                str(task_id),
                "" if expected_retry_count is None else str(expected_retry_count),
                error,
                "1" if retry else "0",
                _serialize_datetime(now),
                self._completion_channel,
                repr(_maintenance_score(now)),
                _serialize_datetime(retry_at),
                repr(_scheduled_score(retry_at)),
                _serialize_datetime(queue_time),
                repr(_ready_score(score_record)) if score_record is not None else "0",
            ],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        record = await self.get_task(task_id)
        if record is not None and _decode(outcome[1]) in {"pending", "scheduled"}:
            await self.notify_new_task(record)
        return record

    async def assign_worker(
        self, task_id: "UUID", *, worker_id: "str", expected_retry_count: "int"
    ) -> "QueuedTaskRecord | None":
        """Persist running-record ownership through the fenced transition script.

        Returns:
            The owned record, or ``None`` when the fence was lost.
        """
        committed = await self._commit_transition(
            task_id,
            expected_status="running",
            expected_retry_count=expected_retry_count,
            patch={"worker_id": worker_id},
        )
        return await self.get_task(task_id) if committed else None

    async def interrupt_task(
        self, task_id: "UUID", *, expected_retry_count: "int", worker_id: "str", queued_at: "datetime"
    ) -> "QueuedTaskRecord | None":
        """Return an owned running attempt to pending through the fenced transition script.

        Returns:
            The requeued record, or ``None`` when the fence was lost.
        """
        record = await self.get_task(task_id)
        if (
            record is None
            or record.status != "running"
            or record.retry_count != expected_retry_count
            or record.worker_id != worker_id
        ):
            return None
        record.status = "pending"
        record.queued_at = queued_at
        record.scheduled_at = None
        record.started_at = None
        record.heartbeat_at = None
        record.completed_at = None
        record.execution_ref = None
        record.worker_id = None
        record.metadata["interruptions"] = interruption_count(record) + 1
        record.retry_count += 1
        zset_action, score = self._index_action(record)
        committed = await self._commit_transition(
            task_id,
            expected_status="running",
            expected_retry_count=expected_retry_count,
            expected_worker_id=worker_id,
            new_status="pending",
            patch={
                "queued_at": _serialize_datetime(queued_at),
                "scheduled_at": "",
                "started_at": "",
                "started_score": "0",
                "heartbeat_at": "",
                "heartbeat_score": "0",
                "completed_at": "",
                "completed_score": "0",
                "execution_ref": "",
                "worker_id": "",
                "retry_count": str(record.retry_count),
                "metadata": _json_dumps(record.metadata),
                "ready_score": repr(_ready_score(record)),
            },
            zset_action=zset_action,
            score=score,
            publish_channel=self._wakeup_channel if (self._notifications and zset_action == "ready") else "",
            publish_payload=_json_dumps({"event": "task_available"}),
        )
        return await self.get_task(task_id) if committed else None

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
            if requeue_on_stale and attempts_consumed(latest) < latest.max_retries:
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
        queued_at, retry_at = retry_schedule(record)
        record.status = "scheduled" if retry_at is not None else "pending"
        record.queued_at = queued_at
        record.scheduled_at = retry_at
        record.priority = stale_requeue_priority(record.priority, self._stale_requeue_priority_policy())
        record.started_at = None
        record.heartbeat_at = None
        record.error = stale_requeue_error(record.error)
        record.retry_count += 1
        zset_action, score = self._index_action(record)
        return await self._commit_transition(
            record.id,
            expected_status="running",
            expected_retry_count=expected_retry,
            new_status=record.status,
            patch={
                "priority": str(record.priority),
                "started_at": "",
                "started_score": "0",
                "heartbeat_at": "",
                "heartbeat_score": "0",
                "error": record.error or "",
                "retry_count": str(record.retry_count),
                "queued_at": _serialize_datetime(queued_at),
                "scheduled_at": _serialize_datetime(retry_at),
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

    async def reserve_external_dispatch(
        self,
        task_id: "UUID",
        execution_backend: "str",
        reservation_ref: "str",
        *,
        execution_profile: "str | None" = None,
        expected_retry_count: "int | None" = None,
    ) -> "QueuedTaskRecord | None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _RESERVE_EXTERNAL_DISPATCH_SCRIPT,
            [self._task_key(task_id), self._maintenance_expiry_key, self._maintenance_external_key],
            [
                str(task_id),
                repr(_maintenance_score(_utc_now())),
                execution_backend,
                execution_profile or "",
                reservation_ref,
                "" if expected_retry_count is None else str(expected_retry_count),
            ],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        return await self.get_task(task_id)

    async def clear_execution_ref(
        self, task_id: "UUID", expected_retry_count: "int", expected_execution_ref: "str"
    ) -> "QueuedTaskRecord | None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _CLEAR_EXECUTION_REF_SCRIPT,
            [self._task_key(task_id)],
            [str(expected_retry_count), expected_execution_ref],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        record = await self.get_task(task_id)
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def replace_execution_ref(
        self, task_id: "UUID", expected_retry_count: "int", expected_execution_ref: "str", execution_ref: "str"
    ) -> "QueuedTaskRecord | None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _REPLACE_EXECUTION_REF_SCRIPT,
            [self._task_key(task_id)],
            [str(expected_retry_count), expected_execution_ref, execution_ref],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        return await self.get_task(task_id)

    async def release_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _RELEASE_EXTERNAL_DISPATCH_SCRIPT,
            [self._task_key(task_id), self._maintenance_expiry_key, self._maintenance_external_key],
            [str(task_id), reservation_ref, execution_backend, execution_profile or ""],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        record = await self.get_task(task_id)
        if record is not None:
            await self.notify_new_task(record)
        return record

    async def finalize_external_dispatch(
        self,
        task_id: "UUID",
        reservation_ref: "str",
        execution_backend: "str",
        execution_ref: "str",
        *,
        execution_profile: "str | None" = None,
    ) -> "QueuedTaskRecord | None":
        client = await self._get_client()
        outcome = await _eval_script(
            client,
            _FINALIZE_EXTERNAL_DISPATCH_SCRIPT,
            [self._task_key(task_id)],
            [reservation_ref, execution_backend, execution_profile or "", execution_ref],
        )
        if not outcome or int(outcome[0]) != 1:
            return None
        return await self.get_task(task_id)

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

    async def get_statistics(self, *, queue: "str | None" = None) -> "QueueStatistics":
        """Return queue status counts."""
        client = await self._get_client()
        statistics = QueueStatistics()
        statuses = sorted(_STATUS_VALUES)
        status_keys = [self._status_key(status) for status in statuses]
        counts = (
            await _pipeline_scard(client, status_keys)
            if queue is None
            else await _eval_script(client, _QUEUE_STATISTICS_SCRIPT, [self._queue_index_key(queue), *status_keys], [])
        )
        for status, count in zip(statuses, counts, strict=True):
            setattr(statistics, status, int(count))
        return statistics

    async def expire_overdue(self, *, limit: "int | None" = None) -> "list[QueuedTaskRecord]":
        """Transition overdue pending and scheduled records to ``expired``."""
        if limit is not None and limit <= 0:
            return []
        await self._require_maintenance_indexes()
        client = await self._get_client()
        now = _utc_now()
        task_ids = await client.zrangebyscore(
            self._maintenance_expiry_key,
            "-inf",
            _maintenance_score(now),
            start=0 if limit is not None else None,
            num=limit,
        )
        records = await self._records_from_ids(task_ids)
        expired: "list[QueuedTaskRecord]" = []
        for record in records:
            if record.status not in _DUE_STATUSES or not record.is_expired:
                await client.zrem(self._maintenance_expiry_key, str(record.id))
                continue
            if await self._expire_record(record, now=now):
                record.status = "expired"
                record.completed_at = now
                record.heartbeat_at = None
                expired.append(record)
        return expired

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
        pipeline.delete(
            self._maintenance_running_key,
            self._maintenance_external_key,
            self._maintenance_terminal_key,
            self._maintenance_expiry_key,
        )
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
            if record.status in _DUE_STATUSES and record.expires_at is not None and record.execution_ref is None:
                pipeline.zadd(self._maintenance_expiry_key, {task_id: _maintenance_score(record.expires_at)})
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
            self._record_wakeup_emitted()

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

    async def notify_worker_control(self, worker_id: "str | None") -> "None":
        """Publish a worker-control hint on the Redis-protocol control channel.

        The hint is lossy by contract: it only shortens the wait before the
        owning worker reconciles durable status.
        """
        if not self._notifications:
            return
        client = await self._get_client()
        await client.publish(self._control_channel, _json_dumps({"event": "worker_control", "worker_id": worker_id}))

    async def wait_for_worker_control(self, *, worker_id: "str", timeout: "float | None" = None) -> "bool":
        """Wait for a Redis-protocol worker-control hint.

        The control subscription and its pending receive are retained across
        worker poll timeouts, exactly like the wakeup subscription, and are
        never shared with it: one in-flight read per subscription.

        Returns:
            True when a control hint was observed.

        Raises:
            Exception: Whatever the pub/sub receive raised, after the
                subscription is reset so the next wait reconnects.
        """
        if not self._notifications:
            return await super().wait_for_worker_control(worker_id=worker_id, timeout=timeout)
        pubsub = await self._get_control_pubsub()
        task = await self._control_pending_read.race(lambda: _receive_pubsub_message(pubsub), timeout)
        if task is None:
            return False
        exc = task.exception()
        if exc is not None:
            await self._reset_control_pubsub()
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

    async def _reset_control_pubsub(self) -> "None":
        """Drop the control subscription so the next wait re-establishes it."""
        await self._control_pending_read.aclose()
        pubsub = self._control_pubsub
        self._control_pubsub = None
        if pubsub is not None:
            await _close_pubsub(pubsub, self._control_channel)

    async def wait_for_completion(self, task_id: "UUID", *, timeout: "float | None" = None) -> "bool":
        """Wait for a terminal completion message naming ``task_id``.

        Returns:
            True when a completion signal for ``task_id`` arrived before the deadline.
        """
        if not self._notifications:
            return False
        loop = asyncio.get_running_loop()
        waiter: "asyncio.Future[bool]" = loop.create_future()
        target = str(task_id)
        async with self._completion_lock:
            await self._ensure_completion_subscriber()
            self._completion_waiters.setdefault(target, set()).add(waiter)
        try:
            record = await self.get_task(task_id)
            if record is not None and record.status in _TERMINAL_STATUSES:
                return True
            if timeout is None:
                return await waiter
            try:
                return await asyncio.wait_for(waiter, timeout=timeout)
            except asyncio.TimeoutError:
                return False
        finally:
            async with self._completion_lock:
                waiters = self._completion_waiters.get(target)
                if waiters is not None:
                    waiters.discard(waiter)
                    if not waiters:
                        self._completion_waiters.pop(target, None)

    async def _ensure_completion_subscriber(self) -> "None":
        """Create the subscriber while ``_completion_lock`` is held."""
        if self._completion_reader_task is not None and not self._completion_reader_task.done():
            return
        client = await self._get_client()
        pubsub = client.pubsub()
        subscribe = pubsub.subscribe(self._completion_channel)
        if inspect.isawaitable(subscribe):
            await subscribe
        self._completion_pubsub = pubsub
        self._completion_reader_task = asyncio.create_task(
            self._read_completion_messages(pubsub), name=f"{self._backend_name}-queue-completions"
        )

    async def _read_completion_messages(self, pubsub: "PubSubLike") -> "None":
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message is None:
                    continue
                target = str(_decode(message.get("data")))
                for waiter in tuple(self._completion_waiters.get(target, ())):
                    if not waiter.done():
                        waiter.set_result(True)
        finally:
            for waiters in tuple(self._completion_waiters.values()):
                for waiter in tuple(waiters):
                    if not waiter.done():
                        waiter.set_result(False)

    async def _close_completion_subscriber(self) -> "None":
        async with self._completion_lock:
            reader, self._completion_reader_task = self._completion_reader_task, None
            pubsub, self._completion_pubsub = self._completion_pubsub, None
        await self._stop_completion_subscriber(reader, pubsub)

    async def _stop_completion_subscriber(
        self, reader: "asyncio.Task[None] | None", pubsub: "PubSubLike | None"
    ) -> "None":
        if reader is not None:
            reader.cancel()
            with suppress(asyncio.CancelledError):
                await reader
        if pubsub is not None:
            await _close_pubsub(pubsub, self._completion_channel)

    def _create_client(self, url: "str") -> "ClientLike":
        from redis import asyncio as redis_asyncio

        return cast("ClientLike", redis_asyncio.from_url(url, decode_responses=True))  # type: ignore[no-untyped-call]

    async def _get_client(self) -> "ClientLike":
        if self._client is None:
            await self.open()
        return cast("ClientLike", self._client)

    async def _get_pubsub(self) -> "PubSubLike":
        if self._pubsub is None:
            client = await self._get_client()
            self._pubsub = client.pubsub()
            subscribe = self._pubsub.subscribe(self._wakeup_channel)
            if inspect.isawaitable(subscribe):
                await subscribe
        return self._pubsub

    async def _get_control_pubsub(self) -> "PubSubLike":
        if self._control_pubsub is None:
            client = await self._get_client()
            self._control_pubsub = client.pubsub()
            subscribe = self._control_pubsub.subscribe(self._control_channel)
            if inspect.isawaitable(subscribe):
                await subscribe
        return self._control_pubsub

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
        expected_worker_id: "str" = "",
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
            expected_worker_id,
        ]
        if patch:
            for field, value in patch.items():
                args.append(field)
                args.append(value)
        outcome = await _eval_script(
            client, _TRANSITION_SCRIPT, [self._task_key(task_id), self._ready_key, self._scheduled_key], args
        )
        return bool(outcome and int(outcome[0]) == 1)

    async def _expire_record(self, record: "QueuedTaskRecord", *, now: "datetime | None" = None) -> "bool":
        expired_at = now or _utc_now()
        return await self._commit_transition(
            record.id,
            expected_status=record.status,
            new_status="expired",
            patch={
                "completed_at": _serialize_datetime(expired_at),
                "completed_score": repr(_maintenance_score(expired_at)),
                "heartbeat_at": "",
                "heartbeat_score": "0",
            },
            zset_action="remove",
            publish_channel=self._completion_channel if self._notifications else "",
            publish_payload=str(record.id),
        )

    async def _enqueue_keyed(self, record: "QueuedTaskRecord", key: "str", *, publish: "bool") -> "QueuedTaskRecord":
        client = await self._get_client()
        args = self._enqueue_args(record, publish=publish)
        args = [*args[:8], key, *args[8:]]
        outcome = await _eval_script(
            client,
            _ENQUEUE_KEYED_SCRIPT,
            [self._ready_key, self._scheduled_key, self._queue_index_key(record.queue)],
            args,
        )
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
        expires_at: "datetime | None",
        key: "str | None",
        execution_backend: "str",
        execution_profile: "str | None",
        metadata: "dict[str, Any] | None",
    ) -> "QueuedTaskRecord":
        now = _utc_now()
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
            expires_at=expires_at,
            key=key,
            metadata=dict(metadata or {}),
            created_at=now,
            queued_at=now,
        )

    async def _save_new_record(self, record: "QueuedTaskRecord", *, publish: "bool") -> "None":
        client = await self._get_client()
        await _eval_script(
            client,
            _ENQUEUE_SCRIPT,
            [self._ready_key, self._scheduled_key, self._queue_index_key(record.queue)],
            self._enqueue_args(record, publish=publish),
        )

    async def _save_new_records(self, records: "Sequence[QueuedTaskRecord]", *, publish: "bool") -> "None":
        if not records:
            return
        client = await self._get_client()
        pipeline = _create_pipeline(client)
        for record in records:
            keys = [self._ready_key, self._scheduled_key, self._queue_index_key(record.queue)]
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
            repr(_maintenance_score(record.expires_at)),
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
    def _maintenance_expiry_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:expiry"

    @property
    def _maintenance_terminal_key(self) -> "str":
        return f"{self._key_prefix}:maintenance:terminal"

    @property
    def _completion_channel(self) -> "str":
        return f"{self._key_prefix}:completions"

    def _status_key(self, status: "str") -> "str":
        return f"{self._key_prefix}:status:{status}"

    def _queue_index_key(self, queue: "str") -> "str":
        return f"{self._key_prefix}:queue:{hashed_index_value(queue)}"

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
            "queue_index_key": self._queue_index_key(record.queue),
            "execution_backend": record.execution_backend,
            "execution_profile": record.execution_profile or "",
            "execution_ref": record.execution_ref or "",
            "worker_id": record.worker_id or "",
            "status": record.status,
            "priority": str(record.priority),
            "max_retries": str(record.max_retries),
            "retry_count": str(record.retry_count),
            "scheduled_at": _serialize_datetime(record.scheduled_at),
            "expires_at": _serialize_datetime(record.expires_at),
            "expires_score": repr(_maintenance_score(record.expires_at)),
            "created_at": _serialize_datetime(record.created_at),
            "created_score": repr(_maintenance_score(record.created_at)),
            "queued_at": _serialize_datetime(record.queued_at),
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
            worker_id=str(mapping["worker_id"]) if mapping.get("worker_id") else None,
            status=_coerce_status(mapping.get("status")),
            priority=int(str(mapping.get("priority") or 0)),
            max_retries=int(str(mapping.get("max_retries") or 0)),
            retry_count=int(str(mapping.get("retry_count") or 0)),
            scheduled_at=_deserialize_datetime(mapping.get("scheduled_at")),
            expires_at=_deserialize_datetime(mapping.get("expires_at")),
            created_at=_deserialize_datetime(mapping.get("created_at")) or _utc_now(),
            queued_at=_deserialize_datetime(mapping.get("queued_at")) or _utc_now(),
            started_at=_deserialize_datetime(mapping.get("started_at")),
            completed_at=_deserialize_datetime(mapping.get("completed_at")),
            heartbeat_at=_deserialize_datetime(mapping.get("heartbeat_at")),
            result=_json_loads(mapping.get("result"), None),
            error=str(mapping["error"]) if mapping.get("error") else None,
            key=str(mapping["key"]) if mapping.get("key") else None,
            metadata=dict(_json_loads(mapping.get("metadata"), {})),
        )


def _create_pipeline(client: "ClientLike") -> "PipelineLike":
    try:
        return client.pipeline(transaction=False)
    except TypeError:
        return client.pipeline()


async def _execute_pipeline(pipeline: "PipelineLike") -> "list[Any]":
    result = pipeline.execute()
    if inspect.isawaitable(result):
        return list(await result)
    return list(cast("list[Any]", result))


async def _eval_script(client: "ClientLike", script: "str", keys: "list[str]", args: "list[str]") -> "list[Any]":
    result = client.eval(script, len(keys), *keys, *args)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    return list(cast("list[Any]", result))


async def _pipeline_hgetall(client: "ClientLike", keys: "list[str]") -> "list[dict[Any, Any]]":
    if not keys:
        return []
    pipeline = _create_pipeline(client)
    for key in keys:
        pipeline.hgetall(key)
    return cast("list[dict[Any, Any]]", await _execute_pipeline(pipeline))


async def _pipeline_smembers(client: "ClientLike", keys: "list[str]") -> "list[set[Any]]":
    if not keys:
        return []
    pipeline = _create_pipeline(client)
    for key in keys:
        pipeline.smembers(key)
    return [set(result) for result in await _execute_pipeline(pipeline)]


async def _pipeline_scard(client: "ClientLike", keys: "list[str]") -> "list[int]":
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
    queued = record.queued_at
    if queued.tzinfo is None:
        queued = queued.replace(tzinfo=timezone.utc)
    queued_ms = queued.astimezone(timezone.utc).timestamp() * 1000.0
    return (-record.priority) * _PRIORITY_STRIDE + queued_ms


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


async def _receive_pubsub_message(pubsub: "PubSubLike") -> "bool":
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


async def _close_pubsub(pubsub: "PubSubLike", channel: "str") -> "None":
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
