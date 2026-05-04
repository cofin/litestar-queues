-- name: create_schema
CREATE TABLE IF NOT EXISTS {table_name} (
    id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    args_json TEXT NOT NULL,
    kwargs_json TEXT NOT NULL,
    queue TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    scheduled_at TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    heartbeat_at TEXT,
    result_json TEXT NOT NULL,
    error TEXT,
    task_key TEXT UNIQUE,
    metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_{table_name}_pending
    ON {table_name} (status, queue, scheduled_at, priority, created_at);
CREATE INDEX IF NOT EXISTS ix_{table_name}_heartbeat
    ON {table_name} (status, heartbeat_at);

-- name: insert_task
INSERT INTO {table_name} (
    id, task_name, args_json, kwargs_json, queue, status, priority,
    max_retries, retry_count, scheduled_at, created_at, started_at,
    completed_at, heartbeat_at, result_json, error, task_key, metadata_json
) VALUES (
    :id, :task_name, :args_json, :kwargs_json, :queue, :status, :priority,
    :max_retries, :retry_count, :scheduled_at, :created_at, :started_at,
    :completed_at, :heartbeat_at, :result_json, :error, :task_key, :metadata_json
)

-- name: get_task
SELECT * FROM {table_name} WHERE id = :id

-- name: get_task_by_key
SELECT * FROM {table_name} WHERE task_key = :task_key

-- name: list_pending
SELECT *
FROM {table_name}
WHERE status IN ('pending', 'scheduled')
  AND (scheduled_at IS NULL OR scheduled_at <= :now)
  AND (:queue_filter IS NULL OR queue = :queue_value)
ORDER BY priority DESC, created_at ASC
LIMIT :limit

-- name: claim_task
UPDATE {table_name}
SET status = 'running',
    started_at = :started_at,
    heartbeat_at = :heartbeat_at
WHERE id = :id
  AND status IN ('pending', 'scheduled')
  AND (scheduled_at IS NULL OR scheduled_at <= :due_at)

-- name: complete_task
UPDATE {table_name}
SET status = 'completed',
    completed_at = :completed_at,
    heartbeat_at = :heartbeat_at,
    result_json = :result_json,
    error = NULL
WHERE id = :id

-- name: retry_task
UPDATE {table_name}
SET status = 'pending',
    retry_count = :retry_count,
    started_at = NULL,
    heartbeat_at = NULL,
    error = :error
WHERE id = :id

-- name: fail_task
UPDATE {table_name}
SET status = 'failed',
    completed_at = :completed_at,
    heartbeat_at = :heartbeat_at,
    error = :error
WHERE id = :id

-- name: cancel_task
UPDATE {table_name}
SET status = 'cancelled',
    completed_at = :completed_at
WHERE id = :id
  AND status IN ('pending', 'scheduled')

-- name: touch_heartbeat
UPDATE {table_name}
SET heartbeat_at = :heartbeat_at
WHERE id = :id
  AND status = 'running'

-- name: requeue_stale
UPDATE {table_name}
SET status = 'pending',
    started_at = NULL,
    heartbeat_at = NULL,
    retry_count = retry_count + 1
WHERE status = 'running'
  AND (heartbeat_at IS NULL OR heartbeat_at < :cutoff)

-- name: clear_key
UPDATE {table_name} SET task_key = NULL WHERE id = :id
