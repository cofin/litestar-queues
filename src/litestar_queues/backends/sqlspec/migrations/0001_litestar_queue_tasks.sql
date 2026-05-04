-- name: migrate-0001-up
CREATE TABLE IF NOT EXISTS litestar_queue_tasks (
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
CREATE INDEX IF NOT EXISTS ix_litestar_queue_tasks_pending
    ON litestar_queue_tasks (status, queue, scheduled_at, priority, created_at);
CREATE INDEX IF NOT EXISTS ix_litestar_queue_tasks_heartbeat
    ON litestar_queue_tasks (status, heartbeat_at);

-- name: migrate-0001-down
DROP INDEX IF EXISTS ix_litestar_queue_tasks_heartbeat;
DROP INDEX IF EXISTS ix_litestar_queue_tasks_pending;
DROP TABLE IF EXISTS litestar_queue_tasks;
