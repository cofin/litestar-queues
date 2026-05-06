"""Create Litestar queue task table."""

from typing import Sequence

from alembic import op
import sqlalchemy as sa

revision = "0001_litestar_queue_tasks"
down_revision = None
branch_labels: Sequence[str] | None = ("litestar_queues",)
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    """Create queue task table and indexes."""
    op.create_table(
        "litestar_queue_tasks",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("task_name", sa.String(length=500), nullable=False),
        sa.Column("args_json", sa.Text(), nullable=False),
        sa.Column("kwargs_json", sa.Text(), nullable=False),
        sa.Column("queue", sa.String(length=255), nullable=False),
        sa.Column("execution_backend", sa.String(length=255), nullable=False),
        sa.Column("execution_profile", sa.String(length=255), nullable=True),
        sa.Column("execution_ref", sa.String(length=1000), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("max_retries", sa.Integer(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("task_key", sa.String(length=500), nullable=True, unique=True),
        sa.Column("metadata_json", sa.Text(), nullable=False),
    )
    op.create_index(
        "ix_litestar_queue_tasks_pending",
        "litestar_queue_tasks",
        ["status", "queue", "scheduled_at", "priority", "created_at"],
    )
    op.create_index("ix_litestar_queue_tasks_heartbeat", "litestar_queue_tasks", ["status", "heartbeat_at"])
    op.create_index("ix_litestar_queue_tasks_execution", "litestar_queue_tasks", ["status", "execution_ref"])


def downgrade() -> None:
    """Drop queue task table and indexes."""
    op.drop_index("ix_litestar_queue_tasks_execution", table_name="litestar_queue_tasks")
    op.drop_index("ix_litestar_queue_tasks_heartbeat", table_name="litestar_queue_tasks")
    op.drop_index("ix_litestar_queue_tasks_pending", table_name="litestar_queue_tasks")
    op.drop_table("litestar_queue_tasks")
