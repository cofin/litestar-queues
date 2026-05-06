"""Advanced Alchemy queue task model."""

from typing import Any

from litestar_queues.backends.advanced_alchemy._typing import missing_advanced_alchemy_error
from litestar_queues.backends.advanced_alchemy.config import DEFAULT_TABLE_NAME

try:
    from advanced_alchemy.base import UUIDAuditBase
    from sqlalchemy import DateTime, Index, Integer, String, Text
    from sqlalchemy.orm import Mapped, mapped_column
except ModuleNotFoundError as exc:
    raise missing_advanced_alchemy_error(exc) from exc

__all__ = ("QueueTaskModel",)


class QueueTaskModel(UUIDAuditBase):
    """Advanced Alchemy model storing queue task records."""

    __tablename__ = DEFAULT_TABLE_NAME
    __table_args__ = (
        Index(
            "ix_litestar_queue_tasks_pending",
            "status",
            "queue",
            "scheduled_at",
            "priority",
            "created_at",
        ),
        Index("ix_litestar_queue_tasks_heartbeat", "status", "heartbeat_at"),
        Index("ix_litestar_queue_tasks_execution", "status", "execution_ref"),
    )

    task_name: Mapped[str] = mapped_column(String(length=500), nullable=False)
    args_json: Mapped[str] = mapped_column(Text(), default="[]", nullable=False)
    kwargs_json: Mapped[str] = mapped_column(Text(), default="{}", nullable=False)
    queue: Mapped[str] = mapped_column(String(length=255), default="default", nullable=False)
    execution_backend: Mapped[str] = mapped_column(String(length=255), default="local", nullable=False)
    execution_profile: Mapped[str | None] = mapped_column(String(length=255), default=None)
    execution_ref: Mapped[str | None] = mapped_column(String(length=1000), default=None)
    status: Mapped[str] = mapped_column(String(length=32), default="pending", nullable=False)
    priority: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    scheduled_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True), default=None)
    started_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True), default=None)
    completed_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True), default=None)
    heartbeat_at: Mapped[Any | None] = mapped_column(DateTime(timezone=True), default=None)
    result_json: Mapped[str] = mapped_column(Text(), default="null", nullable=False)
    error: Mapped[str | None] = mapped_column(Text(), default=None)
    task_key: Mapped[str | None] = mapped_column(String(length=500), unique=True, default=None)
    metadata_json: Mapped[str] = mapped_column(Text(), default="{}", nullable=False)
