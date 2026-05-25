"""Advanced Alchemy queue task mixins."""

from typing import Any, Protocol, cast

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, mapped_column

__all__ = ("QueueTaskModelMixin",)


@declarative_mixin
class QueueTaskModelMixin:
    """Declarative mixin carrying queue task columns and indexes.

    Compose this with an application-owned Advanced Alchemy base that provides
    compatible ``id`` and ``created_at`` columns.
    """

    __abstract__ = True

    @declared_attr.directive
    def __table_args__(cls) -> tuple[Any, ...]:
        table = str(cast("_NamedTable", cls).__tablename__)
        return (
            Index(f"ix_{table}_pending", "status", "queue", "scheduled_at", "priority", "created_at"),
            Index(f"ix_{table}_heartbeat", "status", "heartbeat_at"),
            Index(f"ix_{table}_execution", "status", "execution_ref", mysql_length={"execution_ref": 255}),
        )

    @declared_attr
    def task_name(cls) -> Mapped[str]:
        return mapped_column(String(length=500), nullable=False)

    @declared_attr
    def args_json(cls) -> Mapped[str]:
        return mapped_column(Text(), default="[]", nullable=False)

    @declared_attr
    def kwargs_json(cls) -> Mapped[str]:
        return mapped_column(Text(), default="{}", nullable=False)

    @declared_attr
    def queue(cls) -> Mapped[str]:
        return mapped_column(String(length=255), default="default", nullable=False)

    @declared_attr
    def execution_backend(cls) -> Mapped[str]:
        return mapped_column(String(length=255), default="local", nullable=False)

    @declared_attr
    def execution_profile(cls) -> Mapped[str | None]:
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_ref(cls) -> Mapped[str | None]:
        return mapped_column(String(length=1000), default=None)

    @declared_attr
    def status(cls) -> Mapped[str]:
        return mapped_column(String(length=32), default="pending", nullable=False)

    @declared_attr
    def priority(cls) -> Mapped[int]:
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def max_retries(cls) -> Mapped[int]:
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def retry_count(cls) -> Mapped[int]:
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def scheduled_at(cls) -> Mapped[Any | None]:
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def started_at(cls) -> Mapped[Any | None]:
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def completed_at(cls) -> Mapped[Any | None]:
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def heartbeat_at(cls) -> Mapped[Any | None]:
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def result_json(cls) -> Mapped[str]:
        return mapped_column(Text(), default="null", nullable=False)

    @declared_attr
    def error(cls) -> Mapped[str | None]:
        return mapped_column(Text(), default=None)

    @declared_attr
    def task_key(cls) -> Mapped[str | None]:
        return mapped_column(String(length=500), unique=True, default=None)

    @declared_attr
    def metadata_json(cls) -> Mapped[str]:
        return mapped_column(Text(), default="{}", nullable=False)


class _NamedTable(Protocol):
    __tablename__: str
