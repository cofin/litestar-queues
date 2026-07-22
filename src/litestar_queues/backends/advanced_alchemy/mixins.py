"""Advanced Alchemy queue task mixins."""

from datetime import datetime  # noqa: TC003
from typing import Any, Protocol, TypeAlias, cast

from advanced_alchemy.types import JsonB
from sqlalchemy import DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, mapped_column

__all__ = ("QueueEventLogModelMixin", "QueueMaintenanceLeaseModelMixin", "QueueTaskModelMixin")

JSONValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


@declarative_mixin
class QueueTaskModelMixin:
    """Declarative mixin carrying queue task columns and indexes.

    Compose this with an application-owned Advanced Alchemy base that provides
    compatible ``id`` and ``created_at`` columns.
    """

    __abstract__ = True

    @declared_attr.directive
    def __table_args__(cls) -> "tuple[Any, ...]":
        table = str(cast("_NamedTable", cls).__tablename__)
        return (
            Index(f"ix_{table}_pending", "status", "queue", "scheduled_at", "priority", "created_at"),
            Index(f"ix_{table}_heartbeat", "status", "heartbeat_at"),
            Index(f"ix_{table}_execution", "status", "execution_ref", mysql_length={"execution_ref": 255}),
        )

    @declared_attr
    def task_name(cls) -> "Mapped[str]":
        return mapped_column(String(length=500), nullable=False)

    @declared_attr
    def args_json(cls) -> "Mapped[list[Any]]":
        return mapped_column("task_args", JsonB, default=list, nullable=False)

    @declared_attr
    def kwargs_json(cls) -> "Mapped[dict[str, Any]]":
        return mapped_column("task_kwargs", JsonB, default=dict, nullable=False)

    @declared_attr
    def queue(cls) -> "Mapped[str]":
        return mapped_column(String(length=255), default="default", nullable=False)

    @declared_attr
    def execution_backend(cls) -> "Mapped[str]":
        return mapped_column(String(length=255), default="local", nullable=False)

    @declared_attr
    def execution_profile(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_ref(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=1000), default=None)

    @declared_attr
    def status(cls) -> "Mapped[str]":
        return mapped_column(String(length=32), default="pending", nullable=False)

    @declared_attr
    def priority(cls) -> "Mapped[int]":
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def max_retries(cls) -> "Mapped[int]":
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def retry_count(cls) -> "Mapped[int]":
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def scheduled_at(cls) -> "Mapped[datetime | None]":
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def started_at(cls) -> "Mapped[datetime | None]":
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def completed_at(cls) -> "Mapped[datetime | None]":
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def heartbeat_at(cls) -> "Mapped[datetime | None]":
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def result_json(cls) -> "Mapped[JSONValue]":
        return mapped_column("result", JsonB, default=None, nullable=True)

    @declared_attr
    def error(cls) -> "Mapped[str | None]":
        return mapped_column(Text(), default=None)

    @declared_attr
    def task_key(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=500), unique=True, default=None)

    @declared_attr
    def metadata_json(cls) -> "Mapped[dict[str, Any]]":
        return mapped_column("metadata", JsonB, default=dict, nullable=False)


@declarative_mixin
class QueueEventLogModelMixin:
    """Declarative mixin carrying generic queue event-history columns and indexes."""

    __abstract__ = True

    @declared_attr.directive
    def __table_args__(cls) -> "tuple[Any, ...]":
        table = str(cast("_NamedTable", cls).__tablename__)
        return (
            Index(f"ix_{table}_task_id", "task_id", "sequence", "occurred_at"),
            Index(f"ix_{table}_task_name", "task_name", "occurred_at"),
            Index(f"ix_{table}_event_type", "event_type", "occurred_at"),
            Index(f"ix_{table}_occurred_at", "occurred_at"),
        )

    @declared_attr
    def event_id(cls) -> "Mapped[str]":
        return mapped_column(String(length=64), unique=True, nullable=False)

    @declared_attr
    def event_type(cls) -> "Mapped[str]":
        return mapped_column(String(length=255), nullable=False)

    @declared_attr
    def task_id(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=64), default=None)

    @declared_attr
    def task_name(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=500), default=None)

    @declared_attr
    def queue(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def worker_id(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_backend(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_profile(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def level(cls) -> "Mapped[str | None]":
        return mapped_column(String(length=32), default=None)

    @declared_attr
    def message(cls) -> "Mapped[str | None]":
        return mapped_column(Text(), default=None)

    @declared_attr
    def detail_json(cls) -> "Mapped[dict[str, Any]]":
        return mapped_column(JsonB, default=dict, nullable=False)

    @declared_attr
    def progress_current(cls) -> "Mapped[float | None]":
        return mapped_column(Float(), default=None)

    @declared_attr
    def progress_total(cls) -> "Mapped[float | None]":
        return mapped_column(Float(), default=None)

    @declared_attr
    def progress_percent(cls) -> "Mapped[float | None]":
        return mapped_column(Float(), default=None)

    @declared_attr
    def sequence(cls) -> "Mapped[int | None]":
        return mapped_column(Integer(), default=None)

    @declared_attr
    def occurred_at(cls) -> "Mapped[datetime]":
        return mapped_column(DateTime(timezone=True), nullable=False)


@declarative_mixin
class QueueMaintenanceLeaseModelMixin:
    """Declarative mixin carrying the distributed maintenance-lease columns.

    Compose this with an application-owned Advanced Alchemy base that provides
    a compatible ``id`` primary key. Adopter-owned model and migration setups
    must include the resulting table; the queue backend never calls
    ``metadata.create_all``.
    """

    __abstract__ = True

    @declared_attr
    def name(cls) -> "Mapped[str]":
        return mapped_column(String(length=255), unique=True, nullable=False)

    @declared_attr
    def token(cls) -> "Mapped[str]":
        return mapped_column(String(length=255), nullable=False)

    @declared_attr
    def expires_at(cls) -> "Mapped[datetime]":
        return mapped_column(DateTime(timezone=True), nullable=False)


class _NamedTable(Protocol):
    __tablename__: "str"
