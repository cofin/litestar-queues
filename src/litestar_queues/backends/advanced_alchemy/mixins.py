"""Advanced Alchemy queue task mixins."""

from datetime import datetime  # noqa: TC003
from typing import Any, Protocol, TypeAlias, cast

from advanced_alchemy.types import JsonB
from sqlalchemy import DateTime, Float, Index, Integer, String, Text
from sqlalchemy.dialects import mysql, oracle
from sqlalchemy.orm import Mapped, declared_attr, mapped_column
from sqlalchemy.schema import conv

__all__ = (
    "QueueEventHistoryModelMixin",
    "QueueMaintenanceModelMixin",
    "QueueTaskModelMixin",
    "QueueTaskReservationModelMixin",
)

JSONValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


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
            Index(
                f"ix_{table}_pending",
                "status",
                "queue",
                "scheduled_at",
                "expires_at",
                "priority",
                "queued_at",
                "created_at",
            ),
            Index(f"ix_{table}_heartbeat", "status", "heartbeat_at"),
            Index(f"ix_{table}_execution", "status", "execution_ref", mysql_length={"execution_ref": 255}),
            Index(
                conv(f"ix_{table}_dispatch_repair"),
                "execution_backend",
                "status",
                "dispatch_checked_at",
                "created_at",
                "id",
            ),
        )

    @declared_attr
    def task_name(cls) -> "Mapped[str]":
        """Registered task callable identifier."""
        return mapped_column(String(length=500), nullable=False)

    @declared_attr
    def args_json(cls) -> "Mapped[list[Any]]":
        """Serialized positional task arguments column (``task_args``)."""
        return mapped_column("task_args", JsonB, default=list, nullable=False)

    @declared_attr
    def kwargs_json(cls) -> "Mapped[dict[str, Any]]":
        """Serialized keyword task arguments column (``task_kwargs``)."""
        return mapped_column("task_kwargs", JsonB, default=dict, nullable=False)

    @declared_attr
    def queue(cls) -> "Mapped[str]":
        """Logical queue name."""
        return mapped_column(String(length=255), default="default", nullable=False)

    @declared_attr
    def execution_backend(cls) -> "Mapped[str]":
        """Execution backend identifier."""
        return mapped_column(String(length=255), default="local", nullable=False)

    @declared_attr
    def execution_profile(cls) -> "Mapped[str | None]":
        """Optional execution profile identifier."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_ref(cls) -> "Mapped[str | None]":
        """External execution reference or provisional reservation token."""
        return mapped_column(String(length=1000), default=None)

    @declared_attr
    def worker_id(cls) -> "Mapped[str | None]":
        """Identifier of the worker owning the active attempt."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def status(cls) -> "Mapped[str]":
        """Current task lifecycle status."""
        return mapped_column(String(length=32), default="pending", nullable=False)

    @declared_attr
    def priority(cls) -> "Mapped[int]":
        """Execution priority where larger numbers run first."""
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def max_retries(cls) -> "Mapped[int]":
        """Maximum retry attempts allowed after the initial attempt."""
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def retry_count(cls) -> "Mapped[int]":
        """Number of retries consumed or attempt fencing counter."""
        return mapped_column(Integer(), default=0, nullable=False)

    @declared_attr
    def scheduled_at(cls) -> "Mapped[datetime | None]":
        """Earliest UTC timestamp at which the task is eligible to run."""
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def expires_at(cls) -> "Mapped[datetime | None]":
        """UTC deadline after which an unclaimed task transitions to ``expired``."""
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def queued_at(cls) -> "Mapped[datetime]":
        """UTC timestamp when the current attempt entered the queue."""
        return mapped_column(DateTime(timezone=True), nullable=False)

    @declared_attr
    def started_at(cls) -> "Mapped[datetime | None]":
        """UTC timestamp when the active attempt was claimed."""
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def completed_at(cls) -> "Mapped[datetime | None]":
        """UTC timestamp when the task reached a terminal status."""
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def heartbeat_at(cls) -> "Mapped[datetime | None]":
        """UTC timestamp of the latest worker heartbeat on a running attempt."""
        return mapped_column(DateTime(timezone=True), default=None)

    @declared_attr
    def dispatch_checked_at(cls) -> "Mapped[datetime | None]":
        """UTC timestamp when external dispatch repair last inspected the task."""
        return mapped_column(
            DateTime(timezone=True)
            .with_variant(mysql.DATETIME(fsp=6), "mysql", "mariadb")
            .with_variant(oracle.TIMESTAMP(timezone=True), "oracle"),
            default=None,
            nullable=True,
        )

    @declared_attr
    def result_json(cls) -> "Mapped[JSONValue]":
        """Serialized task return value column (``result``)."""
        return mapped_column("result", JsonB, default=None, nullable=True)

    @declared_attr
    def error(cls) -> "Mapped[str | None]":
        """Last failure message or stale-heartbeat error description."""
        return mapped_column(Text(), default=None)

    @declared_attr
    def task_key(cls) -> "Mapped[str | None]":
        """Active uniqueness deduplication key."""
        return mapped_column(String(length=500), unique=True, default=None)

    @declared_attr
    def metadata_json(cls) -> "Mapped[dict[str, Any]]":
        """Task metadata JSON dictionary column (``metadata``)."""
        return mapped_column("metadata", JsonB, default=dict, nullable=False)


class QueueEventHistoryModelMixin:
    """Declarative mixin carrying generic queue event-history columns and indexes."""

    __abstract__ = True

    @declared_attr.directive
    def __table_args__(cls) -> "tuple[Any, ...]":
        table = str(cast("_NamedTable", cls).__tablename__)
        return (
            Index(f"ix_{table}_task_id", "task_id", "sequence", "occurred_at"),
            Index(f"ix_{table}_task_name", "task_name", "occurred_at"),
            Index(f"ix_{table}_event_type", "event_type", "occurred_at"),
            Index(f"ix_{table}_actor_id", "actor_id", "occurred_at"),
            Index(f"ix_{table}_occurred_at", "occurred_at"),
            Index(f"ix_{table}_scope_key", "scope_key", "occurred_at"),
            Index(f"ix_{table}_entity", "entity", "occurred_at"),
        )

    @declared_attr
    def event_id(cls) -> "Mapped[str]":
        """Unique queue event identifier."""
        return mapped_column(String(length=64), unique=True, nullable=False)

    @declared_attr
    def event_type(cls) -> "Mapped[str]":
        """Event type identifier such as ``task.completed``."""
        return mapped_column(String(length=255), nullable=False)

    @declared_attr
    def task_id(cls) -> "Mapped[str | None]":
        """Associated task UUID string when the event is task-scoped."""
        return mapped_column(String(length=64), default=None)

    @declared_attr
    def task_name(cls) -> "Mapped[str | None]":
        """Registered task callable identifier."""
        return mapped_column(String(length=500), default=None)

    @declared_attr
    def queue(cls) -> "Mapped[str | None]":
        """Logical queue name."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def worker_id(cls) -> "Mapped[str | None]":
        """Worker identifier that emitted the event."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_backend(cls) -> "Mapped[str | None]":
        """Execution backend identifier associated with the event."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def execution_profile(cls) -> "Mapped[str | None]":
        """Optional execution profile identifier associated with the event."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def actor_type(cls) -> "Mapped[str | None]":
        """Category of actor that emitted the event."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def actor_id(cls) -> "Mapped[str | None]":
        """Identifier of the actor that emitted the event."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def level(cls) -> "Mapped[str | None]":
        """Severity level such as ``info``, ``warning``, or ``error``."""
        return mapped_column(String(length=32), default=None)

    @declared_attr
    def message(cls) -> "Mapped[str | None]":
        """Human-readable event message."""
        return mapped_column(Text(), default=None)

    @declared_attr
    def stage(cls) -> "Mapped[str | None]":
        """Logical execution stage label."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def scope(cls) -> "Mapped[str | None]":
        """Application-defined logical event scope."""
        return mapped_column(String(length=255), default=None)

    @declared_attr
    def scope_key(cls) -> "Mapped[str | None]":
        """Application-defined scoping key for filtered queries."""
        return mapped_column(String(length=500), default=None)

    @declared_attr
    def actor(cls) -> "Mapped[str | None]":
        """Application-defined actor label."""
        return mapped_column(String(length=500), default=None)

    @declared_attr
    def entity(cls) -> "Mapped[str | None]":
        """Application-defined entity identifier."""
        return mapped_column(String(length=500), default=None)

    @declared_attr
    def detail_json(cls) -> "Mapped[dict[str, Any]]":
        """Structured event payload dictionary."""
        return mapped_column(JsonB, default=dict, nullable=False)

    @declared_attr
    def progress_current(cls) -> "Mapped[float | None]":
        """Current progress value reported by the task."""
        return mapped_column(Float(), default=None)

    @declared_attr
    def progress_total(cls) -> "Mapped[float | None]":
        """Total progress target reported by the task."""
        return mapped_column(Float(), default=None)

    @declared_attr
    def progress_percent(cls) -> "Mapped[float | None]":
        """Computed or explicit completion percentage."""
        return mapped_column(Float(), default=None)

    @declared_attr
    def duration_ms(cls) -> "Mapped[float | None]":
        """Elapsed duration in milliseconds for terminal or stage events."""
        return mapped_column(Float(), default=None)

    @declared_attr
    def sequence(cls) -> "Mapped[int | None]":
        """Monotonic per-task event sequence number."""
        return mapped_column(Integer(), default=None)

    @declared_attr
    def occurred_at(cls) -> "Mapped[datetime]":
        """UTC timestamp when the event occurred."""
        return mapped_column(DateTime(timezone=True), nullable=False)


class QueueMaintenanceModelMixin:
    """Declarative mixin carrying distributed maintenance coordination columns.

    Compose this with an application-owned Advanced Alchemy base that provides
    a compatible ``id`` primary key. Adopter-owned model and migration setups
    must include the resulting table; the queue backend never calls
    ``metadata.create_all``.
    """

    __abstract__ = True

    @declared_attr
    def name(cls) -> "Mapped[str]":
        """Unique maintenance lock identifier."""
        return mapped_column(String(length=255), unique=True, nullable=False)

    @declared_attr
    def token(cls) -> "Mapped[str]":
        """Fencing token identifying the current maintenance lease holder."""
        return mapped_column(String(length=255), nullable=False)

    @declared_attr
    def expires_at(cls) -> "Mapped[datetime]":
        """UTC timestamp when the maintenance lease expires."""
        return mapped_column(DateTime(timezone=True), nullable=False)


class QueueTaskReservationModelMixin:
    """Declarative mixin carrying forever-uniqueness reservation columns.

    Compose this with an application-owned Advanced Alchemy base that provides
    compatible ``id`` and ``created_at`` columns. The unique ``identity_key``
    column is the atomicity arbiter for forever reservations; the table is
    separate from the queue task table so routine terminal cleanup never touches
    it. It stores only the identity key and the originating task id/name.
    """

    __abstract__ = True

    @declared_attr
    def identity_key(cls) -> "Mapped[str]":
        """Unique forever-identity reservation key."""
        return mapped_column(String(length=500), unique=True, nullable=False)

    @declared_attr
    def task_id(cls) -> "Mapped[str]":
        """Originating task UUID string that claimed the reservation."""
        return mapped_column(String(length=64), nullable=False)

    @declared_attr
    def task_name(cls) -> "Mapped[str]":
        """Registered task callable identifier that claimed the reservation."""
        return mapped_column(String(length=500), nullable=False)


class _NamedTable(Protocol):
    __tablename__: "str"
