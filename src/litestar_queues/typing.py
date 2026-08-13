"""Public typing helpers for optional observability support.

The supported import location for :mod:`litestar_queues._typing`. Each package
publishes its own facade over its own private module -- this one does not
re-export a nested package's types, so event backend protocols live in
:mod:`litestar_queues.events.typing` and adapter protocols beside their
adapter.
"""

from litestar_queues._correlation import SQLSPEC_INSTALLED, sqlspec_correlation_context
from litestar_queues._typing import (
    OPENTELEMETRY_INSTALLED,
    PROMETHEUS_INSTALLED,
    OtelMeter,
    OtelSpan,
    OtelSpanKind,
    OtelStatus,
    OtelStatusCode,
    OtelTracer,
    PrometheusCounter,
    PrometheusGauge,
    PrometheusHistogram,
    otel_context,
    otel_metrics,
    otel_propagate,
    otel_trace,
    prometheus_default_registry,
)
from litestar_queues.config import StaleRequeuePriority
from litestar_queues.task import TaskUniqueBy, TaskUniqueUntil

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "SQLSPEC_INSTALLED",
    "OtelMeter",
    "OtelSpan",
    "OtelSpanKind",
    "OtelStatus",
    "OtelStatusCode",
    "OtelTracer",
    "PrometheusCounter",
    "PrometheusGauge",
    "PrometheusHistogram",
    "StaleRequeuePriority",
    "TaskUniqueBy",
    "TaskUniqueUntil",
    "otel_context",
    "otel_metrics",
    "otel_propagate",
    "otel_trace",
    "prometheus_default_registry",
    "sqlspec_correlation_context",
)
