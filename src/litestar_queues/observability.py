import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast

from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.typing import (
    OPENTELEMETRY_INSTALLED,
    PROMETHEUS_INSTALLED,
    Counter,
    Histogram,
    SpanKind,
    metrics,
    propagate,
    trace,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from litestar import Litestar

__all__ = (
    "TRACE_CONTEXT_METADATA_KEY",
    "QueueObservabilityConfig",
    "QueueObservabilityRuntime",
    "QueueObservabilityRuntimeProtocol",
    "create_observability_runtime",
)

TRACE_CONTEXT_METADATA_KEY = "_otel_context"


@dataclass(slots=True)
class QueueObservabilityConfig:
    """Configuration for optional queue-domain observability."""

    enable_otel: "bool | None" = None
    enable_prometheus: "bool" = False
    tracer_name: "str" = "litestar_queues"
    meter_name: "str" = "litestar_queues"
    service_name: "str | None" = None
    resource_attributes: "dict[str, object]" = field(default_factory=dict)
    tracer_provider: "Any | None" = None
    meter_provider: "Any | None" = None
    prometheus_registry: "Any | None" = None
    metric_prefix: "str" = "litestar_queues"
    disable_sqlspec_queue_observability: "bool" = True

    def should_enable_otel(self, app: "Litestar | None" = None) -> "bool":
        """Return whether OpenTelemetry should be enabled."""
        if self.enable_otel is True:
            if not OPENTELEMETRY_INSTALLED:
                raise MissingDependencyError("opentelemetry", "otel")
            return True
        if self.enable_otel is False:
            return False
        if not OPENTELEMETRY_INSTALLED:
            return False
        return app is not None and _has_otel_plugin(app)

    def should_enable_prometheus(self) -> "bool":
        """Return whether Prometheus metrics should be enabled."""
        if not self.enable_prometheus:
            return False
        if not PROMETHEUS_INSTALLED:
            raise MissingDependencyError("prometheus_client", "prometheus")
        return True


class QueueObservabilityRuntimeProtocol(Protocol):
    """Protocol for queue observability runtimes used by services and workers."""

    enabled: "bool"

    def start_span(
        self,
        name: "str",
        *,
        kind: "str",
        attributes: "Mapping[str, object]",
        parent: "object | None" = None,
    ) -> "Any | None":
        """Start a queue span."""
        ...

    def end_span(self, span: "Any | None") -> "None":
        """End a span."""
        ...

    def record_exception(self, span: "Any | None", exc: "BaseException") -> "None":
        """Record an exception on a span."""
        ...

    def set_attribute(self, span: "Any | None", key: "str", value: "object") -> "None":
        """Set a span attribute."""
        ...

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        """Inject trace context into queue metadata."""
        ...

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        """Extract trace context from queue metadata."""
        ...

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a counter sample."""
        ...

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        """Record a duration sample."""
        ...


class QueueObservabilityRuntime:
    """Runtime helper for queue-domain spans and metrics."""

    __slots__ = (
        "_config",
        "_counters",
        "_durations",
        "_meter",
        "_otel_enabled",
        "_prometheus_enabled",
        "_tracer",
        "enabled",
    )

    def __init__(self, config: "QueueObservabilityConfig | None", *, app: "Litestar | None" = None) -> "None":
        self._config = config
        self._otel_enabled = config.should_enable_otel(app) if config is not None else False
        self._prometheus_enabled = config.should_enable_prometheus() if config is not None else False
        self.enabled = self._otel_enabled or self._prometheus_enabled
        self._tracer: "Any | None" = None
        self._meter: "Any | None" = None
        self._counters: "dict[str, Any]" = {}
        self._durations: "dict[str, Any]" = {}

    def get_tracer(self) -> "Any":
        """Return the configured tracer."""
        if self._tracer is None:
            assert self._config is not None
            self._tracer = trace.get_tracer(self._config.tracer_name, tracer_provider=self._config.tracer_provider)
        return self._tracer

    def get_meter(self) -> "Any":
        """Return the configured meter."""
        if self._meter is None:
            assert self._config is not None
            self._meter = metrics.get_meter(self._config.meter_name, meter_provider=self._config.meter_provider)
        return self._meter

    def start_span(
        self,
        name: "str",
        *,
        kind: "str",
        attributes: "Mapping[str, object]",
        parent: "object | None" = None,
    ) -> "Any | None":
        """Start a queue span, or return ``None`` when tracing is disabled."""
        if not self._otel_enabled:
            return None
        span_kind = SpanKind.PRODUCER if kind == "producer" else SpanKind.CONSUMER if kind == "consumer" else SpanKind.INTERNAL
        return self.get_tracer().start_span(
            name,
            context=cast("Any", parent),
            kind=span_kind,
            attributes=cast("Any", dict(attributes)),
        )

    def end_span(self, span: "Any | None") -> "None":
        """End a span if one was created."""
        if span is not None:
            span.end()

    def record_exception(self, span: "Any | None", exc: "BaseException") -> "None":
        """Record an exception on a span if one was created."""
        if span is not None:
            span.record_exception(exc)

    def set_attribute(self, span: "Any | None", key: "str", value: "object") -> "None":
        """Set a span attribute if one was created."""
        if span is not None:
            span.set_attribute(key, cast("Any", value))

    def inject_trace_context(self, metadata: "dict[str, Any]") -> "None":
        """Inject current W3C trace context into queue metadata."""
        if not self._otel_enabled:
            return
        carrier: "dict[str, str]" = {}
        propagate.inject(carrier)
        if carrier:
            metadata[TRACE_CONTEXT_METADATA_KEY] = carrier

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        """Extract a parent trace context from queue metadata."""
        if not self._otel_enabled:
            return None
        carrier = metadata.get(TRACE_CONTEXT_METADATA_KEY)
        if not isinstance(carrier, dict):
            return None
        return cast("object | None", propagate.extract(carrier))

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a counter value for enabled metrics sinks."""
        if self._otel_enabled:
            counter = self._counters.get(name)
            if counter is None:
                counter = self.get_meter().create_counter(name)
                self._counters[name] = counter
            counter.add(value, attributes=dict(attributes))
        if self._prometheus_enabled:
            counter = self._counters.get(f"prometheus:{name}")
            if counter is None:
                counter = Counter(
                    _prometheus_name(name, self._config),
                    name.replace(".", " "),
                    labelnames=tuple(attributes),
                    registry=self._config.prometheus_registry if self._config is not None else None,
                )
                self._counters[f"prometheus:{name}"] = counter
            counter.labels(**dict(attributes)).inc(value)

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        """Record a duration for enabled metrics sinks."""
        if self._otel_enabled:
            histogram = self._durations.get(name)
            if histogram is None:
                histogram = self.get_meter().create_histogram(name, unit="s")
                self._durations[name] = histogram
            histogram.record(seconds, attributes=dict(attributes))
        if self._prometheus_enabled:
            histogram = self._durations.get(f"prometheus:{name}")
            if histogram is None:
                histogram = Histogram(
                    _prometheus_name(name, self._config),
                    name.replace(".", " "),
                    labelnames=tuple(attributes),
                    registry=self._config.prometheus_registry if self._config is not None else None,
                )
                self._durations[f"prometheus:{name}"] = histogram
            histogram.labels(**dict(attributes)).observe(seconds)


def create_observability_runtime(
    config: "QueueObservabilityConfig | None", *, app: "Litestar | None" = None
) -> "QueueObservabilityRuntime":
    """Create the queue observability runtime for a service."""
    return QueueObservabilityRuntime(config, app=app)


def _has_otel_plugin(app: "Litestar") -> "bool":
    plugins = getattr(getattr(app, "plugins", None), "plugins", ())
    return any(plugin.__class__.__name__ == "OpenTelemetryPlugin" for plugin in plugins)


def _prometheus_name(name: "str", config: "QueueObservabilityConfig | None") -> "str":
    prefix = config.metric_prefix if config is not None else "litestar_queues"
    base = name.removeprefix("litestar_queues.").replace(".", "_")
    return f"{prefix}_{base}"


def _monotonic() -> "float":
    return time.perf_counter()
