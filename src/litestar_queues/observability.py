from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast

from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.typing import (
    OPENTELEMETRY_INSTALLED,
    PROMETHEUS_INSTALLED,
    OtelSpanKind,
    PrometheusCounter,
    PrometheusGauge,
    PrometheusHistogram,
    otel_metrics,
    otel_propagate,
    otel_trace,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from litestar import Litestar

__all__ = (
    "TRACE_CONTEXT_METADATA_KEY",
    "ObservabilityConfig",
    "QueueObservabilityRuntime",
    "QueueObservabilityRuntimeProtocol",
    "create_observability_runtime",
)

TRACE_CONTEXT_METADATA_KEY = "_otel_context"


@dataclass(slots=True)
class ObservabilityConfig:
    """Configuration for optional queue-domain observability."""

    enable_otel: "bool | None" = None
    """OpenTelemetry policy; ``None`` follows the active Litestar telemetry plugin."""

    enable_prometheus: "bool" = False
    """Whether queue metrics are registered with Prometheus."""

    tracer_name: "str" = "litestar_queues"
    """Instrumentation name used to obtain the OpenTelemetry tracer."""

    meter_name: "str" = "litestar_queues"
    """Instrumentation name used to obtain the OpenTelemetry meter."""

    tracer_provider: "Any | None" = None
    """Explicit OpenTelemetry tracer provider; ``None`` uses the global provider."""

    meter_provider: "Any | None" = None
    """Explicit OpenTelemetry meter provider; ``None`` uses the global provider."""

    prometheus_registry: "Any | None" = None
    """Explicit Prometheus registry; ``None`` uses the client default registry."""

    metric_prefix: "str" = "litestar_queues"
    """Prefix applied to package queue metric names."""

    def should_enable_otel(self, app: "Litestar | None" = None) -> "bool":
        """Return whether OpenTelemetry should be enabled.

        Returns:
            Whether OpenTelemetry tracing and metrics should be enabled.
        """
        if self.enable_otel is True:
            if not OPENTELEMETRY_INSTALLED:
                package_name = "opentelemetry"
                extra = "otel"
                raise MissingDependencyError(package_name, extra)
            return True
        if self.enable_otel is False:
            return False
        if not OPENTELEMETRY_INSTALLED:
            return False
        return app is not None and _has_otel_plugin(app)

    def should_enable_prometheus(self) -> "bool":
        """Return whether Prometheus metrics should be enabled.

        Returns:
            Whether Prometheus metrics should be enabled.
        """
        if not self.enable_prometheus:
            return False
        if not PROMETHEUS_INSTALLED:
            package_name = "prometheus_client"
            extra = "prometheus"
            raise MissingDependencyError(package_name, extra)
        return True


class QueueObservabilityRuntimeProtocol(Protocol):
    """Protocol for queue observability runtimes used by services and workers."""

    enabled: "bool"

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "Any | None":
        """Start a queue span.

        Returns:
            The started span, or ``None`` when tracing is disabled.
        """
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
        """Extract trace context from queue metadata.

        Returns:
            Extracted trace context, or ``None`` when unavailable.
        """
        ...

    def record_counter(self, name: "str", value: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a counter sample."""
        ...

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a gauge delta sample."""
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
        "_gauges",
        "_meter",
        "_otel_enabled",
        "_prometheus_enabled",
        "_tracer",
        "enabled",
    )

    def __init__(self, config: "ObservabilityConfig | None", *, app: "Litestar | None" = None) -> "None":
        self._config = config
        self._otel_enabled = config.should_enable_otel(app) if config is not None else False
        self._prometheus_enabled = config.should_enable_prometheus() if config is not None else False
        self.enabled = self._otel_enabled or self._prometheus_enabled
        self._tracer: "Any | None" = None
        self._meter: "Any | None" = None
        self._counters: "dict[str, Any]" = {}
        self._durations: "dict[str, Any]" = {}
        self._gauges: "dict[str, Any]" = {}

    def get_tracer(self) -> "Any":
        """Return the configured tracer.

        Returns:
            The configured OpenTelemetry tracer.
        """
        if self._tracer is None:
            config = self._require_config()
            self._tracer = otel_trace.get_tracer(config.tracer_name, tracer_provider=config.tracer_provider)
        return self._tracer

    def get_meter(self) -> "Any":
        """Return the configured meter.

        Returns:
            The configured OpenTelemetry meter.
        """
        if self._meter is None:
            config = self._require_config()
            self._meter = otel_metrics.get_meter(config.meter_name, meter_provider=config.meter_provider)
        return self._meter

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "Any | None":
        """Start a queue span.

        Returns:
            The started span, or ``None`` when tracing is disabled.
        """
        if not self._otel_enabled:
            return None
        span_kind = (
            OtelSpanKind.PRODUCER
            if kind == "producer"
            else OtelSpanKind.CONSUMER
            if kind == "consumer"
            else OtelSpanKind.INTERNAL
        )
        return self.get_tracer().start_span(
            name, context=cast("Any", parent), kind=span_kind, attributes=cast("Any", dict(attributes))
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
        if self._otel_enabled:
            carrier: "dict[str, str]" = {}
            otel_propagate.inject(carrier)
            if carrier:
                metadata[TRACE_CONTEXT_METADATA_KEY] = carrier

    def extract_trace_context(self, metadata: "Mapping[str, Any]") -> "object | None":
        """Extract a parent trace context from queue metadata.

        Returns:
            Extracted trace context, or ``None`` when unavailable.
        """
        if not self._otel_enabled:
            return None
        carrier = metadata.get(TRACE_CONTEXT_METADATA_KEY)
        if not isinstance(carrier, dict):
            return None
        return cast("object | None", otel_propagate.extract(carrier))

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
                counter = PrometheusCounter(
                    _prometheus_name(name, self._config),
                    name.replace(".", " "),
                    labelnames=tuple(attributes),
                    registry=self._config.prometheus_registry if self._config is not None else None,
                )
                self._counters[f"prometheus:{name}"] = counter
            counter.labels(**dict(attributes)).inc(value)

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a gauge delta for enabled metrics sinks."""
        if self._otel_enabled:
            key = f"updown:{name}"
            gauge = self._gauges.get(key)
            if gauge is None:
                gauge = self.get_meter().create_up_down_counter(name)
                self._gauges[key] = gauge
            gauge.add(delta, attributes=dict(attributes))
        if self._prometheus_enabled:
            key = f"prometheus_gauge:{name}"
            gauge = self._gauges.get(key)
            if gauge is None:
                gauge = PrometheusGauge(
                    _prometheus_name(name, self._config),
                    name.replace(".", " "),
                    labelnames=tuple(attributes),
                    registry=self._config.prometheus_registry if self._config is not None else None,
                )
                self._gauges[key] = gauge
            gauge.labels(**dict(attributes)).inc(delta)

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
                histogram = PrometheusHistogram(
                    _prometheus_name(name, self._config),
                    name.replace(".", " "),
                    labelnames=tuple(attributes),
                    registry=self._config.prometheus_registry if self._config is not None else None,
                )
                self._durations[f"prometheus:{name}"] = histogram
            histogram.labels(**dict(attributes)).observe(seconds)

    def _require_config(self) -> "ObservabilityConfig":
        if self._config is None:
            msg = "Queue observability runtime is not configured."
            raise RuntimeError(msg)
        return self._config


def create_observability_runtime(
    config: "ObservabilityConfig | None", *, app: "Litestar | None" = None
) -> "QueueObservabilityRuntime":
    """Create the queue observability runtime for a service.

    Returns:
        Queue observability runtime instance.
    """
    return QueueObservabilityRuntime(config, app=app)


def _has_otel_plugin(app: "Litestar") -> "bool":
    plugins = getattr(getattr(app, "plugins", None), "plugins", ())
    return any(plugin.__class__.__name__ == "OpenTelemetryPlugin" for plugin in plugins)


def _prometheus_name(name: "str", config: "ObservabilityConfig | None") -> "str":
    prefix = config.metric_prefix if config is not None else "litestar_queues"
    base = name.removeprefix("litestar_queues.").replace(".", "_")
    return f"{prefix}_{base}"
