from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast
from weakref import WeakKeyDictionary

from litestar_queues._correlation import (
    CORRELATION_ID_METADATA_KEY,
    bind_correlation_id,
    capture_correlation_id,
    reset_correlation_id,
)
from litestar_queues.exceptions import MissingDependencyError
from litestar_queues.namespace import DEFAULT_NAMESPACE, QueueNamespace
from litestar_queues.typing import (
    OPENTELEMETRY_INSTALLED,
    PROMETHEUS_INSTALLED,
    OtelSpanKind,
    OtelStatus,
    OtelStatusCode,
    PrometheusCounter,
    PrometheusGauge,
    PrometheusHistogram,
    otel_context,
    otel_metrics,
    otel_propagate,
    otel_trace,
    prometheus_default_registry,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from litestar import Litestar

__all__ = (
    "CORRELATION_ID_METADATA_KEY",
    "DEFAULT_DURATION_BUCKETS",
    "TRACE_CONTEXT_METADATA_KEY",
    "ObservabilityConfig",
    "QueueObservabilityRuntime",
    "QueueObservabilityRuntimeProtocol",
    "bind_correlation_id",
    "capture_correlation_id",
    "create_observability_runtime",
    "reset_correlation_id",
)

TRACE_CONTEXT_METADATA_KEY = "_otel_context"

DEFAULT_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
    600.0,
    1800.0,
)
"""Buckets spanning sub-millisecond enqueues through half-hour task executions.

The ``prometheus_client`` default tops out at ten seconds, which sends every
real task duration into the ``+Inf`` bucket.
"""

_PROMETHEUS_COLLECTORS: "WeakKeyDictionary[Any, dict[str, Any]]" = WeakKeyDictionary()
"""Collectors keyed by registry, so runtimes sharing a registry share collectors.

``prometheus_client`` raises ``Duplicated timeseries in CollectorRegistry`` when
the same metric name is registered twice. Multiple queue services, workers, and
backends in one process legitimately record the same metrics.
"""


@dataclass(frozen=True, slots=True)
class _TransportMetricSpec:
    kind: str
    unit: str
    attributes: frozenset[str]


_TRANSPORT_METRIC_SPECS = {
    "litestar_queues.enqueue.batch.size": _TransportMetricSpec(
        "histogram", "records", frozenset({"queue.backend", "queue.operation"})
    ),
    "litestar_queues.wakeup.emitted": _TransportMetricSpec(
        "counter", "hints", frozenset({"queue.backend", "queue.transport"})
    ),
    "litestar_queues.wakeup.coalesced": _TransportMetricSpec(
        "counter", "hints", frozenset({"queue.backend", "queue.transport"})
    ),
    "litestar_queues.worker.poll.empty": _TransportMetricSpec("counter", "polls", frozenset({"queue.backend"})),
    "litestar_queues.worker.poll.delay": _TransportMetricSpec(
        "histogram", "s", frozenset({"queue.backend", "worker.wait.kind"})
    ),
    "litestar_queues.worker.wait.duration": _TransportMetricSpec(
        "duration", "s", frozenset({"queue.backend", "worker.wait.kind"})
    ),
    "litestar_queues.worker.wakeup_to_claim.duration": _TransportMetricSpec(
        "duration", "s", frozenset({"queue.backend", "queue.transport"})
    ),
    "litestar_queues.listener.reconnect": _TransportMetricSpec(
        "counter", "reconnects", frozenset({"queue.backend", "queue.transport"})
    ),
    "litestar_queues.listener.error": _TransportMetricSpec(
        "counter", "errors", frozenset({"queue.backend", "queue.transport", "queue.outcome"})
    ),
    "litestar_queues.claim.batch.size": _TransportMetricSpec(
        "histogram", "records", frozenset({"queue.backend", "queue.operation"})
    ),
    "litestar_queues.event.flush.size": _TransportMetricSpec(
        "histogram", "events", frozenset({"queue.transport", "queue.outcome"})
    ),
    "litestar_queues.event.flush.duration": _TransportMetricSpec(
        "duration", "s", frozenset({"queue.transport", "queue.outcome"})
    ),
    "litestar_queues.event.dropped": _TransportMetricSpec(
        "counter", "events", frozenset({"queue.transport", "queue.outcome"})
    ),
}
_VALUE_HISTOGRAM_BUCKETS = (1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0)


def _validate_transport_metric(name: str, *, kind: str, unit: str, attributes: "Mapping[str, str]") -> None:
    spec = _TRANSPORT_METRIC_SPECS.get(name)
    if spec is None:
        return
    if spec.kind != kind:
        msg = f"Transport metric {name!r} requires kind {spec.kind!r}, not {kind!r}."
        raise ValueError(msg)
    if spec.unit != unit:
        msg = f"Transport metric {name!r} requires unit {spec.unit!r}, not {unit!r}."
        raise ValueError(msg)
    actual = frozenset(attributes)
    if actual != spec.attributes:
        msg = f"Transport metric {name!r} requires attributes {sorted(spec.attributes)!r}, not {sorted(actual)!r}."
        raise ValueError(msg)


@dataclass(slots=True)
class ObservabilityConfig:
    """Configuration for optional queue-domain observability."""

    enable_otel: "bool | None" = None
    """OpenTelemetry policy; ``None`` follows the active Litestar telemetry plugin."""

    enable_prometheus: "bool | None" = None
    """Prometheus policy; ``None`` follows the app's Litestar Prometheus middleware."""

    enable_sqlcommenter: "bool | None" = None
    """SQLCommenter policy; ``None`` follows resolved queue telemetry."""

    tracer_name: "str | None" = None
    """Instrumentation name; ``None`` uses ``QueueConfig.namespace``."""

    meter_name: "str | None" = None
    """Instrumentation name; ``None`` uses ``QueueConfig.namespace``."""

    tracer_provider: "Any | None" = None
    """Explicit OpenTelemetry tracer provider; ``None`` uses the global provider."""

    meter_provider: "Any | None" = None
    """Explicit OpenTelemetry meter provider; ``None`` uses the global provider."""

    prometheus_registry: "Any | None" = None
    """Explicit Prometheus registry; ``None`` uses the client default registry."""

    metric_prefix: "str | None" = None
    """Prometheus prefix; ``None`` uses ``QueueConfig.namespace``."""

    duration_buckets: "tuple[float, ...]" = field(default=DEFAULT_DURATION_BUCKETS)
    """Prometheus histogram buckets, in seconds, for queue duration metrics."""

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

    def should_enable_prometheus(self, app: "Litestar | None" = None) -> "bool":
        """Return whether Prometheus metrics should be enabled.

        Returns:
            Whether Prometheus metrics should be enabled.
        """
        if self.enable_prometheus is True:
            if not PROMETHEUS_INSTALLED:
                package_name = "prometheus_client"
                extra = "prometheus"
                raise MissingDependencyError(package_name, extra)
            return True
        if self.enable_prometheus is False:
            return False
        if not PROMETHEUS_INSTALLED:
            return False
        return app is not None and _has_prometheus_middleware(app)

    def should_enable_sqlcommenter(self, app: "Litestar | None" = None) -> "bool":
        """Return whether SQLCommenter attribution should be enabled.

        Returns:
            Whether queue-owned SQLCommenter attribution should be enabled.
        """
        if self.enable_sqlcommenter is not None:
            return self.enable_sqlcommenter
        return self.should_enable_otel(app) or self.should_enable_prometheus(app)

    def resolve_prometheus_registry(self) -> "Any":
        """Return the registry queue collectors are registered with.

        Returns:
            The configured registry, or the ``prometheus_client`` default
            registry, which is what Litestar's ``PrometheusController`` scrapes.
        """
        if self.prometheus_registry is not None:
            return self.prometheus_registry
        return prometheus_default_registry()


class QueueObservabilityRuntimeProtocol(Protocol):
    """Protocol for queue observability runtimes used by services and workers."""

    enabled: "bool"

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "Any | None":
        """Start a queue span and make it the current span.

        Returns:
            The started span handle, or ``None`` when tracing is disabled.
        """
        ...

    def end_span(self, span: "Any | None") -> "None":
        """End a span and restore the previous current span."""
        ...

    def record_exception(self, span: "Any | None", exc: "BaseException") -> "None":
        """Record an exception on a span and mark it failed."""
        ...

    def set_status_error(self, span: "Any | None", description: "str") -> "None":
        """Mark a span as failed without an exception."""
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

    def record_histogram(self, name: "str", value: "float", *, unit: "str", attributes: "Mapping[str, str]") -> "None":
        """Record a value histogram sample."""
        ...


class _SpanHandle:
    """A started span plus the context token that made it current."""

    __slots__ = ("span", "token")

    def __init__(self, span: "Any", token: "Any") -> "None":
        self.span = span
        self.token = token


class QueueObservabilityRuntime:
    """Runtime helper for queue-domain spans and metrics."""

    __slots__ = (
        "_config",
        "_counters",
        "_durations",
        "_gauges",
        "_histograms",
        "_meter",
        "_namespace",
        "_otel_enabled",
        "_prometheus_enabled",
        "_registry",
        "_sqlcommenter_enabled",
        "_tracer",
        "enabled",
    )

    def __init__(
        self,
        config: "ObservabilityConfig | None",
        *,
        app: "Litestar | None" = None,
        namespace: "QueueNamespace | str | None" = None,
    ) -> "None":
        self._config = config
        self._namespace = (
            namespace if isinstance(namespace, QueueNamespace) else QueueNamespace(namespace or DEFAULT_NAMESPACE)
        )
        self._otel_enabled = config.should_enable_otel(app) if config is not None else False
        self._prometheus_enabled = config.should_enable_prometheus(app) if config is not None else False
        self._sqlcommenter_enabled = config.should_enable_sqlcommenter(app) if config is not None else False
        self.enabled = self._otel_enabled or self._prometheus_enabled
        self._registry = (
            config.resolve_prometheus_registry() if config is not None and self._prometheus_enabled else None
        )
        self._tracer: "Any | None" = None
        self._meter: "Any | None" = None
        self._counters: "dict[str, Any]" = {}
        self._durations: "dict[str, Any]" = {}
        self._gauges: "dict[str, Any]" = {}
        self._histograms: "dict[tuple[str, str], Any]" = {}

    @property
    def sqlcommenter_enabled(self) -> "bool":
        """Whether backends should attach SQLCommenter attribution to statements."""
        return self._sqlcommenter_enabled

    def get_tracer(self) -> "Any":
        """Return the configured tracer.

        Returns:
            The configured OpenTelemetry tracer.
        """
        if self._tracer is None:
            config = self._require_config()
            self._tracer = otel_trace.get_tracer(
                config.tracer_name or self._namespace.root, tracer_provider=config.tracer_provider
            )
        return self._tracer

    def get_meter(self) -> "Any":
        """Return the configured meter.

        Returns:
            The configured OpenTelemetry meter.
        """
        if self._meter is None:
            config = self._require_config()
            self._meter = otel_metrics.get_meter(
                config.meter_name or self._namespace.root, meter_provider=config.meter_provider
            )
        return self._meter

    def start_span(
        self, name: "str", *, kind: "str", attributes: "Mapping[str, object]", parent: "object | None" = None
    ) -> "Any | None":
        """Start a queue span and make it the current span.

        The span must be current for two reasons: ``inject_trace_context`` serialises
        the *current* context, and any instrumentation running inside the span --
        database drivers, HTTP clients, log correlation -- resolves its parent from
        the current context.

        Returns:
            The started span handle, or ``None`` when tracing is disabled.
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
        runtime_attributes = dict(attributes)
        if runtime_attributes.get("messaging.system") == DEFAULT_NAMESPACE:
            runtime_attributes["messaging.system"] = self._namespace.root
        span = self.get_tracer().start_span(
            self._runtime_name(name),
            context=cast("Any", parent),
            kind=span_kind,
            attributes=cast("Any", runtime_attributes),
        )
        token = otel_context.attach(otel_trace.set_span_in_context(span))
        return _SpanHandle(span, token)

    def end_span(self, span: "Any | None") -> "None":
        """End a span and restore the previous current span."""
        if span is None:
            return
        otel_context.detach(span.token)
        span.span.end()

    def record_exception(self, span: "Any | None", exc: "BaseException") -> "None":
        """Record an exception on a span and mark the span failed."""
        if span is None:
            return
        span.span.record_exception(exc)
        span.span.set_status(OtelStatus(OtelStatusCode.ERROR, type(exc).__name__))

    def set_status_error(self, span: "Any | None", description: "str") -> "None":
        """Mark a span as failed when no exception reached this frame."""
        if span is not None:
            span.span.set_status(OtelStatus(OtelStatusCode.ERROR, description))

    def set_attribute(self, span: "Any | None", key: "str", value: "object") -> "None":
        """Set a span attribute if one was created."""
        if span is not None:
            span.span.set_attribute(key, cast("Any", value))

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
        name = self._runtime_name(name)
        if self._otel_enabled:
            counter = self._counters.get(name)
            if counter is None:
                counter = self.get_meter().create_counter(name)
                self._counters[name] = counter
            counter.add(value, attributes=dict(attributes))
        if self._prometheus_enabled:
            collector = self._prometheus_collector(
                PrometheusCounter, name, _counter_name(name, self._metric_prefix()), attributes
            )
            collector.labels(**dict(attributes)).inc(value)

    def record_gauge_delta(self, name: "str", delta: "int" = 1, *, attributes: "Mapping[str, str]") -> "None":
        """Record a gauge delta for enabled metrics sinks."""
        name = self._runtime_name(name)
        if self._otel_enabled:
            key = f"updown:{name}"
            gauge = self._gauges.get(key)
            if gauge is None:
                gauge = self.get_meter().create_up_down_counter(name)
                self._gauges[key] = gauge
            gauge.add(delta, attributes=dict(attributes))
        if self._prometheus_enabled:
            collector = self._prometheus_collector(
                PrometheusGauge, name, _gauge_name(name, self._metric_prefix()), attributes
            )
            collector.labels(**dict(attributes)).inc(delta)

    def record_duration(self, name: "str", seconds: "float", *, attributes: "Mapping[str, str]") -> "None":
        """Record a duration for enabled metrics sinks."""
        name = self._runtime_name(name)
        if self._otel_enabled:
            histogram = self._durations.get(name)
            if histogram is None:
                histogram = self.get_meter().create_histogram(name, unit="s")
                self._durations[name] = histogram
            histogram.record(seconds, attributes=dict(attributes))
        if self._prometheus_enabled:
            collector = self._prometheus_collector(
                PrometheusHistogram,
                name,
                _duration_name(name, self._metric_prefix()),
                attributes,
                buckets=self._buckets(),
            )
            collector.labels(**dict(attributes)).observe(seconds)

    def record_histogram(self, name: "str", value: "float", *, unit: "str", attributes: "Mapping[str, str]") -> "None":
        """Record a non-duration histogram sample for enabled metric sinks."""
        if not self.enabled:
            return
        _validate_transport_metric(self._canonical_name(name), kind="histogram", unit=unit, attributes=attributes)
        name = self._runtime_name(name)
        if self._otel_enabled:
            key = (name, unit)
            histogram = self._histograms.get(key)
            if histogram is None:
                histogram = self.get_meter().create_histogram(name, unit=unit)
                self._histograms[key] = histogram
            histogram.record(value, attributes=dict(attributes))
        if self._prometheus_enabled:
            collector = self._prometheus_collector(
                PrometheusHistogram,
                name,
                _histogram_name(name, unit, self._metric_prefix()),
                attributes,
                buckets=_VALUE_HISTOGRAM_BUCKETS,
            )
            collector.labels(**dict(attributes)).observe(value)

    def _prometheus_collector(
        self,
        collector_type: "Any",
        metric_name: "str",
        collector_name: "str",
        attributes: "Mapping[str, str]",
        **kwargs: "Any",
    ) -> "Any":
        """Return a collector, reusing any already registered under this registry.

        Returns:
            The Prometheus collector for this metric name and registry.
        """
        registry_collectors = _PROMETHEUS_COLLECTORS.setdefault(self._registry, {})
        collector = registry_collectors.get(collector_name)
        if collector is None:
            collector = collector_type(
                collector_name,
                metric_name.replace(".", " "),
                labelnames=tuple(attributes),
                registry=self._registry,
                **kwargs,
            )
            registry_collectors[collector_name] = collector
        return collector

    def _buckets(self) -> "tuple[float, ...]":
        if self._config is None:
            return DEFAULT_DURATION_BUCKETS
        return self._config.duration_buckets

    def _require_config(self) -> "ObservabilityConfig":
        if self._config is None:
            msg = "Queue observability runtime is not configured."
            raise RuntimeError(msg)
        return self._config

    def _runtime_name(self, name: "str") -> "str":
        suffix = name.removeprefix(f"{DEFAULT_NAMESPACE}.")
        return self._namespace.metric(suffix) if suffix != name else name

    def _canonical_name(self, name: "str") -> "str":
        suffix = name.removeprefix(f"{self._namespace.root}.")
        return f"{DEFAULT_NAMESPACE}.{suffix}" if suffix != name else name

    def _metric_prefix(self) -> "str":
        if self._config is not None and self._config.metric_prefix is not None:
            return self._config.metric_prefix
        return self._namespace.root


def create_observability_runtime(
    config: "ObservabilityConfig | None",
    *,
    app: "Litestar | None" = None,
    namespace: "QueueNamespace | str | None" = None,
) -> "QueueObservabilityRuntime":
    """Create the queue observability runtime for a service.

    Returns:
        Queue observability runtime instance.
    """
    return QueueObservabilityRuntime(config, app=app, namespace=namespace)


def _has_otel_plugin(app: "Litestar") -> "bool":
    plugins = getattr(getattr(app, "plugins", None), "plugins", ())
    return any(plugin.__class__.__name__ == "OpenTelemetryPlugin" for plugin in plugins)


def _has_prometheus_middleware(app: "Litestar") -> "bool":
    """Detect Litestar's Prometheus wiring on the application.

    Litestar ships no Prometheus *plugin*: ``PrometheusConfig.middleware`` produces
    a ``DefineMiddleware`` and ``PrometheusController`` is a route handler, so the
    plugin registry is empty. Matching on the class name keeps
    ``litestar.plugins.prometheus`` unimported, which matters because importing it
    raises when ``prometheus_client`` is absent.

    Returns:
        Whether the app registers Litestar's Prometheus middleware.
    """
    for middleware in getattr(app, "middleware", ()):
        candidate = getattr(middleware, "middleware", middleware)
        mro = getattr(candidate, "__mro__", None) or type(candidate).__mro__
        if any(klass.__name__ == "PrometheusMiddleware" for klass in mro):
            return True
    return False


def _base_name(name: "str", prefix: "str | None") -> "tuple[str, str]":
    prefix = prefix or DEFAULT_NAMESPACE
    return prefix, name.removeprefix(f"{prefix}.").replace(".", "_")


def _counter_name(name: "str", prefix: "str | None") -> "str":
    """Build the Prometheus counter name.

    Counter instruments carry no ``.count`` suffix -- the instrument type already
    conveys it, and ``prometheus_client`` appends ``_total`` on export.

    Returns:
        The Prometheus collector name for this counter.
    """
    prefix, base = _base_name(name, prefix)
    return f"{prefix}_{base}"


def _gauge_name(name: "str", prefix: "str") -> "str":
    """Build the Prometheus gauge name.

    Returns:
        The Prometheus collector name for this gauge.
    """
    prefix, base = _base_name(name, prefix)
    return f"{prefix}_{base}"


def _duration_name(name: "str", prefix: "str") -> "str":
    """Build the Prometheus histogram name, carrying the conventional unit suffix.

    Returns:
        The Prometheus collector name for this duration histogram.
    """
    prefix, base = _base_name(name, prefix)
    if not base.endswith("_seconds"):
        base = f"{base}_seconds"
    return f"{prefix}_{base}"


def _histogram_name(name: "str", unit: "str", prefix: "str") -> "str":
    prefix, base = _base_name(name, prefix)
    return f"{prefix}_{base}_{unit}"
