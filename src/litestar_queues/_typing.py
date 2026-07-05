# ruff: noqa: A002
# pyright: reportMissingImports=false, reportUnknownMemberType=false, reportUnknownVariableType=false
"""Private optional dependency typing shims.

Application and public helper code should import these names from
``litestar_queues.typing`` instead of this private module.
"""

from collections.abc import Mapping
from importlib.util import find_spec
from typing import Any

from typing_extensions import Self

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "Counter",
    "Gauge",
    "Histogram",
    "Meter",
    "Span",
    "SpanKind",
    "Status",
    "StatusCode",
    "Tracer",
    "metrics",
    "propagate",
    "trace",
)

OPENTELEMETRY_INSTALLED = find_spec("opentelemetry") is not None
PROMETHEUS_INSTALLED = find_spec("prometheus_client") is not None


class _FallbackSpan:
    """Fallback OpenTelemetry span shim."""

    def set_attribute(self, key: "str", value: "Any") -> "None":
        return None

    def set_attributes(self, attributes: "Mapping[str, Any]") -> "None":
        return None

    def record_exception(
        self,
        exception: "BaseException",
        attributes: "Mapping[str, Any] | None" = None,
        timestamp: "int | None" = None,
        escaped: "bool" = False,
    ) -> "None":
        return None

    def set_status(self, status: "Any", description: "str | None" = None) -> "None":
        return None

    def end(self, end_time: "int | None" = None) -> "None":
        return None

    def is_recording(self) -> "bool":
        return False

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type: "object", exc_val: "object", exc_tb: "object") -> "None":
        return None


class _FallbackTracer:
    """Fallback OpenTelemetry tracer shim."""

    def start_span(
        self,
        name: "str",
        context: "Any" = None,
        kind: "Any" = None,
        attributes: "Mapping[str, Any] | None" = None,
        links: "Any" = None,
        start_time: "Any" = None,
        record_exception: "bool" = True,
        set_status_on_exception: "bool" = True,
    ) -> "_FallbackSpan":
        return _FallbackSpan()


class _FallbackMeter:
    """Fallback OpenTelemetry meter shim."""

    def create_counter(self, *_args: "Any", **_kwargs: "Any") -> "Any":
        return _OtelMetric()

    def create_histogram(self, *_args: "Any", **_kwargs: "Any") -> "Any":
        return _OtelMetric()


class _OtelMetric:
    def add(self, *_args: "Any", **_kwargs: "Any") -> "None":
        return None

    def record(self, *_args: "Any", **_kwargs: "Any") -> "None":
        return None


class _FallbackSpanKind:
    """Fallback OpenTelemetry SpanKind shim."""

    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class _FallbackStatusCode:
    """Fallback OpenTelemetry StatusCode shim."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


class _FallbackStatus:
    """Fallback OpenTelemetry Status shim."""

    def __init__(self, status_code: "Any" = None, description: "str | None" = None) -> "None":
        self.status_code = status_code
        self.description = description


class _TraceModule:
    def get_tracer(
        self,
        instrumenting_module_name: "str",
        instrumenting_library_version: "str | None" = None,
        schema_url: "str | None" = None,
        tracer_provider: "Any" = None,
    ) -> "_FallbackTracer":
        return _FallbackTracer()


class _MetricsModule:
    def get_meter(
        self,
        name: "str",
        version: "str | None" = None,
        schema_url: "str | None" = None,
        meter_provider: "Any" = None,
    ) -> "_FallbackMeter":
        return _FallbackMeter()


class _PropagateModule:
    def inject(self, carrier: "dict[str, str]") -> "None":
        return None

    def extract(self, carrier: "Mapping[str, str]") -> "Any | None":
        return None


_otel_metrics: "Any"
_otel_propagate: "Any"
_otel_trace: "Any"
_otel_meter: "Any"
_otel_span: "Any"
_otel_span_kind: "Any"
_otel_status: "Any"
_otel_status_code: "Any"
_otel_tracer: "Any"

try:
    from opentelemetry import metrics as _otel_metrics
    from opentelemetry import propagate as _otel_propagate
    from opentelemetry import trace as _otel_trace
    from opentelemetry.metrics import Meter as _otel_meter
    from opentelemetry.trace import Span as _otel_span
    from opentelemetry.trace import SpanKind as _otel_span_kind
    from opentelemetry.trace import Status as _otel_status
    from opentelemetry.trace import StatusCode as _otel_status_code
    from opentelemetry.trace import Tracer as _otel_tracer
except ImportError:
    _otel_metrics = _MetricsModule()
    _otel_propagate = _PropagateModule()
    _otel_trace = _TraceModule()
    _otel_meter = _FallbackMeter
    _otel_span = _FallbackSpan
    _otel_span_kind = _FallbackSpanKind
    _otel_status = _FallbackStatus
    _otel_status_code = _FallbackStatusCode
    _otel_tracer = _FallbackTracer

metrics: "Any" = _otel_metrics
propagate: "Any" = _otel_propagate
trace: "Any" = _otel_trace
Meter: "Any" = _otel_meter
Span: "Any" = _otel_span
SpanKind: "Any" = _otel_span_kind
Status: "Any" = _otel_status
StatusCode: "Any" = _otel_status_code
Tracer: "Any" = _otel_tracer


class _PrometheusMetric:
    def __init__(
        self,
        name: "str",
        documentation: "str",
        labelnames: "tuple[str, ...]" = (),
        namespace: "str" = "",
        subsystem: "str" = "",
        unit: "str" = "",
        registry: "Any" = None,
        **_: "Any",
    ) -> "None":
        return None

    def labels(self, *_labelvalues: "str", **_labelkwargs: "str") -> "_PrometheusMetric":
        return self

    def inc(self, amount: "float" = 1) -> "None":
        return None

    def dec(self, amount: "float" = 1) -> "None":
        return None

    def set(self, value: "float") -> "None":
        return None

    def observe(self, amount: "float") -> "None":
        return None


class _FallbackCounter(_PrometheusMetric):
    """Fallback Prometheus counter shim."""


class _FallbackGauge(_PrometheusMetric):
    """Fallback Prometheus gauge shim."""


class _FallbackHistogram(_PrometheusMetric):
    """Fallback Prometheus histogram shim."""


_prometheus_counter: "Any"
_prometheus_gauge: "Any"
_prometheus_histogram: "Any"

try:
    from prometheus_client import Counter as _prometheus_counter
    from prometheus_client import Gauge as _prometheus_gauge
    from prometheus_client import Histogram as _prometheus_histogram
except ImportError:
    _prometheus_counter = _FallbackCounter
    _prometheus_gauge = _FallbackGauge
    _prometheus_histogram = _FallbackHistogram

Counter: "Any" = _prometheus_counter
Gauge: "Any" = _prometheus_gauge
Histogram: "Any" = _prometheus_histogram
