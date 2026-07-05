# pyright: reportMissingImports=false, reportUnknownMemberType=false, reportUnknownVariableType=false
"""Private optional dependency typing shims.

Application and public helper code should import these names from
``litestar_queues.typing`` instead of this private module.
"""

from importlib import import_module
from importlib.util import find_spec
from types import TracebackType
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "OPENTELEMETRY_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "OtelMeter",
    "OtelSpan",
    "OtelSpanKind",
    "OtelStatus",
    "OtelStatusCode",
    "OtelTracer",
    "PrometheusCounter",
    "PrometheusGauge",
    "PrometheusHistogram",
    "otel_metrics",
    "otel_propagate",
    "otel_trace",
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

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
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
        self, name: "str", version: "str | None" = None, schema_url: "str | None" = None, meter_provider: "Any" = None
    ) -> "_FallbackMeter":
        return _FallbackMeter()


class _PropagateModule:
    def inject(self, carrier: "dict[str, str]") -> "None":
        return None

    def extract(self, carrier: "Mapping[str, str]") -> "Any | None":
        return None


otel_metrics: Any
otel_propagate: Any
otel_trace: Any
OtelMeter: Any
OtelSpan: Any
OtelSpanKind: Any
OtelStatus: Any
OtelStatusCode: Any
OtelTracer: Any

try:
    _otel_metrics_module = import_module("opentelemetry.metrics")
    _otel_propagate_module = import_module("opentelemetry.propagate")
    _otel_trace_module = import_module("opentelemetry.trace")
except ImportError:
    otel_metrics = _MetricsModule()
    otel_propagate = _PropagateModule()
    otel_trace = _TraceModule()
    OtelMeter = _FallbackMeter
    OtelSpan = _FallbackSpan
    OtelSpanKind = _FallbackSpanKind
    OtelStatus = _FallbackStatus
    OtelStatusCode = _FallbackStatusCode
    OtelTracer = _FallbackTracer
else:
    otel_metrics = _otel_metrics_module
    otel_propagate = _otel_propagate_module
    otel_trace = _otel_trace_module
    OtelMeter = _otel_metrics_module.Meter
    OtelSpan = _otel_trace_module.Span
    OtelSpanKind = _otel_trace_module.SpanKind
    OtelStatus = _otel_trace_module.Status
    OtelStatusCode = _otel_trace_module.StatusCode
    OtelTracer = _otel_trace_module.Tracer


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


PrometheusCounter: Any
PrometheusGauge: Any
PrometheusHistogram: Any

try:
    _prometheus_client_module = import_module("prometheus_client")
except ImportError:
    PrometheusCounter = _FallbackCounter
    PrometheusGauge = _FallbackGauge
    PrometheusHistogram = _FallbackHistogram
else:
    PrometheusCounter = _prometheus_client_module.Counter
    PrometheusGauge = _prometheus_client_module.Gauge
    PrometheusHistogram = _prometheus_client_module.Histogram
