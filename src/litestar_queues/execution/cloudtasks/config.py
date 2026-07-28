import math
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import urlsplit

from litestar_queues.exceptions import QueueConfigurationError
from litestar_queues.namespace import QueueNamespace

if TYPE_CHECKING:
    from litestar.types import Guard

    from litestar_queues.config import QueueConfig

__all__ = (
    "CLOUD_TASKS_BACKEND_NAME",
    "CLOUD_TASKS_MAX_SCHEDULE_HORIZON",
    "CLOUD_TASKS_PROTOCOL_VERSION",
    "CloudTasksExecutionConfig",
)

CLOUD_TASKS_BACKEND_NAME = "cloudtasks"
"""Registry name of this execution backend, and its value on every metric label."""

CLOUD_TASKS_MAX_SCHEDULE_HORIZON = timedelta(days=30)
"""Google's fixed ceiling on how far ahead a Cloud Tasks task may be scheduled."""

CLOUD_TASKS_PROTOCOL_VERSION = 1
"""Version stamped on every delivery body and required by the consumer route.

Lives here rather than beside either side of the wire so the producer and the
route cannot drift apart while both still look correct on their own.
"""

_MIN_DISPATCH_DEADLINE = 15
_MAX_DISPATCH_DEADLINE = 1800
_DEFAULT_ROUTE_PATH = cast("str", object())


def _require_finite_positive(name: "str", value: "Any") -> "None":
    """Reject a duration that is not a finite positive number.

    Raises:
        QueueConfigurationError: If the value is not a finite positive number.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        msg = f"{name} must be a finite positive number of seconds."
        raise QueueConfigurationError(msg)


@dataclass(slots=True)
class CloudTasksExecutionConfig:
    """Configuration for Google Cloud Tasks managed dispatch.

    Cloud Tasks delivers each persisted record over HTTP to a private consumer
    service, so this carries both the queue coordinates and the delivery target.
    """

    backend_name: "ClassVar[str]" = "cloudtasks"

    project_id: "str"
    """Google Cloud project owning the Cloud Tasks queue."""

    location: "str"
    """Cloud Tasks queue location, such as ``us-central1``."""

    queue_id: "str"
    """Cloud Tasks queue that receives every dispatched record."""

    service_url: "str"
    """HTTPS origin of the private consumer service."""

    service_account_email: "str"
    """Service account Cloud Tasks mints the OIDC delivery token for."""

    audience: "str | None" = None
    """OIDC audience; ``None`` resolves to the :attr:`service_url` origin."""

    route_path: "str" = _DEFAULT_ROUTE_PATH
    """Path the consumer route is mounted on."""

    _route_path_derived: "bool" = field(init=False, repr=False)

    delivery_name_prefix: "str | None" = None
    """Cloud Tasks task-name prefix; ``None`` derives it from ``QueueConfig.namespace``."""

    dispatch_deadline: "int" = 1800
    """Seconds Cloud Tasks waits for a delivery response before abandoning it."""

    response_margin: "float" = 30.0
    """Seconds reserved for the consumer to answer inside the dispatch deadline."""

    default_task_timeout: "float" = 1740.0
    """Timeout applied to records that declare none of their own."""

    api_timeout: "float" = 10.0
    """Timeout for a single Cloud Tasks API call."""

    trust_platform_auth: "bool" = False
    """Whether Cloud Run IAM alone is accepted as protection for the route."""

    guards: "tuple[Guard, ...]" = ()
    """Application guards applied to the consumer route."""

    def __post_init__(self) -> "None":
        """Validate the delivery target, budgets, and route protection.

        Nothing here is recoverable once a task exists: Cloud Tasks has already
        accepted it and will keep retrying a request that can never succeed. So
        every rule runs before the first record is persisted.
        """
        self._route_path_derived = self.route_path is _DEFAULT_ROUTE_PATH
        if self._route_path_derived:
            self.route_path = "/_litestar-queues/cloud-tasks"
        self._validate_identifiers()
        self._validate_delivery_target()
        self._validate_budgets()
        self._validate_route_protection()
        if self.audience is None:
            # Cloud Run validates the token against the service origin, not the
            # delivery path, so defaulting to target_url would mint a token
            # rejected on every delivery.
            self.audience = self.service_url.rstrip("/")

    def _validate_identifiers(self) -> "None":
        """Reject blank Cloud Tasks coordinates.

        Raises:
            QueueConfigurationError: If an identifier is empty after stripping.
        """
        fields = {
            "project_id": self.project_id,
            "location": self.location,
            "queue_id": self.queue_id,
            "service_account_email": self.service_account_email,
        }
        for name, value in fields.items():
            if not isinstance(value, str) or not value.strip():
                msg = f"CloudTasksExecutionConfig.{name} must be a non-empty value."
                raise QueueConfigurationError(msg)
        if self.delivery_name_prefix is not None and (
            not self.delivery_name_prefix
            or not all(character.isalnum() or character in {"-", "_"} for character in self.delivery_name_prefix)
        ):
            msg = "CloudTasksExecutionConfig.delivery_name_prefix must contain only letters, digits, '-' and '_'."
            raise QueueConfigurationError(msg)

    def _validate_delivery_target(self) -> "None":
        """Reject a service origin or route path Cloud Tasks cannot deliver to.

        Raises:
            QueueConfigurationError: If the origin is not a bare HTTPS origin or
                the route path is not a non-root absolute path.
        """
        origin = urlsplit(self.service_url if isinstance(self.service_url, str) else "")
        if (
            origin.scheme != "https"
            or not origin.hostname
            or origin.username is not None
            or origin.password is not None
            or origin.query
            or origin.fragment
            or origin.path not in {"", "/"}
        ):
            msg = (
                "CloudTasksExecutionConfig.service_url must be a bare HTTPS origin such as "
                "'https://consumer.example.run.app'; a base path would silently prefix every "
                "delivery target."
            )
            raise QueueConfigurationError(msg)

        route = urlsplit(self.route_path if isinstance(self.route_path, str) else "")
        if not route.path.startswith("/") or route.path == "/" or route.query or route.fragment:
            msg = (
                "CloudTasksExecutionConfig.route_path must be an absolute path below the root, "
                "such as '/_litestar-queues/cloud-tasks'."
            )
            raise QueueConfigurationError(msg)

    def _validate_budgets(self) -> "None":
        """Reject deadlines and timeouts Cloud Tasks would not honour.

        Raises:
            QueueConfigurationError: If the deadline is outside Google's range or
                the task budget does not fit inside it.
        """
        deadline = self.dispatch_deadline
        if isinstance(deadline, bool) or not isinstance(deadline, int):
            msg = "CloudTasksExecutionConfig.dispatch_deadline must be an integer number of seconds."
            raise QueueConfigurationError(msg)
        if not _MIN_DISPATCH_DEADLINE <= deadline <= _MAX_DISPATCH_DEADLINE:
            msg = (
                f"CloudTasksExecutionConfig.dispatch_deadline must be between "
                f"{_MIN_DISPATCH_DEADLINE} and {_MAX_DISPATCH_DEADLINE} seconds, which is the "
                f"range Cloud Tasks accepts for an HTTP target."
            )
            raise QueueConfigurationError(msg)

        for name, value in (
            ("response_margin", self.response_margin),
            ("default_task_timeout", self.default_task_timeout),
            ("api_timeout", self.api_timeout),
        ):
            _require_finite_positive(f"CloudTasksExecutionConfig.{name}", value)

        if self.default_task_timeout + self.response_margin > deadline:
            msg = (
                "CloudTasksExecutionConfig.default_task_timeout plus response_margin must fit "
                "inside dispatch_deadline; the response has to leave the consumer before Cloud "
                "Tasks abandons the request."
            )
            raise QueueConfigurationError(msg)

    def _validate_route_protection(self) -> "None":
        """Reject a delivery route with no protection at all.

        Raises:
            QueueConfigurationError: If neither platform auth nor guards apply.
        """
        if not self.trust_platform_auth and not self.guards:
            msg = (
                "Cloud Tasks delivers over public HTTP, so the consumer route needs protection. "
                "Set trust_platform_auth=True when Cloud Run IAM already requires an "
                "authenticated caller, or pass guards."
            )
            raise QueueConfigurationError(msg)

    @property
    def target_url(self) -> "str":
        """Absolute URL Cloud Tasks posts each record to."""
        return f"{self.service_url.rstrip('/')}{self.route_path}"

    def resolve(self, namespace: "QueueNamespace | None" = None) -> "CloudTasksExecutionConfig":
        """Resolve namespace-owned defaults without mutating this reusable config."""
        if not self._route_path_derived and self.delivery_name_prefix is not None:
            return self
        names = namespace or QueueNamespace()
        route_path = self.route_path if not self._route_path_derived else f"/_{names.resource()}/cloud-tasks"
        delivery_name_prefix = self.delivery_name_prefix
        if delivery_name_prefix is None:
            delivery_name_prefix = "lq-" if names.is_default else f"{names.resource()}-"
        return replace(self, route_path=route_path, delivery_name_prefix=delivery_name_prefix)

    @property
    def queue_path(self) -> "str":
        """Fully qualified Cloud Tasks queue resource name."""
        return f"projects/{self.project_id}/locations/{self.location}/queues/{self.queue_id}"


def _execution_config_from_queue_config(config: "QueueConfig | None") -> "CloudTasksExecutionConfig":
    """Resolve the typed Cloud Tasks config from a QueueConfig.

    Returns:
        The resolved Cloud Tasks execution config.

    Raises:
        QueueConfigurationError: If no typed Cloud Tasks config is available.
    """
    if config is not None and isinstance(config.execution_backend, CloudTasksExecutionConfig):
        return config.execution_backend.resolve(config.names)

    msg = (
        "Cloud Tasks execution requires QueueConfig.execution_backend with a "
        "CloudTasksExecutionConfig value; the project, queue, delivery target, and "
        "audience have no defaults."
    )
    raise QueueConfigurationError(msg)
