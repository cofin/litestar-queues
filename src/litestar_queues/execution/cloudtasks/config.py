import math
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlsplit

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar.types import Guard

    from litestar_queues.config import QueueConfig

__all__ = ("CLOUD_TASKS_MAX_SCHEDULE_HORIZON", "CloudTasksExecutionConfig")

CLOUD_TASKS_MAX_SCHEDULE_HORIZON = timedelta(days=30)
"""Google's fixed ceiling on how far ahead a Cloud Tasks task may be scheduled."""

_MIN_DISPATCH_DEADLINE = 15
_MAX_DISPATCH_DEADLINE = 1800


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

    route_path: "str" = "/_litestar-queues/cloud-tasks"
    """Path the consumer route is mounted on."""

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
        return config.execution_backend

    msg = (
        "Cloud Tasks execution requires QueueConfig.execution_backend with a "
        "CloudTasksExecutionConfig value; the project, queue, delivery target, and "
        "audience have no defaults."
    )
    raise QueueConfigurationError(msg)
