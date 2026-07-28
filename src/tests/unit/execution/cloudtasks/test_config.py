"""Typed Cloud Tasks configuration contract.

Cloud Tasks delivers over HTTP to a private service, so a misconfigured target,
deadline, or authentication policy is not recoverable at delivery time: Google
has already accepted the task and will retry a request that can never succeed.
Every rule below therefore runs at construction time, before any record is
persisted and before a Google client is built.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from litestar_queues.exceptions import QueueConfigurationError

if TYPE_CHECKING:
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

_VALID: "dict[str, Any]" = {
    "project_id": "example-project",
    "location": "us-central1",
    "queue_id": "queue-consumer",
    "service_url": "https://queue-consumer-abcdef-uc.a.run.app",
    "service_account_email": "queues@example-project.iam.gserviceaccount.com",
    "trust_platform_auth": True,
}


def _guard(connection: "Any", handler: "Any") -> "None":
    """Stand in for an application guard on the consumer route."""
    del connection, handler


def _config(**overrides: "Any") -> "CloudTasksExecutionConfig":
    """Build a Cloud Tasks config from the valid baseline.

    Returns:
        The constructed typed configuration.
    """
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

    return CloudTasksExecutionConfig(**{**_VALID, **overrides})


# --------------------------------------------------------------------------- selector


def test_backend_name_is_the_registered_selector() -> "None":
    """The class attribute is what placement validation compares, so it is fixed."""
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionConfig

    assert CloudTasksExecutionConfig.backend_name == "cloudtasks"
    assert _config().backend_name == "cloudtasks"


def test_baseline_configuration_is_accepted() -> "None":
    config = _config()

    assert config.route_path == "/_litestar-queues/cloud-tasks"
    assert config.dispatch_deadline == 1800
    assert config.response_margin == 30.0
    assert config.default_task_timeout == 1740.0
    assert config.api_timeout == 10.0


def test_default_route_path_resolves_from_queue_namespace_without_mutating_config() -> "None":
    from litestar_queues import QueueConfig, WorkerConfig
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    execution = _config()
    queue_config = QueueConfig(
        namespace="dma", queue_backend="redis", execution_backend=execution, worker=WorkerConfig(placement="external")
    )

    assert CloudTasksExecutionBackend(queue_config).execution_config.route_path == "/_dma/cloud-tasks"
    assert CloudTasksExecutionBackend(queue_config).execution_config.delivery_name_prefix == "dma-"
    assert execution.route_path == "/_litestar-queues/cloud-tasks"
    assert execution.delivery_name_prefix is None


def test_explicit_delivery_name_prefix_wins_over_queue_namespace() -> "None":
    from litestar_queues import QueueConfig, WorkerConfig
    from litestar_queues.execution.cloudtasks import CloudTasksExecutionBackend

    execution = _config(delivery_name_prefix="custom-")
    queue_config = QueueConfig(
        namespace="dma", queue_backend="redis", execution_backend=execution, worker=WorkerConfig(placement="external")
    )

    assert CloudTasksExecutionBackend(queue_config).execution_config.delivery_name_prefix == "custom-"


# --------------------------------------------------------------------------- target url


def test_target_url_joins_the_service_origin_and_the_route_path() -> "None":
    config = _config(service_url="https://consumer.example.run.app", route_path="/_queues/deliver")

    assert config.target_url == "https://consumer.example.run.app/_queues/deliver"


def test_target_url_does_not_double_the_separator_for_a_trailing_slash() -> "None":
    config = _config(service_url="https://consumer.example.run.app/")

    assert config.target_url == "https://consumer.example.run.app/_litestar-queues/cloud-tasks"


def test_audience_defaults_to_the_service_origin_not_the_target_url() -> "None":
    """The OIDC audience Cloud Run validates is the service origin.

    Defaulting to ``target_url`` produces a token Cloud Run rejects on every
    delivery, which surfaces as an unauthenticated loop rather than a
    configuration error.
    """
    config = _config(service_url="https://consumer.example.run.app")

    assert config.audience == "https://consumer.example.run.app"
    assert config.audience != config.target_url


def test_explicit_audience_is_preserved() -> "None":
    config = _config(audience="https://custom-audience.example.com")

    assert config.audience == "https://custom-audience.example.com"


# --------------------------------------------------------------------------- identifiers


@pytest.mark.parametrize("field", ["project_id", "location", "queue_id", "service_account_email"])
@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_blank_identifiers_are_rejected(field: "str", value: "str") -> "None":
    with pytest.raises(QueueConfigurationError):
        _config(**{field: value})


@pytest.mark.parametrize("delivery_name_prefix", ["", "dma/", "dma prefix", "dma."])
def test_invalid_delivery_name_prefix_is_rejected(delivery_name_prefix: "str") -> "None":
    with pytest.raises(QueueConfigurationError, match="delivery_name_prefix"):
        _config(delivery_name_prefix=delivery_name_prefix)


# --------------------------------------------------------------------------- service url


@pytest.mark.parametrize(
    "service_url",
    [
        "http://consumer.example.run.app",
        "consumer.example.run.app",
        "https://",
        "https:///path",
        "https://user:pass@consumer.example.run.app",
        "https://consumer.example.run.app?token=1",
        "https://consumer.example.run.app#fragment",
        "https://consumer.example.run.app/base",
        "https://consumer.example.run.app/base/",
    ],
)
def test_service_url_must_be_an_absolute_https_origin(service_url: "str") -> "None":
    """A base path would silently prefix every delivery target."""
    with pytest.raises(QueueConfigurationError):
        _config(service_url=service_url)


@pytest.mark.parametrize(
    "service_url",
    ["https://consumer.example.run.app", "https://consumer.example.run.app/", "https://consumer.example.com:8443"],
)
def test_bare_https_origins_are_accepted(service_url: "str") -> "None":
    assert _config(service_url=service_url).service_url == service_url


# --------------------------------------------------------------------------- route path


@pytest.mark.parametrize(
    "route_path", ["", "/", "_litestar-queues/cloud-tasks", "/deliver?token=1", "/deliver#fragment"]
)
def test_route_path_must_be_a_non_root_absolute_path(route_path: "str") -> "None":
    """Mounting on ``/`` would put an unauthenticated delivery route at the origin."""
    with pytest.raises(QueueConfigurationError):
        _config(route_path=route_path)


def test_nested_route_paths_are_accepted() -> "None":
    assert _config(route_path="/internal/queues/deliver").route_path == "/internal/queues/deliver"


# --------------------------------------------------------------------------- deadline


@pytest.mark.parametrize("dispatch_deadline", [15, 900, 1800])
def test_dispatch_deadline_bounds_are_inclusive(dispatch_deadline: "int") -> "None":
    config = _config(dispatch_deadline=dispatch_deadline, default_task_timeout=1.0, response_margin=1.0)

    assert config.dispatch_deadline == dispatch_deadline


@pytest.mark.parametrize("dispatch_deadline", [0, 14, 1801, -1])
def test_dispatch_deadline_outside_the_google_range_is_rejected(dispatch_deadline: "int") -> "None":
    """1800s is Google's hard ceiling on an HTTP target deadline."""
    with pytest.raises(QueueConfigurationError):
        _config(dispatch_deadline=dispatch_deadline, default_task_timeout=1.0, response_margin=1.0)


@pytest.mark.parametrize("dispatch_deadline", [900.0, "900", True, None])
def test_dispatch_deadline_must_be_an_integer(dispatch_deadline: "Any") -> "None":
    """``bool`` is an ``int`` subclass, so a truthy flag would read as 1 second."""
    with pytest.raises(QueueConfigurationError):
        _config(dispatch_deadline=dispatch_deadline, default_task_timeout=1.0, response_margin=1.0)


# --------------------------------------------------------------------------- budgets


@pytest.mark.parametrize("field", ["response_margin", "default_task_timeout", "api_timeout"])
@pytest.mark.parametrize("value", [0, -1.0, float("nan"), float("inf")])
def test_durations_must_be_finite_and_positive(field: "str", value: "float") -> "None":
    with pytest.raises(QueueConfigurationError):
        _config(**{field: value})


def test_task_budget_must_fit_inside_the_dispatch_deadline() -> "None":
    """The response must leave the consumer before Google abandons the request."""
    with pytest.raises(QueueConfigurationError):
        _config(dispatch_deadline=600, default_task_timeout=580.0, response_margin=30.0)


def test_task_budget_exactly_equal_to_the_deadline_is_accepted() -> "None":
    config = _config(dispatch_deadline=600, default_task_timeout=570.0, response_margin=30.0)

    assert config.default_task_timeout + config.response_margin == config.dispatch_deadline


# --------------------------------------------------------------------------- authentication


def test_an_unauthenticated_delivery_route_is_refused() -> "None":
    """Neither platform auth nor guards means the route is open to the internet."""
    with pytest.raises(QueueConfigurationError):
        _config(trust_platform_auth=False, guards=())


def test_platform_auth_alone_is_sufficient() -> "None":
    assert _config(trust_platform_auth=True, guards=()).trust_platform_auth is True


def test_application_guards_alone_are_sufficient() -> "None":
    config = _config(trust_platform_auth=False, guards=(_guard,))

    assert config.guards == (_guard,)


# --------------------------------------------------------------------------- schedule horizon


def test_the_schedule_horizon_matches_googles_fixed_limit() -> "None":
    """Cloud Tasks refuses a schedule time more than 30 days out."""
    from litestar_queues.execution.cloudtasks import CLOUD_TASKS_MAX_SCHEDULE_HORIZON

    assert timedelta(days=30) == CLOUD_TASKS_MAX_SCHEDULE_HORIZON
