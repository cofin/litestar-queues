"""OIDC Bearer token verification guard for Google Cloud Tasks delivery routes."""

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from litestar.exceptions import NotAuthorizedException

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from litestar.connection import ASGIConnection
    from litestar.handlers.base import BaseRouteHandler

__all__ = ("CloudTasksOIDCGuard",)

_BEARER_PREFIX = "Bearer "
_QUEUE_NAME_HEADER = "x-cloudtasks-queuename"


@dataclass(frozen=True, slots=True)
class CloudTasksOIDCGuard:
    """Litestar route guard that verifies Google-signed OIDC Bearer tokens."""

    audience: "str | None" = None
    """Expected OIDC `aud` claim (typically the consumer service origin)."""

    service_account_email: "str | None" = None
    """Expected verified `email` claim matching the Cloud Tasks OIDC service account."""

    queue_id: "str | None" = None
    """Optional expected `X-CloudTasks-QueueName` request header value."""

    token_verifier: "Callable[[str, str | None], Mapping[str, Any]] | None" = None
    """Optional custom token verification callable used in place of `google-auth`."""

    async def __call__(self, connection: "ASGIConnection[Any, Any, Any, Any]", _: "BaseRouteHandler") -> "None":
        """Verify the Cloud Tasks OIDC Bearer token on incoming deliveries.

        Raises:
            NotAuthorizedException: If the Authorization header is missing, the
                OIDC token is invalid, the service account email does not match,
                or the queue name header mismatches.
        """
        auth_header = connection.headers.get("authorization", "")
        if not auth_header.startswith(_BEARER_PREFIX):
            msg = "Missing or invalid Cloud Tasks Authorization Bearer header."
            raise NotAuthorizedException(msg)
        token = auth_header[len(_BEARER_PREFIX) :].strip()
        if not token:
            msg = "Empty Cloud Tasks Bearer token."
            raise NotAuthorizedException(msg)

        verifier = self.token_verifier
        try:
            if verifier is not None:
                claims = verifier(token, self.audience)
            else:
                claims = await asyncio.to_thread(_verify_google_id_token, token, self.audience)
        except Exception as exc:
            msg = "Invalid Cloud Tasks OIDC token."
            raise NotAuthorizedException(msg) from exc

        if self.service_account_email is not None:
            email = claims.get("email")
            email_verified = claims.get("email_verified", False)
            if email != self.service_account_email or not email_verified:
                msg = "Cloud Tasks OIDC token principal does not match configured service_account_email."
                raise NotAuthorizedException(msg)

        if self.queue_id is not None:
            header_queue = connection.headers.get(_QUEUE_NAME_HEADER)
            if header_queue is not None and header_queue != self.queue_id:
                msg = "Cloud Tasks queue header does not match configured queue_id."
                raise NotAuthorizedException(msg)


def _verify_google_id_token(token: "str", audience: "str | None") -> "Mapping[str, Any]":
    """Verify a Google OIDC token using google-auth.

    Returns:
        The decoded and verified token claims mapping.
    """
    import google.auth.transport.requests
    import google.oauth2.id_token

    request = google.auth.transport.requests.Request()
    verified = google.oauth2.id_token.verify_oauth2_token(  # type: ignore[no-untyped-call]
        token, request, audience=audience
    )
    return cast("Mapping[str, Any]", verified)
