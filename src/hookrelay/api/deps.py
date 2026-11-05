"""Authentication and authorisation dependencies (deny by default).

* ``authenticate`` is attached to the whole ``/v1`` router, so a new route is protected even if
  its author forgets to think about auth; routes then *narrow* access with ``ReaderDep``,
  ``PublisherDep`` or ``AdminDep``.
* ``require_operator`` guards ``/admin``: a single deployment-level token compared in constant
  time. It is deliberately not an API key: operators act across tenants, API keys never do.
* A unit test walks every registered route and fails if one is reachable without either.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID

import structlog
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from hookrelay.core.config import Settings
from hookrelay.core.errors import ForbiddenError, UnauthorizedError
from hookrelay.core.security import SecretBox, operator_token_matches
from hookrelay.db.models import ApiKeyRole
from hookrelay.db.session import DatabaseDep
from hookrelay.redis_ import RedisBundle, WakeupPublisher
from hookrelay.redis_.subscription_cache import SubscriptionCache
from hookrelay.services import api_keys as api_key_service
from hookrelay.services.api_keys import BEARER_CHALLENGE, Principal
from hookrelay.services.endpoints import EndpointPolicy

logger = structlog.stdlib.get_logger("hookrelay.auth")

api_key_scheme = HTTPBearer(
    scheme_name="ApiKey",
    description="Tenant API key: `Authorization: Bearer hrk_<prefix>_<secret>`.",
    auto_error=False,
)
operator_scheme = HTTPBearer(
    scheme_name="OperatorToken",
    description="Deployment operator token (`OPERATOR_TOKEN`) for the `/admin` API.",
    auto_error=False,
)

Credentials = Annotated[HTTPAuthorizationCredentials | None, Depends(api_key_scheme)]
OperatorCredentials = Annotated[HTTPAuthorizationCredentials | None, Depends(operator_scheme)]


def get_app_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_secret_box(request: Request) -> SecretBox:
    box: SecretBox = request.app.state.secret_box
    return box


def get_endpoint_policy(request: Request) -> EndpointPolicy:
    policy: EndpointPolicy = request.app.state.endpoint_policy
    return policy


SettingsDep = Annotated[Settings, Depends(get_app_settings)]
SecretBoxDep = Annotated[SecretBox, Depends(get_secret_box)]
EndpointPolicyDep = Annotated[EndpointPolicy, Depends(get_endpoint_policy)]


async def authenticate(
    request: Request, database: DatabaseDep, settings: SettingsDep, credentials: Credentials
) -> Principal:
    if credentials is None:
        raise UnauthorizedError(
            "Missing API key (send Authorization: Bearer <key>)", headers=BEARER_CHALLENGE
        )
    api_key_service.require_well_formed(credentials.credentials)
    # Own short transaction (not the route's unit of work): the key lookup holds no locks past
    # authentication, and the throttled last_used_at write commits even if the route fails.
    async with database.session() as session:
        principal = await api_key_service.authenticate(
            session,
            credentials.credentials,
            last_used_resolution_s=settings.api_key_last_used_resolution_s,
        )
    request.state.principal = principal
    # Every later log line of this request carries the tenant + key (never the secret).
    structlog.contextvars.bind_contextvars(
        tenant_id=str(principal.tenant_id), api_key_id=str(principal.api_key_id)
    )
    return principal


PrincipalDep = Annotated[Principal, Depends(authenticate)]


def require_role(role: ApiKeyRole) -> Callable[[Principal], Awaitable[Principal]]:
    async def guard(principal: PrincipalDep) -> Principal:
        if not principal.can(role):
            logger.info("auth.forbidden", required_role=role.value, role=principal.role.value)
            raise ForbiddenError(
                f"This operation requires an API key with the {role.value!r} role",
                code="insufficient_role",
                details={"required_role": role.value, "role": principal.role.value},
            )
        return principal

    guard.__name__ = f"require_{role.value}"
    return guard


ReaderDep = Annotated[Principal, Depends(require_role(ApiKeyRole.READER))]
PublisherDep = Annotated[Principal, Depends(require_role(ApiKeyRole.PUBLISHER))]
AdminDep = Annotated[Principal, Depends(require_role(ApiKeyRole.ADMIN))]


async def require_operator(settings: SettingsDep, credentials: OperatorCredentials) -> None:
    expected = settings.operator_token
    if expected is None:
        raise ForbiddenError(
            "The operator API is disabled (OPERATOR_TOKEN is not configured)",
            code="operator_api_disabled",
        )
    if credentials is None:
        raise UnauthorizedError("Missing operator token", headers=BEARER_CHALLENGE)
    if not operator_token_matches(credentials.credentials, expected.get_secret_value()):
        logger.warning("auth.operator.rejected")
        raise UnauthorizedError("Invalid operator token", headers=BEARER_CHALLENGE)
    structlog.contextvars.bind_contextvars(actor="operator")


def get_redis_bundle(request: Request) -> RedisBundle | None:
    bundle: RedisBundle | None = getattr(request.app.state, "redis", None)
    return bundle


RedisBundleDep = Annotated[RedisBundle | None, Depends(get_redis_bundle)]


async def invalidate_subscription_cache(request: Request, tenant_id: object) -> None:
    """Drop cached fan-out membership for a tenant after a membership-changing mutation."""
    bundle = get_redis_bundle(request)
    cache: SubscriptionCache | None = None if bundle is None else bundle.cache
    if cache is None:
        return
    tid = tenant_id if isinstance(tenant_id, UUID) else UUID(str(tenant_id))
    await cache.invalidate_tenant(tid)


def notify_workers(request: Request) -> None:
    """Publish a deliveries wake after replay re-queues work."""
    bundle = get_redis_bundle(request)
    publisher: WakeupPublisher | None = None if bundle is None else bundle.publisher
    if publisher is not None:
        publisher.notify()
