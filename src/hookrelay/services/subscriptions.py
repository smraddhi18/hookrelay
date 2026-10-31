"""Subscriptions (endpoint ↔ event type pattern) and fan-out matching.

Pattern semantics (``*`` is only allowed as the final segment):

* ``invoice.paid`` matches exactly ``invoice.paid``;
* ``invoice.*`` matches every type that starts with ``invoice.`` (``invoice.paid``,
  ``invoice.line.added``) but not ``invoice`` itself;
* ``*`` matches everything.

Matching runs in SQL so the M2 ingest path is one indexed query per message; ``pattern_matches``
is the reference implementation that the integration tests compare the SQL against.
"""

from __future__ import annotations

import uuid

import structlog
from sqlalchemy import and_, func, literal, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import ConflictError, NotFoundError, UnprocessableEntityError
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import Endpoint, EventType, Subscription
from hookrelay.services.endpoints import get_endpoint
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.subscriptions")

WILDCARD = "*"


def is_wildcard(pattern: str) -> bool:
    return pattern.endswith(WILDCARD)


def pattern_matches(pattern: str, event_type: str) -> bool:
    if pattern == WILDCARD:
        return True
    if is_wildcard(pattern):
        return event_type.startswith(pattern[:-1])  # "invoice.*" -> prefix "invoice."
    return pattern == event_type


async def create_subscription(
    session: AsyncSession, *, tenant_id: uuid.UUID, endpoint_id: uuid.UUID, pattern: str
) -> Subscription:
    await get_endpoint(session, tenant_id, endpoint_id)  # 404 for unknown / foreign / deleted
    event_type_id: uuid.UUID | None = None
    if not is_wildcard(pattern):
        # FOR SHARE: a concurrent archive cannot slip in between the check and the insert.
        event_type = await session.scalar(
            select(EventType)
            .where(EventType.tenant_id == tenant_id, EventType.name == pattern)
            .with_for_update(read=True)
        )
        if event_type is None or event_type.archived_at is not None:
            raise UnprocessableEntityError(
                f"Event type {pattern!r} is not registered (or is archived)",
                code="unknown_event_type",
                details=[{"loc": ["body", "event_type"]}],
            )
        event_type_id = event_type.id
    subscription = Subscription(
        tenant_id=tenant_id,
        endpoint_id=endpoint_id,
        event_type_pattern=pattern,
        event_type_id=event_type_id,
    )
    session.add(subscription)
    try:
        await session.flush()
    except IntegrityError as exc:
        if violated_constraint(exc) == "uq_subscriptions_endpoint_pattern":
            raise ConflictError(
                f"Endpoint is already subscribed to {pattern!r}", code="subscription_exists"
            ) from exc
        raise
    logger.info(
        "subscription.created",
        tenant_id=str(tenant_id),
        endpoint_id=str(endpoint_id),
        pattern=pattern,
    )
    return subscription


async def list_subscriptions(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    cursor: str | None,
    limit: int,
) -> Page[Subscription]:
    await get_endpoint(session, tenant_id, endpoint_id)
    stmt = select(Subscription).where(
        Subscription.tenant_id == tenant_id, Subscription.endpoint_id == endpoint_id
    )
    return await paginate(
        session,
        stmt,
        created_col=Subscription.created_at,
        id_col=Subscription.id,
        cursor=cursor,
        limit=limit,
    )


async def delete_subscription(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    subscription_id: uuid.UUID,
) -> None:
    subscription = await session.scalar(
        select(Subscription).where(
            Subscription.tenant_id == tenant_id,
            Subscription.endpoint_id == endpoint_id,
            Subscription.id == subscription_id,
        )
    )
    if subscription is None:
        raise NotFoundError("Subscription not found", code="subscription_not_found")
    await session.delete(subscription)
    await session.flush()
    logger.info(
        "subscription.deleted",
        tenant_id=str(tenant_id),
        endpoint_id=str(endpoint_id),
        subscription_id=str(subscription_id),
    )


async def match_endpoints(
    session: AsyncSession, tenant_id: uuid.UUID, event_type: str
) -> list[tuple[Endpoint, list[str]]]:
    """Live, enabled endpoints subscribed to ``event_type`` plus the patterns that matched."""
    pattern = Subscription.event_type_pattern
    matched = or_(
        pattern == event_type,
        and_(
            func.right(pattern, 1) == WILDCARD,
            func.starts_with(literal(event_type), func.left(pattern, -1)),
        ),
    )
    rows = (
        await session.execute(
            select(Endpoint, func.array_agg(pattern.distinct()).label("patterns"))
            .join(
                Subscription,
                and_(
                    Subscription.tenant_id == Endpoint.tenant_id,
                    Subscription.endpoint_id == Endpoint.id,
                ),
            )
            .where(
                Endpoint.tenant_id == tenant_id,
                Endpoint.deleted_at.is_(None),
                Endpoint.enabled.is_(True),
                Subscription.tenant_id == tenant_id,
                matched,
            )
            .group_by(Endpoint.id)
            .order_by(Endpoint.created_at, Endpoint.id)
        )
    ).all()
    return [(row[0], sorted(row[1])) for row in rows]
