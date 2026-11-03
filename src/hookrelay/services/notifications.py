"""Operator notifications: enqueue in-transaction, dispatch from the relay (Milestone 4).

The table is an outbox, so the hook is decoupled from the code that detects the condition:
the worker that auto-disables an endpoint only inserts a row, and the relay hands pending rows
to a :class:`Notifier`. The default :class:`LoggingNotifier` emits a structured
``notification`` log line (ship it to your alerting via the log pipeline); a Slack, email or
PagerDuty notifier is one class implementing ``send``. Dispatch is at-least-once: a crash
between ``send`` and the commit re-sends, so notifiers should key on ``Notification.id``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, Protocol

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import Notification, NotificationStatus
from hookrelay.db.models.notification import MAX_NOTIFICATION_ERROR_LENGTH

logger = structlog.stdlib.get_logger("hookrelay.notifications")

MAX_RETRY_DELAY_S: Final = 300


@dataclass(frozen=True, slots=True)
class NotificationMessage:
    id: uuid.UUID
    tenant_id: uuid.UUID
    endpoint_id: uuid.UUID | None
    kind: str
    payload: dict[str, Any]
    attempt: int


class Notifier(Protocol):
    async def send(self, message: NotificationMessage) -> None:
        """Deliver one notification; raise to have it retried with backoff."""


class LoggingNotifier:
    """Default notifier: a structured WARNING log line per notification."""

    async def send(self, message: NotificationMessage) -> None:
        logger.warning(
            "notification",
            notification_id=str(message.id),
            tenant_id=str(message.tenant_id),
            endpoint_id=None if message.endpoint_id is None else str(message.endpoint_id),
            kind=message.kind,
            payload=message.payload,
            attempt=message.attempt,
        )


@dataclass(frozen=True, slots=True)
class DispatchResult:
    sent: int = 0
    failed: int = 0
    abandoned: int = 0

    @property
    def total(self) -> int:
        return self.sent + self.failed + self.abandoned


async def enqueue(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID | None,
    kind: str,
    payload: dict[str, Any],
) -> None:
    session.add(
        Notification(tenant_id=tenant_id, endpoint_id=endpoint_id, kind=kind, payload=payload)
    )
    await session.flush()


def retry_delay(attempts: int) -> timedelta:
    """Exponential backoff for a failing notifier: 2, 4, 8, ... seconds, capped."""
    return timedelta(seconds=min(MAX_RETRY_DELAY_S, 2 ** min(max(attempts, 1), 16)))


async def dispatch_due(
    session: AsyncSession, notifier: Notifier, *, batch_size: int, max_attempts: int
) -> DispatchResult:
    """Send up to ``batch_size`` due notifications; rows are locked ``SKIP LOCKED`` so several
    relays share the queue."""
    rows = (
        await session.scalars(
            select(Notification)
            .where(
                Notification.status == NotificationStatus.PENDING.value,
                Notification.next_attempt_at <= func.now(),
            )
            .order_by(Notification.next_attempt_at, Notification.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).all()
    sent = failed = abandoned = 0
    for row in rows:
        attempt = row.attempts + 1
        message = NotificationMessage(
            id=row.id,
            tenant_id=row.tenant_id,
            endpoint_id=row.endpoint_id,
            kind=row.kind,
            payload=dict(row.payload),
            attempt=attempt,
        )
        values: dict[str, Any] = {"attempts": attempt}
        try:
            await notifier.send(message)
        except Exception as exc:  # any notifier failure is retried, never fatal
            values["last_error"] = f"{type(exc).__name__}: {exc}"[:MAX_NOTIFICATION_ERROR_LENGTH]
            if attempt >= max_attempts:
                values.update(status=NotificationStatus.ABANDONED.value, completed_at=func.now())
                abandoned += 1
                logger.exception(
                    "notification.abandoned", notification_id=str(row.id), kind=row.kind
                )
            else:
                values["next_attempt_at"] = func.now() + retry_delay(attempt)
                failed += 1
                logger.warning(
                    "notification.failed",
                    notification_id=str(row.id),
                    error=type(exc).__name__,
                    attempt=attempt,
                )
        else:
            values.update(status=NotificationStatus.DISPATCHED.value, completed_at=func.now())
            sent += 1
        await session.execute(
            update(Notification)
            .where(Notification.id == row.id)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
    return DispatchResult(sent=sent, failed=failed, abandoned=abandoned)


async def purge_completed(session: AsyncSession, *, older_than_s: int, batch_size: int) -> int:
    doomed = (
        select(Notification.id)
        .where(
            Notification.status != NotificationStatus.PENDING.value,
            Notification.completed_at <= func.now() - timedelta(seconds=older_than_s),
        )
        .order_by(Notification.completed_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(Notification)
        .where(Notification.id.in_(doomed))
        .returning(Notification.id)
        .execution_options(synchronize_session=False)
    )
    return len(result.all())
