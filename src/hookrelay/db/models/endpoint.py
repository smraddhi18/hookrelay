"""Webhook endpoints: where a tenant wants events delivered."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin

MIN_TIMEOUT_MS = 1_000
MAX_TIMEOUT_MS = 60_000
MAX_URL_LENGTH = 2_048


class Endpoint(TimestampMixin, Base):
    __tablename__ = "endpoints"
    __table_args__ = (
        # Target of the composite (tenant_id, endpoint_id) FK from subscriptions: the database
        # itself refuses a subscription that points at another tenant's endpoint.
        UniqueConstraint("tenant_id", "id", name="uq_endpoints_tenant_id_id"),
        CheckConstraint(
            f"url ~ '^https?://' AND length(url) <= {MAX_URL_LENGTH}", name="url_format"
        ),
        CheckConstraint(
            f"timeout_ms BETWEEN {MIN_TIMEOUT_MS} AND {MAX_TIMEOUT_MS}", name="timeout_range"
        ),
        CheckConstraint("jsonb_typeof(headers) = 'object'", name="headers_object"),
        CheckConstraint("version >= 1", name="version_positive"),
        CheckConstraint("deleted_at IS NULL OR NOT enabled", name="deleted_is_disabled"),
        CheckConstraint("secret_ciphertext LIKE 'v1.%'", name="secret_encrypted"),
        # One live endpoint per URL per tenant; soft-deleted rows do not block re-creation.
        Index(
            "uq_endpoints_tenant_url_live",
            "tenant_id",
            "url",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        Index(
            "ix_endpoints_tenant_created_live",
            "tenant_id",
            "created_at",
            "id",
            postgresql_where=text("deleted_at IS NULL"),
        ),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("10000"))
    # AES-256-GCM ciphertext (see hookrelay.core.security.SecretBox), bound to tenant+endpoint.
    secret_ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"Endpoint(id={self.id!s}, tenant_id={self.tenant_id!s}, enabled={self.enabled})"
