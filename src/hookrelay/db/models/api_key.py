"""Tenant API keys. Only a SHA-256 digest of the key is stored; the plaintext is shown once."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    LargeBinary,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin


class ApiKeyRole(StrEnum):
    """Hierarchical: admin ⊇ publisher ⊇ reader."""

    READER = "reader"
    PUBLISHER = "publisher"
    ADMIN = "admin"

    @property
    def rank(self) -> int:
        return _ROLE_RANK[self]

    def satisfies(self, required: ApiKeyRole) -> bool:
        return self.rank >= required.rank


_ROLE_RANK = {ApiKeyRole.READER: 0, ApiKeyRole.PUBLISHER: 1, ApiKeyRole.ADMIN: 2}


class ApiKeyStatus(StrEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class ApiKey(TimestampMixin, Base):
    __tablename__ = "api_keys"
    __table_args__ = (
        CheckConstraint("role IN ('admin', 'publisher', 'reader')", name="role_valid"),
        CheckConstraint("prefix ~ '^[a-z0-9]{12}$'", name="prefix_format"),
        CheckConstraint("octet_length(key_hash) = 32", name="key_hash_sha256"),
        CheckConstraint("length(btrim(name)) BETWEEN 1 AND 100", name="name_length"),
        CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at", name="expiry_after_creation"
        ),
        CheckConstraint(
            "rotated_from_id IS NULL OR rotated_from_id <> id", name="not_self_rotated"
        ),
        # Keyset pagination of a tenant's keys: ORDER BY created_at DESC, id DESC.
        Index("ix_api_keys_tenant_created", "tenant_id", "created_at", "id"),
        # At most one successor per rotated key (a second concurrent rotation gets a 409).
        Index(
            "uq_api_keys_rotated_from_id",
            "rotated_from_id",
            unique=True,
            postgresql_where=text("rotated_from_id IS NOT NULL"),
        ),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    # Public lookup handle embedded in the key (``hrk_<prefix>_<secret>``); globally unique.
    prefix: Mapped[str] = mapped_column(String(12), nullable=False, unique=True)
    key_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rotated_from_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("api_keys.id", ondelete="SET NULL")
    )

    def status_at(self, now: datetime) -> ApiKeyStatus:
        if self.revoked_at is not None:
            return ApiKeyStatus.REVOKED
        if self.expires_at is not None and self.expires_at <= now:
            return ApiKeyStatus.EXPIRED
        return ApiKeyStatus.ACTIVE

    def __repr__(self) -> str:  # never includes key_hash
        return f"ApiKey(id={self.id!s}, prefix={self.prefix!r}, role={self.role!r})"
