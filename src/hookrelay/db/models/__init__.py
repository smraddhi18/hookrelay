"""ORM models. Import every model here so ``Base.metadata`` is complete for Alembic."""

from hookrelay.db.models.tenant import Tenant, TenantStatus

__all__ = ["Tenant", "TenantStatus"]
