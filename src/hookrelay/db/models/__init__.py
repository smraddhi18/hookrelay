"""ORM models. Import every model here so ``Base.metadata`` is complete for Alembic."""

from hookrelay.db.models.api_key import ApiKey, ApiKeyRole, ApiKeyStatus
from hookrelay.db.models.endpoint import Endpoint
from hookrelay.db.models.event_type import EventType
from hookrelay.db.models.subscription import Subscription
from hookrelay.db.models.tenant import Tenant, TenantStatus

__all__ = [
    "ApiKey",
    "ApiKeyRole",
    "ApiKeyStatus",
    "Endpoint",
    "EventType",
    "Subscription",
    "Tenant",
    "TenantStatus",
]
