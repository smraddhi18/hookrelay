"""Map Postgres integrity errors to the constraint that fired (for precise 409/422 responses)."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError


def violated_constraint(exc: IntegrityError) -> str | None:
    """Name of the violated constraint/index, or ``None`` if the driver did not report one.

    SQLAlchemy wraps the asyncpg exception (``exc.orig``), whose ``__cause__`` is the original
    ``asyncpg.PostgresError`` carrying ``constraint_name``.
    """
    candidates: list[object] = [exc.orig, getattr(exc.orig, "__cause__", None)]
    for candidate in candidates:
        name = getattr(candidate, "constraint_name", None)
        if isinstance(name, str) and name:
            return name
    return None
