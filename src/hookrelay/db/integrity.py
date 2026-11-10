"""Classify driver errors: the constraint that fired (for precise 409/422 responses), the
SQLSTATE, and whether a failure is transient (worth retrying on a fresh connection)."""

from __future__ import annotations

from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    PendingRollbackError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

LOCK_NOT_AVAILABLE = "55P03"


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


def sqlstate(exc: DBAPIError) -> str | None:
    """SQLSTATE of a wrapped driver error (e.g. ``55P03`` lock_not_available), if reported."""
    candidates: list[object] = [exc.orig, getattr(exc.orig, "__cause__", None)]
    for candidate in candidates:
        for attribute in ("sqlstate", "pgcode"):
            code = getattr(candidate, attribute, None)
            if isinstance(code, str) and code:
                return code
    return None


def is_disconnect(exc: BaseException) -> bool:
    """The connection itself is gone (server restart, terminated backend, failover).

    SQLAlchemy marks such errors ``connection_invalidated``; the pool discards the connection
    and the next checkout opens a fresh one. Nothing about the statement was wrong.
    """
    return isinstance(exc, DBAPIError) and exc.connection_invalidated


# SQLSTATEs a server sends while it cannot serve *this connection* right now, none of which
# says anything about the statement: class 08 (connection exception), 57P01 admin_shutdown,
# 57P02 crash_shutdown, 57P03 cannot_connect_now ("the database system is starting up /
# shutting down" while a restart or failover is in progress) and 53300 too_many_connections.
# asyncpg raises these as plain ``PostgresError`` subclasses, so SQLAlchemy wraps them in a
# generic ``DBAPIError`` that is not marked ``connection_invalidated`` when the failure
# happens while *opening* a connection.
TRANSIENT_SQLSTATE_PREFIXES = ("08",)
TRANSIENT_SQLSTATES = frozenset({"57P01", "57P02", "57P03", "53300"})


def is_transient_sqlstate(code: str | None) -> bool:
    return code is not None and (
        code in TRANSIENT_SQLSTATES or code.startswith(TRANSIENT_SQLSTATE_PREFIXES)
    )


def is_transient_db_error(exc: BaseException) -> bool:
    """Errors after which the same unit of work may succeed on a fresh connection.

    Disconnects, operational / interface errors, pool checkout timeouts, socket errors and the
    connection-level SQLSTATEs above (a server restarting or failing over) qualify; constraint
    violations and programming errors (and anything raised by our own code) do not, so
    retrying can never mask a bug.
    """
    if isinstance(exc, PoolTimeoutError | OSError | TimeoutError):
        return True
    if is_disconnect(exc) or isinstance(exc, OperationalError | InterfaceError):
        return True
    return isinstance(exc, DBAPIError) and is_transient_sqlstate(sqlstate(exc))


def is_connection_lost(exc: BaseException) -> bool:
    """The session's connection is gone or refused, so nothing it was doing can be salvaged
    on it: a disconnect, a connection-level SQLSTATE (restart / failover in progress), or
    SQLAlchemy refusing to go on because the transaction died with its connection. Unlike
    :func:`is_transient_db_error` this excludes statement-level failures such as timeouts,
    so a genuinely poisonous statement is still isolated rather than retried forever."""
    if is_disconnect(exc) or isinstance(exc, PendingRollbackError):
        return True
    return isinstance(exc, DBAPIError) and is_transient_sqlstate(sqlstate(exc))
