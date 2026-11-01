"""Shared contract pieces: pagination envelope/params and documented error responses."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Query
from pydantic import BaseModel, Field

from hookrelay.core.errors import ErrorEnvelope
from hookrelay.services.pagination import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

LimitQuery = Annotated[
    int,
    Query(ge=1, le=MAX_PAGE_SIZE, description=f"Page size (1 to {MAX_PAGE_SIZE})."),
]
CursorQuery = Annotated[
    str | None,
    Query(
        max_length=512,
        description="Opaque `next_cursor` from the previous page; omit for the first page.",
    ),
]
DEFAULT_LIMIT = DEFAULT_PAGE_SIZE


class PageResponse[T](BaseModel):
    """Keyset-paginated list, newest first."""

    data: list[T]
    next_cursor: str | None = Field(
        description="Pass as `cursor` to fetch the next page; null on the last page."
    )
    has_more: bool


_ERROR_DESCRIPTIONS: dict[int, str] = {
    400: "Malformed request (e.g. invalid cursor)",
    401: "Missing or invalid credentials",
    403: "Authenticated but not allowed (role too low, tenant suspended)",
    404: "Not found (or owned by another tenant)",
    409: "Conflicts with the current state",
    412: "If-Match did not match the current ETag",
    413: "Request body or payload exceeds the configured size limit",
    415: "Content-Type must be application/json",
    422: "Validation error",
    428: "If-Match header is required",
}


def error_responses(*codes: int) -> dict[int | str, dict[str, Any]]:
    return {
        code: {"model": ErrorEnvelope, "description": _ERROR_DESCRIPTIONS[code]} for code in codes
    }


AUTH_ERRORS = (401, 403)
