"""Typed application errors and the single JSON error envelope used by every non-2xx response.

Envelope::

    {"error": {"code": "not_found", "message": "...", "details": ..., "request_id": "..."}}

``code`` is a stable machine-readable identifier clients can switch on; ``message`` is for
humans and may change. 5xx responses never leak exception text.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from hookrelay.core.request_context import request_id_from_scope

logger = structlog.stdlib.get_logger("hookrelay.errors")


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Any | None = None
    request_id: str | None = None


class ErrorEnvelope(BaseModel):
    """OpenAPI schema for every error response."""

    error: ErrorBody


class AppError(Exception):
    """Base for errors the service raises on purpose. Subclasses fix ``status_code``/``code``."""

    status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR
    code: str = "internal_error"

    def __init__(
        self,
        message: str,
        *,
        details: Any | None = None,
        code: str | None = None,
        status_code: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details
        self.headers = headers
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


class BadRequestError(AppError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "bad_request"


class UnauthorizedError(AppError):
    status_code = HTTPStatus.UNAUTHORIZED
    code = "unauthorized"


class ForbiddenError(AppError):
    status_code = HTTPStatus.FORBIDDEN
    code = "forbidden"


class NotFoundError(AppError):
    status_code = HTTPStatus.NOT_FOUND
    code = "not_found"


class ConflictError(AppError):
    status_code = HTTPStatus.CONFLICT
    code = "conflict"


class PreconditionFailedError(AppError):
    """``If-Match`` did not match the current version (optimistic concurrency conflict)."""

    status_code = HTTPStatus.PRECONDITION_FAILED
    code = "precondition_failed"


class UnprocessableEntityError(AppError):
    """Well-formed request that fails a semantic check the schema cannot express."""

    status_code = HTTPStatus.UNPROCESSABLE_ENTITY
    code = "validation_error"


class PreconditionRequiredError(AppError):
    """An update was sent without ``If-Match`` (lost-update protection is mandatory)."""

    status_code = HTTPStatus.PRECONDITION_REQUIRED
    code = "precondition_required"


class ServiceUnavailableError(AppError):
    status_code = HTTPStatus.SERVICE_UNAVAILABLE
    code = "service_unavailable"


_HTTP_STATUS_CODES: dict[int, str] = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    406: "not_acceptable",
    409: "conflict",
    412: "precondition_failed",
    413: "payload_too_large",
    415: "unsupported_media_type",
    422: "validation_error",
    428: "precondition_required",
    429: "rate_limited",
    500: "internal_error",
    503: "service_unavailable",
}


def code_for_status(status_code: int) -> str:
    if status_code in _HTTP_STATUS_CODES:
        return _HTTP_STATUS_CODES[status_code]
    try:
        return HTTPStatus(status_code).phrase.lower().replace(" ", "_").replace("-", "_")
    except ValueError:
        return "http_error"


def error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: Any | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    request_id = request_id_from_scope(request.scope)
    body = ErrorEnvelope(
        error=ErrorBody(code=code, message=message, details=details, request_id=request_id)
    )
    merged_headers = dict(headers or {})
    if request_id:
        # Set explicitly: for unhandled errors this response is produced by Starlette's
        # ServerErrorMiddleware, which sits *outside* the request-context middleware.
        header_name = getattr(request.app.state, "request_id_header", "X-Request-ID")
        merged_headers[header_name] = request_id
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder(body.model_dump()),
        headers=merged_headers,
    )


async def app_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AppError)  # noqa: S101 - registered only for AppError
    log = logger.error if exc.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR else logger.info
    log("app.error", code=exc.code, status_code=exc.status_code, path=request.url.path)
    return error_response(
        request,
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        details=exc.details,
        headers=exc.headers,
    )


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)  # noqa: S101
    message = exc.detail if isinstance(exc.detail, str) else HTTPStatus(exc.status_code).phrase
    details = None if isinstance(exc.detail, str) else exc.detail
    return error_response(
        request,
        status_code=exc.status_code,
        code=code_for_status(exc.status_code),
        message=message,
        details=details,
        headers=dict(exc.headers) if exc.headers else None,
    )


async def validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)  # noqa: S101
    details = [
        {
            "loc": list(err.get("loc", ())),
            "msg": err.get("msg", ""),
            "type": err.get("type", ""),
        }
        for err in exc.errors()
    ]
    return error_response(
        request,
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        code="validation_error",
        message="Request validation failed",
        details=details,
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = request_id_from_scope(request.scope)
    logger.exception(
        "unhandled.exception",
        exc_type=type(exc).__name__,
        path=request.url.path,
        method=request.method,
        request_id=request_id,
    )
    return error_response(
        request,
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        code="internal_error",
        message="An unexpected error occurred",
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
