from __future__ import annotations

from typing import Annotated, Any

import pytest
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from hookrelay.core.errors import (
    AppError,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
    code_for_status,
)
from tests.conftest import serve


def _with_routes(app: FastAPI) -> FastAPI:
    @app.get("/_test/not-found")
    async def not_found() -> None:
        raise NotFoundError("Endpoint ep_123 not found", details={"endpoint_id": "ep_123"})

    @app.get("/_test/conflict")
    async def conflict() -> None:
        raise ConflictError("Slug already taken")

    @app.get("/_test/custom")
    async def custom() -> None:
        raise AppError(
            "Slow down", code="rate_limited", status_code=429, headers={"Retry-After": "3"}
        )

    @app.get("/_test/http")
    async def http_exc() -> None:
        raise HTTPException(status_code=418, detail={"reason": "teapot"})

    @app.get("/_test/boom")
    async def boom() -> None:
        raise RuntimeError("secret connection string postgres://u:pw@db")

    @app.get("/_test/validate")
    async def validate(limit: Annotated[int, Query(ge=1, le=100)]) -> dict[str, int]:
        return {"limit": limit}

    return app


@pytest.fixture
def app(app_factory) -> FastAPI:
    return _with_routes(app_factory())


async def test_app_error_envelope(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/not-found")
    assert resp.status_code == 404
    body = resp.json()
    assert set(body) == {"error"}
    err = body["error"]
    assert err["code"] == "not_found"
    assert err["message"] == "Endpoint ep_123 not found"
    assert err["details"] == {"endpoint_id": "ep_123"}
    assert err["request_id"] == resp.headers["x-request-id"]


async def test_app_error_overrides_and_headers(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/custom")
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "3"
    assert resp.json()["error"]["code"] == "rate_limited"


async def test_conflict(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/conflict")
    assert (resp.status_code, resp.json()["error"]["code"]) == (409, "conflict")


async def test_validation_error_envelope(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/validate", params={"limit": 0})
    assert resp.status_code == 422
    err = resp.json()["error"]
    assert err["code"] == "validation_error"
    assert err["details"][0]["loc"] == ["query", "limit"]
    assert err["details"][0]["type"] == "greater_than_equal"


async def test_missing_param_is_validation_error(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/validate")
    assert resp.status_code == 422
    assert resp.json()["error"]["details"][0]["type"] == "missing"


async def test_unknown_route_is_404_envelope(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_wrong_method_is_405_envelope(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.post("/health")
    assert resp.status_code == 405
    assert resp.json()["error"]["code"] == "method_not_allowed"
    assert "GET" in resp.headers["allow"]


async def test_http_exception_with_structured_detail(app: FastAPI) -> None:
    async with serve(app) as client:
        resp = await client.get("/_test/http")
    assert resp.status_code == 418
    err = resp.json()["error"]
    assert err["details"] == {"reason": "teapot"}
    assert err["code"] == "i'm_a_teapot"


async def test_unhandled_exception_is_generic_500(app: FastAPI) -> None:
    async with serve(app, raise_app_exceptions=False) as client:
        resp = await client.get("/_test/boom", headers={"X-Request-ID": "boom-1"})
    assert resp.status_code == 500
    err = resp.json()["error"]
    assert err == {
        "code": "internal_error",
        "message": "An unexpected error occurred",
        "details": None,
        "request_id": "boom-1",
    }
    assert "pw@db" not in resp.text
    assert resp.headers["x-request-id"] == "boom-1"


@pytest.mark.parametrize(
    ("exc_type", "status", "code"),
    [
        (BadRequestError, 400, "bad_request"),
        (UnauthorizedError, 401, "unauthorized"),
        (ForbiddenError, 403, "forbidden"),
        (NotFoundError, 404, "not_found"),
        (ConflictError, 409, "conflict"),
        (ServiceUnavailableError, 503, "service_unavailable"),
        (AppError, 500, "internal_error"),
    ],
)
def test_error_types_carry_status_and_code(
    exc_type: type[AppError], status: int, code: str
) -> None:
    exc = exc_type("msg")
    assert (exc.status_code, exc.code, exc.message, str(exc)) == (status, code, "msg", "msg")


@pytest.mark.parametrize(
    ("status", "code"),
    [(404, "not_found"), (429, "rate_limited"), (502, "bad_gateway"), (599, "http_error")],
)
def test_code_for_status(status: int, code: str) -> None:
    assert code_for_status(status) == code


async def test_transient_database_errors_are_retryable_503s(app_factory: Any) -> None:
    app: FastAPI = app_factory()

    @app.get("/_test/pool")
    async def pool() -> None:
        raise PoolTimeoutError("QueuePool limit of size 10 overflow 5 reached")

    @app.get("/_test/disconnect")
    async def disconnect() -> None:
        raise DBAPIError("SELECT 1", {}, ConnectionError("gone"), connection_invalidated=True)

    @app.get("/_test/refused")
    async def refused() -> None:
        raise ConnectionRefusedError("connect to db:5432")

    @app.get("/_test/restarting")
    async def restarting() -> None:
        cause = Exception("the database system is shutting down")
        cause.sqlstate = "57P03"  # type: ignore[attr-defined]
        orig = Exception("wrapped")
        orig.__cause__ = cause
        raise DBAPIError("SELECT 1", {}, orig)

    @app.get("/_test/integrity")
    async def integrity() -> None:
        raise IntegrityError("INSERT", {}, Exception("duplicate key secret-value"))

    async with serve(app, raise_app_exceptions=False) as client:
        for path in ("/_test/pool", "/_test/disconnect", "/_test/refused", "/_test/restarting"):
            resp = await client.get(path)
            assert resp.status_code == 503, path
            assert resp.headers["retry-after"] == "1"
            assert resp.json()["error"]["code"] == "database_unavailable"
            assert resp.headers["x-request-id"] == resp.json()["error"]["request_id"]
        bug = await client.get("/_test/integrity")
    # A non-transient database error is still a bug: an opaque 500 that leaks nothing.
    assert bug.status_code == 500
    assert bug.json()["error"]["code"] == "internal_error"
    assert "secret-value" not in bug.text
