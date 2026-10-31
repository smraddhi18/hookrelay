"""Deny-by-default audit: every route must be protected unless explicitly allow-listed."""

from __future__ import annotations

import re
from typing import Any

import pytest

from hookrelay.main import create_app
from tests.conftest import TEST_OPERATOR_TOKEN, FakeDatabase, make_settings, serve

PUBLIC_PATHS = {"/health", "/ready"}
ZERO_ID = "00000000-0000-0000-0000-000000000000"
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


def _operations() -> list[tuple[str, str, dict[str, Any]]]:
    # The OpenAPI document is generated from the effective routes (after router-level
    # dependencies are merged), so it is the authoritative list of what the app serves.
    spec = create_app(make_settings(), database=FakeDatabase().as_database()).openapi()
    return [
        (method.upper(), path, operation)
        for path, item in spec["paths"].items()
        for method, operation in item.items()
        if method in HTTP_METHODS
    ]


def test_every_operation_is_public_tenant_or_operator() -> None:
    operations = _operations()
    assert len(operations) > 20
    for method, path, operation in operations:
        security = operation.get("security")
        if path in PUBLIC_PATHS:
            assert security is None, f"{method} {path} should be public"
            continue
        expected = "OperatorToken" if path.startswith("/admin/") else "ApiKey"
        assert path.startswith(("/admin/", "/v1/")), f"{method} {path} is outside a guarded router"
        assert security == [{expected: []}], f"{method} {path} has security {security}"
        assert "401" in operation["responses"], f"{method} {path} does not document 401"


def _requests() -> list[tuple[str, str]]:
    out = []
    for method, path, _ in _operations():
        if path in PUBLIC_PATHS:
            continue
        concrete = re.sub(r"\{\w+_id\}", ZERO_ID, path).replace("{name}", "invoice.paid")
        assert "{" not in concrete, concrete
        out.append((method, concrete))
    return out


@pytest.mark.parametrize(("method", "path"), _requests())
async def test_protected_routes_reject_anonymous_callers(method: str, path: str) -> None:
    db = FakeDatabase()
    app = create_app(make_settings(operator_token=TEST_OPERATOR_TOKEN), database=db.as_database())
    async with serve(app) as client:
        response = await client.request(method, path, json={})
    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"].startswith("Bearer")
    assert response.json()["error"]["code"] == "unauthorized"


@pytest.mark.parametrize("header", ["Bearer nope", "Basic abc", "Bearer hrk_short"])
async def test_malformed_api_keys_are_rejected_without_touching_the_database(header: str) -> None:
    app = create_app(make_settings(), database=FakeDatabase().as_database())
    async with serve(app) as client:
        response = await client.get("/v1/me", headers={"Authorization": header})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_operator_api_is_disabled_without_a_token() -> None:
    app = create_app(make_settings(), database=FakeDatabase().as_database())
    async with serve(app) as client:
        response = await client.get(
            "/admin/tenants", headers={"Authorization": f"Bearer {TEST_OPERATOR_TOKEN}"}
        )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "operator_api_disabled"


async def test_wrong_operator_token_is_rejected() -> None:
    app = create_app(
        make_settings(operator_token=TEST_OPERATOR_TOKEN), database=FakeDatabase().as_database()
    )
    async with serve(app) as client:
        response = await client.get(
            "/admin/tenants", headers={"Authorization": f"Bearer {TEST_OPERATOR_TOKEN}x"}
        )
    assert response.status_code == 401
    assert response.json()["error"]["message"] == "Invalid operator token"


async def test_openapi_documents_security_and_examples() -> None:
    app = create_app(make_settings(), database=FakeDatabase().as_database())
    async with serve(app) as client:
        spec = (await client.get("/openapi.json")).json()
    schemes = spec["components"]["securitySchemes"]
    assert schemes["ApiKey"]["scheme"] == "bearer"
    assert schemes["OperatorToken"]["scheme"] == "bearer"
    endpoint_post = spec["paths"]["/v1/endpoints"]["post"]
    assert {"ApiKey": []} in endpoint_post["security"]
    assert {"OperatorToken": []} in spec["paths"]["/admin/tenants"]["post"]["security"]
    assert "security" not in spec["paths"]["/health"]["get"]
    assert spec["components"]["schemas"]["EndpointCreate"].get("examples")
    patch_params = {
        p["name"] for p in spec["paths"]["/v1/endpoints/{endpoint_id}"]["patch"]["parameters"]
    }
    assert "If-Match" in patch_params
