"""Milestone 5 signing-secret rotation on real Postgres: the admin API (If-Match, overlap
limits, bring-your-own secret, roles, tenant scope), dual signing by the worker during the
overlap window, immediate cutover, ending the overlap early, expiry by the retention job and
concurrent rotations.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from typing import Any

import httpx
import pytest

from hookrelay.core.signing import WebhookVerificationError, verify
from hookrelay.db.engine import Database
from hookrelay.relay import Relay
from hookrelay.services.retention import RetentionPolicy, RetentionRunner
from tests.integration.conftest import bearer, create_tenant, issue_key, publish
from tests.integration.test_worker import (  # noqa: F401 - pytest fixtures
    _fetch,
    _setup,
    _val,
    api,
    database,
    db_url,
    make_worker,
    receiver,
    settings_for,
    worker_template,
)
from tests.receiver import Receiver


async def _etag(api: httpx.AsyncClient, headers: dict[str, str], endpoint_id: str) -> str:
    response = await api.get(f"/v1/endpoints/{endpoint_id}", headers=headers)
    assert response.status_code == 200, response.text
    return str(response.headers["etag"])


async def _rotate(
    api: httpx.AsyncClient,
    headers: dict[str, str],
    endpoint_id: str,
    *,
    if_match: str | None,
    **body: Any,
) -> httpx.Response:
    h = dict(headers)
    if if_match is not None:
        h["If-Match"] = if_match
    return await api.post(f"/v1/endpoints/{endpoint_id}/secret/rotate", json=body, headers=h)


def _signatures(received: Any) -> list[str]:
    return str(received.headers["webhook-signature"]).split(" ")


async def test_rotation_signs_with_both_secrets_during_the_overlap(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    tenant, endpoint_id, old_secret, _ = await _setup(api, settings_for(), receiver)
    etag = await _etag(api, tenant.headers, endpoint_id)
    rotated = await _rotate(api, tenant.headers, endpoint_id, if_match=etag, overlap_s=3600)
    assert rotated.status_code == 200, rotated.text
    assert rotated.headers["cache-control"] == "no-store"
    body = rotated.json()
    new_secret = body["signing_secret"]
    assert new_secret.startswith("whsec_")
    assert new_secret != old_secret
    assert body["previous_secret_expires_at"] is not None
    assert rotated.headers["etag"] == f'"{body["version"]}"'
    assert body["version"] == int(etag.strip('"')) + 1

    # At rest: both ciphertexts, neither in clear.
    [row] = await _fetch(
        db_url,
        "SELECT secret_ciphertext, previous_secret_ciphertext, previous_secret_expires_at "
        "- secret_rotated_at AS overlap FROM endpoints",
    )
    assert row["previous_secret_ciphertext"].startswith("v1.")
    assert new_secret not in row["secret_ciphertext"]
    assert row["overlap"].total_seconds() == pytest.approx(3600, abs=1)

    # The endpoint read model shows the rotation; GET /secret returns the new secret.
    got = await api.get(f"/v1/endpoints/{endpoint_id}", headers=tenant.headers)
    assert got.json()["secret_rotated_at"] is not None
    assert got.json()["previous_secret_expires_at"] == body["previous_secret_expires_at"]
    revealed = await api.get(f"/v1/endpoints/{endpoint_id}/secret", headers=tenant.headers)
    assert revealed.json()["signing_secret"] == new_secret

    await make_worker().run_once()
    [received] = receiver.requests
    new_sig, old_sig = _signatures(received)
    assert new_sig.startswith("v1,")
    assert old_sig.startswith("v1,")
    # A receiver holding either secret verifies the same request.
    assert verify(new_secret, received.headers, received.body)
    assert verify(old_secret, received.headers, received.body)
    # The attempt log still redacts the signature.
    attempts = await _fetch(db_url, "SELECT request_headers FROM delivery_attempts")
    stored = json.loads(attempts[0]["request_headers"])
    assert stored["webhook-signature"] == "v1,[redacted]"


async def test_zero_overlap_is_an_immediate_cutover(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    tenant, endpoint_id, old_secret, _ = await _setup(api, settings_for(), receiver)
    rotated = await _rotate(api, tenant.headers, endpoint_id, if_match="*", overlap_s=0)
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["previous_secret_expires_at"] is None
    assert await _val(db_url, "SELECT previous_secret_ciphertext FROM endpoints") is None
    await make_worker().run_once()
    [received] = receiver.requests
    assert len(_signatures(received)) == 1
    assert verify(rotated.json()["signing_secret"], received.headers, received.body)
    with pytest.raises(WebhookVerificationError):
        verify(old_secret, received.headers, received.body)


async def test_expired_overlap_stops_dual_signing_and_retention_clears_it(
    api: httpx.AsyncClient,
    *,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    database: Database,
    db_url: str,
) -> None:
    tenant, endpoint_id, old_secret, _ = await _setup(api, settings_for(), receiver)
    rotated = await _rotate(api, tenant.headers, endpoint_id, if_match="*", overlap_s=600)
    new_secret = rotated.json()["signing_secret"]
    # Fast-forward: the window ended but the retention job has not run yet.
    await _fetch(
        db_url,
        "UPDATE endpoints SET previous_secret_expires_at = now() - interval '1 second' "
        "RETURNING id",
    )
    await make_worker().run_once()
    [received] = receiver.requests
    assert len(_signatures(received)) == 1
    assert verify(new_secret, received.headers, received.body)
    with pytest.raises(WebhookVerificationError):
        verify(old_secret, received.headers, received.body)
    # Not shown as active any more either.
    got = await api.get(f"/v1/endpoints/{endpoint_id}", headers=tenant.headers)
    assert got.json()["previous_secret_expires_at"] is not None  # until the cleanup runs

    version = await _val(db_url, "SELECT version FROM endpoints")
    report = await RetentionRunner(database, RetentionPolicy(batch_size=10, max_batches=5)).run()
    assert report.previous_secrets == 1
    assert report.complete
    [row] = await _fetch(
        db_url,
        "SELECT previous_secret_ciphertext, previous_secret_expires_at, version FROM endpoints",
    )
    assert row["previous_secret_ciphertext"] is None
    assert row["previous_secret_expires_at"] is None
    assert row["version"] == version  # a cleanup, not a change a client must re-read
    again = await RetentionRunner(database, RetentionPolicy(batch_size=10, max_batches=5)).run()
    assert again.previous_secrets == 0


async def test_rotating_again_replaces_the_previous_secret(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any
) -> None:
    tenant, endpoint_id, first, _ = await _setup(api, settings_for(), receiver)
    second = (await _rotate(api, tenant.headers, endpoint_id, if_match="*")).json()
    third = (await _rotate(api, tenant.headers, endpoint_id, if_match="*")).json()
    await make_worker().run_once()
    [received] = receiver.requests
    assert len(_signatures(received)) == 2  # never more than two
    assert verify(third["signing_secret"], received.headers, received.body)
    assert verify(second["signing_secret"], received.headers, received.body)
    with pytest.raises(WebhookVerificationError):
        verify(first, received.headers, received.body)


async def test_end_overlap_early_is_idempotent(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    rotated = await _rotate(api, tenant.headers, endpoint_id, if_match="*", overlap_s=3600)
    version = rotated.json()["version"]

    stale = await api.delete(
        f"/v1/endpoints/{endpoint_id}/secret/previous",
        headers={**tenant.headers, "If-Match": f'"{version - 1}"'},
    )
    assert stale.status_code == 412
    assert stale.json()["error"]["details"]["current_version"] == version

    ended = await api.delete(
        f"/v1/endpoints/{endpoint_id}/secret/previous",
        headers={**tenant.headers, "If-Match": f'"{version}"'},
    )
    assert ended.status_code == 200, ended.text
    assert ended.json()["previous_secret_expires_at"] is None
    assert ended.json()["version"] == version + 1
    assert ended.headers["etag"] == f'"{version + 1}"'

    # Again: nothing to end, no version bump; a stale If-Match is still 412.
    again = await api.delete(f"/v1/endpoints/{endpoint_id}/secret/previous", headers=tenant.headers)
    assert again.status_code == 200
    assert again.json()["version"] == version + 1
    stale_again = await api.delete(
        f"/v1/endpoints/{endpoint_id}/secret/previous",
        headers={**tenant.headers, "If-Match": f'"{version}"'},
    )
    assert stale_again.status_code == 412

    await make_worker().run_once()
    [received] = receiver.requests
    assert len(_signatures(received)) == 1


async def test_rotation_validation_and_preconditions(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    etag = await _etag(api, tenant.headers, endpoint_id)

    missing = await _rotate(api, tenant.headers, endpoint_id, if_match=None)
    assert missing.status_code == 428
    stale = await _rotate(api, tenant.headers, endpoint_id, if_match='"999"')
    assert stale.status_code == 412
    assert stale.headers["etag"] == etag
    weak = await _rotate(api, tenant.headers, endpoint_id, if_match='W/"1"')
    assert weak.status_code == 412

    too_long = await _rotate(
        api, tenant.headers, endpoint_id, if_match=etag, overlap_s=7 * 86400 + 1
    )
    assert too_long.status_code == 422
    assert too_long.json()["error"]["code"] == "overlap_too_long"
    negative = await _rotate(api, tenant.headers, endpoint_id, if_match=etag, overlap_s=-1)
    assert negative.status_code == 422
    unknown = await _rotate(api, tenant.headers, endpoint_id, if_match=etag, nope=1)
    assert unknown.status_code == 422

    for bad in (
        "whsec_" + base64.b64encode(os.urandom(16)).decode(),  # too short
        "whsec_" + base64.b64encode(os.urandom(65)).decode(),  # too long
        "whsec_not*base64!!",
        base64.b64encode(os.urandom(32)).decode(),  # no prefix
    ):
        response = await _rotate(api, tenant.headers, endpoint_id, if_match=etag, secret=bad)
        assert response.status_code == 422, bad
        assert response.json()["error"]["code"] == "invalid_signing_secret"
    # Nothing changed.
    assert await _etag(api, tenant.headers, endpoint_id) == etag
    assert await _val(db_url, "SELECT secret_rotated_at FROM endpoints") is None

    # Bring-your-own secret is stored encrypted and returned once.
    own = "whsec_" + base64.b64encode(os.urandom(32)).decode()
    imported = await _rotate(api, tenant.headers, endpoint_id, if_match=etag, secret=own)
    assert imported.status_code == 200, imported.text
    assert imported.json()["signing_secret"] == own
    assert own not in await _val(db_url, "SELECT secret_ciphertext FROM endpoints")

    # An empty body works too (all defaults: the deployment's overlap).
    empty = await api.post(
        f"/v1/endpoints/{endpoint_id}/secret/rotate",
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert empty.status_code == 200, empty.text
    assert empty.json()["previous_secret_expires_at"] is not None


async def test_rotation_is_admin_only_and_tenant_scoped(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    for role in ("reader", "publisher"):
        key = bearer(await issue_key(api, tenant, role))
        denied = await _rotate(api, key, endpoint_id, if_match="*")
        assert denied.status_code == 403, role
        ended = await api.delete(f"/v1/endpoints/{endpoint_id}/secret/previous", headers=key)
        assert ended.status_code == 403
    other = await create_tenant(api)
    assert (await _rotate(api, other.headers, endpoint_id, if_match="*")).status_code == 404
    gone = await api.delete(f"/v1/endpoints/{endpoint_id}/secret/previous", headers=other.headers)
    assert gone.status_code == 404
    unauth = await api.post(f"/v1/endpoints/{endpoint_id}/secret/rotate")
    assert unauth.status_code == 401

    # A deleted endpoint cannot be rotated.
    assert (
        await api.delete(
            f"/v1/endpoints/{endpoint_id}", headers={**tenant.headers, "If-Match": "*"}
        )
    ).status_code == 204
    assert (await _rotate(api, tenant.headers, endpoint_id, if_match="*")).status_code == 404


async def test_concurrent_rotations_with_the_same_etag_have_one_winner(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    etag = await _etag(api, tenant.headers, endpoint_id)
    responses = await asyncio.gather(
        *(_rotate(api, tenant.headers, endpoint_id, if_match=etag) for _ in range(6))
    )
    codes = sorted(r.status_code for r in responses)
    assert codes == [200, 412, 412, 412, 412, 412]
    [winner] = [r for r in responses if r.status_code == 200]
    revealed = await api.get(f"/v1/endpoints/{endpoint_id}/secret", headers=tenant.headers)
    assert revealed.json()["signing_secret"] == winner.json()["signing_secret"]
    assert await _val(db_url, "SELECT version FROM endpoints") == int(etag.strip('"')) + 1


async def test_undecryptable_previous_secret_falls_back_to_the_current_one(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    rotated = await _rotate(api, tenant.headers, endpoint_id, if_match="*", overlap_s=3600)
    # Simulate a previous secret encrypted under a retired key / wrong AAD: copy another
    # endpoint's ciphertext (valid format, wrong associated data).
    other = await api.post(
        "/v1/endpoints",
        json={"url": f"http://receiver.test:{receiver.port}/other"},
        headers=tenant.headers,
    )
    await _fetch(
        db_url,
        "UPDATE endpoints SET previous_secret_ciphertext = "
        "(SELECT secret_ciphertext FROM endpoints WHERE id = $1) WHERE id = $2 RETURNING id",
        uuid.UUID(other.json()["id"]),
        uuid.UUID(endpoint_id),
    )
    stats = await make_worker().run_once()
    assert stats.succeeded == 1
    [received] = receiver.requests
    assert len(_signatures(received)) == 1
    assert verify(rotated.json()["signing_secret"], received.headers, received.body)


async def test_publishing_during_rotation_keeps_delivering(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    """Rotations interleaved with deliveries: every request verifies with the newest secret."""
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=2)
    worker = make_worker()
    secrets = []
    for _ in range(3):
        secrets.append(
            (await _rotate(api, tenant.headers, endpoint_id, if_match="*")).json()["signing_secret"]
        )
        assert (await publish(api, tenant, "invoice.paid", {"x": 1})).status_code == 202
    db = Database.from_settings(settings_for())
    try:
        relay = Relay(db, settings_for())
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()
    previous = -1
    while worker.stats.leased != previous:  # stats are cumulative: stop once nothing new
        previous = worker.stats.leased
        await worker.run_once()
    assert len(receiver.requests) == 5
    for received in receiver.requests:
        assert verify(secrets[-1], received.headers, received.body)
        assert verify(secrets[-2], received.headers, received.body)
