from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime

import pytest

from hookrelay.core.errors import BadRequestError
from hookrelay.services.pagination import Page, decode_cursor, encode_cursor


def test_cursor_round_trip_is_opaque_and_url_safe() -> None:
    created = datetime(2026, 10, 31, 9, 30, 0, 123456, tzinfo=UTC)
    row_id = uuid.uuid4()
    token = encode_cursor(created, row_id)
    assert "=" not in token
    assert "+" not in token
    assert "/" not in token
    cursor = decode_cursor(token)
    assert cursor.created_at == created
    assert cursor.id == row_id


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


@pytest.mark.parametrize(
    "token",
    [
        "%%%",
        "a",
        _b64(b"not json"),
        _b64(b"[]"),
        _b64(b'{"c": "2026-10-31T00:00:00+00:00"}'),
        _b64(b'{"c": "yesterday", "i": "00000000-0000-0000-0000-000000000000"}'),
        _b64(b'{"c": "2026-10-31T00:00:00+00:00", "i": "nope"}'),
        _b64(b'{"c": "2026-10-31T00:00:00", "i": "00000000-0000-0000-0000-000000000000"}'),
        _b64(b"\xff\xfe"),
    ],
)
def test_malformed_cursors_are_bad_requests(token: str) -> None:
    with pytest.raises(BadRequestError) as info:
        decode_cursor(token)
    assert info.value.code == "invalid_cursor"


def test_page_has_more() -> None:
    assert Page(items=[1], next_cursor="x").has_more
    assert not Page(items=[1], next_cursor=None).has_more
