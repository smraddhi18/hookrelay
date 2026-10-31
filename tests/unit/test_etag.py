from __future__ import annotations

import pytest
from fastapi import Response

from hookrelay.api.etag import etag_for, parse_if_match, set_etag
from hookrelay.core.errors import PreconditionFailedError, PreconditionRequiredError


def test_etag_format() -> None:
    assert etag_for(7) == '"7"'
    response = Response()
    set_etag(response, 3)
    assert response.headers["ETag"] == '"3"'


@pytest.mark.parametrize(("value", "expected"), [('"1"', 1), (' "42" ', 42), ("*", None)])
def test_parse_valid(value: str, expected: int | None) -> None:
    assert parse_if_match(value, required=True) == expected


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_is_428_when_required(value: str | None) -> None:
    with pytest.raises(PreconditionRequiredError):
        parse_if_match(value, required=True)
    assert parse_if_match(value, required=False) is None


@pytest.mark.parametrize(
    "value", ['W/"1"', "1", '"0"', '"01"', '"abc"', '"1", "2"', '"99999999999"', '"3000000000"']
)
def test_weak_or_malformed_tags_never_match(value: str) -> None:
    with pytest.raises(PreconditionFailedError):
        parse_if_match(value, required=True)
