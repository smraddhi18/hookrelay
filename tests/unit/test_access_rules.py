from __future__ import annotations

import re

import pytest

from hookrelay.db.models.api_key import ApiKeyRole
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN
from hookrelay.db.models.subscription import SUBSCRIPTION_PATTERN
from hookrelay.services.subscriptions import is_wildcard, pattern_matches


@pytest.mark.parametrize(
    ("held", "required", "ok"),
    [
        (ApiKeyRole.ADMIN, ApiKeyRole.ADMIN, True),
        (ApiKeyRole.ADMIN, ApiKeyRole.PUBLISHER, True),
        (ApiKeyRole.ADMIN, ApiKeyRole.READER, True),
        (ApiKeyRole.PUBLISHER, ApiKeyRole.ADMIN, False),
        (ApiKeyRole.PUBLISHER, ApiKeyRole.PUBLISHER, True),
        (ApiKeyRole.PUBLISHER, ApiKeyRole.READER, True),
        (ApiKeyRole.READER, ApiKeyRole.ADMIN, False),
        (ApiKeyRole.READER, ApiKeyRole.PUBLISHER, False),
        (ApiKeyRole.READER, ApiKeyRole.READER, True),
    ],
)
def test_role_hierarchy(held: ApiKeyRole, required: ApiKeyRole, ok: bool) -> None:
    assert held.satisfies(required) is ok


@pytest.mark.parametrize(
    ("pattern", "event_type", "expected"),
    [
        ("*", "invoice.paid", True),
        ("invoice.paid", "invoice.paid", True),
        ("invoice.paid", "invoice.paid.partial", False),
        ("invoice.*", "invoice.paid", True),
        ("invoice.*", "invoice.paid.partial", True),
        ("invoice.*", "invoice", False),
        ("invoice.*", "invoices.paid", False),
        ("user.*", "invoice.paid", False),
    ],
)
def test_pattern_matching(pattern: str, event_type: str, expected: bool) -> None:
    assert pattern_matches(pattern, event_type) is expected


def test_is_wildcard() -> None:
    assert is_wildcard("*")
    assert is_wildcard("a.*")
    assert not is_wildcard("a.b")


@pytest.mark.parametrize(
    ("name", "ok"),
    [
        ("invoice.paid", True),
        ("user.email_changed", True),
        ("a", True),
        ("a.b.c.d.e.f.g.h", True),
        ("a.b.c.d.e.f.g.h.i", False),
        ("Invoice.paid", False),
        ("invoice..paid", False),
        (".invoice", False),
        ("invoice.", False),
        ("invoice.*", False),
        ("_x", False),
    ],
)
def test_event_type_name_format(name: str, ok: bool) -> None:
    assert (re.fullmatch(EVENT_TYPE_PATTERN, name) is not None) is ok


@pytest.mark.parametrize(
    ("pattern", "ok"),
    [
        ("*", True),
        ("invoice.*", True),
        ("invoice.paid", True),
        ("invoice.paid.*", True),
        ("invoice*", False),
        ("*.paid", False),
        ("invoice.*.paid", False),
        ("**", False),
        ("", False),
    ],
)
def test_subscription_pattern_format(pattern: str, ok: bool) -> None:
    assert (re.fullmatch(SUBSCRIPTION_PATTERN, pattern) is not None) is ok
