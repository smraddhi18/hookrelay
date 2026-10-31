"""ETag / If-Match helpers for optimistic concurrency.

The ETag is the row's ``version`` as a strong validator (``"3"``). Updates *require*
``If-Match`` (428 otherwise) so two admins editing the same endpoint cannot silently overwrite
each other (the lost-update problem); ``If-Match: *`` is the explicit "I accept last write wins".
"""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import Header, Response

from hookrelay.core.errors import PreconditionFailedError, PreconditionRequiredError

_STRONG_ETAG = re.compile(r'^"([1-9][0-9]{0,9})"$')
MAX_VERSION = 2**31 - 1

IfMatchHeader = Annotated[
    str | None,
    Header(
        alias="If-Match",
        description='ETag from a previous GET (e.g. `"3"`), or `*` to skip the version check.',
        examples=['"3"'],
    ),
]


def etag_for(version: int) -> str:
    return f'"{version}"'


def set_etag(response: Response, version: int) -> None:
    response.headers["ETag"] = etag_for(version)


def parse_if_match(value: str | None, *, required: bool) -> int | None:
    """Return the expected version, or ``None`` for ``*`` / an absent optional header."""
    if value is None or not value.strip():
        if required:
            raise PreconditionRequiredError(
                "This update requires an If-Match header with the resource's ETag"
            )
        return None
    value = value.strip()
    if value == "*":
        return None
    match = _STRONG_ETAG.fullmatch(value)
    if match is None or int(match.group(1)) > MAX_VERSION:
        # RFC 9110: If-Match uses strong comparison; weak or malformed tags never match.
        raise PreconditionFailedError(
            'If-Match must be a single strong ETag such as "3" or *',
            details={"if_match": value[:64]},
        )
    return int(match.group(1))
