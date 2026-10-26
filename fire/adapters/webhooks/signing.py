"""Standard Webhooks signatures, as incident.io signs its webhooks through Svix.

The signature is an HMAC-SHA256, keyed with the base64-decoded secret after `whsec_`, of
`{webhook-id}.{webhook-timestamp}.{body}`, base64-encoded. `webhook-signature` holds one or more
space-separated `v1,<signature>`; any one that matches verifies. Contract:
docs/specs/ingest-drift-and-feedback.md#incidentio-webhook.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum

SECRET_PREFIX = "whsec_"


class SignatureError(StrEnum):
    """Why a webhook does not verify."""

    MISSING_HEADER = "missing_header"
    BAD_TIMESTAMP = "bad_timestamp"
    STALE_TIMESTAMP = "stale_timestamp"
    NO_MATCH = "no_match"


class SignatureRejected(Exception):
    def __init__(self, reason: SignatureError) -> None:
        super().__init__(reason.value)
        self.reason = reason


def decode_secret(secret: str) -> bytes:
    """The HMAC key of a signing secret, with or without its `whsec_` prefix.

    Raises `ValueError` when the rest is not base64 or is empty.
    """
    encoded = secret.removeprefix(SECRET_PREFIX)
    try:
        key = base64.b64decode(encoded, validate=True)
    except binascii.Error:
        raise ValueError("signing secret is not base64") from None
    if not key:
        raise ValueError("signing secret is empty")
    return key


def sign(key: bytes, webhook_id: str, timestamp: str, body: bytes) -> str:
    """The base64 signature of one webhook, without its `v1,` prefix."""
    content = f"{webhook_id}.{timestamp}.".encode() + body
    return base64.b64encode(hmac.new(key, content, hashlib.sha256).digest()).decode()


def verify(
    key: bytes,
    headers: Mapping[str, str],
    body: bytes,
    now: datetime,
    tolerance_s: int,
) -> None:
    """Raises `SignatureRejected` unless `headers` sign `body` with `key` within `tolerance_s`
    of `now`, either way. `headers` looks names up case-insensitively, as Starlette's do.
    """
    webhook_id = headers.get("webhook-id")
    timestamp = headers.get("webhook-timestamp")
    signatures = headers.get("webhook-signature")
    if not webhook_id or not timestamp or not signatures:
        raise SignatureRejected(SignatureError.MISSING_HEADER)
    if not timestamp.isascii() or not timestamp.isdigit():
        raise SignatureRejected(SignatureError.BAD_TIMESTAMP)
    if abs(now.timestamp() - int(timestamp)) > tolerance_s:
        raise SignatureRejected(SignatureError.STALE_TIMESTAMP)
    expected = sign(key, webhook_id, timestamp, body).encode()
    # Every candidate is compared, so the time taken does not tell which one matched.
    matched = False
    for candidate in signatures.split(" "):
        version, _, signature = candidate.partition(",")
        if version == "v1" and hmac.compare_digest(signature.encode(), expected):
            matched = True
    if not matched:
        raise SignatureRejected(SignatureError.NO_MATCH)
