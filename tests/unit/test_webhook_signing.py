"""Standard Webhooks signatures: incident.io's scheme, through Svix."""

from __future__ import annotations

import base64
import hmac
from datetime import UTC, datetime, timedelta

import pytest

from fire.adapters.webhooks.signing import (
    SignatureError,
    SignatureRejected,
    decode_secret,
    sign,
    verify,
)

# Svix's example in https://docs.svix.com/receiving/verifying-payloads/how-manual.
SVIX_SECRET = "whsec_plJ3nmyCDGBKInavdOK15jsl"
SVIX_BODY = b'{"event_type":"ping","data":{"success":true}}'
SVIX_ID = "msg_loFOjxBNrRLzqYUf"
SVIX_TIMESTAMP = "1731705121"
SVIX_SIGNATURE = "v1,rAvfW3dJ/X/qxhsaXPOyyCGmRKsaKWcsNccKXlIktD0="
SVIX_NOW = datetime.fromtimestamp(int(SVIX_TIMESTAMP), UTC)
KEY = decode_secret(SVIX_SECRET)


def _headers(signature: str = SVIX_SIGNATURE, timestamp: str = SVIX_TIMESTAMP) -> dict[str, str]:
    return {"webhook-id": SVIX_ID, "webhook-timestamp": timestamp, "webhook-signature": signature}


def _rejected(
    headers: dict[str, str], body: bytes = SVIX_BODY, now: datetime = SVIX_NOW
) -> SignatureError:
    with pytest.raises(SignatureRejected) as caught:
        verify(KEY, headers, body, now, 300)
    return caught.value.reason


@pytest.mark.unit
def test_svix_example_signs_and_verifies() -> None:
    assert f"v1,{sign(KEY, SVIX_ID, SVIX_TIMESTAMP, SVIX_BODY)}" == SVIX_SIGNATURE
    verify(KEY, _headers(), SVIX_BODY, SVIX_NOW, 300)


@pytest.mark.unit
def test_secret_decodes_with_or_without_its_prefix() -> None:
    assert decode_secret("plJ3nmyCDGBKInavdOK15jsl") == KEY
    assert base64.b64decode("plJ3nmyCDGBKInavdOK15jsl") == KEY


@pytest.mark.unit
@pytest.mark.parametrize("secret", ["whsec_", "whsec_not base64!", "", "whsec_abc"])
def test_secret_that_is_not_base64_raises(secret: str) -> None:
    with pytest.raises(ValueError):
        decode_secret(secret)


@pytest.mark.unit
def test_any_v1_signature_of_several_verifies() -> None:
    signatures = f"v1,bm9ldHUjKzFob2VudXRob2VodWUzMjRvdWVvdW9ldQo= {SVIX_SIGNATURE} v2,abc"
    verify(KEY, _headers(signatures), SVIX_BODY, SVIX_NOW, 300)


@pytest.mark.unit
def test_every_candidate_is_compared_in_constant_time(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[bytes, bytes]] = []
    real = hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(hmac, "compare_digest", spy)
    verify(KEY, _headers(f"{SVIX_SIGNATURE} v1,abc"), SVIX_BODY, SVIX_NOW, 300)
    assert len(calls) == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "signature",
    [
        "v1,bm9ldHUjKzFob2VudXRob2VodWUzMjRvdWVvdW9ldQo=",
        SVIX_SIGNATURE.replace("v1,", "v2,"),
        SVIX_SIGNATURE.removeprefix("v1,"),
        "v1,",
    ],
    ids=["other-signature", "other-version", "no-version", "empty"],
)
def test_no_matching_v1_signature_is_rejected(signature: str) -> None:
    assert _rejected(_headers(signature)) is SignatureError.NO_MATCH


@pytest.mark.unit
def test_a_changed_body_or_another_secret_is_rejected() -> None:
    assert _rejected(_headers(), SVIX_BODY + b" ") is SignatureError.NO_MATCH
    with pytest.raises(SignatureRejected):
        verify(b"another key", _headers(), SVIX_BODY, SVIX_NOW, 300)


@pytest.mark.unit
@pytest.mark.parametrize("header", ["webhook-id", "webhook-timestamp", "webhook-signature"])
def test_a_missing_or_empty_header_is_rejected(header: str) -> None:
    headers = _headers()
    del headers[header]
    assert _rejected(headers) is SignatureError.MISSING_HEADER
    assert _rejected({**headers, header: ""}) is SignatureError.MISSING_HEADER


@pytest.mark.unit
@pytest.mark.parametrize("timestamp", ["-1731705121", "1731705121.0", "soon", "١٧٣١٧٠٥١٢١"])
def test_a_timestamp_that_is_not_unix_seconds_is_rejected(timestamp: str) -> None:
    assert _rejected(_headers(timestamp=timestamp)) is SignatureError.BAD_TIMESTAMP


@pytest.mark.unit
@pytest.mark.parametrize("seconds", [-301, 301])
def test_a_timestamp_outside_the_tolerance_is_rejected(seconds: int) -> None:
    now = SVIX_NOW + timedelta(seconds=seconds)
    assert _rejected(_headers(), now=now) is SignatureError.STALE_TIMESTAMP


@pytest.mark.unit
@pytest.mark.parametrize("seconds", [-300, 300])
def test_a_timestamp_at_the_tolerance_verifies(seconds: int) -> None:
    verify(KEY, _headers(), SVIX_BODY, SVIX_NOW + timedelta(seconds=seconds), 300)
