"""ExchangePipeline: per-part checks in order, content codings, sampling, and the observations."""

from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from canary import Canary

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.dossier import Shape
from fire.core.domain.observation import ShapeObservation
from fire.core.ports.exchange_source import HttpExchange, HttpMessage
from fire.core.ports.shape_generalizer import ShapeGeneralizer
from fire.core.services.exchange_pipeline import (
    ExchangeOutcome,
    ExchangePipeline,
    PartSkipped,
    PartSkipReason,
)
from fire.core.services.record_pipeline import sampled_in

NOW = datetime(2026, 10, 11, 16, 30, 5, tzinfo=UTC)
REQUEST_AT = datetime(2026, 10, 11, 16, 30, 1, tzinfo=UTC)
RESPONSE_AT = datetime(2026, 10, 11, 16, 30, 2, tzinfo=UTC)
EXCHANGE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
TEMPLATE = "/v1/orders/checkout"
MAX_BODY_BYTES = 4096
ORDER = {"order_id": "o-1", "items": [{"sku": "s-1", "quantity": 1}]}
ACCEPTED = {"status": "accepted"}
ORDER_SHAPE = FlatStarShapeGeneralizer().generalize(ORDER)

NO_BODY = PartSkipped(PartSkipReason.NO_BODY)
NOT_JSON = PartSkipped(PartSkipReason.BODY_NOT_JSON)
TOO_LARGE = PartSkipped(PartSkipReason.BODY_TOO_LARGE)
UNSUPPORTED = PartSkipped(PartSkipReason.BODY_ENCODING_UNSUPPORTED)
SHAPING_FAILED = PartSkipped(PartSkipReason.SHAPING_FAILED)


def _json(value: Any = ORDER) -> bytes:
    return json.dumps(value).encode()


def _identity(body: bytes) -> bytes:
    return body


def _raw_deflate(body: bytes) -> bytes:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    return compressor.compress(body) + compressor.flush()


def _padded(size: int) -> bytes:
    """A JSON array of exactly `size` bytes."""
    return b"[" + b" " * (size - 2) + b"]"


def _message(
    body: bytes | None = None,
    *,
    content_type: str | None = "application/json",
    content_encoding: str | None = None,
    complete: bool = True,
    received_at: datetime = REQUEST_AT,
) -> HttpMessage:
    return HttpMessage(
        received_at=received_at,
        content_type=content_type,
        content_encoding=content_encoding,
        body=_json() if body is None else body,
        complete=complete,
    )


def _exchange(
    request: HttpMessage | None = None, response: HttpMessage | None = None, **overrides: Any
) -> HttpExchange:
    fields: dict[str, Any] = {
        "exchange_id": EXCHANGE_ID,
        "method": "POST",
        "template": TEMPLATE,
        "status_code": 200,
        "trace_id": TRACE_ID,
        "span_id": SPAN_ID,
    }
    fields.update(overrides)
    if request is None:
        request = _message()
    if response is None:
        response = _message(_json(ACCEPTED), received_at=RESPONSE_AT)
    return HttpExchange(request=request, response=response, **fields)


def _pipeline(
    *,
    sample_rate: float = 1.0,
    max_body_bytes: int = MAX_BODY_BYTES,
    generalizer: ShapeGeneralizer | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
) -> ExchangePipeline:
    return ExchangePipeline(
        system="shop",
        source="shop-api",
        sample_rate=sample_rate,
        max_body_bytes=max_body_bytes,
        generalizer=generalizer or FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        clock=clock,
    )


def _process(
    exchange: HttpExchange, pipeline: ExchangePipeline | None = None
) -> list[ExchangeOutcome]:
    return (pipeline or _pipeline()).process(exchange)


def _observations(
    exchange: HttpExchange, pipeline: ExchangePipeline | None = None
) -> list[ShapeObservation]:
    outcomes = _process(exchange, pipeline)
    observations = [outcome for outcome in outcomes if isinstance(outcome, ShapeObservation)]
    assert len(observations) == len(outcomes) == 2, outcomes
    return observations


def _request(message: HttpMessage, pipeline: ExchangePipeline | None = None) -> ExchangeOutcome:
    """The outcome of the request part when `message` is the request."""
    return _process(_exchange(request=message), pipeline)[0]


def _shaped(message: HttpMessage) -> ShapeObservation:
    outcome = _request(message)
    assert isinstance(outcome, ShapeObservation), outcome
    return outcome


class _RaisingGeneralizer:
    """Raises with the payload in its message, as a careless adapter might."""

    def __init__(self, error: type[Exception]) -> None:
        self._error = error

    def generalize(self, payload: Any) -> Shape:
        raise self._error(json.dumps(payload))


@pytest.mark.unit
def test_builds_request_and_response_observations() -> None:
    request, response = _observations(_exchange())

    assert request.channel.key == "http:inbound:POST /v1/orders/checkout:request"
    assert response.channel.key == "http:inbound:POST /v1/orders/checkout:response:2xx"
    assert request.observation_id == f"{EXCHANGE_ID}:request"
    assert response.observation_id == f"{EXCHANGE_ID}:response"
    assert (request.occurred_at, response.occurred_at) == (REQUEST_AT, RESPONSE_AT)
    for observation in (request, response):
        assert (observation.system, observation.source) == ("shop", "shop-api")
        assert observation.observed_at == NOW
        assert (observation.trace_id, observation.span_id) == (TRACE_ID, SPAN_ID)
        assert observation.region is None
        assert observation.work_id is None and observation.correlation_id is None
    assert request.shape == ORDER_SHAPE
    assert request.fingerprint == Sha256ShapeFingerprinter().fingerprint(ORDER_SHAPE)
    assert response.shape == FlatStarShapeGeneralizer().generalize(ACCEPTED)


@pytest.mark.unit
def test_trace_ids_are_none_when_the_exchange_has_none() -> None:
    for observation in _observations(_exchange(trace_id=None, span_id=None)):
        assert observation.trace_id is None and observation.span_id is None


@pytest.mark.unit
def test_observed_at_is_the_clock_time_when_each_part_is_built() -> None:
    times = iter([NOW, NOW + timedelta(seconds=1)])
    request, response = _observations(_exchange(), _pipeline(clock=lambda: next(times)))
    assert (request.observed_at, response.observed_at) == (NOW, NOW + timedelta(seconds=1))


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [ORDER, [ORDER, {"order_id": "o-2"}], [], {}],
    ids=["object", "array", "empty-array", "empty-object"],
)
def test_json_objects_and_arrays_are_shaped(value: Any) -> None:
    assert _shaped(_message(_json(value))).shape == FlatStarShapeGeneralizer().generalize(value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "Application/JSON",
        "application/problem+json; charset=utf-8",
        " application/json ;charset=utf-8",
        "application/vnd.api+json",
    ],
    ids=["plain", "case", "problem", "padded", "vendor"],
)
def test_json_media_types_are_shaped(content_type: str) -> None:
    assert _shaped(_message(content_type=content_type)).shape == ORDER_SHAPE


@pytest.mark.unit
@pytest.mark.parametrize(
    "content_type",
    [
        None,
        "",
        "text/plain",
        "text/json",
        "application/jsonp",
        "application/json+xml",
        "application/x-www-form-urlencoded",
        "text/plain; type=application/json",
    ],
    ids=["missing", "empty", "text", "text-json", "jsonp", "json-plus", "form", "in-param"],
)
def test_other_media_types_are_not_json(content_type: str | None) -> None:
    assert _request(_message(content_type=content_type)) == NOT_JSON


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content_encoding", "encode"),
    [
        (None, _identity),
        ("", _identity),
        ("identity", _identity),
        ("Identity, ", _identity),
        ("gzip", gzip.compress),
        ("x-gzip", gzip.compress),
        (" GZIP ", gzip.compress),
        ("identity, gzip", gzip.compress),
        ("gzip, identity", gzip.compress),
        (",gzip,,", gzip.compress),
        ("deflate", zlib.compress),
        ("deflate", _raw_deflate),
    ],
    ids=[
        "absent",
        "empty",
        "identity",
        "identity-padded",
        "gzip",
        "x-gzip",
        "gzip-case",
        "identity-then-gzip",
        "gzip-then-identity",
        "empty-tokens",
        "deflate-zlib",
        "deflate-raw",
    ],
)
def test_supported_content_codings_are_decoded(
    content_encoding: str | None, encode: Callable[[bytes], bytes]
) -> None:
    message = _message(encode(_json()), content_encoding=content_encoding)
    assert _shaped(message).shape == ORDER_SHAPE


@pytest.mark.unit
@pytest.mark.parametrize(
    "content_encoding",
    ["br", "zstd", "compress", "gzip, br", "gzip, gzip", "deflate, gzip", "gzip;q=1"],
)
def test_other_content_codings_are_unsupported(content_encoding: str) -> None:
    message = _message(gzip.compress(_json()), content_encoding=content_encoding)
    assert _request(message) == UNSUPPORTED


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content_encoding", "body"),
    [
        ("gzip", b"\x1f\x8b not a gzip stream"),
        ("gzip", gzip.compress(_json())[:-8]),
        ("gzip", gzip.compress(_json()) + b"tail"),
        ("gzip", _json()),
        ("deflate", zlib.compress(_json())[:-4]),
        ("deflate", b"neither zlib nor raw deflate"),
    ],
    ids=["corrupt", "truncated", "trailing-bytes", "not-compressed", "zlib-truncated", "deflate"],
)
def test_corrupt_or_truncated_streams_are_not_json(content_encoding: str, body: bytes) -> None:
    assert _request(_message(body, content_encoding=content_encoding)) == NOT_JSON


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content_encoding", "encode"),
    [("gzip", gzip.compress), ("deflate", zlib.compress), ("deflate", _raw_deflate)],
    ids=["gzip", "deflate-zlib", "deflate-raw"],
)
def test_a_small_stream_that_inflates_past_max_body_bytes_is_too_large(
    content_encoding: str, encode: Callable[[bytes], bytes]
) -> None:
    bomb = encode(_padded(2_000_000))
    assert len(bomb) < MAX_BODY_BYTES
    assert _request(_message(bomb, content_encoding=content_encoding)) == TOO_LARGE


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content_encoding", "encode"),
    [(None, _identity), ("gzip", gzip.compress)],
    ids=["identity", "gzip"],
)
def test_max_body_bytes_bounds_the_decoded_body(
    content_encoding: str | None, encode: Callable[[bytes], bytes]
) -> None:
    fits = _message(encode(_padded(MAX_BODY_BYTES)), content_encoding=content_encoding)
    assert isinstance(_request(fits), ShapeObservation)
    over = _message(encode(_padded(MAX_BODY_BYTES + 1)), content_encoding=content_encoding)
    assert _request(over) == TOO_LARGE


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "content_encoding"),
    [
        (_json()[:10], None),
        (_json(), None),
        (b"", None),
        (gzip.compress(_json())[:20], "gzip"),
    ],
    ids=["prefix", "whole-value", "empty", "gzip-prefix"],
)
def test_an_incomplete_body_is_too_large(body: bytes, content_encoding: str | None) -> None:
    message = _message(body, content_encoding=content_encoding, complete=False)
    assert _request(message) == TOO_LARGE


@pytest.mark.unit
def test_an_empty_body_is_no_body_on_either_part() -> None:
    exchange = _exchange(
        request=_message(b"", content_type=None),
        response=_message(b"", content_type=None, received_at=RESPONSE_AT),
        method="GET",
        status_code=204,
    )
    assert _process(exchange) == [NO_BODY, NO_BODY]
    assert _request(_message(b"", content_encoding="gzip")) == NO_BODY


@pytest.mark.unit
@pytest.mark.parametrize("body", [b'"text"', b"0", b"1.5", b"true", b"null"], ids=str)
def test_a_json_scalar_is_not_json(body: bytes) -> None:
    assert _request(_message(body)) == NOT_JSON


@pytest.mark.unit
@pytest.mark.parametrize(
    "body",
    [b"{", b"not json", b" ", b'{"a": "\xff"}', b"\xff\xfe{"],
    ids=["unclosed", "text", "blank", "invalid-utf-8", "odd-utf-16"],
)
def test_a_body_that_does_not_parse_is_not_json(body: bytes) -> None:
    assert _request(_message(body)) == NOT_JSON


@pytest.mark.unit
@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-32"])
def test_json_in_any_unicode_encoding_is_shaped(encoding: str) -> None:
    assert _shaped(_message(json.dumps(ORDER).encode(encoding))).shape == ORDER_SHAPE


@pytest.mark.unit
@pytest.mark.parametrize(
    "body",
    [b"[" * 100_000, b"[" * 100_000 + b"]" * 100_000, b"[" * 5_000 + b"]" * 5_000],
    ids=["unclosed", "closed", "parses-but-too-deep"],
)
def test_deep_nesting_is_skipped_without_raising(body: bytes) -> None:
    outcome = _request(_message(body), _pipeline(max_body_bytes=len(body)))
    assert outcome in (NOT_JSON, SHAPING_FAILED)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status_code", "status_class"),
    [
        (200, "2xx"),
        (204, "2xx"),
        (299, "2xx"),
        (300, "3xx"),
        (301, "3xx"),
        (399, "3xx"),
        (400, "4xx"),
        (404, "4xx"),
        (499, "4xx"),
        (500, "5xx"),
        (503, "5xx"),
        (599, "5xx"),
    ],
)
def test_the_response_channel_carries_the_status_class(
    status_code: int, status_class: str
) -> None:
    _, response = _observations(_exchange(status_code=status_code))
    assert response.channel.key == f"http:inbound:POST {TEMPLATE}:response:{status_class}"


@pytest.mark.unit
@pytest.mark.parametrize("status_code", [199, 600, 100, 0, -200])
def test_an_unclassified_status_skips_only_the_response(status_code: int) -> None:
    request, response = _process(_exchange(status_code=status_code))
    assert isinstance(request, ShapeObservation)
    assert response == PartSkipped(PartSkipReason.STATUS_UNCLASSIFIED)


@pytest.mark.unit
def test_first_failing_check_decides() -> None:
    empty_response = _message(b"", content_type=None, received_at=RESPONSE_AT)
    unclassified = _process(_exchange(response=empty_response, status_code=600))[1]
    assert unclassified == PartSkipped(PartSkipReason.STATUS_UNCLASSIFIED)

    cases = [
        (_message(b"", content_type="text/plain", content_encoding="br"), NO_BODY),
        (
            _message(b"x", content_type="text/plain", content_encoding="br", complete=False),
            NOT_JSON,
        ),
        (_message(b"x", content_encoding="br", complete=False), UNSUPPORTED),
        (_message(b"x", content_encoding="gzip", complete=False), TOO_LARGE),
        (_message(gzip.compress(b"{" * 10_000), content_encoding="gzip"), TOO_LARGE),
    ]
    for message, outcome in cases:
        assert _request(message) == outcome


@pytest.mark.unit
def test_rate_one_keeps_every_exchange() -> None:
    for index in range(50):
        _observations(_exchange(exchange_id=f"{index:032x}"))


@pytest.mark.unit
def test_sampling_hashes_the_exchange_id_and_drops_both_parts_together() -> None:
    ids = [f"{index:032x}" for index in range(1000)]
    kept = next(
        id_ for id_ in ids if sampled_in(id_, 0.5) and not sampled_in(f"{id_}:request", 0.5)
    )
    dropped = next(
        id_ for id_ in ids if not sampled_in(id_, 0.5) and sampled_in(f"{id_}:request", 0.5)
    )
    pipeline = _pipeline(sample_rate=0.5)

    _observations(_exchange(exchange_id=kept), pipeline)
    sampled_out = [PartSkipped(PartSkipReason.SAMPLED_OUT)]
    assert _process(_exchange(exchange_id=dropped), pipeline) == sampled_out
    unshapeable = _exchange(request=_message(b""), exchange_id=dropped, status_code=600)
    assert _process(unshapeable, pipeline) == sampled_out


@pytest.mark.unit
@pytest.mark.parametrize("error", [ValueError, RuntimeError, KeyError, RecursionError])
def test_a_generalizer_that_raises_gives_shaping_failed(
    error: type[Exception], canary: Canary
) -> None:
    request = _message(_json(canary.plant(ORDER)))
    pipeline = _pipeline(generalizer=_RaisingGeneralizer(error))
    outcomes = _process(_exchange(request=request), pipeline)
    assert outcomes == [SHAPING_FAILED, SHAPING_FAILED]
    assert canary.leaks(repr(outcomes), envelope=False) == []


@pytest.mark.unit
def test_a_channel_name_over_256_characters_fails_shaping() -> None:
    longest = "/" + "a" * (256 - len("POST /"))
    assert len(f"POST {longest}") == 256
    _observations(_exchange(template=longest))
    assert _process(_exchange(template=longest + "a")) == [SHAPING_FAILED, SHAPING_FAILED]


@pytest.mark.unit
def test_an_observation_that_fails_validation_fails_shaping() -> None:
    assert _process(_exchange(trace_id="0" * 32)) == [SHAPING_FAILED, SHAPING_FAILED]
    assert _request(_message(b'{"k\\udc00": 1}')) == SHAPING_FAILED


@pytest.mark.unit
def test_max_body_bytes_cannot_be_negative() -> None:
    with pytest.raises(ValueError):
        _pipeline(max_body_bytes=-1)


@pytest.mark.unit
def test_canary_values_reach_no_observation(canary: Canary) -> None:
    request_body = canary.plant(
        {
            "order": {"id": "o-1", "lines": [{"sku": "s-1", "quantity": 2, "price": 9.5}]},
            "customer": {"email": "a@example.com", "active": True, "note": None},
            "tags": ["a", "b", "c"],
        }
    )
    response_body = canary.plant({"status": "accepted", "order_ids": ["o-1", "o-2"]})
    exchange = _exchange(
        request=_message(
            _json(request_body), content_type=f"application/json; x={canary.token()}"
        ),
        response=_message(
            gzip.compress(_json(response_body)),
            content_type=f"application/problem+json; charset={canary.token()}",
            content_encoding="gzip",
            received_at=RESPONSE_AT,
        ),
        status_code=422,
    )
    assert canary.leaks(_json(request_body).decode(), envelope=False)

    observations = _observations(exchange)

    dumped = json.dumps([observation.model_dump(mode="json") for observation in observations])
    assert canary.leaks(dumped, envelope=False) == []
    assert canary.leaks(repr(observations), envelope=False) == []
    assert canary.leaks(repr(exchange), envelope=False) == []
