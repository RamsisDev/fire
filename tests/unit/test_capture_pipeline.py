"""CapturePipeline: pair checks in order, each layout, the work id, sampling and duplicates, the
span join, part checks in order, and the observations.
"""

from __future__ import annotations

import gc
import json
import re
import weakref
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from canary import Canary

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.capture import CaptureIndex, CaptureLayout, CaptureRule, SpanRef
from fire.core.domain.dossier import Shape
from fire.core.domain.observation import ShapeObservation, StatusClass
from fire.core.ports.capture_source import ObjectState, RawCapture
from fire.core.ports.shape_generalizer import ShapeGeneralizer
from fire.core.services.capture_pipeline import (
    CaptureNote,
    CaptureOutcome,
    CapturePipeline,
    CaptureRejectReason,
    CaptureSkipReason,
)
from fire.core.services.record_pipeline import LOCAL_DEDUPE_WINDOW, sampled_in

SYSTEM = "shop"
SOURCE = "partner-captures"
WORK = "01JABCDEFGHJKMNPQRSTVWXYZ0"
ULID = re.compile(r"[0-7][0-9A-HJKMNP-TV-Z]{25}")
STAMP = "20261011_163001_123456"
AT = datetime(2026, 10, 11, 16, 30, 1, 123456, tzinfo=UTC)
NOW = datetime(2026, 10, 11, 16, 30, 20, tzinfo=UTC)
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
SPAN = SpanRef(TRACE, SPAN_ID)
OTHER_SPAN = SpanRef("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
BILLING = CaptureRule(
    prefix="captures/billing/", label="billing", layout=CaptureLayout.WORK_UNIT_ENTITY
)
SHIPPING = CaptureRule(
    prefix="captures/shipping/", label="shipping", layout=CaptureLayout.WORK_UNIT_ENTITY_ACTION
)
NOTIFICATIONS = CaptureRule(
    prefix="captures/notifications/",
    label="notifications",
    layout=CaptureLayout.WORK_UNIT_ACTION,
)
BILLING_NAME = f"{WORK}/SO-1234567_{STAMP}.json"
SHIPPING_NAME = f"{WORK}/SO-1234567_create_shipment_{STAMP}.json"
PAIR_ID = f"billing:{WORK}:invoice_place:{STAMP}"
CHANNEL = "http:outbound:billing:invoice_place"
RESPONSE_URI = f"s3://capture-bucket/captures/billing/responses/{BILLING_NAME}"
REQUEST_URI = f"s3://capture-bucket/captures/billing/requests/{BILLING_NAME}"
ESTIMATE = {
    "destination": {"postal_code": "x", "country": "US"},
    "lines": [{"sku": "a", "quantity": 1}],
}
ESTIMATED = {"estimates": [{"carrier": "x", "days": 3, "price": 9.5}]}

FOUND = ObjectState.FOUND
SHAPING_FAILED = CaptureSkipReason.SHAPING_FAILED
SAMPLED_OUT = CaptureOutcome(skipped=(CaptureSkipReason.SAMPLED_OUT,))
DUPLICATE = CaptureOutcome(skipped=(CaptureSkipReason.LOCAL_DUPLICATE,))
UNJOINED = (CaptureNote.UNJOINED,)
OBJECT_SKIPS = [
    (ObjectState.MISSING, CaptureSkipReason.OBJECT_MISSING),
    (ObjectState.UNDECODABLE, CaptureSkipReason.OBJECT_UNDECODABLE),
    (ObjectState.TOO_LARGE, CaptureSkipReason.OBJECT_TOO_LARGE),
]


def _request(body: Any = ESTIMATE) -> dict[str, Any]:
    return {
        "method": "POST",
        "url": "https://api.partner.example.com/v1/estimates",
        "headers": {"Content-Type": "application/json"},
        "body": body,
    }


def _response(body: Any = ESTIMATED, status_code: Any = 200) -> dict[str, Any]:
    return {
        "status_code": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": body,
        "error": None,
        "error_type": None,
    }


def _without(document: dict[str, Any], key: str) -> dict[str, Any]:
    return {name: value for name, value in document.items() if name != key}


def _raw(**overrides: Any) -> RawCapture:
    raw = RawCapture(
        source=SOURCE,
        rule=BILLING,
        name=BILLING_NAME,
        response_state=FOUND,
        response=_response(),
        request_state=FOUND,
        request=_request(),
        metadata_action="invoice_place",
    )
    return replace(raw, **overrides)


def _missing(**overrides: Any) -> RawCapture:
    """A pair whose two objects are missing."""
    missing = ObjectState.MISSING
    return _raw(
        request_state=missing, request=None, response_state=missing, response=None, **overrides
    )


def _pipeline(
    *,
    sample_rate: float = 1.0,
    work_id_pattern: re.Pattern[str] | None = None,
    index: CaptureIndex | None = None,
    generalizer: ShapeGeneralizer | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
    dedupe_window: int = LOCAL_DEDUPE_WINDOW,
) -> CapturePipeline:
    return CapturePipeline(
        system=SYSTEM,
        source=SOURCE,
        sample_rate=sample_rate,
        work_id_pattern=work_id_pattern,
        index=index,
        generalizer=generalizer or FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        clock=clock,
        dedupe_window=dedupe_window,
    )


def _process(raw: RawCapture, pipeline: CapturePipeline | None = None) -> CaptureOutcome:
    return (pipeline or _pipeline()).process(raw)


def _keys(outcome: CaptureOutcome) -> list[str]:
    return [shape.channel.key for shape in outcome.shapes]


def _shaped(outcome: CaptureOutcome) -> tuple[ShapeObservation, ShapeObservation]:
    assert outcome.skipped == () and len(outcome.shapes) == 2, outcome
    request, response = outcome.shapes
    return request, response


def _index(
    *uris: str, span: SpanRef = SPAN, ttl_s: float = 900, at: datetime = NOW
) -> CaptureIndex:
    index = CaptureIndex()
    for uri in uris:
        index.put(uri, span, ttl_s, at)
    return index


def _shape(value: Any) -> Shape:
    return FlatStarShapeGeneralizer().generalize(value)


class _CountingGeneralizer:
    def __init__(self) -> None:
        self.calls = 0

    def generalize(self, payload: Any) -> Shape:
        self.calls += 1
        return _shape(payload)


class _RaisingGeneralizer:
    """Raises with the payload in its message, as a careless adapter might."""

    def __init__(self, error: type[Exception]) -> None:
        self._error = error

    def generalize(self, payload: Any) -> Shape:
        raise self._error(json.dumps(payload))


class _SpyDocument(Mapping[str, Any]):
    """A decoded capture document that records each key read; iterating or sizing it fails."""

    def __init__(self, document: Mapping[str, Any]) -> None:
        self._document = dict(document)
        self.read: list[str] = []

    def __getitem__(self, key: str) -> Any:
        self.read.append(key)
        return self._document[key]

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("a capture document was iterated")

    def __len__(self) -> int:
        raise AssertionError("a capture document was sized")


@pytest.mark.unit
def test_a_pair_gives_its_request_and_response_shapes() -> None:
    outcome = _process(_raw())

    assert outcome.rejected is None
    assert outcome.noted == UNJOINED
    request, response = _shaped(outcome)
    assert _keys(outcome) == [f"{CHANNEL}:request", f"{CHANNEL}:response:2xx"]
    assert request.observation_id == f"{PAIR_ID}:request"
    assert response.observation_id == f"{PAIR_ID}:response"
    for shape in outcome.shapes:
        assert (shape.system, shape.source) == (SYSTEM, SOURCE)
        assert (shape.occurred_at, shape.observed_at) == (AT, NOW)
        assert shape.work_id == WORK
        assert shape.trace_id is shape.span_id is shape.region is shape.correlation_id is None
        assert shape.fingerprint == Sha256ShapeFingerprinter().fingerprint(shape.shape)
    assert request.shape == _shape(ESTIMATE)
    assert response.shape == _shape(ESTIMATED)


@pytest.mark.unit
def test_observed_at_is_the_clock_time_when_each_part_is_built() -> None:
    times = iter([NOW, NOW + timedelta(seconds=1)])
    request, response = _shaped(_process(_raw(), _pipeline(clock=lambda: next(times))))
    assert (request.observed_at, response.observed_at) == (NOW, NOW + timedelta(seconds=1))
    assert request.occurred_at == response.occurred_at == AT


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rule", "name", "metadata_action", "channel", "pair_id"),
    [
        (
            NOTIFICATIONS,
            f"{WORK}/email_{STAMP}.json",
            None,
            "notifications:email",
            f"notifications:{WORK}:email:{STAMP}",
        ),
        (
            SHIPPING,
            SHIPPING_NAME,
            "create_shipment",
            "shipping:create_shipment",
            f"shipping:{WORK}:create_shipment:{STAMP}",
        ),
        (BILLING, BILLING_NAME, "invoice_place", "billing:invoice_place", PAIR_ID),
    ],
    ids=["work_unit_action", "work_unit_entity_action", "work_unit_entity"],
)
def test_each_layout_end_to_end(
    rule: CaptureRule, name: str, metadata_action: str | None, channel: str, pair_id: str
) -> None:
    raw = _raw(
        rule=rule,
        name=name,
        metadata_action=metadata_action,
        response=_response(status_code=422),
    )
    outcome = _process(raw, _pipeline(work_id_pattern=ULID))

    assert _keys(outcome) == [
        f"http:outbound:{channel}:request",
        f"http:outbound:{channel}:response:4xx",
    ]
    request, response = _shaped(outcome)
    assert request.observation_id == f"{pair_id}:request"
    assert response.observation_id == f"{pair_id}:response"
    for shape in outcome.shapes:
        assert (shape.work_id, shape.occurred_at) == (WORK, AT)
        assert "SO-1234567" not in shape.model_dump_json()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status_code", "status_class"),
    [
        (200, "2xx"),
        (299, "2xx"),
        (302, "3xx"),
        (404, "4xx"),
        (499, "4xx"),
        (500, "5xx"),
        (599, "5xx"),
    ],
)
def test_the_response_channel_carries_the_status_class(
    status_code: int, status_class: str
) -> None:
    outcome = _process(_raw(response=_response(status_code=status_code)))
    assert _keys(outcome)[1] == f"{CHANNEL}:response:{status_class}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rule", "name"),
    [
        (BILLING, f"SO-1234567_{STAMP}.json"),
        (BILLING, f"{WORK}/deeper/SO-1234567_{STAMP}.json"),
        (BILLING, f"{WORK}/SO-1234567_{STAMP}.xml"),
        (BILLING, f"{WORK}/SO-1234567.json"),
        (BILLING, f"{WORK}/SO-1234567_20261311_163001_123456.json"),
        (SHIPPING, f"{WORK}/createshipment_{STAMP}.json"),
        (NOTIFICATIONS, f"{WORK}/{STAMP}.json"),
    ],
    ids=[
        "no-work-segment",
        "deeper-path",
        "another-suffix",
        "no-timestamp",
        "no-date",
        "entity-action-without-underscore",
        "no-action",
    ],
)
def test_a_key_of_no_layout_rejects_the_pair_and_builds_nothing(
    rule: CaptureRule, name: str
) -> None:
    generalizer = _CountingGeneralizer()
    outcome = _process(_raw(rule=rule, name=name), _pipeline(generalizer=generalizer))
    assert outcome == CaptureOutcome(rejected=CaptureRejectReason.NO_LAYOUT)
    assert generalizer.calls == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rule", "name"),
    [(BILLING, BILLING_NAME), (SHIPPING, SHIPPING_NAME)],
    ids=["entity", "entity-action"],
)
def test_no_metadata_action_rejects_the_pair(rule: CaptureRule, name: str) -> None:
    generalizer = _CountingGeneralizer()
    raw = _raw(rule=rule, name=name, metadata_action=None)
    outcome = _process(raw, _pipeline(generalizer=generalizer))
    assert outcome == CaptureOutcome(rejected=CaptureRejectReason.MISSING_ACTION)
    assert generalizer.calls == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"metadata_action": "SO-1234567"},
        {"metadata_action": "TPC4ZEG0A0"},
        {"metadata_action": WORK},
        {"metadata_action": "send invoice"},
        {"metadata_action": "orders/place"},
        {"metadata_action": ""},
        {"metadata_action": "x" * 65},
        {"rule": NOTIFICATIONS, "name": f"{WORK}/1234567_{STAMP}.json"},
        {"rule": NOTIFICATIONS, "name": f"{WORK}/SO-1234567_{STAMP}.json"},
    ],
    ids=[
        "order-number",
        "mixed",
        "ulid",
        "space",
        "slash",
        "empty",
        "too-long",
        "key-digits",
        "key-order-number",
    ],
)
def test_an_action_that_is_no_name_rejects_the_pair(overrides: dict[str, Any]) -> None:
    generalizer = _CountingGeneralizer()
    outcome = _process(_raw(**overrides), _pipeline(generalizer=generalizer))
    assert outcome == CaptureOutcome(rejected=CaptureRejectReason.BAD_ACTION)
    assert generalizer.calls == 0


@pytest.mark.unit
def test_the_first_failing_pair_check_decides() -> None:
    no_layout = _missing(name=f"{WORK}/x.json", metadata_action=None)
    assert _process(no_layout).rejected is CaptureRejectReason.NO_LAYOUT
    missing_action = _missing(metadata_action=None)
    assert _process(missing_action).rejected is CaptureRejectReason.MISSING_ACTION
    bad_action = _missing(metadata_action="SO-1234567")
    assert _process(bad_action).rejected is CaptureRejectReason.BAD_ACTION
    never_sampled = _pipeline(sample_rate=0)
    assert _process(bad_action, never_sampled).rejected is CaptureRejectReason.BAD_ACTION
    assert _process(_missing(), never_sampled) == SAMPLED_OUT


@pytest.mark.unit
def test_work_unit_entity_action_takes_the_action_from_metadata_never_from_the_key() -> None:
    name = f"{WORK}/SO-1234567_cancel_shipment_{STAMP}.json"
    outcome = _process(_raw(rule=SHIPPING, name=name, metadata_action="create_shipment"))
    assert _keys(outcome)[0] == "http:outbound:shipping:create_shipment:request"
    assert all("cancel" not in shape.model_dump_json() for shape in outcome.shapes)


@pytest.mark.unit
def test_work_unit_action_takes_the_action_from_the_key_never_from_metadata() -> None:
    raw = _raw(rule=NOTIFICATIONS, name=f"{WORK}/email_{STAMP}.json", metadata_action="sms")
    assert _keys(_process(raw))[0] == "http:outbound:notifications:email:request"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("work", "work_id"),
    [
        (WORK, WORK),
        ("unattributed", None),
        ("123e4567-e89b-42d3-a456-426614174000", None),
        (WORK.lower(), None),
        (WORK + "0", None),
        ("x" + WORK, None),
    ],
    ids=["ulid", "unattributed", "execution-id", "lowercase", "suffix", "prefix"],
)
def test_the_work_id_is_the_work_segment_only_when_it_fits_the_whole_pattern(
    work: str, work_id: str | None
) -> None:
    raw = _raw(
        rule=SHIPPING,
        name=f"{work}/SO-1234567_create_shipment_{STAMP}.json",
        metadata_action="create_shipment",
    )
    outcome = _process(raw, _pipeline(work_id_pattern=ULID))

    pair_id = f"shipping:{work_id or '-'}:create_shipment:{STAMP}"
    request, response = _shaped(outcome)
    assert (request.work_id, response.work_id) == (work_id, work_id)
    assert request.observation_id == f"{pair_id}:request"
    assert response.observation_id == f"{pair_id}:response"
    if work_id is None:
        assert all(work not in shape.model_dump_json() for shape in outcome.shapes)


@pytest.mark.unit
def test_with_no_pattern_any_work_segment_is_the_work_id() -> None:
    request, _ = _shaped(_process(_raw(name=f"unattributed/SO-1234567_{STAMP}.json")))
    assert request.work_id == "unattributed"
    assert request.observation_id == f"billing:unattributed:invoice_place:{STAMP}:request"


def _stamped(stamp: str) -> RawCapture:
    return _raw(name=f"{WORK}/SO-1234567_{stamp}.json")


def _stamped_pair_id(stamp: str) -> str:
    return f"billing:{WORK}:invoice_place:{stamp}"


STAMPS = [f"20261011_163001_{index:06d}" for index in range(1000)]


@pytest.mark.unit
def test_sampling_hashes_the_pair_id_and_drops_the_pair_once() -> None:
    kept = next(
        stamp
        for stamp in STAMPS
        if sampled_in(_stamped_pair_id(stamp), 0.5)
        and not sampled_in(f"{_stamped_pair_id(stamp)}:request", 0.5)
    )
    dropped = next(
        stamp
        for stamp in STAMPS
        if not sampled_in(_stamped_pair_id(stamp), 0.5)
        and sampled_in(f"{_stamped_pair_id(stamp)}:request", 0.5)
    )
    generalizer = _CountingGeneralizer()
    pipeline = _pipeline(sample_rate=0.5, generalizer=generalizer)

    _shaped(_process(_stamped(kept), pipeline))
    assert generalizer.calls == 2
    assert _process(_stamped(dropped), pipeline) == SAMPLED_OUT
    unshapeable = replace(_stamped(dropped), request_state=ObjectState.MISSING, request=None)
    assert _process(unshapeable, pipeline) == SAMPLED_OUT
    assert generalizer.calls == 2


@pytest.mark.unit
def test_rate_one_keeps_every_pair_and_rate_zero_none() -> None:
    always, never = _pipeline(sample_rate=1), _pipeline(sample_rate=0)
    for stamp in STAMPS[:50]:
        _shaped(_process(_stamped(stamp), always))
        assert _process(_stamped(stamp), never) == SAMPLED_OUT


@pytest.mark.unit
def test_a_pair_seen_again_is_one_local_duplicate() -> None:
    generalizer = _CountingGeneralizer()
    pipeline = _pipeline(generalizer=generalizer)

    _shaped(_process(_raw(), pipeline))
    assert _process(_raw(), pipeline) == DUPLICATE
    assert _process(_missing(), pipeline) == DUPLICATE
    assert generalizer.calls == 2


@pytest.mark.unit
def test_a_pair_whose_parts_were_skipped_is_remembered() -> None:
    pipeline = _pipeline()
    assert len(_process(_missing(), pipeline).skipped) == 2
    assert _process(_raw(), pipeline) == DUPLICATE


@pytest.mark.unit
def test_the_entity_id_segment_is_not_part_of_the_pair_id() -> None:
    pipeline = _pipeline()
    _shaped(_process(_raw(), pipeline))
    assert _process(_raw(name=f"{WORK}/SO-7654321_{STAMP}.json"), pipeline) == DUPLICATE


@pytest.mark.unit
def test_the_label_and_the_action_are_part_of_the_pair_id() -> None:
    pipeline = _pipeline()
    _shaped(_process(_raw(), pipeline))
    other_client = CaptureRule(
        prefix="captures/billing-eu/", label="billing-eu", layout=CaptureLayout.WORK_UNIT_ENTITY
    )
    _shaped(_process(_raw(rule=other_client), pipeline))
    _shaped(_process(_raw(metadata_action="invoice_refund"), pipeline))


@pytest.mark.unit
def test_the_dedupe_window_holds_only_the_last_pair_ids() -> None:
    pipeline = _pipeline(dedupe_window=2)
    first, second, third = STAMPS[:3]
    for stamp in (first, second, third):
        _shaped(_process(_stamped(stamp), pipeline))
    assert _process(_stamped(third), pipeline) == DUPLICATE
    _shaped(_process(_stamped(first), pipeline))


@pytest.mark.unit
@pytest.mark.parametrize(
    "uris", [(RESPONSE_URI,), (REQUEST_URI,)], ids=["response-key", "request-key"]
)
def test_a_pair_joins_the_span_its_capture_event_named(uris: tuple[str, ...]) -> None:
    outcome = _process(_raw(), _pipeline(index=_index(*uris)))
    _shaped(outcome)
    assert outcome.noted == ()
    for shape in outcome.shapes:
        assert (shape.trace_id, shape.span_id) == (TRACE, SPAN_ID)


@pytest.mark.unit
def test_the_response_key_wins_over_the_request_key() -> None:
    index = _index(RESPONSE_URI)
    index.put(REQUEST_URI, OTHER_SPAN, 900, NOW)
    for shape in _process(_raw(), _pipeline(index=index)).shapes:
        assert (shape.trace_id, shape.span_id) == (TRACE, SPAN_ID)


@pytest.mark.unit
def test_a_bare_key_in_the_index_joins_too() -> None:
    index = _index(f"captures/billing/responses/{BILLING_NAME}")
    assert _process(_raw(), _pipeline(index=index)).noted == ()


@pytest.mark.unit
def test_an_expired_entry_does_not_join() -> None:
    expiring = _index(RESPONSE_URI, ttl_s=900, at=NOW - timedelta(seconds=900))
    outcome = _process(_raw(), _pipeline(index=expiring))
    assert outcome.noted == UNJOINED
    assert all(shape.trace_id is shape.span_id is None for shape in outcome.shapes)

    live = _index(RESPONSE_URI, ttl_s=900, at=NOW - timedelta(seconds=899))
    assert _process(_raw(), _pipeline(index=live)).noted == ()


@pytest.mark.unit
@pytest.mark.parametrize(
    "index",
    [None, CaptureIndex(), _index("s3://capture-bucket/captures/billing/responses/other.json")],
    ids=["no-index", "empty-index", "another-key"],
)
def test_a_pair_with_no_span_is_noted_unjoined(index: CaptureIndex | None) -> None:
    outcome = _process(_raw(), _pipeline(index=index))
    _shaped(outcome)
    assert outcome.noted == UNJOINED
    assert all(shape.trace_id is shape.span_id is None for shape in outcome.shapes)


@pytest.mark.unit
def test_unjoined_is_noted_only_when_a_shape_was_built() -> None:
    skipped = (CaptureSkipReason.OBJECT_MISSING, CaptureSkipReason.OBJECT_MISSING)
    assert _process(_missing()) == CaptureOutcome(skipped=skipped)
    one_part = _process(_raw(request_state=ObjectState.MISSING, request=None))
    assert (len(one_part.shapes), one_part.noted) == (1, UNJOINED)


@pytest.mark.unit
@pytest.mark.parametrize(("state", "reason"), OBJECT_SKIPS, ids=[s.value for s, _ in OBJECT_SKIPS])
def test_a_request_object_not_found_skips_only_the_request(
    state: ObjectState, reason: CaptureSkipReason
) -> None:
    outcome = _process(_raw(request_state=state, request=None))
    assert outcome.skipped == (reason,)
    assert _keys(outcome) == [f"{CHANNEL}:response:2xx"]


@pytest.mark.unit
@pytest.mark.parametrize(("state", "reason"), OBJECT_SKIPS, ids=[s.value for s, _ in OBJECT_SKIPS])
def test_a_response_object_not_found_skips_only_the_response(
    state: ObjectState, reason: CaptureSkipReason
) -> None:
    outcome = _process(_raw(response_state=state, response=None))
    assert outcome.skipped == (reason,)
    assert _keys(outcome) == [f"{CHANNEL}:request"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "value"),
    [
        (ESTIMATE, ESTIMATE),
        ([ESTIMATE, {"lines": []}], [ESTIMATE, {"lines": []}]),
        ({}, {}),
        ([], []),
        (json.dumps(ESTIMATE), ESTIMATE),
        (" [1, 2.5] ", [1, 2.5]),
    ],
    ids=["object", "array", "empty-object", "empty-array", "object-text", "array-text"],
)
def test_object_and_array_bodies_are_shaped(body: Any, value: Any) -> None:
    raw = _raw(request=_request(body), response=_response(body))
    request, response = _shaped(_process(raw))
    assert request.shape == response.shape == _shape(value)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (None, CaptureSkipReason.NO_BODY),
        ("not json", CaptureSkipReason.BODY_NOT_JSON),
        ("", CaptureSkipReason.BODY_NOT_JSON),
        ('{"lines": ', CaptureSkipReason.BODY_NOT_JSON),
        ("[" * 100_000, CaptureSkipReason.BODY_NOT_JSON),
        ('"text"', CaptureSkipReason.BODY_NOT_CONTAINER),
        ("1", CaptureSkipReason.BODY_NOT_CONTAINER),
        ("true", CaptureSkipReason.BODY_NOT_CONTAINER),
        ("null", CaptureSkipReason.BODY_NOT_CONTAINER),
        (7, CaptureSkipReason.BODY_NOT_CONTAINER),
        (0, CaptureSkipReason.BODY_NOT_CONTAINER),
        (1.5, CaptureSkipReason.BODY_NOT_CONTAINER),
        (True, CaptureSkipReason.BODY_NOT_CONTAINER),
        (False, CaptureSkipReason.BODY_NOT_CONTAINER),
    ],
    ids=[
        "null",
        "text",
        "empty-text",
        "broken-json",
        "too-deep",
        "string-text",
        "number-text",
        "boolean-text",
        "null-text",
        "number",
        "zero",
        "float",
        "true",
        "false",
    ],
)
def test_body_outcomes_on_either_part(body: Any, reason: CaptureSkipReason) -> None:
    outcome = _process(_raw(request=_request(body), response=_response(body)))
    assert outcome == CaptureOutcome(skipped=(reason, reason))


@pytest.mark.unit
def test_an_absent_body_is_no_body_on_either_part() -> None:
    raw = _raw(request=_without(_request(), "body"), response=_without(_response(), "body"))
    no_body = CaptureSkipReason.NO_BODY
    assert _process(raw) == CaptureOutcome(skipped=(no_body, no_body))


@pytest.mark.unit
@pytest.mark.parametrize("status", ["null", "absent"])
@pytest.mark.parametrize("body", [ESTIMATED, "not json", None], ids=["object", "text", "null"])
def test_a_response_with_no_status_code_is_the_empty_none_shape_and_its_body_is_unread(
    status: str, body: Any, canary: Canary
) -> None:
    document = _response(canary.plant(body), status_code=None)
    if status == "absent":
        document = _without(document, "status_code")
    spy = _SpyDocument(document)

    outcome = _process(_raw(response=spy))

    _, response = _shaped(outcome)
    assert response.channel.key == f"{CHANNEL}:response:none"
    assert response.channel.status_class is StatusClass.NO_RESPONSE
    assert response.shape == Shape(fields=[]) == _shape({})
    assert response.fingerprint == Sha256ShapeFingerprinter().fingerprint(Shape(fields=[]))
    assert response.observation_id == f"{PAIR_ID}:response"
    assert spy.read == ["status_code"]
    assert canary.leaks(response.model_dump_json()) == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "status_code",
    [True, False, "200", "", 200.0, [200], 199, 600, 100, 0, -1],
    ids=["true", "false", "text", "empty-text", "float", "list", "199", "600", "100", "0", "-1"],
)
def test_an_unclassified_status_skips_only_the_response(status_code: Any) -> None:
    outcome = _process(_raw(response=_response(status_code=status_code)))
    assert outcome.skipped == (CaptureSkipReason.STATUS_UNCLASSIFIED,)
    assert _keys(outcome) == [f"{CHANNEL}:request"]


@pytest.mark.unit
def test_the_status_is_checked_before_the_body() -> None:
    unclassified = (CaptureSkipReason.STATUS_UNCLASSIFIED,)
    for response in (
        _without(_response(status_code=600), "body"),
        _response("not json", status_code=True),
        _response(7, status_code="200"),
    ):
        assert _process(_raw(response=response)).skipped == unclassified
    no_body = _without(_response(status_code=204), "body")
    assert _process(_raw(response=no_body)).skipped == (CaptureSkipReason.NO_BODY,)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "skipped"),
    [
        (
            {
                "request_state": ObjectState.MISSING,
                "request": None,
                "response_state": ObjectState.TOO_LARGE,
                "response": None,
            },
            (CaptureSkipReason.OBJECT_MISSING, CaptureSkipReason.OBJECT_TOO_LARGE),
        ),
        (
            {"request": _request(None), "response": _response(status_code=600)},
            (CaptureSkipReason.NO_BODY, CaptureSkipReason.STATUS_UNCLASSIFIED),
        ),
        (
            {
                "request_state": ObjectState.UNDECODABLE,
                "request": None,
                "response": _response("{"),
            },
            (CaptureSkipReason.OBJECT_UNDECODABLE, CaptureSkipReason.BODY_NOT_JSON),
        ),
        (
            {"request": _request("[]x"), "response": _response('"text"')},
            (CaptureSkipReason.BODY_NOT_JSON, CaptureSkipReason.BODY_NOT_CONTAINER),
        ),
    ],
    ids=["objects", "no-body-and-status", "undecodable-and-not-json", "not-json-and-scalar"],
)
def test_skips_are_listed_in_part_order(
    overrides: dict[str, Any], skipped: tuple[CaptureSkipReason, ...]
) -> None:
    assert _process(_raw(**overrides)) == CaptureOutcome(skipped=skipped)


@pytest.mark.unit
def test_only_body_and_status_code_are_read() -> None:
    request, response = _SpyDocument(_request()), _SpyDocument(_response())
    _shaped(_process(_raw(request=request, response=response)))
    assert request.read == ["body"]
    assert response.read == ["status_code", "body"]


@pytest.mark.unit
def test_no_document_is_kept_after_process() -> None:
    request, response = _SpyDocument(_request()), _SpyDocument(_response())
    references = [weakref.ref(request), weakref.ref(response)]
    pipeline = _pipeline(index=_index(RESPONSE_URI))
    _shaped(_process(_raw(request=request, response=response), pipeline))
    del request, response
    gc.collect()
    assert [reference() for reference in references] == [None, None]


@pytest.mark.unit
@pytest.mark.parametrize("error", [ValueError, RuntimeError, KeyError, RecursionError])
def test_a_generalizer_that_raises_gives_shaping_failed(
    error: type[Exception], canary: Canary
) -> None:
    raw = _raw(request=_request(canary.plant(ESTIMATE)), response=_response(canary.plant({})))
    outcome = _process(raw, _pipeline(generalizer=_RaisingGeneralizer(error)))
    assert outcome == CaptureOutcome(skipped=(SHAPING_FAILED, SHAPING_FAILED))
    assert canary.leaks(repr(outcome)) == []


@pytest.mark.unit
def test_a_none_response_whose_shaping_fails_gives_shaping_failed() -> None:
    raw = _raw(response=_response(status_code=None))
    outcome = _process(raw, _pipeline(generalizer=_RaisingGeneralizer(RuntimeError)))
    assert outcome.skipped == (SHAPING_FAILED, SHAPING_FAILED)


@pytest.mark.unit
def test_an_observation_that_fails_validation_fails_shaping() -> None:
    both = CaptureOutcome(skipped=(SHAPING_FAILED, SHAPING_FAILED))
    long_label = CaptureRule(prefix="p/", label="a" * 250, layout=CaptureLayout.WORK_UNIT_ENTITY)
    assert _process(_raw(rule=long_label)) == both
    long_work = _raw(name=f"{'w' * 257}/SO-1234567_{STAMP}.json")
    assert _process(long_work) == both
    invalid_span = _index(RESPONSE_URI, span=SpanRef("0" * 32, SPAN_ID))
    assert _process(_raw(), _pipeline(index=invalid_span)) == both
    surrogate_key = _process(_raw(request=_request('{"k\\udc00": 1}')))
    assert surrogate_key.skipped == (SHAPING_FAILED,)


@pytest.mark.unit
def test_a_found_object_with_no_document_fails_shaping() -> None:
    outcome = _process(_raw(request=None, response=None))
    assert outcome == CaptureOutcome(skipped=(SHAPING_FAILED, SHAPING_FAILED))


def _planted_documents(canary: Canary) -> tuple[dict[str, Any], dict[str, Any]]:
    """Both documents with a canary in every value; the response body as JSON text."""
    request = canary.plant(
        {
            "method": "POST",
            "url": "https://api.partner.example.com/v1/orders/SO-1",
            "headers": {"Authorization": "Bearer x", "X-Order-Id": "SO-1", "X-Retry": 2},
            "body": {
                "order": {"id": "o-1", "lines": [{"sku": "s-1", "quantity": 2, "price": 9.5}]},
                "customer": {"email": "a@example.com", "active": True, "note": None},
                "tags": ["a", "b"],
            },
            "metadata": {"order-id": "SO-1", "partner": "x"},
        }
    )
    response = canary.plant(
        {
            "headers": {"Set-Cookie": "x", "X-Request-Id": "r-1"},
            "error": "upstream said x",
            "error_type": "HTTPStatusError",
            "metadata": {"order-id": "SO-1"},
        }
    )
    response["status_code"] = 422
    response["body"] = json.dumps(canary.plant({"errors": [{"field": "x", "code": 7}]}))
    return request, response


@pytest.mark.unit
@pytest.mark.parametrize(
    ("rule", "template", "metadata_action"),
    [
        (NOTIFICATIONS, "{work}/email_{stamp}.json", "sms"),
        (SHIPPING, "{work}/{entity}_create_shipment_{stamp}.json", "create_shipment"),
        (BILLING, "{work}/{entity}_{stamp}.json", "invoice_place"),
    ],
    ids=["work_unit_action", "work_unit_entity_action", "work_unit_entity"],
)
def test_canary_values_reach_no_observation(
    rule: CaptureRule, template: str, metadata_action: str, canary: Canary
) -> None:
    request, response = _planted_documents(canary)
    name = template.format(work=WORK, entity=canary.token(), stamp=STAMP)
    raw = _raw(
        rule=rule, name=name, request=request, response=response, metadata_action=metadata_action
    )
    assert canary.leaks(json.dumps([request, response, name]))

    outcome = _process(raw, _pipeline(work_id_pattern=ULID))

    shaped_request, shaped_response = _shaped(outcome)
    assert shaped_request.shape == _shape(request["body"])
    assert shaped_response.shape == _shape(json.loads(response["body"]))
    dumped = "".join(shape.model_dump_json() for shape in outcome.shapes)
    assert canary.leaks(dumped) == []
    assert canary.leaks(repr(outcome)) == []
    assert canary.leaks(repr(raw)) == []


@pytest.mark.unit
def test_a_work_segment_that_is_no_work_id_reaches_no_observation(canary: Canary) -> None:
    request, response = _planted_documents(canary)
    name = f"{canary.token()}/{canary.token()}_{STAMP}.json"
    raw = _raw(name=name, request=request, response=response)
    index = _index(f"s3://capture-bucket/{raw.response_key}")

    outcome = _process(raw, _pipeline(work_id_pattern=ULID, index=index))

    request_shape, _ = _shaped(outcome)
    assert (request_shape.work_id, request_shape.trace_id) == (None, TRACE)
    assert request_shape.observation_id == f"billing:-:invoice_place:{STAMP}:request"
    dumped = "".join(shape.model_dump_json() for shape in outcome.shapes)
    assert canary.leaks(dumped) == []
    assert canary.leaks(repr(outcome)) == []
