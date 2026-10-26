"""Span pipeline: span mapping, rejections, links, and the bodies of client spans."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from canary import Canary

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.observation import SpanKind, SpanLink, StatusClass
from fire.core.domain.operation import OperationInputs, OperationNamer, OperationRules
from fire.core.ports.span_source import RawLink, RawSpan
from fire.core.services.span_pipeline import (
    BodySkipReason,
    SpanNote,
    SpanOutcome,
    SpanPipeline,
    SpanRejectReason,
)

TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN = "00f067aa0ba902b7"
PARENT = "53995c3f42cd8ad8"
START_NS = 1_791_768_368_257_502_981
END_NS = START_NS + 110_129_373
OBSERVED = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
ORDER = '{"orderId": "SO-1", "items": [{"sku": "a", "qty": 1}]}'
PARTNER = {"hosts": ["api.partner.example.com"], "alias": "partner", "paths": ["/v1/orders"]}


def _pipeline(sample_rate: float = 1.0) -> SpanPipeline:
    return SpanPipeline(
        system="shop",
        source="traces",
        sample_rate=sample_rate,
        namer=OperationNamer(OperationRules.model_validate({"http": [PARTNER]})),
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        clock=lambda: OBSERVED,
    )


def _server(**overrides: Any) -> RawSpan:
    naming = OperationInputs(
        SpanKind.SERVER, "POST /v1/orders", method="POST", route="/v1/orders/{order_id}"
    )
    span = RawSpan(
        trace_id=TRACE,
        span_id=SPAN,
        parent_span_id=PARENT,
        service="orders-api",
        kind=SpanKind.SERVER,
        start_ns=START_NS,
        end_ns=END_NS,
        error=False,
        naming=naming,
        status_code=201,
    )
    return replace(span, **overrides)


def _client(url: str = "https://api.example.net/v1/orders/42", **overrides: Any) -> RawSpan:
    naming = OperationInputs(SpanKind.CLIENT, "POST", method="POST", url=url)
    return _server(**{"kind": SpanKind.CLIENT, "naming": naming, "status_code": 200, **overrides})


def _process(raw: RawSpan, sample_rate: float = 1.0) -> SpanOutcome:
    return _pipeline(sample_rate).process(raw)


@pytest.mark.unit
def test_a_server_span_maps_every_field() -> None:
    outcome = _process(_server())

    assert outcome.rejected is None
    assert outcome.shapes == outcome.skipped == outcome.noted == ()
    span = outcome.span
    assert span is not None
    assert span.model_dump() == {
        "system": "shop",
        "source": "traces",
        "trace_id": TRACE,
        "span_id": SPAN,
        "parent_span_id": PARENT,
        "links": [],
        "service": "orders-api",
        "kind": SpanKind.SERVER,
        "operation": "POST /v1/orders/{order_id}",
        "status_code": 201,
        "error": False,
        "start": datetime(2026, 10, 12, 1, 26, 8, 257502, tzinfo=UTC),
        "duration_ms": 110.129373,
    }
    assert outcome.observations == [span]


@pytest.mark.unit
@pytest.mark.parametrize("parent", [None, ""])
def test_a_root_span_has_no_parent(parent: str | None) -> None:
    span = _process(_server(parent_span_id=parent)).span
    assert span is not None and span.parent_span_id is None


@pytest.mark.unit
def test_ids_are_read_in_either_case_and_kept_lowercase() -> None:
    span = _process(_server(trace_id=TRACE.upper(), span_id=SPAN.upper())).span
    assert span is not None and (span.trace_id, span.span_id) == (TRACE, SPAN)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status_code", "error", "expected"),
    [
        (201, False, (201, False)),
        (404, False, (404, False)),
        (500, False, (500, True)),
        (503, True, (503, True)),
        (200, True, (200, True)),
        (None, False, (None, False)),
        (None, True, (None, True)),
        (99, False, (None, False)),
        (600, False, (None, False)),
        (0, False, (None, False)),
    ],
)
def test_status_code_and_error(
    status_code: int | None, error: bool, expected: tuple[int | None, bool]
) -> None:
    span = _process(_server(status_code=status_code, error=error)).span
    assert span is not None and (span.status_code, span.error) == expected


@pytest.mark.unit
def test_valid_links_are_kept_and_invalid_ones_dropped_and_noted() -> None:
    links = (
        RawLink(TRACE, PARENT),
        RawLink(None, PARENT),
        RawLink(TRACE, "0" * 16),
        RawLink(TRACE[:-1], PARENT),
        RawLink(TRACE.upper(), SPAN),
    )
    outcome = _process(_server(links=links))
    assert outcome.span is not None
    assert outcome.span.links == [
        SpanLink(trace_id=TRACE, span_id=PARENT),
        SpanLink(trace_id=TRACE, span_id=SPAN),
    ]
    assert outcome.noted == (SpanNote.INVALID_LINK,) * 3


@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"trace_id": None}, SpanRejectReason.INVALID_IDS),
        ({"trace_id": TRACE[:-1]}, SpanRejectReason.INVALID_IDS),
        ({"trace_id": "g" * 32}, SpanRejectReason.INVALID_IDS),
        ({"trace_id": "0" * 32}, SpanRejectReason.INVALID_IDS),
        ({"span_id": None}, SpanRejectReason.INVALID_IDS),
        ({"span_id": SPAN + "00"}, SpanRejectReason.INVALID_IDS),
        ({"span_id": "0" * 16}, SpanRejectReason.INVALID_IDS),
        ({"parent_span_id": "xyz"}, SpanRejectReason.INVALID_IDS),
        ({"parent_span_id": "0" * 16}, SpanRejectReason.INVALID_IDS),
        ({"start_ns": None}, SpanRejectReason.INVALID_TIMES),
        ({"end_ns": None}, SpanRejectReason.INVALID_TIMES),
        ({"start_ns": 0}, SpanRejectReason.INVALID_TIMES),
        ({"end_ns": START_NS - 1}, SpanRejectReason.INVALID_TIMES),
        ({"start_ns": -5, "end_ns": 5}, SpanRejectReason.INVALID_TIMES),
        ({"end_ns": 10**30}, SpanRejectReason.INVALID_TIMES),
        ({"service": None}, SpanRejectReason.NO_SERVICE),
        ({"service": ""}, SpanRejectReason.NO_SERVICE),
        ({"naming": OperationInputs(SpanKind.INTERNAL, "")}, SpanRejectReason.NO_OPERATION),
        (
            {"naming": OperationInputs(SpanKind.INTERNAL, " ".join(["ab"] * 86))},
            SpanRejectReason.NO_OPERATION,
        ),
        ({"service": "s" * 257}, SpanRejectReason.INVALID),
    ],
    ids=[
        "trace-missing",
        "trace-short",
        "trace-not-hex",
        "trace-zeros",
        "span-missing",
        "span-long",
        "span-zeros",
        "parent-not-hex",
        "parent-zeros",
        "start-missing",
        "end-missing",
        "start-zero",
        "end-before-start",
        "start-negative",
        "end-past-9999",
        "service-missing",
        "service-empty",
        "operation-empty",
        "operation-long",
        "service-long",
    ],
)
def test_rejections(overrides: dict[str, Any], reason: SpanRejectReason) -> None:
    outcome = _process(_server(**overrides))
    assert outcome == SpanOutcome(rejected=reason)


@pytest.mark.unit
def test_the_first_failing_check_decides() -> None:
    outcome = _process(_server(trace_id=None, start_ns=None, service=None))
    assert outcome.rejected is SpanRejectReason.INVALID_IDS
    outcome = _process(_server(start_ns=None, service=None))
    assert outcome.rejected is SpanRejectReason.INVALID_TIMES


@pytest.mark.unit
def test_an_unlisted_path_of_a_declared_host_is_noted() -> None:
    outcome = _process(_client("https://api.partner.example.com/v2/refunds"))
    assert outcome.span is not None
    assert outcome.span.operation == "POST partner/{unlisted}"
    assert outcome.noted == (SpanNote.OPERATION_UNLISTED,)


def _keys(outcome: SpanOutcome) -> list[str]:
    return [shape.channel.key for shape in outcome.shapes]


@pytest.mark.unit
def test_complete_bodies_of_a_client_span_become_outbound_shapes() -> None:
    outcome = _process(_client(request_body=ORDER, response_body='[{"id": 1}]'))

    assert outcome.skipped == ()
    assert _keys(outcome) == [
        "http:outbound:POST api.example.net/v1/orders/{id}:request",
        "http:outbound:POST api.example.net/v1/orders/{id}:response:2xx",
    ]
    request, response = outcome.shapes
    assert request.observation_id == f"{TRACE}:{SPAN}:request"
    assert response.observation_id == f"{TRACE}:{SPAN}:response"
    assert request.occurred_at == datetime(2026, 10, 12, 1, 26, 8, 257502, tzinfo=UTC)
    assert response.occurred_at == datetime(2026, 10, 12, 1, 26, 8, 367632, tzinfo=UTC)
    for shape in outcome.shapes:
        assert (shape.system, shape.source, shape.observed_at) == ("shop", "traces", OBSERVED)
        assert (shape.trace_id, shape.span_id) == (TRACE, SPAN)
        assert shape.region is shape.work_id is shape.correlation_id is None
        assert shape.fingerprint == Sha256ShapeFingerprinter().fingerprint(shape.shape)
    assert request.shape == FlatStarShapeGeneralizer().generalize(json.loads(ORDER))
    assert response.shape == FlatStarShapeGeneralizer().generalize([{"id": 1}])
    assert outcome.observations == [outcome.span, request, response]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (
            '{"orderId": "SO-1", "note": "abc...[truncated 1200 bytes]',
            BodySkipReason.BODY_TRUNCATED,
        ),
        ('{"a": 1}...[truncated 7 bytes]', BodySkipReason.BODY_TRUNCATED),
        ("", BodySkipReason.BODY_EMPTY),
        ("7b226f72646572223a317d", BodySkipReason.BODY_NOT_JSON),
        ("a=1&b=2", BodySkipReason.BODY_NOT_JSON),
        ('{"orderId": ', BodySkipReason.BODY_NOT_JSON),
        ("[" * 100_000, BodySkipReason.BODY_NOT_JSON),
        ('"just a string"', BodySkipReason.BODY_NOT_CONTAINER),
        ("null", BodySkipReason.BODY_NOT_CONTAINER),
    ],
    ids=[
        "truncated",
        "truncated-after-json",
        "empty",
        "hex-fallback",
        "form",
        "broken-json",
        "too-deep",
        "string",
        "null",
    ],
)
def test_body_outcomes(body: str, reason: BodySkipReason) -> None:
    outcome = _process(_client(request_body=body, response_body=body))
    assert outcome.shapes == ()
    assert outcome.skipped == (reason, reason)


@pytest.mark.unit
def test_a_call_with_no_response_gives_an_empty_none_shape() -> None:
    outcome = _process(_client(request_body=ORDER, status_code=None))
    assert _keys(outcome) == [
        "http:outbound:POST api.example.net/v1/orders/{id}:request",
        "http:outbound:POST api.example.net/v1/orders/{id}:response:none",
    ]
    none = outcome.shapes[1]
    assert none.shape.fields == []
    assert none.channel.status_class is StatusClass.NO_RESPONSE


@pytest.mark.unit
def test_a_response_body_without_a_status_code_is_unclassified() -> None:
    outcome = _process(_client(request_body=ORDER, response_body=ORDER, status_code=None))
    assert _keys(outcome)[1].endswith(":response:none")
    assert outcome.skipped == (BodySkipReason.STATUS_UNCLASSIFIED,)
    alone = _process(_client(response_body=ORDER, status_code=None))
    assert alone.shapes == ()
    assert alone.skipped == (BodySkipReason.STATUS_UNCLASSIFIED,)


@pytest.mark.unit
def test_no_none_shape_without_a_request_shape() -> None:
    outcome = _process(_client(request_body="", status_code=None))
    assert outcome.shapes == ()
    assert outcome.skipped == (BodySkipReason.BODY_EMPTY,)


@pytest.mark.unit
def test_an_informational_status_has_no_class() -> None:
    outcome = _process(_client(request_body=ORDER, response_body=ORDER, status_code=101))
    assert _keys(outcome) == ["http:outbound:POST api.example.net/v1/orders/{id}:request"]
    assert outcome.skipped == (BodySkipReason.STATUS_UNCLASSIFIED,)


@pytest.mark.unit
def test_response_classes_follow_the_status() -> None:
    outcome = _process(_client(response_body=ORDER, status_code=503))
    assert _keys(outcome) == ["http:outbound:POST api.example.net/v1/orders/{id}:response:5xx"]


@pytest.mark.unit
@pytest.mark.parametrize("kind", [SpanKind.SERVER, SpanKind.INTERNAL, SpanKind.PRODUCER])
def test_bodies_on_other_kinds_are_ignored(kind: SpanKind) -> None:
    outcome = _process(_server(kind=kind, request_body=ORDER, response_body=ORDER))
    assert outcome.span is not None
    assert outcome.shapes == outcome.skipped == ()


@pytest.mark.unit
def test_a_rejected_span_gives_no_shape() -> None:
    outcome = _process(_client(request_body=ORDER, service=None))
    assert outcome == SpanOutcome(rejected=SpanRejectReason.NO_SERVICE)


@pytest.mark.unit
def test_sampling_drops_both_parts_together_and_never_the_span() -> None:
    kept = dropped = 0
    for index in range(200):
        span_id = f"{index + 1:016x}"
        outcome = _process(
            _client(span_id=span_id, request_body=ORDER, response_body=ORDER), sample_rate=0.5
        )
        assert outcome.span is not None
        if outcome.skipped:
            assert outcome.skipped == (BodySkipReason.SAMPLED_OUT,)
            assert outcome.shapes == ()
            dropped += 1
        else:
            assert len(outcome.shapes) == 2
            kept += 1
    assert kept and dropped


@pytest.mark.unit
def test_a_resent_span_gives_the_same_ids() -> None:
    first = _process(_client(request_body=ORDER, response_body=ORDER))
    again = _process(_client(request_body=ORDER, response_body=ORDER))
    assert first == again
    assert first.span is not None and again.span is not None
    assert first.span.dedupe_key == again.span.dedupe_key
    assert [s.dedupe_key for s in first.shapes] == [s.dedupe_key for s in again.shapes]


@pytest.mark.unit
def test_body_values_never_reach_an_observation(canary: Canary) -> None:
    order = canary.plant(json.loads(ORDER) | {"email": "x", "total": 1.5, "items": [{"sku": "a"}]})
    truncated = json.dumps(canary.plant({"note": "x"}))[:-2] + "...[truncated 9 bytes]"
    for request, response in [(json.dumps(order), canary.string()), (truncated, canary.token())]:
        outcome = _process(_client(request_body=request, response_body=response))
        dumped = json.dumps([item.model_dump(mode="json") for item in outcome.observations])
        assert canary.leaks(dumped) == []
        assert canary.leaks(repr(outcome)) == []
