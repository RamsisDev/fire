"""5xx bursts: the detector's window, threshold, and routes, and the incident a burst opens."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError
from spans import ENTRY, SERVICE, SYSTEM, T0, Clock, at, order_spans, span, span_id, trace_id

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.wiring import log_http_errors
from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeField,
    ValueType,
)
from fire.core.domain.flow import CompleteFlow, Node, PathPolicy
from fire.core.domain.http_errors import HttpErrorBurst, HttpErrorDetector, HttpErrorsTrigger
from fire.core.domain.incident import Trigger
from fire.core.domain.observation import Channel, ShapeObservation, SpanKind, SpanObservation
from fire.core.domain.trigger_event import TriggerEvent
from fire.core.services.http_errors import HttpErrorsService
from fire.core.services.incident_correlator import IncidentCorrelator

THRESHOLD = 3
WINDOW_S = 10
TRIGGER = HttpErrorsTrigger(status_min=500, threshold=THRESHOLD, window_s=WINDOW_S)
ROUTE = Node(service=SERVICE, operation=ENTRY)
REFUND = "POST /refunds"
PROXY = 0xF00
SOURCE = "api-proxy"
REQUEST = f"http:inbound:{ENTRY}:request"
RESPONSE_5XX = f"http:inbound:{ENTRY}:response:5xx"
PUT = "DynamoDB.PutItem orders"
FINGERPRINTER = Sha256ShapeFingerprinter()


def _error(
    trace: int,
    *,
    status: int | None = 503,
    operation: str = ENTRY,
    kind: SpanKind = SpanKind.SERVER,
    service: str = SERVICE,
    start: float = 0,
    system: str = SYSTEM,
) -> SpanObservation:
    return span(
        trace,
        1,
        operation,
        kind=kind,
        service=service,
        status_code=status,
        error=status is not None and status >= 500,
        start=start,
        system=system,
    )


class _Detector:
    """The detector on a hand-moved clock: `send` observes at the current time."""

    def __init__(self, triggers: dict[str, HttpErrorsTrigger] | None = None) -> None:
        self.detector = HttpErrorDetector({SYSTEM: TRIGGER} if triggers is None else triggers)
        self.now: datetime = T0

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    def send(self, *spans: SpanObservation) -> list[HttpErrorBurst]:
        bursts = [self.detector.observe(item, self.now) for item in spans]
        return [burst for burst in bursts if burst is not None]


# The trigger


@pytest.mark.unit
def test_the_trigger_defaults_to_five_errors_of_500_or_more_within_a_minute() -> None:
    assert HttpErrorsTrigger().model_dump() == {"status_min": 500, "threshold": 5, "window_s": 60}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("status_min", 99),
        ("status_min", 600),
        ("threshold", 0),
        ("window_s", 0),
        ("window_s", float("inf")),
        ("count", 5),
    ],
    ids=[
        "status-below-100",
        "status-above-599",
        "threshold-zero",
        "window-zero",
        "inf",
        "unknown",
    ],
)
def test_the_trigger_rejects_out_of_range_values_and_unknown_keys(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        HttpErrorsTrigger.model_validate({key: value})
    assert [error["loc"] for error in caught.value.errors()] == [(key,)]


# Threshold and window


@pytest.mark.unit
def test_one_error_fewer_than_the_threshold_raises_nothing_and_the_threshold_raises_one() -> None:
    detector = _Detector()

    assert detector.send(_error(1), _error(2)) == []
    [burst] = detector.send(_error(3, status=502, start=4))

    assert burst == HttpErrorBurst(
        system=SYSTEM,
        route=ROUTE,
        status_min=500,
        threshold=THRESHOLD,
        window_s=WINDOW_S,
        errors=THRESHOLD,
        statuses={"502": 1, "503": 2},
        first_error=at(0),
        last_error=at(4),
        trace_ids=[trace_id(1), trace_id(2), trace_id(3)],
    )


@pytest.mark.unit
def test_an_error_window_s_old_has_left_the_window() -> None:
    detector = _Detector()
    detector.send(_error(1))
    detector.advance(1)
    detector.send(_error(2))
    detector.advance(WINDOW_S - 1)

    assert detector.send(_error(3)) == []


@pytest.mark.unit
def test_an_error_just_under_window_s_old_still_counts() -> None:
    detector = _Detector()
    detector.send(_error(1))
    detector.advance(1)
    detector.send(_error(2))
    detector.advance(WINDOW_S - 1.001)

    [burst] = detector.send(_error(3))
    assert burst.trace_ids == [trace_id(1), trace_id(2), trace_id(3)]


@pytest.mark.unit
def test_the_window_is_the_bodys_clock_not_the_span_start() -> None:
    detector = _Detector()

    [burst] = detector.send(*(_error(trace, start=trace * 100) for trace in (1, 2, 3)))

    assert (burst.first_error, burst.last_error) == (at(100), at(300))


@pytest.mark.unit
def test_errors_on_two_routes_or_two_services_do_not_add_up() -> None:
    detector = _Detector()

    assert (
        detector.send(
            _error(1),
            _error(2),
            _error(3, operation=REFUND),
            _error(4, operation=REFUND),
            _error(5, service="billing-api"),
            _error(6, service="billing-api"),
        )
        == []
    )
    [burst] = detector.send(_error(7, operation=REFUND))
    assert burst.route == Node(service=SERVICE, operation=REFUND)


@pytest.mark.unit
@pytest.mark.parametrize(
    "spans",
    [
        [_error(1, status=499), _error(2, status=404), _error(3, status=200)],
        [_error(1, kind=SpanKind.CLIENT), _error(2, kind=SpanKind.CONSUMER), _error(3)],
        [_error(1, status=None), _error(2, status=None), _error(3)],
    ],
    ids=["below-status-min", "not-server", "no-status"],
)
def test_only_server_spans_at_or_above_status_min_count(spans: list[SpanObservation]) -> None:
    assert _Detector().send(*spans) == []


@pytest.mark.unit
def test_status_min_counts_its_own_status() -> None:
    detector = _Detector({SYSTEM: TRIGGER.model_copy(update={"status_min": 429})})

    [burst] = detector.send(_error(1, status=429), _error(2, status=429), _error(3, status=500))

    assert burst.statuses == {"429": 2, "500": 1}


@pytest.mark.unit
def test_a_system_without_the_trigger_is_never_counted_and_each_system_has_its_own() -> None:
    detector = _Detector({"billing": TRIGGER.model_copy(update={"threshold": 1})})

    assert detector.send(*(_error(trace) for trace in range(1, 6))) == []
    [burst] = detector.send(_error(9, system="billing"))
    assert (burst.system, burst.threshold) == ("billing", 1)


# Re-firing


@pytest.mark.unit
def test_errors_while_a_route_bursts_open_nothing_while_they_keep_coming() -> None:
    detector = _Detector()
    [_burst] = detector.send(_error(1), _error(2), _error(3))

    for trace in range(4, 12):
        detector.advance(WINDOW_S - 1)
        assert detector.send(_error(trace)) == []


@pytest.mark.unit
def test_a_burst_ends_after_window_s_with_no_error_and_the_next_counts_from_one() -> None:
    detector = _Detector()
    detector.send(_error(1), _error(2), _error(3))
    detector.advance(WINDOW_S - 1)
    detector.send(_error(4))
    detector.advance(WINDOW_S)

    assert detector.send(_error(5), _error(6)) == []
    [burst] = detector.send(_error(7))
    assert burst.trace_ids == [trace_id(5), trace_id(6), trace_id(7)]


@pytest.mark.unit
def test_another_route_bursts_while_one_does() -> None:
    detector = _Detector()
    detector.send(_error(1), _error(2), _error(3))

    [burst] = detector.send(*(_error(trace, operation=REFUND) for trace in (4, 5, 6)))
    assert burst.route.operation == REFUND


@pytest.mark.unit
def test_log_http_errors_writes_one_info_line_with_the_route_and_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    [burst] = _Detector().send(_error(1), _error(2), _error(3))

    log_http_errors(burst)

    assert caplog.record_tuples == [
        (
            "fire.apps.incident_engine.wiring",
            logging.INFO,
            f"http errors raised: system=shop service={SERVICE} route={ENTRY} errors=3 "
            "status_min=500 window_s=10",
        )
    ]


@pytest.mark.unit
def test_the_burst_model_rejects_unknown_fields() -> None:
    burst = _Detector().send(_error(1), _error(2), _error(3))[0].model_dump()
    with pytest.raises(ValidationError):
        HttpErrorBurst.model_validate({**burst, "body": "x"})


# The trigger event


def _shape_of(*paths: str) -> Shape:
    return Shape(fields=[ShapeField(path=path, value_type=ValueType.STRING) for path in paths])


BASELINE = _shape_of("order_id", "total")
DRIFTED = _shape_of("order_id")
ERROR_BODY = _shape_of("detail")


def _proxy_shape(
    observation_id: str, trace: int, shape: Shape, *, part: str = "request", name: str = ENTRY
) -> ShapeObservation:
    channel: dict[str, Any] = {"kind": "http", "direction": "inbound", "name": name, "part": part}
    if part == "response":
        channel["status_class"] = "5xx"
    return ShapeObservation(
        system=SYSTEM,
        source=SOURCE,
        channel=Channel.model_validate(channel),
        observation_id=observation_id,
        occurred_at=T0,
        observed_at=T0,
        trace_id=trace_id(trace),
        span_id=span_id(PROXY),
        shape=shape,
        fingerprint=FINGERPRINTER.fingerprint(shape),
    )


def _failing_order(trace: int) -> list[SpanObservation]:
    """A checkout whose DynamoDB write fails and whose server span answers 503."""
    root, put = order_spans(trace, PUT, failing=[PUT])
    return [root.model_copy(update={"status_code": 503, "error": True}), put]


class _Body:
    """A catalog and a burst service on one hand-moved clock, warmed with two request shapes
    unless `warm` is false.
    """

    def __init__(self, settle_s: float = 60, warm: bool = True) -> None:
        self.clock = Clock()
        self.catalog = MemoryObservationCatalog(
            {SYSTEM: DriftPolicy(min_samples=2)},
            path_policies={SYSTEM: PathPolicy(settle_s=settle_s)},
            clock=self.clock,
        )
        self.service = HttpErrorsService(self.catalog, {SYSTEM: TRIGGER}, clock=self.clock)
        for index in range(2 if warm else 0):
            self.catalog.add_shape(_proxy_shape(f"warm-{index}", 100 + index, BASELINE))

    def order(self, trace: int, *shapes: ShapeObservation) -> HttpErrorBurst | None:
        for shape in shapes:
            self.catalog.add_shape(shape)
        bursts = []
        for item in _failing_order(trace):
            self.catalog.add_span(item)
            bursts.append(self.service.observe(item))
        return next((burst for burst in bursts if burst is not None), None)


def _burst(body: _Body, shaped: bool = True) -> HttpErrorBurst:
    """Three failing orders; the first two posted through the proxy, the third drifted."""
    shapes: dict[int, list[ShapeObservation]] = {
        1: [
            _proxy_shape("req-1", 1, BASELINE),
            _proxy_shape("res-1", 1, ERROR_BODY, part="response"),
        ],
        2: [_proxy_shape("req-2", 2, BASELINE)],
        3: [_proxy_shape("req-3", 3, DRIFTED)],
    }
    assert body.order(1, *shapes[1] if shaped else ()) is None
    assert body.order(2, *shapes[2] if shaped else ()) is None
    burst = body.order(3, *shapes[3] if shaped else ())
    assert burst is not None
    return burst


def _fragment(event: TriggerEvent, collector: str) -> EvidenceFragment:
    [fragment] = [fragment for fragment in event.fragments if fragment.collector == collector]
    return fragment


@pytest.mark.unit
def test_observe_counts_on_the_services_clock() -> None:
    body = _Body()
    body.order(1)
    body.order(2)
    body.clock.advance(WINDOW_S)

    assert body.order(3) is None


@pytest.mark.unit
def test_the_event_names_the_route_from_its_first_error() -> None:
    body = _Body()
    event = body.service.trigger_event(_burst(body))

    assert (event.system, event.trigger, event.service) == (SYSTEM, Trigger.HTTP_ERRORS, SERVICE)
    assert event.summary == f"HTTP errors on {ENTRY}"
    assert event.started_at == at(0)
    assert (event.shape, event.fingerprint) == (None, None)
    assert [(f.collector, f.quality) for f in event.fragments] == [
        ("http_errors", EvidenceQuality.PRESENT),
        ("failing_shapes", EvidenceQuality.PRESENT),
        ("baseline", EvidenceQuality.PRESENT),
        ("process_path", EvidenceQuality.PRESENT),
    ]


@pytest.mark.unit
def test_the_http_errors_fragment_holds_the_route_statuses_window_and_traces() -> None:
    body = _Body()
    event = body.service.trigger_event(_burst(body))

    assert _fragment(event, "http_errors") == EvidenceFragment(
        collector="http_errors",
        quality=EvidenceQuality.PRESENT,
        summary=f"3 responses with status >= 500 on {ENTRY} within 10s",
        data={
            "route": {"service": SERVICE, "operation": ENTRY},
            "status_min": 500,
            "threshold": THRESHOLD,
            "window_s": WINDOW_S,
            "errors": 3,
            "statuses": {"503": 3},
            "first_error": "2026-10-11T16:40:00Z",
            "last_error": "2026-10-11T16:40:00Z",
            "trace_ids": [trace_id(1), trace_id(2), trace_id(3)],
        },
    )


@pytest.mark.unit
def test_failing_shapes_are_the_routes_shapes_of_the_failing_traces_with_their_drift() -> None:
    body = _Body()
    # Another route's shape in a failing trace, and the route's shape in another trace.
    body.catalog.add_shape(_proxy_shape("other-route", 1, BASELINE, name=REFUND))
    body.catalog.add_shape(_proxy_shape("other-trace", 50, DRIFTED))
    event = body.service.trigger_event(_burst(body))

    fragment = _fragment(event, "failing_shapes")
    assert fragment.summary == "3 of 3 failing requests shaped: 3 fingerprints, 1 drifted"
    assert fragment.data == {
        "route": {"service": SERVICE, "operation": ENTRY},
        "requests": 3,
        "shaped": 3,
        "shapes": [
            {
                "source": SOURCE,
                "stream": REQUEST,
                "schema_hash": schema_hash,
                "requests": requests,
                "drifted": drifted,
            }
            for schema_hash, requests, drifted in sorted(
                [
                    (FINGERPRINTER.fingerprint(BASELINE).schema_hash, 2, False),
                    (FINGERPRINTER.fingerprint(DRIFTED).schema_hash, 1, True),
                ]
            )
        ]
        + [
            {
                "source": SOURCE,
                "stream": RESPONSE_5XX,
                "schema_hash": FINGERPRINTER.fingerprint(ERROR_BODY).schema_hash,
                "requests": 1,
                "drifted": False,
            }
        ],
    }


@pytest.mark.unit
def test_failing_shapes_and_the_baseline_are_missing_without_shapes_of_the_route() -> None:
    body = _Body(warm=False)
    event = body.service.trigger_event(_burst(body, shaped=False))

    route = {"service": SERVICE, "operation": ENTRY}
    assert _fragment(event, "failing_shapes") == EvidenceFragment(
        collector="failing_shapes",
        quality=EvidenceQuality.MISSING,
        summary="No shape of the failing requests",
        data={"route": route, "requests": 3},
    )
    assert _fragment(event, "baseline") == EvidenceFragment(
        collector="baseline",
        quality=EvidenceQuality.MISSING,
        summary="No baseline for this route",
        data={"route": route},
    )


@pytest.mark.unit
def test_the_baseline_is_one_fragment_per_request_stream_of_the_route() -> None:
    body = _Body()
    validation = _proxy_shape("validation-0", 60, BASELINE).model_copy(
        update={"source": "validation-proxy"}
    )
    body.catalog.add_shape(validation)
    event = body.service.trigger_event(_burst(body))

    baselines = [f for f in event.fragments if f.collector == "baseline"]
    assert [fragment.data for fragment in baselines] == [
        {
            "source": SOURCE,
            "stream": REQUEST,
            "status": "warm",
            "samples": 4,
            "fingerprints": 1,
            "observations": 5,
            "drift_signals": 1,
        },
        {
            "source": "validation-proxy",
            "stream": REQUEST,
            "status": "baseline rebuilding",
            "samples": 1,
            "fingerprints": 1,
            "observations": 1,
            "drift_signals": 0,
        },
    ]
    assert baselines[0].summary == "warm: 4 samples, 1 fingerprints known"


@pytest.mark.unit
def test_the_process_path_is_the_failing_requests_open_flows_together() -> None:
    body = _Body()
    event = body.service.trigger_event(_burst(body))

    spans = [item for trace in (1, 2, 3) for item in _failing_order(trace)]
    together = CompleteFlow(
        head=trace_id(1),
        traces=(trace_id(1), trace_id(2), trace_id(3)),
        spans=tuple(spans),
    )
    assert event.process_path == together.process_path()
    assert event.process_path.hops == [
        ProcessHop(ref=SERVICE, operation=ENTRY, span_kind="server", status="error 503"),
        ProcessHop(ref=SERVICE, operation=PUT, span_kind="client", status="error 502"),
    ]
    assert _fragment(event, "process_path") == EvidenceFragment(
        collector="process_path",
        quality=EvidenceQuality.PRESENT,
        summary="Open flows of 3 of 3 failing requests, as assembled so far",
        data={"flows": 3, "held": 3, "requests": 3},
    )


@pytest.mark.unit
def test_failing_requests_whose_flows_completed_are_left_out_of_the_process_path() -> None:
    body = _Body(settle_s=5)
    body.order(1)
    body.clock.advance(6)
    body.catalog.complete_flows()
    body.order(2)
    burst = body.order(3)
    assert burst is not None

    event = body.service.trigger_event(burst)

    assert _fragment(event, "process_path") == EvidenceFragment(
        collector="process_path",
        quality=EvidenceQuality.PRESENT,
        summary="Open flows of 2 of 3 failing requests, as assembled so far",
        data={"flows": 2, "held": 2, "requests": 3},
    )


@pytest.mark.unit
def test_the_process_path_is_missing_when_no_failing_flow_is_open() -> None:
    body = _Body()
    burst = _burst(body)
    body.clock.advance(60)
    body.catalog.complete_flows()

    event = body.service.trigger_event(burst)

    assert event.process_path == ProcessPath()
    assert _fragment(event, "process_path") == EvidenceFragment(
        collector="process_path",
        quality=EvidenceQuality.MISSING,
        summary="No open flow of the failing requests",
    )


# The incident a burst opens


@pytest.mark.unit
def test_a_burst_opens_one_incident_whose_dossier_holds_the_events_evidence() -> None:
    body = _Body()
    event = body.service.trigger_event(_burst(body))
    correlator = IncidentCorrelator({}, clock=lambda: at(5), new_id=lambda: "incident-1")

    run = correlator.take(correlator.add(event).incident_id)

    assert run is not None
    incident, dossier = run
    assert incident.model_dump() == {
        "id": "incident-1",
        "system": SYSTEM,
        "trigger": Trigger.HTTP_ERRORS,
        "triggers": [Trigger.HTTP_ERRORS],
        "service": SERVICE,
        "summary": f"HTTP errors on {ENTRY}",
        "started_at": at(0),
    }
    assert dossier.model_dump(exclude={"triggers"}) == Dossier(
        incident_id="incident-1",
        system=SYSTEM,
        service=SERVICE,
        process_path=event.process_path,
        fragments=event.fragments,
    ).model_dump(exclude={"triggers"})
    dossier.fragments[0].data["errors"] = 99
    assert event.fragments[0].data["errors"] == 3


@pytest.mark.unit
def test_a_trigger_event_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TriggerEvent.model_validate(
            {
                "system": SYSTEM,
                "trigger": "http_errors",
                "summary": "s",
                "started_at": T0,
                "payload": {},
            }
        )
