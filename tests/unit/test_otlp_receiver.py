"""OtlpReceiver: OTLP JSON parsing, the attribute allowlist, and the HTTP answers."""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field, fields
from typing import Any

import httpx
import pytest
from canary import Canary
from otlp_json import START_NS, attributes, event, link, request, resource_spans, span

from fire.adapters.sources.listener import listen
from fire.adapters.sources.otlp import OtlpReceiver, parse_traces
from fire.core.domain.capture import CaptureEvent
from fire.core.domain.observation import SpanKind
from fire.core.ports.span_source import (
    RawLink,
    RawSpan,
    SpanBatchResult,
    SpanRequestNote,
    SpanSource,
)

TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN = "00f067aa0ba902b7"
PARENT = "53995c3f42cd8ad8"
JSON = {"content-type": "application/json"}
CAPTURE = CaptureEvent(
    name="capture.written",
    request_uri="acme.capture.request_uri",
    response_uri="acme.capture.response_uri",
)
REQUEST_URI = "s3://captures/shipping/requests/wu-1/submit_20261011_120000_000001.json"
RESPONSE_URI = "s3://captures/shipping/responses/wu-1/submit_20261011_120000_000001.json"


def _one(document: dict[str, Any], capture_event: CaptureEvent | None = None) -> RawSpan:
    [raw] = parse_traces(document, capture_event)
    return raw


def _client_span(items: dict[str, Any], **kw: Any) -> dict[str, Any]:
    return request(
        resource_spans("orders", [span(TRACE, SPAN, "POST", kind=3, items=items, **kw)])
    )


def _span_with(*events: dict[str, Any]) -> dict[str, Any]:
    """One internal span holding `events`: a capture event lands on a client span's parent."""
    return request(resource_spans("orders", [span(TRACE, SPAN, "submit", events=events)]))


def _capture(request_uri: str = REQUEST_URI, response_uri: str = RESPONSE_URI) -> dict[str, Any]:
    return event(
        CAPTURE.name, {CAPTURE.request_uri: request_uri, CAPTURE.response_uri: response_uri}
    )


@pytest.mark.unit
def test_a_client_span_keeps_only_allowlisted_attributes() -> None:
    items = {
        "http.request.method": "POST",
        "url.full": "https://api.example.net/v1/orders?email=a@b.c",
        "http.response.status_code": 201,
        "server.address": "api.example.net",
        "server.port": 8443,
        "http.request.body": '{"order": 1}',
        "http.request.header.authorization": "Bearer abc",
        "net.peer.ip": "10.0.0.1",
    }
    body = event("http.response.body", {"http.response.body": "[1]", "http.response.body.size": 3})
    raw = _one(
        _client_span(items, parent=PARENT, events=[event("exception"), body], status={"code": 2})
    )

    assert (raw.trace_id, raw.span_id, raw.parent_span_id) == (TRACE, SPAN, PARENT)
    assert (raw.service, raw.kind, raw.error, raw.status_code) == (
        "orders",
        SpanKind.CLIENT,
        True,
        201,
    )
    assert (raw.start_ns, raw.end_ns) == (START_NS, START_NS + 5_000_000)
    assert (raw.request_body, raw.response_body) == ('{"order": 1}', "[1]")
    naming = raw.naming
    assert (naming.kind, naming.span_name, naming.method) == (SpanKind.CLIENT, "POST", "POST")
    assert naming.url == "https://api.example.net/v1/orders?email=a@b.c"
    assert (naming.server_address, naming.server_port) == ("api.example.net", 8443)


@pytest.mark.unit
def test_the_stable_name_is_read_before_the_old_one() -> None:
    old = {"http.method": "GET", "http.url": "https://old.example/x", "http.status_code": 404}
    raw = _one(_client_span(old))
    assert (raw.naming.method, raw.naming.url, raw.status_code) == (
        "GET",
        "https://old.example/x",
        404,
    )
    both = old | {
        "http.request.method": "POST",
        "url.full": "https://new.example/y",
        "http.response.status_code": 201,
    }
    raw = _one(_client_span(both))
    assert (raw.naming.method, raw.naming.url, raw.status_code) == (
        "POST",
        "https://new.example/y",
        201,
    )


@pytest.mark.unit
def test_server_and_aws_attributes() -> None:
    server = span(
        TRACE,
        SPAN,
        "POST /v1/orders",
        kind=2,
        items={"http.method": "POST", "http.route": "/v1/orders"},
    )
    dynamodb = span(
        TRACE,
        PARENT,
        "DynamoDB.BatchGetItem",
        kind=3,
        items={
            "rpc.system": "aws-api",
            "rpc.service": "DynamoDB",
            "rpc.method": "BatchGetItem",
            "aws.dynamodb.table_names": ["b", "a"],
        },
    )
    sns = span(
        TRACE,
        "1" * 16,
        "orders send",
        kind=4,
        items={
            "aws.sns.topic.arn": "arn:aws:sns:us-east-1:123456789012:orders",
            "messaging.destination": "arn:aws:sns:us-east-1:123456789012:orders",
            "messaging.destination.name": "arn:aws:sns:us-east-1:123456789012:orders",
            "aws.s3.bucket": "captures",
        },
    )
    raws = parse_traces(request(resource_spans("orders", [server, dynamodb, sns])))
    assert raws[0].naming.route == "/v1/orders"
    assert raws[1].naming.dynamodb_tables == ("b", "a")
    assert (raws[1].naming.rpc_system, raws[1].naming.rpc_service) == ("aws-api", "DynamoDB")
    assert raws[2].naming.sns_topic_arn == "arn:aws:sns:us-east-1:123456789012:orders"
    assert raws[2].naming.s3_bucket == "captures"
    assert raws[2].kind is SpanKind.PRODUCER


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (0, SpanKind.INTERNAL),
        (1, SpanKind.INTERNAL),
        (2, SpanKind.SERVER),
        (3, SpanKind.CLIENT),
        (4, SpanKind.PRODUCER),
        (5, SpanKind.CONSUMER),
        (9, SpanKind.INTERNAL),
        ("SPAN_KIND_UNSPECIFIED", SpanKind.INTERNAL),
        ("SPAN_KIND_SERVER", SpanKind.SERVER),
        ("SPAN_KIND_CLIENT", SpanKind.CLIENT),
        ("SPAN_KIND_PRODUCER", SpanKind.PRODUCER),
        ("SPAN_KIND_CONSUMER", SpanKind.CONSUMER),
        ("server", SpanKind.INTERNAL),
        (None, SpanKind.INTERNAL),
        ([2], SpanKind.INTERNAL),
    ],
)
def test_kinds_as_integers_or_names(kind: Any, expected: SpanKind) -> None:
    raw = _one(request(resource_spans("s", [span(TRACE, SPAN, "x", kind=kind)])))
    assert raw.kind is raw.naming.kind is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status", "error"),
    [
        ({}, False),
        ({"code": 1}, False),
        ({"code": 2}, True),
        ({"code": "STATUS_CODE_ERROR"}, True),
        ({"code": "STATUS_CODE_OK"}, False),
        ({"message": "boom"}, False),
    ],
)
def test_error_comes_from_the_status_code(status: dict[str, Any], error: bool) -> None:
    raw = _one(request(resource_spans("s", [span(TRACE, SPAN, "x", status=status)])))
    assert raw.error is error


@pytest.mark.unit
def test_leaves_of_the_wrong_type_read_as_absent() -> None:
    weird = {
        "traceId": 4,
        "spanId": ["x"],
        "parentSpanId": {},
        "name": 7,
        "startTimeUnixNano": "12a",
        "endTimeUnixNano": True,
        "attributes": [
            {"key": "http.method", "value": {"intValue": "1"}},
            {"key": "server.port", "value": {"stringValue": "8443"}},
            {"key": "http.status_code", "value": {"boolValue": True}},
            {"key": 5, "value": {"stringValue": "x"}},
            {"key": "rpc.system", "value": "aws-api"},
            {
                "key": "aws.dynamodb.table_names",
                "value": {"arrayValue": {"values": [{"intValue": "1"}]}},
            },
        ],
    }
    raw = _one(request(resource_spans("s", [weird])))
    assert (raw.trace_id, raw.span_id, raw.parent_span_id) == (None, None, None)
    assert (raw.start_ns, raw.end_ns, raw.status_code) == (None, None, None)
    assert raw.naming.span_name == ""
    assert (raw.naming.method, raw.naming.server_port, raw.naming.rpc_system) == (None, None, None)
    assert raw.naming.dynamodb_tables == ()


@pytest.mark.unit
def test_times_as_strings_or_integers_and_the_first_attribute_of_a_key_wins() -> None:
    built = span(TRACE, SPAN, "x")
    built["startTimeUnixNano"] = START_NS
    built["attributes"] = attributes({"http.route": "/a"}) + attributes({"http.route": "/b"})
    raw = _one(request(resource_spans("s", [built])))
    assert raw.start_ns == START_NS
    assert raw.naming.route == "/a"


@pytest.mark.unit
def test_each_resource_names_its_spans_service_and_links_are_read() -> None:
    first = resource_spans(
        "api", [span(TRACE, SPAN, "a", links=[link(TRACE, PARENT, {"k": "v"})])]
    )
    second = {"scopeSpans": [{"spans": [span(TRACE, PARENT, "b")]}]}
    raws = parse_traces(request(first, second))
    assert [raw.service for raw in raws] == ["api", None]
    assert raws[0].links == (RawLink(TRACE, PARENT),)
    assert raws[1].links == ()


@pytest.mark.unit
def test_only_the_first_response_body_event_is_read() -> None:
    events = [
        event("capture.written", {"acme.capture.request_uri": "requests/x.json"}),
        event("http.response.body", {"http.response.body": "first"}),
        event("http.response.body", {"http.response.body": "second"}),
    ]
    assert _one(_client_span({}, events=events)).response_body == "first"
    assert _one(_client_span({}, events=[event("exception")])).response_body is None


@pytest.mark.unit
def test_a_capture_event_gives_its_request_then_its_response_uri() -> None:
    response_first = event(
        CAPTURE.name,
        {
            CAPTURE.response_uri: RESPONSE_URI,
            "acme.capture.status_code": 201,
            CAPTURE.request_uri: REQUEST_URI,
        },
    )
    document = _span_with(response_first)
    assert _one(document, CAPTURE).capture_uris == (REQUEST_URI, RESPONSE_URI)
    assert _one(document).capture_uris == ()


@pytest.mark.unit
def test_every_capture_event_of_a_span_is_read_in_event_order() -> None:
    second = (REQUEST_URI.replace("wu-1", "wu-2"), RESPONSE_URI.replace("wu-1", "wu-2"))
    body = event("http.response.body", {"http.response.body": "[1]"})
    raw = _one(_span_with(_capture(), body, _capture(*second)), CAPTURE)
    assert raw.capture_uris == (REQUEST_URI, RESPONSE_URI, *second)
    assert raw.response_body == "[1]"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad",
    [None, "", 201, True, 1.5, [REQUEST_URI], {"uri": REQUEST_URI}],
    ids=["missing", "empty", "integer", "boolean", "double", "array", "map"],
)
def test_a_uri_that_is_missing_or_no_non_empty_string_reads_as_absent(bad: Any) -> None:
    def item(key: str) -> dict[str, Any]:
        return {} if bad is None else {key: bad}

    no_request = event(CAPTURE.name, item(CAPTURE.request_uri) | {CAPTURE.response_uri: "r"})
    no_response = event(CAPTURE.name, {CAPTURE.request_uri: "q"} | item(CAPTURE.response_uri))
    neither = event(CAPTURE.name, item(CAPTURE.request_uri) | item(CAPTURE.response_uri))
    raw = _one(_span_with(no_request, no_response, neither), CAPTURE)
    assert raw.capture_uris == ("r", "q")


@pytest.mark.unit
def test_events_of_other_names_give_no_capture_uris() -> None:
    uris = {CAPTURE.request_uri: REQUEST_URI, CAPTURE.response_uri: RESPONSE_URI}
    events = [
        event("capture.written.v2", uris),
        event("exception", uris),
        event("http.response.body", uris | {"http.response.body": "[1]"}),
        {"attributes": attributes(uris)},
        {"name": [CAPTURE.name], "attributes": attributes(uris)},
    ]
    raw = _one(_span_with(*events), CAPTURE)
    assert raw.capture_uris == ()
    assert raw.response_body == "[1]"


@pytest.mark.unit
def test_only_the_uris_of_a_capture_event_are_read_and_repr_shows_none(canary: Canary) -> None:
    request_uri = f"s3://captures/shipping/requests/{canary.token()}/submit.json"
    response_uri = f"s3://captures/shipping/responses/{canary.token()}/submit.json"
    planted = {
        "acme.capture.status_code": canary.number(),
        "acme.environment": canary.token(),
        "service.name": canary.token(),
        CAPTURE.request_uri: request_uri,
        CAPTURE.response_uri: response_uri,
    }
    events = [
        event(CAPTURE.name, planted),
        event(canary.token(), {CAPTURE.request_uri: canary.string(), "x": canary.token()}),
    ]
    raw = _one(_span_with(*events), CAPTURE)

    assert raw.capture_uris == (request_uri, response_uri)
    assert canary.leaks(repr(raw)) == []
    others = [repr(getattr(raw, item.name)) for item in fields(raw) if item.name != "capture_uris"]
    assert canary.leaks("".join(others) + repr(vars(raw.naming))) == []


@pytest.mark.unit
@pytest.mark.parametrize("document", [{}, {"resourceSpans": []}, {"resourceSpans": None}])
def test_an_empty_request_has_no_spans(document: dict[str, Any]) -> None:
    assert parse_traces(document) == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "document",
    [
        [],
        "x",
        {"resourceSpans": {}},
        {"resourceSpans": [1]},
        {"resourceSpans": [{"resource": []}]},
        {"resourceSpans": [{"scopeSpans": [{"spans": "x"}]}]},
        {"resourceSpans": [{"scopeSpans": [{"spans": [1]}]}]},
        request(resource_spans("s", [{**span(TRACE, SPAN, "x"), "attributes": {}}])),
        request(resource_spans("s", [{**span(TRACE, SPAN, "x"), "attributes": ["k"]}])),
        request(resource_spans("s", [{**span(TRACE, SPAN, "x"), "status": "ERROR"}])),
        request(resource_spans("s", [{**span(TRACE, SPAN, "x"), "events": [1]}])),
        request(resource_spans("s", [{**span(TRACE, SPAN, "x"), "links": {}}])),
    ],
    ids=[
        "list",
        "string",
        "resource-spans-object",
        "resource-spans-item",
        "resource-list",
        "spans-string",
        "span-number",
        "attributes-object",
        "attribute-string",
        "status-string",
        "event-number",
        "links-object",
    ],
)
def test_a_container_of_the_wrong_type_is_malformed(document: Any) -> None:
    with pytest.raises(ValueError):
        parse_traces(document)


@pytest.mark.unit
def test_dropped_values_never_reach_a_raw_span(canary: Canary) -> None:
    dropped = {
        "http.request.header.cookie": canary.token(),
        "http.response.header.set-cookie": canary.token(),
        "http.target": f"/v1/orders?email={canary.string()}",
        "http.host": canary.token(),
        "http.user_agent": canary.token(),
        "url.query": canary.token(),
        "net.peer.ip": canary.token(),
        "aws.queue_url": canary.string(),
        "aws.request_id": canary.token(),
        "messaging.message_id": canary.token(),
        "cloud.region": canary.token(),
        "db.statement": canary.token(),
        "acme.work_unit.id": canary.token(),
        "lambda.request_id": canary.token(),
        "http.request.body.size": canary.number(),
        "custom.list": [canary.token(), canary.number()],
        "custom.map": {"inner": canary.token()},
    }
    events = [
        event(
            "exception",
            {"exception.message": canary.token(), "exception.stacktrace": canary.token()},
        ),
        event("capture.written", {"acme.capture.request_uri": canary.string()}),
        event(canary.token(), {"x": canary.token()}),
    ]
    status = {"code": 2, "message": canary.token()}
    links = [link(TRACE, PARENT, {"writer": canary.token()})]
    spans = [
        span(TRACE, SPAN, "POST", kind=3, items=dropped, events=events, status=status, links=links)
    ]
    resource = {"service.version": canary.token(), "host.name": canary.token()}
    raw = _one(request(resource_spans("orders", spans, resource)))

    bodies = [raw.request_body, raw.response_body, raw.capture_uris]
    shown = repr(raw) + repr(vars(raw.naming)) + repr(bodies)
    assert canary.leaks(shown) == []


@dataclass
class Recorder:
    """A sink that records every call and answers with `taken` and `rejected`."""

    taken: bool = True
    rejected: int = 0
    fail: str | None = None
    batches: list[list[RawSpan]] = field(default_factory=list)
    notes: list[SpanRequestNote] = field(default_factory=list)

    def spans(self, spans: Sequence[RawSpan]) -> SpanBatchResult:
        self.batches.append(list(spans))
        if self.fail is not None:
            raise RuntimeError(self.fail)
        return SpanBatchResult(taken=self.taken, rejected=self.rejected)

    def note(self, reason: SpanRequestNote) -> None:
        self.notes.append(reason)


@contextlib.asynccontextmanager
async def _receiver(sink: Recorder, **options: Any) -> AsyncIterator[str]:
    """A receiver serving until the block ends; yields its base URL."""
    sock = listen("127.0.0.1", 0)
    receiver = OtlpReceiver(source="traces", sock=sock, shutdown_s=1.0, **options)
    assert isinstance(receiver, SpanSource)
    serving = asyncio.create_task(receiver.serve(sink))
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        serving.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await serving


async def _post(
    url: str, content: bytes, headers: dict[str, str] | None = None, path: str = "/v1/traces"
) -> httpx.Response:
    async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
        return await client.post(
            url + path, content=content, headers=JSON if headers is None else headers
        )


TWO_SPANS = json.dumps(
    request(resource_spans("orders", [span(TRACE, SPAN, "a"), span(TRACE, PARENT, "b")]))
).encode()


@pytest.mark.unit
async def test_spans_are_reported_and_answered_200() -> None:
    sink = Recorder()
    async with _receiver(sink) as url:
        response = await _post(url, TWO_SPANS)
        charset = await _post(url, TWO_SPANS, {"content-type": "Application/JSON; charset=utf-8"})
    assert (response.status_code, response.json()) == (200, {})
    assert response.headers["content-type"] == "application/json"
    assert charset.status_code == 200
    assert [[raw.naming.span_name for raw in batch] for batch in sink.batches] == [["a", "b"]] * 2
    assert sink.notes == []


@pytest.mark.unit
async def test_a_receiver_with_a_capture_event_reports_capture_uris_and_logs_none(
    canary: Canary, caplog: pytest.LogCaptureFixture, quiet_httpx: None
) -> None:
    caplog.set_level(logging.DEBUG)
    uris = (
        f"s3://captures/shipping/requests/{canary.token()}/submit.json",
        f"s3://captures/shipping/responses/{canary.token()}/submit.json",
    )
    content = json.dumps(_span_with(_capture(*uris))).encode()
    capturing, plain = Recorder(), Recorder()
    async with _receiver(capturing, capture_event=CAPTURE) as url:
        response = await _post(url, content)
    async with _receiver(plain) as url:
        await _post(url, content)
    assert response.status_code == 200
    assert [raw.capture_uris for raw in capturing.batches[0]] == [uris]
    assert [raw.capture_uris for raw in plain.batches[0]] == [()]
    assert canary.leaks(caplog.text) == []


@pytest.mark.unit
async def test_gzip_is_decoded() -> None:
    sink = Recorder()
    async with _receiver(sink) as url:
        response = await _post(url, gzip.compress(TWO_SPANS), JSON | {"content-encoding": "gzip"})
    assert response.status_code == 200
    assert len(sink.batches[0]) == 2


@pytest.mark.unit
async def test_rejected_spans_are_a_partial_success() -> None:
    async with _receiver(Recorder(rejected=2)) as url:
        response = await _post(url, TWO_SPANS)
    assert (response.status_code, response.json()) == (
        200,
        {"partialSuccess": {"rejectedSpans": 2}},
    )


@pytest.mark.unit
async def test_a_batch_the_sink_cannot_take_is_503_with_retry_after() -> None:
    sink = Recorder(taken=False)
    async with _receiver(sink) as url:
        response = await _post(url, TWO_SPANS)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert response.json()["code"] == 14


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content", "headers", "status", "note"),
    [
        (
            b"\x0a\x00",
            {"content-type": "application/x-protobuf"},
            415,
            SpanRequestNote.UNSUPPORTED_MEDIA_TYPE,
        ),
        (TWO_SPANS, {}, 415, SpanRequestNote.UNSUPPORTED_MEDIA_TYPE),
        (TWO_SPANS, {"content-type": "text/plain"}, 415, SpanRequestNote.UNSUPPORTED_MEDIA_TYPE),
        (
            TWO_SPANS,
            JSON | {"content-encoding": "br"},
            415,
            SpanRequestNote.UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            TWO_SPANS,
            JSON | {"content-encoding": "gzip, gzip"},
            415,
            SpanRequestNote.UNSUPPORTED_MEDIA_TYPE,
        ),
        (b"not json", JSON, 400, SpanRequestNote.MALFORMED_REQUEST),
        (b"[]", JSON, 400, SpanRequestNote.MALFORMED_REQUEST),
        (b'{"resourceSpans": {}}', JSON, 400, SpanRequestNote.MALFORMED_REQUEST),
        (b"[" * 4000, JSON, 400, SpanRequestNote.MALFORMED_REQUEST),
        (TWO_SPANS, JSON | {"content-encoding": "gzip"}, 400, SpanRequestNote.MALFORMED_REQUEST),
        (
            gzip.compress(TWO_SPANS)[:-4],
            JSON | {"content-encoding": "gzip"},
            400,
            SpanRequestNote.MALFORMED_REQUEST,
        ),
        (
            gzip.compress(TWO_SPANS) * 2,
            JSON | {"content-encoding": "gzip"},
            400,
            SpanRequestNote.MALFORMED_REQUEST,
        ),
        (b" " * 5000, JSON, 413, SpanRequestNote.REQUEST_TOO_LARGE),
        (
            gzip.compress(b" " * 5000),
            JSON | {"content-encoding": "gzip"},
            413,
            SpanRequestNote.REQUEST_TOO_LARGE,
        ),
    ],
    ids=[
        "protobuf",
        "no-content-type",
        "text",
        "brotli",
        "two-codings",
        "not-json",
        "array",
        "resource-spans-object",
        "too-deep",
        "gzip-header-plain-body",
        "gzip-truncated",
        "gzip-two-members",
        "too-large",
        "too-large-decoded",
    ],
)
async def test_refused_requests(
    content: bytes, headers: dict[str, str], status: int, note: SpanRequestNote
) -> None:
    sink = Recorder()
    async with _receiver(sink, max_request_bytes=4096) as url:
        response = await _post(url, content, headers)
    assert response.status_code == status
    assert response.headers["content-type"] == "application/json"
    assert set(response.json()) == {"code", "message"}
    assert sink.batches == []
    assert sink.notes == [note]


@pytest.mark.unit
async def test_other_paths_and_methods() -> None:
    sink = Recorder()
    async with _receiver(sink) as url, httpx.AsyncClient(trust_env=False) as client:
        get = await client.get(url + "/v1/traces")
        metrics = await _post(url, TWO_SPANS, path="/v1/metrics")
    assert (get.status_code, get.headers["allow"]) == (405, "POST")
    assert metrics.status_code == 404
    assert sink.batches == sink.notes == []


@pytest.mark.unit
async def test_each_refusal_is_logged_once_and_spans_received_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    sink = Recorder()
    protobuf = {"content-type": "application/x-protobuf"}
    async with _receiver(sink) as url:
        for _ in range(3):
            await _post(url, b"\x0a", protobuf)
            await _post(url, TWO_SPANS)
    assert (
        caplog.text.count(
            "request refused: source=traces status=415 reason=unsupported_media_type"
        )
        == 1
    )
    assert caplog.text.count("spans received: source=traces") == 1
    assert sink.notes == [SpanRequestNote.UNSUPPORTED_MEDIA_TYPE] * 3


@pytest.mark.unit
async def test_a_failing_sink_is_500_and_logged_by_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    async with _receiver(Recorder(fail="planted-sink-detail")) as url:
        response = await _post(url, TWO_SPANS)
    assert response.status_code == 500
    assert "request failed: source=traces error=RuntimeError" in caplog.text
    assert "planted-sink-detail" not in caplog.text


@pytest.mark.unit
async def test_no_request_data_is_logged(
    canary: Canary, caplog: pytest.LogCaptureFixture, quiet_httpx: None
) -> None:
    caplog.set_level(logging.DEBUG)
    planted = request(
        resource_spans(
            "orders",
            [
                span(
                    TRACE,
                    SPAN,
                    "x",
                    items={"http.url": canary.string()},
                    status={"message": canary.token()},
                )
            ],
        )
    )
    async with _receiver(Recorder()) as url:
        await _post(url, json.dumps(planted).encode(), JSON | {"x-planted": canary.token()})
        await _post(url, canary.token().encode(), JSON)
        await _post(
            url, b"{}", {"content-type": canary.token()}, path=f"/v1/traces?q={canary.token()}"
        )
        await _post(url, b"{}", JSON, path=f"/{canary.token()}")
    assert canary.leaks(caplog.text) == []
