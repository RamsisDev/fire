"""OTLP/HTTP JSON receiver SpanSource: takes `POST /v1/traces` from an OpenTelemetry collector
and reports each request's spans to a sink, holding only the attributes of the allowlist and,
when a capture event is set, the two URIs of each capture event.

The OTLP JSON encoding is parsed with the standard library: hex ids, integer or named enums,
and nanosecond timestamps as strings. Logs hold the source id, status codes, reason codes, and
exception type names; never an attribute, a body, a capture URI, or the text of an exception.
Contract: docs/specs/http-and-span-capture.md#otlp-receiver and #capture-events.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import zlib
from collections.abc import Mapping, Sequence
from typing import Any

from fire.adapters.sources.listener import (
    Message,
    Receive,
    Scope,
    Send,
    quiet_server,
    serve_until_cancelled,
)
from fire.core.domain.capture import CaptureEvent
from fire.core.domain.observation import SpanKind
from fire.core.domain.operation import OperationInputs
from fire.core.ports.span_source import (
    RawLink,
    RawSpan,
    SpanRequestNote,
    SpanSink,
)

logger = logging.getLogger(__name__)

TRACES_PATH = "/v1/traces"
MAX_REQUEST_BYTES = 16 * 1024 * 1024
RETRY_AFTER_S = 1
RESPONSE_BODY_EVENT = "http.response.body"

# The span attributes read; every other attribute is skipped unread.
SPAN_ATTRIBUTES = frozenset(
    {
        "http.request.method",
        "http.method",
        "http.route",
        "http.response.status_code",
        "http.status_code",
        "server.address",
        "server.port",
        "url.full",
        "http.url",
        "rpc.system",
        "rpc.service",
        "rpc.method",
        "aws.dynamodb.table_names",
        "aws.sns.topic.arn",
        "aws.s3.bucket",
        "messaging.destination.name",
        "messaging.destination",
        "http.request.body",
    }
)
RESOURCE_ATTRIBUTES = frozenset({"service.name"})
EVENT_ATTRIBUTES = frozenset({RESPONSE_BODY_EVENT})

_KINDS: dict[int | str, SpanKind] = {
    2: SpanKind.SERVER,
    3: SpanKind.CLIENT,
    4: SpanKind.PRODUCER,
    5: SpanKind.CONSUMER,
    "SPAN_KIND_SERVER": SpanKind.SERVER,
    "SPAN_KIND_CLIENT": SpanKind.CLIENT,
    "SPAN_KIND_PRODUCER": SpanKind.PRODUCER,
    "SPAN_KIND_CONSUMER": SpanKind.CONSUMER,
}
_STATUS_ERROR = (2, "STATUS_CODE_ERROR")
_JSON = (b"content-type", b"application/json")

type Headers = Sequence[tuple[bytes, bytes]]


class OtlpReceiver:
    """Serves `POST /v1/traces` on `sock` and reports each request's spans to the sink.

    `application/json`, plain or gzip, up to `max_request_bytes` as sent and once decoded.
    Protobuf or another coding gets `415`, a body that is not an `ExportTraceServiceRequest`
    `400`, a larger one `413`, and a batch the sink has no room for `503` with `Retry-After`;
    otherwise `200`. With `capture_event`, each span's events of that name give its
    `capture_uris`. `serve` runs once and closes `sock` when it stops.
    """

    def __init__(
        self,
        *,
        source: str,
        sock: socket.socket,
        shutdown_s: float = 5.0,
        max_request_bytes: int = MAX_REQUEST_BYTES,
        capture_event: CaptureEvent | None = None,
    ) -> None:
        self._source = source
        self._sock = sock
        self._shutdown_s = shutdown_s
        self._max_request_bytes = max_request_bytes
        self._capture_event = capture_event
        self._refusals_logged: set[SpanRequestNote] = set()
        self._receiving = False

    async def serve(self, sink: SpanSink) -> None:
        """Serves until cancelled; then stops accepting, lets the requests in flight finish
        within `shutdown_s`, and re-raises the cancellation.
        """

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] == "http":
                await self._guarded(scope, receive, send, sink)

        await serve_until_cancelled(quiet_server(app, shutdown_s=self._shutdown_s), self._sock)

    def close(self) -> None:
        """Closes the listening socket, for a receiver whose `serve` never ran."""
        self._sock.close()

    async def _guarded(self, scope: Scope, receive: Receive, send: Send, sink: SpanSink) -> None:
        """`_request`, letting nothing reach uvicorn, which would log it with its message."""
        response = _Response(send)
        try:
            await self._request(scope, receive, response, sink)
        except (asyncio.CancelledError, _ClientGone):
            return
        except Exception as error:
            logger.error("request failed: source=%s error=%s", self._source, type(error).__name__)
            if not response.started:
                await response.status(500, 13, "internal error")

    async def _request(
        self, scope: Scope, receive: Receive, response: _Response, sink: SpanSink
    ) -> None:
        if scope["path"] != TRACES_PATH:
            await response.status(404, 5, "not found")
            return
        if scope["method"] != "POST":
            await response.status(405, 12, "use POST", [(b"allow", b"POST")])
            return
        received = scope["headers"]
        coding = (_header(received, b"content-encoding") or "identity").strip().lower()
        if _media_type(received) != "application/json" or coding not in {"identity", "gzip"}:
            await self._refuse(response, sink, SpanRequestNote.UNSUPPORTED_MEDIA_TYPE)
            return
        body = await _read_body(receive, self._max_request_bytes)
        try:
            if body is not None and coding == "gzip":
                body = _gunzip(body, self._max_request_bytes)
        except ValueError:
            await self._refuse(response, sink, SpanRequestNote.MALFORMED_REQUEST)
            return
        if body is None:
            await self._refuse(response, sink, SpanRequestNote.REQUEST_TOO_LARGE)
            return
        try:
            spans = parse_traces(json.loads(body), self._capture_event)
        except (ValueError, RecursionError):
            await self._refuse(response, sink, SpanRequestNote.MALFORMED_REQUEST)
            return
        result = sink.spans(spans)
        if not result.taken:
            retry = [(b"retry-after", str(RETRY_AFTER_S).encode("ascii"))]
            await response.status(503, 14, "buffer full, retry later", retry)
            return
        if not self._receiving and spans:
            self._receiving = True
            logger.info("spans received: source=%s", self._source)
        if result.rejected:
            await response.json(200, {"partialSuccess": {"rejectedSpans": result.rejected}})
        else:
            await response.json(200, {})

    async def _refuse(self, response: _Response, sink: SpanSink, note: SpanRequestNote) -> None:
        status = _REFUSALS[note]
        sink.note(note)
        if note not in self._refusals_logged:
            self._refusals_logged.add(note)
            logger.warning(
                "request refused: source=%s status=%d reason=%s", self._source, status[0], note
            )
        await response.status(*status)


_REFUSALS: dict[SpanRequestNote, tuple[int, int, str]] = {
    SpanRequestNote.UNSUPPORTED_MEDIA_TYPE: (
        415,
        3,
        "send OTLP JSON: Content-Type application/json, plain or gzip",
    ),
    SpanRequestNote.MALFORMED_REQUEST: (400, 3, "not an OTLP JSON ExportTraceServiceRequest"),
    SpanRequestNote.REQUEST_TOO_LARGE: (413, 8, "request too large"),
}


class _ClientGone(Exception):
    """The client disconnected before its request body ended."""


class _Response:
    """One response, sent whole."""

    def __init__(self, send: Send) -> None:
        self._send = send
        self.started = False

    async def json(self, status: int, body: Mapping[str, Any], headers: Headers = ()) -> None:
        content = json.dumps(body, separators=(",", ":")).encode()
        self.started = True
        length = (b"content-length", str(len(content)).encode("ascii"))
        await self._send(
            {"type": "http.response.start", "status": status, "headers": [_JSON, length, *headers]}
        )
        await self._send({"type": "http.response.body", "body": content})

    async def status(self, status: int, code: int, message: str, headers: Headers = ()) -> None:
        """An error status with a `google.rpc.Status` body, as OTLP/HTTP asks."""
        await self.json(status, {"code": code, "message": message}, headers)


async def _read_body(receive: Receive, limit: int) -> bytes | None:
    """The request body; None as soon as it passes `limit` bytes."""
    body = bytearray()
    more = True
    while more:
        message: Message = await receive()
        if message["type"] == "http.disconnect":
            raise _ClientGone
        body += message.get("body", b"")
        if len(body) > limit:
            return None
        more = message.get("more_body", False)
    return bytes(body)


def _gunzip(body: bytes, limit: int) -> bytes | None:
    """The gzip stream decoded; None when it passes `limit` bytes. Raises ValueError when it is
    corrupt, truncated, or followed by more bytes.
    """
    inflater = zlib.decompressobj(zlib.MAX_WBITS | 16)
    try:
        decoded = inflater.decompress(body, limit + 1)
    except zlib.error:
        raise ValueError("not a gzip stream") from None
    if len(decoded) > limit:
        return None
    if not inflater.eof or inflater.unused_data:
        raise ValueError("not one whole gzip stream")
    return decoded


def _header(headers: list[tuple[bytes, bytes]], name: bytes) -> str | None:
    return next((value.decode("latin-1") for key, value in headers if key.lower() == name), None)


def _media_type(headers: list[tuple[bytes, bytes]]) -> str | None:
    content_type = _header(headers, b"content-type")
    return None if content_type is None else content_type.split(";", 1)[0].strip().lower()


def parse_traces(document: Any, capture_event: CaptureEvent | None = None) -> list[RawSpan]:
    """The spans of an OTLP JSON `ExportTraceServiceRequest`.

    With `capture_event`, a span's events of its name give the span's `capture_uris`; without
    it, those events are skipped unread. Raises ValueError when a container (the request,
    `resourceSpans`, `resource`, `scopeSpans`, `spans`, `attributes`, `events`, `links`,
    `status`, or an attribute) has the wrong JSON type. A leaf of the wrong type, such as a
    numeric `traceId`, reads as absent.
    """
    spans: list[RawSpan] = []
    for resource_spans in _objects(_object(document), "resourceSpans"):
        resource = _attributes(_object(resource_spans.get("resource")), RESOURCE_ATTRIBUTES)
        service = _string(resource.get("service.name"))
        for scope_spans in _objects(resource_spans, "scopeSpans"):
            spans.extend(
                _span(span, service, capture_event) for span in _objects(scope_spans, "spans")
            )
    return spans


def _span(
    span: Mapping[str, Any], service: str | None, capture_event: CaptureEvent | None
) -> RawSpan:
    attributes = _attributes(span, SPAN_ATTRIBUTES)
    kind_value = span.get("kind")
    kind = (
        _KINDS.get(kind_value, SpanKind.INTERNAL) if _hashable(kind_value) else SpanKind.INTERNAL
    )
    return RawSpan(
        trace_id=_string(span.get("traceId")),
        span_id=_string(span.get("spanId")),
        parent_span_id=_string(span.get("parentSpanId")),
        service=service,
        kind=kind,
        start_ns=_integer(span.get("startTimeUnixNano")),
        end_ns=_integer(span.get("endTimeUnixNano")),
        error=_object(span.get("status")).get("code") in _STATUS_ERROR,
        naming=OperationInputs(
            kind=kind,
            span_name=_string(span.get("name")) or "",
            method=_first_string(attributes, "http.request.method", "http.method"),
            route=_string(attributes.get("http.route")),
            server_address=_string(attributes.get("server.address")),
            server_port=_int(attributes.get("server.port")),
            url=_first_string(attributes, "url.full", "http.url"),
            rpc_system=_string(attributes.get("rpc.system")),
            rpc_service=_string(attributes.get("rpc.service")),
            rpc_method=_string(attributes.get("rpc.method")),
            dynamodb_tables=_strings(attributes.get("aws.dynamodb.table_names")),
            sns_topic_arn=_string(attributes.get("aws.sns.topic.arn")),
            messaging_destination_name=_string(attributes.get("messaging.destination.name")),
            messaging_destination=_string(attributes.get("messaging.destination")),
            s3_bucket=_string(attributes.get("aws.s3.bucket")),
        ),
        status_code=_first_int(attributes, "http.response.status_code", "http.status_code"),
        links=tuple(
            RawLink(_string(link.get("traceId")), _string(link.get("spanId")))
            for link in _objects(span, "links")
        ),
        request_body=_string(attributes.get("http.request.body")),
        response_body=_response_body(span),
        capture_uris=_capture_uris(span, capture_event),
    )


def _response_body(span: Mapping[str, Any]) -> str | None:
    """The first `http.response.body` event's body; every other event is skipped unread."""
    for event in _objects(span, "events"):
        if event.get("name") == RESPONSE_BODY_EVENT:
            return _string(_attributes(event, EVENT_ATTRIBUTES).get(RESPONSE_BODY_EVENT))
    return None


def _capture_uris(span: Mapping[str, Any], capture_event: CaptureEvent | None) -> tuple[str, ...]:
    """The non-empty string URIs of each event named `capture_event.name`, request then
    response, in event order. Every other attribute of those events, and every event of another
    name, is skipped unread.
    """
    if capture_event is None:
        return ()
    keys = (capture_event.request_uri, capture_event.response_uri)
    allowed = frozenset(keys)
    uris: list[str] = []
    for event in _objects(span, "events"):
        if event.get("name") == capture_event.name:
            found = _attributes(event, allowed)
            uris.extend(uri for key in keys if (uri := _string(found.get(key))))
    return tuple(uris)


def _object(value: Any) -> Mapping[str, Any]:
    """A JSON object; absent or null reads as empty."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def _objects(container: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    """A list of JSON objects at `key`; absent or null reads as empty."""
    value = container.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{key} is not a list of objects")
    return value


def _attributes(container: Mapping[str, Any], keys: frozenset[str]) -> dict[str, Any]:
    """The decoded values of `keys` among the container's attributes; the first of a key wins."""
    found: dict[str, Any] = {}
    for attribute in _objects(container, "attributes"):
        key = attribute.get("key")
        if isinstance(key, str) and key in keys and key not in found:
            found[key] = _any_value(attribute.get("value"))
    return found


def _any_value(value: Any) -> Any:
    """An OTLP `AnyValue`: a string, an integer, a boolean, a number, or a tuple of them."""
    if not isinstance(value, dict):
        return None
    if "stringValue" in value:
        return _string(value["stringValue"])
    if "intValue" in value:
        return _integer(value["intValue"])
    if "boolValue" in value:
        return value["boolValue"] if isinstance(value["boolValue"], bool) else None
    if "doubleValue" in value:
        number = value["doubleValue"]
        return number if isinstance(number, int | float) and not isinstance(number, bool) else None
    if "arrayValue" in value:
        values = _object(value["arrayValue"]).get("values")
        return tuple(_any_value(item) for item in values) if isinstance(values, list) else None
    return None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _int(value: Any) -> int | None:
    """A decoded attribute's integer: only an `intValue` gives one."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _integer(value: Any) -> int | None:
    """An integer, or a string of decimal digits as OTLP JSON writes 64-bit integers."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.lstrip("-").isascii() and value.lstrip("-").isdigit():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, tuple):
        return tuple(item for item in value if isinstance(item, str))
    return ()


def _first_string(attributes: Mapping[str, Any], *keys: str) -> str | None:
    """The first key's value that is a non-empty string: the stable name before the old one."""
    return next((value for key in keys if (value := _string(attributes.get(key)))), None)


def _first_int(attributes: Mapping[str, Any], *keys: str) -> int | None:
    return next((value for key in keys if (value := _int(attributes.get(key))) is not None), None)


def _hashable(value: Any) -> bool:
    return isinstance(value, int | str)
