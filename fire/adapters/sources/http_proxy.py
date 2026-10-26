"""Inbound HTTP proxy ExchangeSource: forwards every request to one upstream unchanged and
reports each exchange, with the route template it matched, to a sink.

Bodies stream through as they arrive; up to `max_body_bytes` of each is copied aside for the
sink, which shapes it after the response is sent. Logs hold the source id, reason codes,
counts, and exception type names; never a URL, a header value, a body byte, or the text of an
exception. Contract: docs/specs/http-and-span-capture.md#inbound-proxy.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import socket
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpcore
import httpx

from fire.adapters.manifests.openapi import OpenApiRoutes, openapi_routes
from fire.adapters.sources.listener import (
    Receive,
    Scope,
    Send,
    quiet_server,
    serve_until_cancelled,
)
from fire.core.domain.route_template import RouteTable
from fire.core.ports.exchange_source import (
    ExchangeNote,
    ExchangeSink,
    ExchangeSkipReason,
    HttpExchange,
    HttpMessage,
)

type Headers = list[tuple[bytes, bytes]]

logger = logging.getLogger(__name__)

OPENAPI_TIMEOUT_S = 10.0
# Below uvicorn's default 5 s keep-alive: the pool drops an idle connection before such an
# upstream closes it under a request.
KEEPALIVE_EXPIRY_S = 4.0
HOP_BY_HOP = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    }
)

# A `Connection` token never drops these: the upstream frames and routes a request by them.
_KEPT = frozenset({b"host", b"content-length"})
_TRACEPARENT = re.compile(r"([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?")
_DEFAULT_PORTS = {"http": 80, "https": 443}
_UNREACHABLE: Headers = [(b"content-length", b"0"), (b"x-fire-proxy", b"upstream-unreachable")]
_ABORTED: Headers = [(b"content-length", b"0"), (b"x-fire-proxy", b"upstream-aborted")]


class HttpProxy:
    """Forwards each request to `upstream` unchanged and reports each exchange to the sink.

    `upstream` is an `http` or `https` base URL; its path prefixes every forwarded target.
    Route templates come from the upstream's OpenAPI document at `openapi_path`, fetched every
    `retry_s` until it loads and every `openapi_refresh_s` after. The proxy adds a
    `traceparent` to a request without one and changes nothing else. `serve` runs once and
    closes `sock` when it stops.
    """

    def __init__(
        self,
        *,
        source: str,
        sock: socket.socket,
        upstream: str,
        openapi_path: str = "/openapi.json",
        openapi_refresh_s: float = 300.0,
        max_body_bytes: int = 1_048_576,
        retry_s: float = 5.0,
        connect_timeout_s: float = 5.0,
        shutdown_s: float = 5.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        url = httpx.URL(upstream)
        self._source = source
        self._sock = sock
        self._scheme = url.scheme.encode("ascii")
        self._host = url.raw_host
        self._port = url.port or _DEFAULT_PORTS[url.scheme]
        self._authority = url.netloc
        self._prefix = url.raw_path.partition(b"?")[0].rstrip(b"/")
        self._openapi_target = self._prefix + openapi_path.encode("ascii")
        self._refresh_s = openapi_refresh_s
        self._max_body_bytes = max_body_bytes
        self._retry_s = retry_s
        self._connect_timeout_s = connect_timeout_s
        self._shutdown_s = shutdown_s
        self._clock = clock
        self._table: RouteTable | None = None
        self._pool = httpcore.AsyncConnectionPool(
            max_connections=None, keepalive_expiry=KEEPALIVE_EXPIRY_S
        )

    async def serve(self, sink: ExchangeSink) -> None:
        """Serves until cancelled; then stops accepting, lets the exchanges in flight finish
        within `shutdown_s`, and re-raises the cancellation.
        """
        _quiet_httpcore()

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] == "http":
                await self._guarded(scope, receive, send, sink)

        server = quiet_server(app, shutdown_s=self._shutdown_s)
        templates = asyncio.create_task(self._keep_templates(sink))
        try:
            await serve_until_cancelled(server, self._sock)
        finally:
            templates.cancel()
            await asyncio.wait({templates})
            await self._pool.aclose()

    def close(self) -> None:
        """Closes the listening socket, for a proxy whose `serve` never ran."""
        self._sock.close()

    async def _keep_templates(self, sink: ExchangeSink) -> None:
        """Fetches the route templates until they load, then refreshes them."""
        loaded: tuple[int, int] | None = None
        unavailable_logged = False
        while True:
            outcome = await self._fetch_routes()
            if isinstance(outcome, OpenApiRoutes):
                self._table = RouteTable(outcome.routes)
                counts = (len(self._table), outcome.dropped)
                if counts != loaded:
                    logger.info(
                        "templates loaded: source=%s templates=%d dropped=%d",
                        self._source,
                        *counts,
                    )
                    loaded = counts
            elif self._table is None:
                if not unavailable_logged:
                    logger.warning(
                        "templates unavailable: source=%s error=%s", self._source, outcome
                    )
                    unavailable_logged = True
            else:
                sink.note(ExchangeNote.TEMPLATES_REFRESH_FAILED)
                logger.warning(
                    "templates refresh failed: source=%s error=%s", self._source, outcome
                )
            await asyncio.sleep(self._retry_s if self._table is None else self._refresh_s)

    async def _fetch_routes(self) -> OpenApiRoutes | str:
        """The upstream's routes, or why not: an exception type name or `status=<code>`."""
        url = httpcore.URL(
            scheme=self._scheme, host=self._host, port=self._port, target=self._openapi_target
        )
        timeouts = dict.fromkeys(("connect", "read", "write", "pool"), OPENAPI_TIMEOUT_S)
        try:
            response = await self._pool.request(
                "GET", url, headers=[(b"host", self._authority)], extensions={"timeout": timeouts}
            )
        except Exception as error:
            return type(error).__name__
        if response.status != 200:
            return f"status={response.status}"
        try:
            return openapi_routes(json.loads(response.content))
        except (ValueError, RecursionError) as error:
            return type(error).__name__

    async def _guarded(
        self, scope: Scope, receive: Receive, send: Send, sink: ExchangeSink
    ) -> None:
        """`_exchange`, letting nothing reach uvicorn, which would log it with its message."""
        try:
            await self._exchange(scope, receive, send, sink)
        except asyncio.CancelledError:
            # uvicorn cancels the exchanges still running when its graceful shutdown times out.
            return
        except Exception as error:
            logger.error("exchange failed: source=%s error=%s", self._source, type(error).__name__)

    async def _exchange(
        self, scope: Scope, receive: Receive, send: Send, sink: ExchangeSink
    ) -> None:
        received_at = self._clock()
        exchange_id = secrets.token_hex(16)
        received: Headers = list(scope["headers"])
        if b"websocket" in _tokens(received, b"upgrade"):
            await _Flow(send).answer(501, [(b"content-length", b"0")])
            sink.skip(ExchangeSkipReason.UPGRADE_UNSUPPORTED)
            return

        table = self._table
        template = None if table is None else table.match(scope["method"], scope["path"])
        limit = self._max_body_bytes if template is not None else 0
        flow = _Flow(send, request_copy=_Copy(limit), response_copy=_Copy(limit))
        forwarded, has_body = self._forwarded(received)
        trace_id, span_id = _trace_context(received, forwarded, sink)
        target = scope["raw_path"]
        if scope["query_string"]:
            target += b"?" + scope["query_string"]
        timeouts = {"connect": self._connect_timeout_s, "read": None, "write": None, "pool": None}
        request = httpcore.Request(
            scope["method"],
            httpcore.URL(
                scheme=self._scheme, host=self._host, port=self._port, target=self._prefix + target
            ),
            headers=forwarded,
            content=_request_body(receive, flow) if has_body else b"",
            extensions={"timeout": timeouts},
        )
        if not has_body:
            flow.body_read.set()

        try:
            outcome = await self._forward_watched(flow, request, receive)
        except asyncio.CancelledError:
            sink.skip(ExchangeSkipReason.UPSTREAM_ABORTED)
            raise
        if isinstance(outcome, ExchangeSkipReason):
            sink.skip(outcome)
        elif template is None:
            sink.skip(
                ExchangeSkipReason.TEMPLATES_UNAVAILABLE
                if table is None
                else ExchangeSkipReason.UNMATCHED_ROUTE
            )
        else:
            exchange = HttpExchange(
                exchange_id=exchange_id,
                method=scope["method"],
                template=template,
                status_code=outcome.status,
                request=flow.request_copy.message(
                    received_at, received, ended=flow.body_read.is_set()
                ),
                response=flow.response_copy.message(outcome.received_at, outcome.headers),
                trace_id=trace_id,
                span_id=span_id,
            )
            asyncio.get_running_loop().call_soon(self._report, sink, exchange)

    def _forwarded(self, received: Headers) -> tuple[Headers, bool]:
        """The request headers for the upstream, and whether a body follows them."""
        forwarded = _end_to_end(received)
        names = {name for name, _ in received}
        if b"host" not in names:
            forwarded.insert(0, (b"host", self._authority))
        if b"transfer-encoding" in names:
            forwarded.append((b"transfer-encoding", b"chunked"))
            return forwarded, True
        return forwarded, b"content-length" in names

    async def _forward_watched(
        self, flow: _Flow, request: httpcore.Request, receive: Receive
    ) -> _Head | ExchangeSkipReason:
        """`_forward`, cancelled when the client disconnects before the response completes."""
        forwarding = asyncio.create_task(self._forward(flow, request))
        watching = asyncio.create_task(_watch(receive, flow))
        try:
            await asyncio.wait({forwarding, watching}, return_when=asyncio.FIRST_COMPLETED)
            client_gone = watching.done() and watching.result()
            if client_gone:
                forwarding.cancel()
            await asyncio.wait({forwarding})
        except asyncio.CancelledError:
            forwarding.cancel()
            await asyncio.wait({forwarding})
            if not flow.started:
                await flow.answer(502, _ABORTED)
            raise
        finally:
            watching.cancel()
        if forwarding.cancelled():
            return ExchangeSkipReason.CLIENT_ABORTED
        outcome = forwarding.result()
        if client_gone and isinstance(outcome, ExchangeSkipReason):
            return ExchangeSkipReason.CLIENT_ABORTED
        return outcome

    async def _forward(self, flow: _Flow, request: httpcore.Request) -> _Head | ExchangeSkipReason:
        """Sends the request upstream and streams the response back; why not, when it fails."""
        try:
            response = await self._pool.handle_async_request(request)
        except _ClientGone:
            return ExchangeSkipReason.CLIENT_ABORTED
        except (httpcore.ConnectError, httpcore.ConnectTimeout, OSError) as error:
            self._upstream_failed("connect", error)
            await flow.answer(502, _UNREACHABLE)
            return ExchangeSkipReason.UPSTREAM_UNREACHABLE
        except Exception as error:
            self._upstream_failed("before_response", error)
            await flow.answer(502, _ABORTED)
            return ExchangeSkipReason.UPSTREAM_ABORTED
        try:
            return await self._relay(flow, response)
        except Exception as error:
            self._upstream_failed("mid_response", error)
            return ExchangeSkipReason.UPSTREAM_ABORTED
        finally:
            with contextlib.suppress(Exception):
                await response.aclose()

    async def _relay(self, flow: _Flow, response: httpcore.Response) -> _Head:
        head = _Head(received_at=self._clock(), status=response.status, headers=response.headers)
        await flow.start(head.status, _end_to_end(head.headers))
        async with contextlib.aclosing(response.aiter_stream()) as chunks:
            async for chunk in chunks:
                flow.response_copy.add(chunk)
                if chunk:
                    await flow.body(chunk, more=True)
        await flow.body(b"", more=False)
        return head

    def _upstream_failed(self, stage: str, error: Exception) -> None:
        logger.debug(
            "upstream failed: source=%s stage=%s error=%s",
            self._source,
            stage,
            type(error).__name__,
        )

    def _report(self, sink: ExchangeSink, exchange: HttpExchange) -> None:
        try:
            sink.exchange(exchange)
        except Exception as error:
            logger.warning(
                "shaping failed: source=%s error=%s", self._source, type(error).__name__
            )


class _ClientGone(Exception):
    """The client disconnected before its request body ended."""


@dataclass
class _Copy:
    """Up to `limit` bytes of a body, copied aside as it streams past."""

    limit: int = 0
    data: bytearray = field(default_factory=bytearray)
    complete: bool = True

    def add(self, chunk: bytes) -> None:
        if not self.complete:
            return
        room = self.limit - len(self.data)
        self.data += chunk[:room]
        self.complete = len(chunk) <= room

    def message(
        self, received_at: datetime, headers: Headers, *, ended: bool = True
    ) -> HttpMessage:
        """The copy as one side of an exchange; incomplete when the body did not end."""
        return HttpMessage(
            received_at=received_at,
            content_type=_header(headers, b"content-type"),
            content_encoding=_header(headers, b"content-encoding"),
            body=bytes(self.data),
            complete=self.complete and ended,
        )


@dataclass(frozen=True)
class _Head:
    """The upstream's status and raw headers, and when they arrived."""

    received_at: datetime
    status: int
    headers: Headers


@dataclass
class _Flow:
    """One exchange in flight: what the client was sent, and the body copies."""

    send: Send
    request_copy: _Copy = field(default_factory=_Copy)
    response_copy: _Copy = field(default_factory=_Copy)
    body_read: asyncio.Event = field(default_factory=asyncio.Event)
    started: bool = False
    finishing: bool = False

    async def start(self, status: int, headers: Headers) -> None:
        self.started = True
        await self.send({"type": "http.response.start", "status": status, "headers": headers})

    async def body(self, chunk: bytes, *, more: bool) -> None:
        self.finishing = not more
        await self.send({"type": "http.response.body", "body": chunk, "more_body": more})

    async def answer(self, status: int, headers: Headers) -> None:
        """A whole response with an empty body, from the proxy itself."""
        await self.start(status, headers)
        await self.body(b"", more=False)


async def _request_body(receive: Receive, flow: _Flow) -> AsyncIterator[bytes]:
    """The request body as the client sends it; raises `_ClientGone` if the client leaves."""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":
            raise _ClientGone
        chunk: bytes = message.get("body", b"")
        more = message.get("more_body", False)
        flow.request_copy.add(chunk)
        if chunk:
            yield chunk
    flow.body_read.set()


async def _watch(receive: Receive, flow: _Flow) -> bool:
    """Returns on `http.disconnect`, which uvicorn also gives once the response completed:
    True when the client left before the proxy sent the response's last message.
    """
    await flow.body_read.wait()
    while (await receive())["type"] != "http.disconnect":
        pass
    return not flow.finishing


def _trace_context(
    received: Headers, forwarded: Headers, sink: ExchangeSink
) -> tuple[str | None, str | None]:
    """The trace id and parent id the upstream receives. Appends a fresh `traceparent` to
    `forwarded` when the request has none; notes an invalid one, or more than one.
    """
    values = [value for name, value in received if name == b"traceparent"]
    if not values:
        trace_id, span_id = _random_id(16), _random_id(8)
        forwarded.append((b"traceparent", f"00-{trace_id}-{span_id}-01".encode("ascii")))
        return trace_id, span_id
    ids = _traceparent_ids(values[0]) if len(values) == 1 else None
    if ids is None:
        sink.note(ExchangeNote.INVALID_TRACEPARENT)
        return None, None
    return ids


def _traceparent_ids(value: bytes) -> tuple[str, str] | None:
    """The trace id and parent id of a valid W3C trace-context `traceparent` value."""
    match = _TRACEPARENT.fullmatch(value.decode("latin-1"))
    if match is None:
        return None
    version, trace_id, parent_id, _flags, suffix = match.groups()
    if version == "ff" or (version == "00" and suffix is not None):
        return None
    if not trace_id.strip("0") or not parent_id.strip("0"):
        return None
    return trace_id, parent_id


def _random_id(size: int) -> str:
    """`size` random bytes as hex, never all zeros, which trace context forbids."""
    value = secrets.token_hex(size)
    while not value.strip("0"):
        value = secrets.token_hex(size)
    return value


def _tokens(headers: Headers, name: bytes) -> set[bytes]:
    """The comma-separated tokens of every `name` header, lowercased."""
    return {
        token.strip().lower()
        for key, value in headers
        if key.lower() == name
        for token in value.split(b",")
    }


def _end_to_end(headers: Headers) -> Headers:
    """`headers` in order, without hop-by-hop ones (RFC 9110, section 7.6.1) and without
    `Content-Length` when `Transfer-Encoding` frames the message (RFC 9112, section 6.3).
    """
    dropped = HOP_BY_HOP | (_tokens(headers, b"connection") - _KEPT)
    if any(name.lower() == b"transfer-encoding" for name, _ in headers):
        dropped |= {b"content-length"}
    return [(name, value) for name, value in headers if name.lower() not in dropped]


def _header(headers: Headers, name: bytes) -> str | None:
    """The first `name` header's value as latin-1 text; None when absent."""
    return next((value.decode("latin-1") for key, value in headers if key.lower() == name), None)


def _quiet_httpcore() -> None:
    """httpcore logs response headers at DEBUG."""
    httpcore_logger = logging.getLogger("httpcore")
    if httpcore_logger.level < logging.INFO:
        httpcore_logger.setLevel(logging.INFO)
