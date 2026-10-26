"""HttpProxy: byte-for-byte forwarding, route templates, trace context, skips, and privacy.

A fake FastAPI upstream runs with uvicorn on the loopback for the whole module; each test runs
its own proxy in front of it with a recording sink.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import logging
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx
import pytest
import pytest_asyncio
import uvicorn
from canary import Canary
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from uvicorn.protocols.http.h11_impl import H11Protocol

from fire.adapters.sources.http_proxy import HttpProxy
from fire.adapters.sources.listener import listen
from fire.core.ports.exchange_source import (
    ExchangeNote,
    ExchangeSkipReason,
    ExchangeSource,
    HttpExchange,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")

SOURCE = "api"
HOST = "shop.example:8443"
MAX_BODY = 1024
T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"
VALID = f"00-{TRACE_ID}-{PARENT_ID}-01"
FRESH = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-01")
GZIPPED = gzip.compress(b'{"zipped": true}', mtime=0)
STREAMED = [b"[", b'{"chunk": 0}', b",", b'{"chunk": 1}', b"]"]
ORDER = json.dumps({"customer": {"email": "a@b.c"}, "items": [{"sku": "s", "qty": 1}]}).encode()
LARGE_ORDER = json.dumps({"customer": {"note": "y" * 3000}}).encode()
TEMPLATES = 13
PREFIX = "/base"
RAW_BODY = b'{"raw": true}'
# Header names as an upstream that does not lowercase them sends them, repeats included.
RAW_HEADERS = [
    (b"Content-Type", b"application/json"),
    (b"X-Mixed-Case", b"Value"),
    (b"Set-Cookie", b"a=1"),
    (b"set-cookie", b"b=2"),
    (b"Content-Length", str(len(RAW_BODY)).encode()),
]
# uvicorn's TRACE level, below DEBUG, at which it can log every ASGI scope and message.
TRACE = 5


async def _chunks(parts: list[bytes]) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


class Upstream:
    """The fake upstream: FastAPI routes, and switches a test flips."""

    def __init__(self) -> None:
        self.app = _routes(self)
        self.port = 0
        self.reset()

    def reset(self) -> None:
        self.openapi_status = 200
        self.slow_started = asyncio.Event()
        self.release = asyncio.Event()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("path", "").startswith(PREFIX + "/"):
            prefix = len(PREFIX)
            scope = scope | {
                "path": scope["path"][prefix:],
                "raw_path": scope["raw_path"][prefix:],
            }
        if scope.get("path") == "/openapi.json" and self.openapi_status != 200:
            await self._answer(send, self.openapi_status, [(b"content-length", b"0")], b"")
        elif scope.get("path") == "/v1/raw":
            await self._answer(send, 200, RAW_HEADERS, RAW_BODY)
        else:
            await self.app(scope, receive, send)

    @staticmethod
    async def _answer(
        send: Any, status: int, headers: list[tuple[bytes, bytes]], body: bytes
    ) -> None:
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


def _routes(upstream: Upstream) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/orders/checkout")
    async def checkout(request: Request) -> JSONResponse:
        return JSONResponse({"order": await request.json(), "status": "placed"}, status_code=201)

    @app.get("/v1/orders/search")
    async def search() -> dict[str, Any]:
        return {"results": [], "count": 0}

    @app.get("/v1/orders/{order_id}")
    async def get_order(order_id: str) -> JSONResponse:
        order = {"orderId": order_id, "items": [{"sku": order_id}]}
        return JSONResponse(order, headers={"x-order-id": order_id})

    @app.head("/v1/orders/{order_id}")
    async def head_order(order_id: str) -> dict[str, Any]:
        return {"orderId": order_id, "items": [{"sku": order_id}]}

    @app.delete("/v1/orders/{order_id}", status_code=204)
    async def delete_order(order_id: str) -> Response:
        return Response(status_code=204)

    @app.get("/v1/text")
    async def text() -> PlainTextResponse:
        return PlainTextResponse("plain text, not JSON")

    @app.get("/v1/gzip")
    async def gzipped() -> Response:
        return Response(
            GZIPPED, media_type="application/json", headers={"content-encoding": "gzip"}
        )

    @app.get("/v1/stream")
    async def stream() -> StreamingResponse:
        return StreamingResponse(_chunks(STREAMED), media_type="application/json")

    @app.get("/v1/large")
    async def large() -> dict[str, Any]:
        return {"items": ["x" * 100] * 50}

    @app.get("/v1/broken")
    async def broken() -> StreamingResponse:
        async def first_chunk_then_fail() -> AsyncIterator[bytes]:
            yield b'{"partial": '
            raise RuntimeError("the fake upstream stream broke")

        return StreamingResponse(first_chunk_then_fail(), media_type="application/json")

    @app.get("/v1/slow")
    async def slow() -> dict[str, bool]:
        upstream.slow_started.set()
        await upstream.release.wait()
        return {"slow": True}

    @app.get("/v1/trickle")
    async def trickle() -> StreamingResponse:
        async def first_chunk_then_wait() -> AsyncIterator[bytes]:
            yield b'{"first": 1, '
            upstream.slow_started.set()
            await upstream.release.wait()
            yield b'"last": 2}'

        return StreamingResponse(first_chunk_then_wait(), media_type="application/json")

    @app.get("/v1/echo/headers")
    async def echo_headers(request: Request) -> list[list[str]]:
        return [
            [name.decode("latin-1"), value.decode("latin-1")]
            for name, value in request.headers.raw
        ]

    return app


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class _QuietH11(H11Protocol):
    """The upstream's protocol without an access log, so only the proxy's logs are checked."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.access_log = False


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def upstream_server() -> AsyncIterator[Upstream]:
    upstream = Upstream()
    sock = listen("127.0.0.1", 0)
    upstream.port = sock.getsockname()[1]
    config = uvicorn.Config(
        upstream,
        interface="asgi3",
        http=_QuietH11,
        ws="none",
        lifespan="off",
        access_log=False,
        log_config=None,
        date_header=False,
        timeout_graceful_shutdown=1,
    )
    server = _Server(config)
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    await _until(lambda: server.started)
    yield upstream
    upstream.release.set()
    server.should_exit = True
    await serving


@pytest.fixture
def upstream(upstream_server: Upstream) -> Iterator[Upstream]:
    upstream_server.reset()
    yield upstream_server
    upstream_server.release.set()


@dataclass
class Recorder:
    """A sink that records every call; with `fail`, `exchange` raises it after recording."""

    fail: str | None = None
    exchanges: list[HttpExchange] = field(default_factory=list)
    skips: list[ExchangeSkipReason] = field(default_factory=list)
    notes: list[ExchangeNote] = field(default_factory=list)

    def exchange(self, exchange: HttpExchange) -> None:
        self.exchanges.append(exchange)
        if self.fail is not None:
            raise RuntimeError(self.fail)

    def skip(self, reason: ExchangeSkipReason) -> None:
        self.skips.append(reason)

    def note(self, reason: ExchangeNote) -> None:
        self.notes.append(reason)

    @property
    def reports(self) -> int:
        return len(self.exchanges) + len(self.skips)

    def clear(self) -> None:
        self.exchanges.clear()
        self.skips.clear()
        self.notes.clear()


class Clock:
    """One second later on every call, from T0."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        return T0 + timedelta(seconds=self.calls)


async def _until(condition: Callable[[], object], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


async def _warm_up(url: str, sink: Recorder) -> None:
    """Waits until the proxy routes by templates, then forgets the warm-up's reports."""
    async with httpx.AsyncClient(trust_env=False) as client, asyncio.timeout(5):
        while not sink.exchanges:
            sink.clear()
            await client.get(f"{url}/v1/orders/search")
            await _until(lambda: sink.reports)
    sink.clear()


@contextlib.asynccontextmanager
async def _proxy(
    upstream: str, sink: Recorder, *, warm: bool = True, **options: Any
) -> AsyncIterator[str]:
    """A proxy in front of `upstream`, serving until the block ends; yields its base URL."""
    sock = listen("127.0.0.1", 0)
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    settings = {"max_body_bytes": MAX_BODY, "retry_s": 0.05, "shutdown_s": 1.0} | options
    serving = asyncio.create_task(
        HttpProxy(source=SOURCE, sock=sock, upstream=upstream, **settings).serve(sink)
    )
    try:
        if warm:
            await _warm_up(url, sink)
        yield url
    finally:
        serving.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await serving


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(trust_env=False, timeout=5)


def _port(url: str) -> int:
    return int(url.rsplit(":", 1)[1])


def _closed_port() -> int:
    sock = listen("127.0.0.1", 0)
    port = sock.getsockname()[1]
    sock.close()
    return port


type Raw = tuple[int, list[tuple[bytes, bytes]], bytes]


async def _fetch(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: Any = None,
    body: bytes | None = None,
    chunked: bool = False,
) -> Raw:
    """Status, raw header list, and raw body bytes, neither content-decoded nor normalized."""
    content: Any = body
    if body is not None and chunked:
        third = len(body) // 3
        content = _chunks([body[:third], body[third : 2 * third], body[2 * third :]])
    async with client.stream(method, url, headers=headers, content=content) as response:
        raw = b"".join([chunk async for chunk in response.aiter_raw()])
        return response.status_code, list(response.headers.raw), raw


async def _reported(
    sink: Recorder, request: Awaitable[httpx.Response]
) -> tuple[httpx.Response, HttpExchange]:
    """The response to `request` and the exchange the proxy reported for it."""
    before = len(sink.exchanges)
    response = await request
    await _until(lambda: len(sink.exchanges) > before)
    return response, sink.exchanges[-1]


def _request(method: str, target: str, headers: list[tuple[str, str]], body: bytes = b"") -> bytes:
    """An HTTP/1.1 request that asks the server to close the connection after answering."""
    lines = [f"{method} {target} HTTP/1.1", f"Host: {HOST}", "Connection: close"]
    lines += [f"{name}: {value}" for name, value in headers]
    if body:
        lines.append(f"Content-Length: {len(body)}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body


async def _raw(port: int, request: bytes) -> bytes:
    """Sends `request` on a fresh connection and reads until the server closes it."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(request)
    await writer.drain()
    try:
        return await asyncio.wait_for(reader.read(), 5)
    finally:
        writer.close()
        await writer.wait_closed()


@dataclass(frozen=True)
class Case:
    name: str
    method: str
    path: str
    body: bytes | None = None
    chunked: bool = False

    @property
    def headers(self) -> dict[str, str]:
        if self.body is None:
            return {"host": HOST}
        return {"host": HOST, "content-type": "application/json"}


CASES = [
    Case("json", "POST", "/v1/orders/checkout", ORDER),
    Case("chunked request", "POST", "/v1/orders/checkout", ORDER, chunked=True),
    Case("oversized request", "POST", "/v1/orders/checkout", LARGE_ORDER),
    Case("not json", "GET", "/v1/text"),
    Case("gzip", "GET", "/v1/gzip"),
    Case("empty 204", "DELETE", "/v1/orders/SO-1"),
    Case("streamed", "GET", "/v1/stream"),
    Case("oversized response", "GET", "/v1/large"),
    Case("head", "HEAD", "/v1/orders/SO-1"),
    Case("raw header names", "GET", "/v1/raw"),
    Case("unmatched", "GET", "/v1/unknown/route"),
]


@pytest.mark.unit
async def test_listen_binds_a_free_port_and_refuses_a_busy_one() -> None:
    sock = listen("127.0.0.1", 0)
    try:
        port = sock.getsockname()[1]
        assert port > 0
        with pytest.raises(OSError):
            listen("127.0.0.1", port)
    finally:
        sock.close()


@pytest.mark.unit
async def test_is_an_exchange_source_whose_close_releases_the_socket() -> None:
    sock = listen("127.0.0.1", 0)
    proxy = HttpProxy(source=SOURCE, sock=sock, upstream="http://x")
    assert isinstance(proxy, ExchangeSource)
    proxy.close()
    assert sock.fileno() == -1


@pytest.mark.unit
@pytest.mark.parametrize("sink_raises", [False, True], ids=["sink-ok", "sink-raises"])
async def test_clients_get_the_same_status_headers_and_body_bytes(
    upstream: Upstream, canary: Canary, caplog: pytest.LogCaptureFixture, sink_raises: bool
) -> None:
    caplog.set_level(logging.DEBUG)
    secret = canary.token()
    sink = Recorder(fail=secret if sink_raises else None)
    async with _proxy(upstream.url, sink) as url, _client() as client:
        for case in CASES:
            options = {"headers": case.headers, "body": case.body, "chunked": case.chunked}
            direct = await _fetch(client, case.method, upstream.url + case.path, **options)
            proxied = await _fetch(client, case.method, url + case.path, **options)
            assert proxied == direct, case.name
        await _until(lambda: sink.reports == len(CASES))
    assert len(sink.exchanges) == len(CASES) - 2
    assert sink.skips == [ExchangeSkipReason.UNMATCHED_ROUTE] * 2
    assert ("shaping failed: source=api error=RuntimeError" in caplog.text) is sink_raises
    assert secret not in caplog.text
    assert "Exception in ASGI application" not in caplog.text


@pytest.mark.unit
async def test_exchanges_carry_the_template_status_bodies_and_trace_ids(
    upstream: Upstream,
) -> None:
    sink = Recorder()
    json_headers = {"content-type": "application/json"}
    async with _proxy(upstream.url + "/", sink, clock=Clock()) as url, _client() as client:
        traced = {**json_headers, "traceparent": VALID}
        response, placed = await _reported(
            sink, client.post(f"{url}/v1/orders/checkout", content=ORDER, headers=traced)
        )
        assert placed.method == "POST"
        assert placed.template == "/v1/orders/checkout"
        assert placed.status_code == 201
        assert re.fullmatch(r"[0-9a-f]{32}", placed.exchange_id)
        assert (placed.trace_id, placed.span_id) == (TRACE_ID, PARENT_ID)
        assert placed.request.body == ORDER
        assert placed.request.complete
        assert placed.request.content_type == "application/json"
        assert placed.request.content_encoding is None
        assert placed.response.body == response.content
        assert placed.response.complete
        assert placed.response.content_type == "application/json"
        assert T0 < placed.request.received_at < placed.response.received_at

        _, chunked = await _reported(
            sink,
            client.post(
                f"{url}/v1/orders/checkout",
                content=_chunks([ORDER[:9], ORDER[9:]]),
                headers=json_headers,
            ),
        )
        assert (chunked.request.body, chunked.request.complete) == (ORDER, True)

        _, concrete = await _reported(sink, client.get(f"{url}/v1/orders/SO-1234567"))
        assert concrete.template == "/v1/orders/{order_id}"
        assert "SO-1234567" not in repr(concrete)

        _, literal = await _reported(sink, client.get(f"{url}/v1/orders/search"))
        assert literal.template == "/v1/orders/search"

        _, zipped = await _reported(sink, client.get(f"{url}/v1/gzip"))
        assert (zipped.response.content_encoding, zipped.response.body) == ("gzip", GZIPPED)

        _, streamed = await _reported(sink, client.get(f"{url}/v1/stream"))
        assert (streamed.response.body, streamed.response.complete) == (b"".join(STREAMED), True)

        response, large = await _reported(sink, client.get(f"{url}/v1/large"))
        assert large.response.body == response.content[:MAX_BODY]
        assert not large.response.complete

        _, large_request = await _reported(
            sink,
            client.post(f"{url}/v1/orders/checkout", content=LARGE_ORDER, headers=json_headers),
        )
        assert large_request.request.body == LARGE_ORDER[:MAX_BODY]
        assert not large_request.request.complete

        _, deleted = await _reported(sink, client.delete(f"{url}/v1/orders/SO-1"))
        assert (deleted.status_code, deleted.response.body, deleted.response.complete) == (
            204,
            b"",
            True,
        )

        _, head = await _reported(sink, client.head(f"{url}/v1/orders/SO-1"))
        assert (head.method, head.template, head.response.body) == (
            "HEAD",
            "/v1/orders/{order_id}",
            b"",
        )
    assert sink.skips == []
    assert sink.notes == []


@pytest.mark.unit
async def test_unreachable_upstream_gives_an_empty_502_and_no_observation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    sink = Recorder()
    upstream = f"http://127.0.0.1:{_closed_port()}"
    async with _proxy(upstream, sink, warm=False) as url, _client() as client:
        status, headers, body = await _fetch(client, "GET", f"{url}/v1/orders/search")
        assert status == 502
        assert headers == [(b"content-length", b"0"), (b"x-fire-proxy", b"upstream-unreachable")]
        assert body == b""
        await _until(lambda: sink.reports)
        await asyncio.sleep(0.2)
    assert sink.skips == [ExchangeSkipReason.UPSTREAM_UNREACHABLE]
    assert caplog.text.count("templates unavailable: source=api error=ConnectError") == 1


@pytest.mark.unit
async def test_upstream_closing_before_answering_gives_502_upstream_aborted() -> None:
    async def hang_up(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(Exception):
            await reader.readuntil(b"\r\n\r\n")
        writer.close()

    server = await asyncio.start_server(hang_up, "127.0.0.1", 0)
    sink = Recorder()
    upstream = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    async with server, _proxy(upstream, sink, warm=False) as url, _client() as client:
        status, headers, body = await _fetch(client, "GET", f"{url}/v1/orders/search")
        assert (status, headers, body) == (
            502,
            [(b"content-length", b"0"), (b"x-fire-proxy", b"upstream-aborted")],
            b"",
        )
        await _until(lambda: sink.reports)
    assert sink.skips == [ExchangeSkipReason.UPSTREAM_ABORTED]


@pytest.mark.unit
async def test_a_request_without_traceparent_gets_a_fresh_one_last(upstream: Upstream) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url, _client() as client:
        ids = []
        for _ in range(2):
            response, exchange = await _reported(sink, client.get(f"{url}/v1/echo/headers"))
            seen = response.json()
            name, value = seen[-1]
            match = FRESH.fullmatch(value)
            assert name == "traceparent"
            assert match is not None
            assert [name for name, _ in seen].count("traceparent") == 1
            assert (exchange.trace_id, exchange.span_id) == match.groups()
            ids.append(match.groups())
    assert ids[0] != ids[1]
    assert sink.notes == []


@pytest.mark.unit
@pytest.mark.parametrize("value", [VALID, f"01-{TRACE_ID}-{PARENT_ID}-00-future"])
async def test_a_valid_traceparent_is_forwarded_unchanged_and_read(
    upstream: Upstream, value: str
) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url, _client() as client:
        response, exchange = await _reported(
            sink, client.get(f"{url}/v1/echo/headers", headers={"traceparent": value})
        )
    assert [seen for seen in response.json() if seen[0] == "traceparent"] == [
        ["traceparent", value]
    ]
    assert (exchange.trace_id, exchange.span_id) == (TRACE_ID, PARENT_ID)
    assert sink.notes == []


INVALID = [
    [("traceparent", "not-a-traceparent")],
    [("traceparent", f"00-{'0' * 32}-{PARENT_ID}-01")],
    [("traceparent", f"00-{TRACE_ID}-{'0' * 16}-01")],
    [("traceparent", f"ff-{TRACE_ID}-{PARENT_ID}-01")],
    [("traceparent", f"00-{TRACE_ID}-{PARENT_ID}-01-suffix")],
    [("traceparent", f"00-{TRACE_ID.upper()}-{PARENT_ID}-01")],
    [("traceparent", VALID), ("traceparent", f"00-{TRACE_ID}-{'1' * 16}-01")],
]


@pytest.mark.unit
async def test_an_invalid_or_repeated_traceparent_is_forwarded_unchanged_and_noted(
    upstream: Upstream,
) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url, _client() as client:
        for headers in INVALID:
            response, exchange = await _reported(
                sink, client.get(f"{url}/v1/echo/headers", headers=headers)
            )
            seen = [tuple(pair) for pair in response.json() if pair[0] == "traceparent"]
            assert seen == headers
            assert (exchange.trace_id, exchange.span_id) == (None, None)
    assert sink.notes == [ExchangeNote.INVALID_TRACEPARENT] * len(INVALID)


@pytest.mark.unit
async def test_forwarded_headers_lose_hop_by_hop_ones_and_gain_nothing(
    upstream: Upstream,
) -> None:
    sink = Recorder()
    headers = [
        ("host", HOST),
        ("x-keep", "kept"),
        ("connection", "x-custom"),
        ("x-custom", "dropped"),
        ("keep-alive", "timeout=5"),
        ("te", "trailers"),
        ("traceparent", VALID),
    ]
    h2c = [
        ("connection", "Upgrade, HTTP2-Settings"),
        ("upgrade", "h2c"),
        ("http2-settings", "AAMA"),
    ]
    async with _proxy(upstream.url, sink) as url, _client() as client:
        for sent in (headers, [("host", HOST), ("traceparent", VALID), *h2c]):
            direct = (await client.get(f"{upstream.url}/v1/echo/headers", headers=sent)).json()
            proxied = (await client.get(f"{url}/v1/echo/headers", headers=sent)).json()
            hop_by_hop = {
                "connection",
                "keep-alive",
                "te",
                "x-custom",
                "upgrade",
                "http2-settings",
            }
            assert proxied == [pair for pair in direct if pair[0] not in hop_by_hop]
            assert ["host", HOST] in proxied
            assert not [
                name for name, _ in proxied if name == "via" or name.startswith("x-forwarded")
            ]

        answer = await _raw(_port(url), b"GET /v1/echo/headers HTTP/1.0\r\nX-Keep: kept\r\n\r\n")
        seen = json.loads(answer.partition(b"\r\n\r\n")[2])
        assert seen[0] == ["host", f"127.0.0.1:{upstream.port}"]
        assert seen[1] == ["x-keep", "kept"]


@pytest.mark.unit
async def test_an_unmatched_route_is_forwarded_and_skipped(upstream: Upstream) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url, _client() as client:
        response = await client.get(f"{url}/v1/orders/SO-1/unknown")
        assert response.status_code == 404
        await _until(lambda: sink.reports)
    assert sink.skips == [ExchangeSkipReason.UNMATCHED_ROUTE]
    assert sink.exchanges == []


@pytest.mark.unit
async def test_an_upstream_path_prefixes_every_forwarded_target(upstream: Upstream) -> None:
    sink = Recorder()
    async with _proxy(upstream.url + PREFIX + "/", sink) as url, _client() as client:
        response, exchange = await _reported(sink, client.get(f"{url}/v1/orders/SO-1?a=1"))
    assert response.json()["orderId"] == "SO-1"
    assert exchange.template == "/v1/orders/{order_id}"


@pytest.mark.unit
async def test_requests_before_templates_load_are_forwarded_and_skipped(
    upstream: Upstream, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    upstream.openapi_status = 404
    sink = Recorder()
    async with _proxy(upstream.url, sink, warm=False) as url, _client() as client:
        response = await client.get(f"{url}/v1/orders/search")
        assert response.json() == {"results": [], "count": 0}
        await _until(lambda: sink.reports)
        assert sink.skips == [ExchangeSkipReason.TEMPLATES_UNAVAILABLE]
        await asyncio.sleep(0.2)
        assert caplog.text.count("templates unavailable: source=api error=status=404") == 1
        assert "templates loaded" not in caplog.text

        upstream.openapi_status = 200
        await _warm_up(url, sink)
    assert f"templates loaded: source=api templates={TEMPLATES} dropped=0" in caplog.text


@pytest.mark.unit
async def test_a_failed_refresh_keeps_the_templates_and_is_noted(
    upstream: Upstream, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    sink = Recorder()
    async with _proxy(upstream.url, sink, openapi_refresh_s=0.05) as url, _client() as client:
        await asyncio.sleep(0.15)
        upstream.openapi_status = 500
        await _until(lambda: ExchangeNote.TEMPLATES_REFRESH_FAILED in sink.notes)
        _, exchange = await _reported(sink, client.get(f"{url}/v1/orders/search"))
        assert exchange.template == "/v1/orders/search"
    assert "templates refresh failed: source=api error=status=500" in caplog.text
    assert caplog.text.count("templates loaded") == 1


@pytest.mark.unit
async def test_a_websocket_upgrade_gets_501(upstream: Upstream) -> None:
    sink = Recorder()
    upgrade = {
        "connection": "Upgrade",
        "upgrade": "websocket",
        "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
        "sec-websocket-version": "13",
    }
    async with _proxy(upstream.url, sink) as url, _client() as client:
        answer = await _fetch(client, "GET", f"{url}/v1/echo/headers", headers=upgrade)
        assert answer == (501, [(b"content-length", b"0")], b"")
        await _until(lambda: sink.reports)
    assert sink.skips == [ExchangeSkipReason.UPGRADE_UNSUPPORTED]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("path", "seen_first"),
    [("/v1/slow", b""), ("/v1/trickle", b'{"first": 1, ')],
    ids=["before-headers", "mid-body"],
)
async def test_a_client_that_disconnects_aborts_the_exchange(
    upstream: Upstream, path: str, seen_first: bytes
) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url:
        reader, writer = await asyncio.open_connection("127.0.0.1", _port(url))
        writer.write(_request("GET", path, []))
        await writer.drain()
        await asyncio.wait_for(upstream.slow_started.wait(), 5)
        if seen_first:
            await asyncio.wait_for(reader.readuntil(seen_first), 5)
        writer.close()
        await writer.wait_closed()
        await _until(lambda: sink.reports)
    assert sink.skips == [ExchangeSkipReason.CLIENT_ABORTED]
    assert sink.exchanges == []


@pytest.mark.unit
async def test_an_upstream_failing_mid_response_cuts_the_client_off(upstream: Upstream) -> None:
    sink = Recorder()
    async with _proxy(upstream.url, sink) as url, _client() as client:
        received = b""
        with pytest.raises(httpx.RemoteProtocolError):
            async with client.stream("GET", f"{url}/v1/broken") as response:
                assert response.status_code == 200
                async for chunk in response.aiter_raw():
                    received += chunk
        assert received in {b"", b'{"partial": '}
        await _until(lambda: sink.reports)
    assert sink.skips == [ExchangeSkipReason.UPSTREAM_ABORTED]


@pytest.mark.unit
async def test_cancelling_serve_returns_promptly_and_stops_accepting(upstream: Upstream) -> None:
    sink = Recorder()
    sock = listen("127.0.0.1", 0)
    port = sock.getsockname()[1]
    serving = asyncio.create_task(
        HttpProxy(source=SOURCE, sock=sock, upstream=upstream.url, retry_s=0.05).serve(sink)
    )
    await _warm_up(f"http://127.0.0.1:{port}", sink)
    started = time.monotonic()
    serving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(serving, 3)
    assert time.monotonic() - started < 1
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", port)


@pytest.mark.unit
async def test_cancelling_serve_lets_an_exchange_in_flight_finish(upstream: Upstream) -> None:
    sink = Recorder()
    sock = listen("127.0.0.1", 0)
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    serving = asyncio.create_task(
        HttpProxy(source=SOURCE, sock=sock, upstream=upstream.url, shutdown_s=3).serve(sink)
    )
    await _warm_up(url, sink)
    async with _client() as client:
        slow = asyncio.create_task(client.get(f"{url}/v1/slow"))
        await asyncio.wait_for(upstream.slow_started.wait(), 5)
        serving.cancel()
        await asyncio.sleep(0.3)
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", _port(url))
        upstream.release.set()
        assert (await slow).json() == {"slow": True}
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(serving, 3)
    await _until(lambda: sink.reports)
    assert [exchange.template for exchange in sink.exchanges] == ["/v1/slow"]


@pytest.mark.unit
async def test_planted_values_reach_no_log_output_or_exchange_repr(
    upstream: Upstream,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(TRACE)

    def planted_headers() -> list[tuple[str, str]]:
        return [
            ("X-Customer", canary.string()),
            ("Authorization", f"Bearer {canary.token()}"),
            ("Cookie", f"session={canary.token()}"),
            ("User-Agent", canary.token()),
        ]

    order = json.dumps(
        canary.plant(
            {"customer": {"email": "a@b.c", "name": "n"}, "items": [{"sku": "s", "qty": 1}]}
        )
    ).encode()
    query = f"email={quote(canary.string(), safe='')}&n={canary.number()}"
    json_type = [("Content-Type", "application/json")]
    requests = [
        _request("GET", f"/v1/orders/{canary.token()}?{query}", planted_headers()),
        _request(
            "POST",
            f"/v1/orders/checkout?ref={canary.token()}",
            json_type + planted_headers(),
            order,
        ),
        _request("GET", f"/v1/unknown/{canary.token()}?q={canary.token()}", planted_headers()),
        _request("GET", f"/v1/broken?x={canary.token()}", planted_headers()),
        _request("GET", "/v1/echo/headers", [("traceparent", canary.token()), *planted_headers()]),
    ]
    sink = Recorder()
    access = logging.getLogger("uvicorn.access")
    async with _proxy(upstream.url, sink) as url:
        # A logging config, or pytest, can attach a handler after uvicorn cleared them.
        access.addHandler(caplog.handler)
        try:
            for request in requests:
                await _raw(_port(url), request)
        finally:
            access.removeHandler(caplog.handler)
        await _until(lambda: sink.reports == len(requests))
    unreachable = Recorder()
    async with _proxy(f"http://127.0.0.1:{_closed_port()}", unreachable, warm=False) as url:
        await _raw(
            _port(url), _request("GET", f"/v1/orders/{canary.token()}?{query}", planted_headers())
        )
        await _until(lambda: unreachable.reports)

    assert len(sink.exchanges) == 3
    assert canary.leaks(b"".join(e.response.body for e in sink.exchanges).decode(), envelope=False)
    output = caplog.text + "".join(capfd.readouterr())
    reports = repr([sink.exchanges, sink.skips, sink.notes, unreachable.skips, unreachable.notes])
    assert canary.leaks(output, envelope=False) == []
    assert canary.leaks(reports, envelope=False) == []
