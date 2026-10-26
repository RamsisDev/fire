"""The inbound proxy end to end: the tentacle forwards requests to a fake upstream unchanged and
sends their shapes to the body, which opens one drift incident when a drifted request comes.

The upstream is a FastAPI app served by uvicorn on the loopback; the body runs in-process
through `httpx.ASGITransport`. Payloads are the acme `order_payload.json` fixture and the
warm-up variants and drifted copy of test_drift_incidents.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import socket
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
import yaml
from canary import Canary
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import fire.apps.incident_engine.app as app_module
from fire.adapters.sources.listener import listen
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "tests" / "fixtures" / "acme" / "api_requests" / "order_payload.json"
TOKEN = "proxy-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = "api"
REQUEST = "http:inbound:POST /v1/orders/checkout:request"
RESPONSE = "http:inbound:POST /v1/orders/checkout:response:2xx"
ORDER = "http:inbound:GET /v1/orders/{order_id}:response:2xx"
MIN_SAMPLES = 5
OPTIONAL = ("adminId", "adminName", "partyId")
NOTIFY = "[fire] notify incident="


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


def _upstream(fetched: asyncio.Event) -> FastAPI:
    """Answers like an order API: checkout returns one response shape whatever the order, with
    the order's id in it, and `/openapi.json` is FastAPI's.
    """
    upstream = FastAPI()

    @upstream.middleware("http")
    async def watch_templates(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        if request.url.path == "/openapi.json":
            fetched.set()
        return response

    @upstream.post("/v1/orders/checkout")
    async def checkout(request: Request) -> JSONResponse:
        order = await request.json()
        headers = {"x-order-echo": request.headers.get("x-planted", "none")}
        placed = {"orderId": str(order.get("referenceOrderId")), "status": "placed"}
        return JSONResponse(placed, 201, headers=headers)

    @upstream.get("/v1/orders/{order_id}")
    async def get_order(order_id: str) -> dict[str, Any]:
        return {"orderId": order_id, "items": [{"sku": order_id}]}

    return upstream


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    @property
    def shapes(self) -> list[dict[str, Any]]:
        return [shape for body in self.bodies for shape in json.loads(body)["shapes"]]


@dataclass
class Stack:
    proxy: str
    upstream: str
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]

    async def finish(self) -> int:
        self.stop.set()
        return await asyncio.wait_for(self.running, 10)


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    fetched = asyncio.Event()
    upstream_socket = listen("127.0.0.1", 0)
    upstream_url = f"http://127.0.0.1:{upstream_socket.getsockname()[1]}"
    config = uvicorn.Config(_upstream(fetched), log_config=None, access_log=False, lifespan="off")
    server = _Server(config)
    serving = asyncio.create_task(server.serve(sockets=[upstream_socket]))

    port = _free_port()
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {"id": SOURCE, "kind": "http_proxy", "listen_port": port, "upstream_env": "UPSTREAM"}
        ],
        "drift": {"min_samples": MIN_SAMPLES, "required_ratio": 1.0},
    }
    (fire_yml.parent / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(fire_yml)
    environ = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body", "UPSTREAM": upstream_url}
    plan = prepare("shop", config_path=fire_yml, environ=environ)
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(run(plan, stop, transport=transport))
    await asyncio.wait_for(fetched.wait(), 5)
    await asyncio.sleep(0.1)

    yield Stack(f"http://127.0.0.1:{port}", upstream_url, transport, stop, running)

    if not running.done():
        stop.set()
        await asyncio.wait_for(running, 10)
    server.should_exit = True
    await asyncio.wait_for(serving, 10)
    app_module._container = None


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _without(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in keys}


def _warm_up_variant(index: int) -> dict[str, Any]:
    payload = _fixture()
    if index % 2 == 0:
        return _without(payload, "adminId", "adminName")
    if index % 3 == 0:
        return _without(payload, "partyId")
    return payload


def _drifted() -> dict[str, Any]:
    payload = _without(_fixture(), "timezone")
    payload["referenceOrderId"] = 12345
    return payload


async def _post_orders(base: str, payloads: list[dict[str, Any]]) -> list[httpx.Response]:
    async with httpx.AsyncClient(base_url=base, trust_env=False) as client:
        return [await client.post("/v1/orders/checkout", json=body) for body in payloads]


async def _streams() -> dict[str, Any]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get("/v0/systems/shop/streams", headers=HEADERS)
    assert response.status_code == 200
    return {stream["key"]: stream for stream in response.json()["streams"]}


def _notifications(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


@pytest.mark.integration
async def test_proxied_requests_give_request_and_response_channels(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    orders = await _post_orders(stack.proxy, [_fixture(), {**_fixture(), "adminId": None}])
    async with httpx.AsyncClient(base_url=stack.proxy, trust_env=False) as client:
        order = await client.get("/v1/orders/300000123456789")
        unrouted = await client.get("/v1/unrouted")
    direct = await _post_orders(stack.upstream, [_fixture()])

    assert [response.status_code for response in orders] == [201, 201]
    assert orders[0].content == direct[0].content
    assert (order.status_code, unrouted.status_code) == (200, 404)
    assert await stack.finish() == EXIT_OK

    streams = await _streams()
    assert {key: stream["samples"] for key, stream in streams.items()} == {
        REQUEST: 2,
        RESPONSE: 2,
        ORDER: 1,
    }
    shapes = stack.transport.shapes
    assert len(shapes) == 5
    for shape in shapes:
        assert shape["source"] == SOURCE
        assert len(shape["trace_id"]) == 32 and len(shape["span_id"]) == 16
    pairs = {shape["observation_id"].split(":")[0] for shape in shapes}
    assert len(pairs) == 3
    assert (
        "source=api read=4 built=5 sent=5 unsent=0 pending=0 rejected=- "
        "skipped=no_body:1,unmatched_route:1 noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.integration
async def test_inbound_drift_through_the_proxy_notifies_once(
    stack: Stack, capfd: pytest.CaptureFixture[str]
) -> None:
    warm = [_warm_up_variant(index) for index in range(MIN_SAMPLES)]
    warm.append(_without(_fixture(), *OPTIONAL))
    await _post_orders(stack.proxy, warm)
    await asyncio.sleep(0.3)
    assert _notifications(capfd.readouterr().out) == []

    await _post_orders(stack.proxy, [_drifted(), _drifted()])
    assert await stack.finish() == EXIT_OK

    out = capfd.readouterr().out
    [notification] = _notifications(out)
    assert notification.endswith("system=shop trigger=shape_drift")
    for name in (REQUEST, "referenceOrderId", "timezone"):
        assert name in out
    streams = await _streams()
    [signal] = streams[REQUEST]["drift"]
    assert signal["occurrences"] == 2


@pytest.mark.integration
async def test_proxy_canary(
    stack: Stack,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
    quiet_httpx: None,
) -> None:
    caplog.set_level(logging.DEBUG)
    orders = [canary.plant(_fixture()) for _ in range(3)]
    async with httpx.AsyncClient(base_url=stack.proxy, trust_env=False) as client:
        for order in orders:
            response = await client.post(
                "/v1/orders/checkout",
                params={"email": canary.string()},
                headers={"x-planted": canary.token(), "cookie": f"session={canary.token()}"},
                json=order,
            )
            assert response.status_code == 201
        await client.get(f"/v1/orders/{canary.token()}", params={"q": canary.token()})
        await client.get(f"/v1/unrouted/{canary.token()}")
        await client.post(
            "/v1/orders/checkout",
            content=json.dumps(canary.plant(_fixture())),
            headers={"content-type": f"application/json; x={canary.token()}"},
        )
        await client.post(
            "/v1/orders/checkout",
            content=canary.token(),
            headers={"content-type": "text/plain", "traceparent": canary.token()},
        )
    assert await stack.finish() == EXIT_OK

    for body in stack.transport.bodies:
        assert canary.leaks(body, envelope=False) == []
    catalog_state = repr(vars(app_module.get_container().catalog))
    assert canary.leaks(catalog_state, envelope=False) == []
    streams = await _streams()
    assert canary.leaks(json.dumps(streams), envelope=False) == []
    assert REQUEST in streams
    captured = capfd.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert canary.leaks(output, envelope=False) == []
