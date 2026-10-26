"""The OTLP receiver end to end: OTLP JSON posted to the tentacle reaches the body as spans and
outbound shapes, and nothing else does.

The body runs in-process through `httpx.ASGITransport`. The L1 tests post what a collector
exported for one checkout flow each (`tests/fixtures/acme/otlp/`, in the form
`dev/record_otlp.py` records and scrubs).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import socket
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from canary import Canary
from otlp_json import event, link, request, resource_spans, span
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run

REPO = Path(__file__).resolve().parents[2]
RECORDINGS = REPO / "tests" / "fixtures" / "acme" / "otlp"
ORDER = REPO / "tests" / "fixtures" / "acme" / "api_requests" / "order_payload.json"
TOKEN = "otlp-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = "traces"
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SERVER, CLIENT, DYNAMODB = "00f067aa0ba902b7", "53995c3f42cd8ad8", "1a2b3c4d5e6f7a8b"
PARTNER_CALL = "POST api.partner.example.com/v1/orders/{id}"
ROUTE = "POST /v1/orders/checkout"
PLACEHOLDER = "redacted"
# Each recording: the checkout flow, 15 spans in the API's trace and 15 in the worker's, and 12
# of the billing publisher retrying an earlier work unit whose billing call failed.
RECORDED_SPANS = 42
BILLING = "billing.partner.example.com"
PAYMENTS = "payments.partner.example.com"


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    def posted(self, kind: str) -> list[dict[str, Any]]:
        return [item for body in self.bodies for item in json.loads(body)[kind]]


@dataclass
class Stack:
    receiver: str
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]
    # The body's clock.
    clock: Clock

    async def post(self, document: Any) -> httpx.Response:
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            return await client.post(
                f"{self.receiver}/v1/traces",
                content=json.dumps(document),
                headers={"content-type": "application/json"},
            )

    async def finish(self) -> int:
        self.stop.set()
        return await asyncio.wait_for(self.running, 10)


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Stack]:
    port = _free_port()
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [{"id": SOURCE, "kind": "otlp", "listen_env": "LISTEN"}],
    }
    (fire_yml.parent / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    clock = Clock()
    app_module._container = build_container(fire_yml, clock=clock)
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "LISTEN": f"127.0.0.1:{port}",
    }
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(prepare("shop", config_path=fire_yml, environ=environ), stop, transport=transport)
    )

    yield Stack(f"http://127.0.0.1:{port}", transport, stop, running, clock)

    if not running.done():
        stop.set()
        await asyncio.wait_for(running, 10)
    app_module._container = None


async def _get(path: str) -> dict[str, Any]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(path, headers=HEADERS)
    assert response.status_code == 200
    return response.json()


async def _streams() -> dict[str, Any]:
    body = await _get("/v0/systems/shop/streams")
    return {stream["key"]: stream for stream in body["streams"]}


def _order_flow(order: dict[str, Any]) -> dict[str, Any]:
    """A server span with a partner call carrying both bodies, a DynamoDB call, and a span
    with an invalid trace id.
    """
    server = span(
        TRACE,
        SERVER,
        ROUTE,
        kind=2,
        items={
            "http.method": "POST",
            "http.route": "/v1/orders/checkout",
            "http.status_code": 201,
        },
    )
    partner = span(
        TRACE,
        CLIENT,
        "POST",
        kind=3,
        parent=SERVER,
        items={
            "http.method": "POST",
            "http.url": "https://api.partner.example.com/v1/orders/300000123456789?x=1",
            "http.status_code": 200,
            "http.request.body": json.dumps(order),
        },
        events=[event("http.response.body", {"http.response.body": '{"placed": true}'})],
    )
    dynamodb = span(
        TRACE,
        DYNAMODB,
        "DynamoDB.PutItem",
        kind=3,
        parent=SERVER,
        items={
            "rpc.system": "aws-api",
            "rpc.service": "DynamoDB",
            "rpc.method": "PutItem",
            "aws.dynamodb.table_names": ["orders"],
        },
    )
    invalid = span("0" * 32, "9" * 16, "lost")
    return request(resource_spans("orders-api", [server, partner, dynamodb, invalid]))


@pytest.mark.integration
async def test_spans_and_client_bodies_reach_the_body(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    order = json.loads(ORDER.read_text(encoding="utf-8"))
    first = await stack.post(_order_flow(order))
    again = await stack.post(_order_flow(order))
    assert await stack.finish() == EXIT_OK

    for response in (first, again):
        assert (response.status_code, response.json()) == (
            200,
            {"partialSuccess": {"rejectedSpans": 1}},
        )
    spans = {item["span_id"]: item for item in stack.transport.posted("spans")}
    assert {key: value["operation"] for key, value in spans.items()} == {
        SERVER: ROUTE,
        CLIENT: PARTNER_CALL,
        DYNAMODB: "DynamoDB.PutItem orders",
    }
    assert spans[CLIENT]["parent_span_id"] == spans[DYNAMODB]["parent_span_id"] == SERVER
    streams = await _streams()
    assert {key: stream["observations"] for key, stream in streams.items()} == {
        f"http:outbound:{PARTNER_CALL}:request": 1,
        f"http:outbound:{PARTNER_CALL}:response:2xx": 1,
    }
    assert (
        "source=traces read=8 built=10 sent=10 unsent=0 pending=0 rejected=invalid_ids:2 "
        "skipped=- noted=- body_duplicate=5 body_rejected=-"
    ) in caplog.text


@pytest.mark.integration
async def test_otlp_canary(
    stack: Stack,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
    quiet_httpx: None,
) -> None:
    caplog.set_level(logging.DEBUG)
    order = canary.plant(json.loads(ORDER.read_text(encoding="utf-8")))
    dropped = {
        "http.request.header.cookie": f"session={canary.token()}",
        "http.request.header.authorization": canary.token(),
        "http.response.header.set-cookie": canary.token(),
        "http.target": f"/v1/orders?email={canary.string()}",
        "http.host": canary.token(),
        "url.query": canary.token(),
        "net.peer.ip": canary.token(),
        "aws.queue_url": canary.string(),
        "aws.request_id": canary.token(),
        "messaging.url": canary.string(),
        "db.statement": canary.token(),
        "acme.work_unit.id": canary.token(),
        "acme.billing.order_ids": [canary.token(), canary.number()],
        "http.request.body.size": canary.number(),
    }
    url = f"https://api.example.net/v1/customers/{canary.token()}/orders?email={canary.string()}"
    events = [
        event("http.response.body", {"http.response.body": json.dumps(canary.plant(order))}),
        event(
            "exception",
            {"exception.message": canary.token(), "exception.stacktrace": canary.token()},
        ),
        event("capture.written", {"acme.capture.request_uri": canary.string()}),
    ]
    client = span(
        TRACE,
        CLIENT,
        "POST",
        kind=3,
        parent=SERVER,
        items={
            "http.method": "POST",
            "http.url": url,
            "http.status_code": 422,
            "http.request.body": json.dumps(order),
        }
        | dropped,
        events=events,
        status={"code": 2, "message": canary.token()},
        links=[link(TRACE, DYNAMODB, {"writer_traceparent": canary.token()})],
    )
    truncated = json.dumps(canary.plant(order))[:4000] + "...[truncated 900 bytes]"
    cut = span(
        TRACE,
        DYNAMODB,
        "POST",
        kind=3,
        items={
            "http.method": "POST",
            "http.url": f"https://api.example.net/v1/{canary.number()}",
            "http.request.body": truncated,
        },
        events=[event("http.response.body", {"http.response.body": canary.string()})],
    )
    internal = span(TRACE, SERVER, f"retry {canary.token()} /v1/{canary.number()}", items=dropped)
    rejected = span(canary.token(), canary.token(), canary.token(), items=dropped)
    resource = {"service.version": canary.token(), "host.name": canary.token()}
    document = request(resource_spans("orders-api", [client, cut, internal, rejected], resource))
    response = await stack.post(document)
    await stack.post(canary.token())
    assert await stack.finish() == EXIT_OK

    assert response.json() == {"partialSuccess": {"rejectedSpans": 1}}
    operations = sorted(item["operation"] for item in stack.transport.posted("spans"))
    assert operations == [
        "POST api.example.net/v1/customers/{id}/orders",
        "POST api.example.net/v1/{id}",
        "retry {id} /v1/{id}",
    ]
    for body in stack.transport.bodies:
        assert canary.leaks(body, envelope=False) == []
    catalog_state = repr(vars(app_module.get_container().catalog))
    assert canary.leaks(catalog_state, envelope=False) == []
    streams = await _streams()
    assert "http:outbound:POST api.example.net/v1/customers/{id}/orders:response:4xx" in streams
    assert canary.leaks(json.dumps(streams), envelope=False) == []
    captured = capfd.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert canary.leaks(output, envelope=False) == []


def recorded(name: str) -> list[dict[str, Any]]:
    """One recording's requests, in the order the collector sent them."""
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((RECORDINGS / name).glob("*.json"))
    ]


def _one(spans: list[dict[str, Any]], operation: str) -> dict[str, Any]:
    [found] = [item for item in spans if item["operation"] == operation]
    return found


@pytest.mark.integration
@pytest.mark.parametrize("name", ["checkout_1", "checkout_2"])
async def test_a_recorded_checkout_joins_its_spans(stack: Stack, name: str) -> None:
    for document in recorded(name):
        response = await stack.post(document)
        assert (response.status_code, response.json()) == (200, {})
    assert await stack.finish() == EXIT_OK

    spans = stack.transport.posted("spans")
    assert len(spans) == RECORDED_SPANS
    server = _one(spans, ROUTE)
    assert (server["kind"], server["status_code"], server["parent_span_id"]) == (
        "server",
        201,
        None,
    )
    assert len([item for item in spans if item["trace_id"] == server["trace_id"]]) == 15
    children = [item for item in spans if item["parent_span_id"] == server["span_id"]]
    assert {item["trace_id"] for item in children} == {server["trace_id"]}
    assert {
        item["operation"] for item in children if item["operation"].startswith("DynamoDB.")
    } == {
        "DynamoDB.PutItem acme-dev-orders",
        "DynamoDB.BatchGetItem acme-catalog-products",
        "DynamoDB.GetItem acme-catalog-products",
        "DynamoDB.UpdateItem acme-dev-worker_leases",
        "DynamoDB.TransactWriteItems",
    }
    worker = _one(spans, "worker.distribute_work")
    assert worker["trace_id"] != server["trace_id"]
    assert worker["links"] == [{"trace_id": server["trace_id"], "span_id": server["span_id"]}]
    assert len([item for item in spans if item["trace_id"] == worker["trace_id"]]) == 15
    sns = _one(spans, "SNS.Publish acme-billing-events-dev")
    assert (sns["kind"], sns["trace_id"], sns["parent_span_id"]) == (
        "producer",
        worker["trace_id"],
        worker["span_id"],
    )
    assert stack.transport.posted("shapes") == []


def _plant_recording(canary: Canary, document: Any, path: tuple[str, ...] = ()) -> Any:
    """The recording with a canary in every scrubbed value, query value, and status message,
    and a canary id appended to every URL's path.
    """
    if isinstance(document, dict):
        planted = {
            key: _plant_recording(canary, value, (*path, key)) for key, value in document.items()
        }
        if planted.get("key") in {"http.url", "url.full"}:
            parts = planted["value"]["stringValue"].split("?", 1)
            parts[0] += f"/{canary.token()}"
            planted["value"]["stringValue"] = "?".join(parts).replace(PLACEHOLDER, canary.token())
        return planted
    if isinstance(document, list):
        return [_plant_recording(canary, item, path) for item in document]
    if document == PLACEHOLDER or path[-1:] == ("message",):
        return canary.string()
    if path[-1:] == ("intValue",) and document == "0":
        return str(canary.number())
    return document


@pytest.mark.integration
async def test_recorded_spans_canary(
    stack: Stack,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
    quiet_httpx: None,
) -> None:
    caplog.set_level(logging.DEBUG)
    for document in recorded("checkout_1"):
        planted = _plant_recording(canary, copy.deepcopy(document))
        assert json.dumps(planted) != json.dumps(document)
        await stack.post(planted)
    assert await stack.finish() == EXIT_OK

    spans = stack.transport.posted("spans")
    assert len(spans) == RECORDED_SPANS
    calls = {item["operation"] for item in spans if item["kind"] == "client"}
    calls = {operation for operation in calls if not operation.startswith(("DynamoDB", "S3"))}
    assert sorted(calls) == [
        "GET identity.example.com/api/v1/users/{id}",
        f"POST {BILLING}/v2/invoices/place/{{id}}",
        f"POST {PAYMENTS}/v1/payments/order-payments/create/{{id}}",
        f"POST {PAYMENTS}/v2/tax/calculate-tax/estimate/{{id}}",
        f"POST {PAYMENTS}/v2/tax/tax-code/{{id}}",
    ]
    for body in stack.transport.bodies:
        assert canary.leaks(body, envelope=False) == []
    stack.clock.advance(60)
    paths = await _get("/v0/systems/shop/paths")
    [path] = paths["paths"]
    assert path["entry"]["operation"] == ROUTE
    assert canary.leaks(json.dumps(paths), envelope=False) == []
    captured = capfd.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert canary.leaks(output, envelope=False) == []
