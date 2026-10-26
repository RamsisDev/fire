"""Capture pairs end to end: a capture event posted to the tentacle's OTLP receiver
indexes a pair's keys, the pairs written to a bucket reach the body as outbound shapes joined to
that span, and nothing else does. A recorded checkout whose capture event names a fixture pair
gets that pair on the span's node of its path, and a redelivery the event never named on the
flow, through the work index.

S3 is moto's. The body runs in-process through `httpx.ASGITransport` on a hand-moved clock, and
the tentacle reads with `--start oldest`. A test waits for the counters line of the capture
source to show every pair read and sent, then stops the tentacle.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import socket
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
import httpx
import pytest
import yaml
from canary import Canary
from moto import mock_aws
from otlp_json import event, request, resource_spans, span
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, prepare, run
from fire.core.ports.record_source import StartPosition

ACME = Path(__file__).resolve().parents[1] / "fixtures" / "acme"
CAPTURE_FIXTURES = ACME / "captures"
RECORDING = ACME / "otlp" / "checkout_1"
TOKEN = "capture-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
BUCKET = "capture-data"
TRACES, CAPTURES = "traces", "captures"
ULID = "[0-7][0-9A-HJKMNP-TV-Z]{25}"
WORK = "01JABCDEFGHJKMNPQRSTVWXYZ0"
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN = "00f067aa0ba902b7"
UUID = "550e8400-e29b-41d4-a716-446655440000"
NOTIFICATIONS = {
    "prefix": "notification-payloads/",
    "label": "notifications",
    "layout": "work_unit_action",
}
SHIPPING = {
    "prefix": "shipping-payloads/",
    "label": "shipping",
    "layout": "work_unit_entity_action",
}
BILLING = {"prefix": "billing-payloads/", "label": "billing", "layout": "work_unit_entity"}
CAPTURE_EVENT = {
    "name": "capture.written",
    "request_uri": "acme.capture.request_uri",
    "response_uri": "acme.capture.response_uri",
}
CONFIRMATION = "http:outbound:notifications:send_confirmation"
INVOICE = "http:outbound:billing:invoice_place"
ENTRY = {"service": "acme-api", "operation": "POST /v1/orders/checkout"}
WORKER, BUSINESS = "worker.distribute_work", "billing-publisher.business_logic"
# Spans the recording holds: the checkout flow's 30 and an orphan retry's 12.
RECORDED_SPANS = 42
SETTLE = 60
SHIPMENT = "http:outbound:shipping:create_shipment"
REQUEST = {
    "method": "POST",
    "url": "https://api.partner.example.com/v1/shipments",
    "headers": {"content-type": "application/json", "authorization": "Bearer x"},
    "body": {"order": {"lines": [{"sku": "x", "quantity": 1}], "email": "a@example.com"}},
}
RESPONSE = {
    "status_code": 201,
    "headers": {"content-type": "application/json"},
    "body": {"shipment_id": "x", "carrier": "y", "eta_days": 3},
}
NO_RESPONSE = {"status_code": None, "error_type": "ConnectError", "error": "connection refused"}


def _capture_fixture(name: str) -> dict[str, Any]:
    return json.loads((CAPTURE_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _stamp(serial: int) -> str:
    return f"20261011_163001_{serial:06d}"


def _key(rule: Mapping[str, str], part: str, name: str) -> str:
    return f"{rule['prefix']}{part}/{name}"


def _uri(rule: Mapping[str, str], part: str, name: str) -> str:
    return f"s3://{BUCKET}/{_key(rule, part, name)}"


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
class Tentacle:
    receiver: str
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]

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


@dataclass
class Harness:
    """The bucket, the body, and tentacles started on them, each on its own listen port."""

    fire_yml: Path
    s3: Any
    caplog: pytest.LogCaptureFixture
    clock: Clock
    started: list[Tentacle] = field(default_factory=list)

    def put(self, key: str, document: Any, metadata: Mapping[str, str] | None = None) -> None:
        body = document if isinstance(document, bytes) else json.dumps(document).encode()
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body, Metadata=dict(metadata or {}))

    def put_pair(
        self,
        rule: Mapping[str, str],
        name: str,
        *,
        request_document: Any = REQUEST,
        response_document: Any = RESPONSE,
        metadata: Mapping[str, str] | None = None,
    ) -> None:
        """Writes the request, unless None, then the response, as a capturing client does."""
        if request_document is not None:
            self.put(_key(rule, "requests", name), request_document, metadata)
        self.put(_key(rule, "responses", name), response_document, metadata)

    async def start(self) -> Tentacle:
        port = _free_port()
        environ = {
            "FIRE_API_TOKEN": TOKEN,
            "FIRE_BODY_URL": "http://body",
            "LISTEN": f"127.0.0.1:{port}",
            "BUCKET": BUCKET,
        }
        plan = prepare(
            "shop", start=StartPosition.OLDEST, config_path=self.fire_yml, environ=environ
        )
        transport = RecordingTransport(httpx.ASGITransport(app=app))
        stop = asyncio.Event()
        running = asyncio.create_task(
            run(plan, stop, transport=transport, counters_interval_s=0.05)
        )
        tentacle = Tentacle(f"http://127.0.0.1:{port}", transport, stop, running)
        self.started.append(tentacle)
        return tentacle

    async def read_and_sent(
        self, pairs: int, timeout_s: float = 10, source: str = CAPTURES
    ) -> None:
        """Waits for a counters line of `source` with `pairs` read and none pending."""
        prefix = f"tentacle counters: source={source} read={pairs} "

        def done() -> bool:
            return any(
                message.startswith(prefix) and " pending=0 " in message
                for message in self.caplog.messages
            )

        async with asyncio.timeout(timeout_s):
            while not done():
                await asyncio.sleep(0.02)


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fake credentials; no endpoint, profile, or AWS file from outside the repo."""
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))


@pytest.fixture
async def harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    aws: None,
) -> AsyncIterator[Harness]:
    caplog.set_level(logging.INFO)
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
    sources = [
        {"id": TRACES, "kind": "otlp", "listen_env": "LISTEN", "capture_event": CAPTURE_EVENT},
        {
            "id": CAPTURES,
            "kind": "s3_capture",
            "bucket_env": "BUCKET",
            "poll_s": 0.05,
            "settle_s": 0,
            "work_id_pattern": ULID,
            "captures": [NOTIFICATIONS, SHIPPING, BILLING],
        },
    ]
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": sources,
        "paths": {"settle_s": SETTLE},
    }
    (fire_yml.parent / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    clock = Clock()
    app_module._container = build_container(fire_yml, clock=clock)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        harness = Harness(fire_yml, s3, caplog, clock)
        yield harness
        for tentacle in harness.started:
            if not tentacle.running.done():
                await tentacle.finish()
        # S3 calls still in worker threads end inside the mock.
        await asyncio.get_running_loop().shutdown_default_executor()
    app_module._container = None


async def _get(path: str) -> Any:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(path, headers=HEADERS)
    assert response.status_code == 200
    return response.json()


async def _streams() -> dict[str, Any]:
    body = await _get("/v0/systems/shop/streams")
    return {stream["key"]: stream for stream in body["streams"]}


def _observations(streams: Mapping[str, Any]) -> dict[str, int]:
    return {key: stream["observations"] for key, stream in streams.items()}


def _capture_span(*events: Mapping[str, Any], resource: Mapping[str, Any] | None = None) -> Any:
    """An OTLP request with one span of `orders-worker` carrying a capture event per mapping."""
    captured = [event(CAPTURE_EVENT["name"], items) for items in events]
    worker = span(TRACE, SPAN, "billing-publisher.business_logic", events=captured)
    return request(resource_spans("orders-worker", [worker], resource))


def _names(rule: Mapping[str, str], name: str) -> dict[str, str]:
    """A capture event's two URI attributes for the pair `name` under `rule`."""
    return {
        CAPTURE_EVENT["request_uri"]: _uri(rule, "requests", name),
        CAPTURE_EVENT["response_uri"]: _uri(rule, "responses", name),
    }


@pytest.mark.integration
async def test_a_capture_event_joins_its_pair_to_its_span(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    name = f"{WORK}/send_confirmation_{_stamp(1)}.json"
    tentacle = await harness.start()

    response = await tentacle.post(_capture_span(_names(NOTIFICATIONS, name)))
    assert (response.status_code, response.json()) == (200, {})
    harness.put_pair(NOTIFICATIONS, name)
    await harness.read_and_sent(1)
    assert await tentacle.finish() == EXIT_OK

    assert _observations(await _streams()) == {
        f"{CONFIRMATION}:request": 1,
        f"{CONFIRMATION}:response:2xx": 1,
    }
    shapes = tentacle.transport.posted("shapes")
    assert sorted(shape["observation_id"] for shape in shapes) == [
        f"notifications:{WORK}:send_confirmation:{_stamp(1)}:request",
        f"notifications:{WORK}:send_confirmation:{_stamp(1)}:response",
    ]
    assert {(shape["trace_id"], shape["span_id"], shape["work_id"]) for shape in shapes} == {
        (TRACE, SPAN, WORK)
    }
    [posted_span] = tentacle.transport.posted("spans")
    assert (posted_span["trace_id"], posted_span["span_id"]) == (TRACE, SPAN)
    assert (
        f"source={TRACES} read=1 built=1 sent=1 unsent=0 pending=0 rejected=- skipped=- "
        "noted=capture_indexed:2 body_duplicate=0 body_rejected=-"
    ) in caplog.text
    assert (
        f"source={CAPTURES} read=1 built=2 sent=2 unsent=0 pending=0 rejected=- skipped=- "
        "noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.integration
async def test_a_pair_no_capture_event_names_joins_by_work_id_alone(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    named = f"{WORK}/send_reminder_{_stamp(1)}.json"
    unnamed = f"{WORK}/send_confirmation_{_stamp(2)}.json"
    harness.put_pair(NOTIFICATIONS, unnamed)
    tentacle = await harness.start()

    await tentacle.post(_capture_span(_names(NOTIFICATIONS, named)))
    await harness.read_and_sent(1)
    assert await tentacle.finish() == EXIT_OK

    shapes = tentacle.transport.posted("shapes")
    assert len(shapes) == 2
    assert {(shape["trace_id"], shape["span_id"], shape["work_id"]) for shape in shapes} == {
        (None, None, WORK)
    }
    assert "noted=capture_indexed:2 " in caplog.text
    assert (
        f"source={CAPTURES} read=1 built=2 sent=2 unsent=0 pending=0 rejected=- skipped=- "
        "noted=unjoined:1 body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.integration
async def test_a_call_with_no_response_gives_the_none_status_class(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    recorded = _capture_fixture("shipping/001")
    recorded_response = dict(recorded["response"]["document"])
    del recorded_response["status_code"]
    refused = f"{WORK}/send_confirmation_{_stamp(1)}.json"
    recorded_name = f"{WORK}/ENTITY1_create_shipment_{_stamp(2)}.json"
    harness.put_pair(NOTIFICATIONS, refused, response_document=NO_RESPONSE)
    harness.put_pair(
        SHIPPING,
        recorded_name,
        request_document=recorded["request"]["document"],
        response_document=recorded_response,
        metadata={"action": "create_shipment"},
    )
    tentacle = await harness.start()

    await harness.read_and_sent(2)
    assert await tentacle.finish() == EXIT_OK

    assert _observations(await _streams()) == {
        f"{CONFIRMATION}:request": 1,
        f"{CONFIRMATION}:response:none": 1,
        f"{SHIPMENT}:request": 1,
        f"{SHIPMENT}:response:none": 1,
    }
    shapes = tentacle.transport.posted("shapes")
    responses = [shape for shape in shapes if shape["channel"]["part"] == "response"]
    assert [shape["shape"] for shape in responses] == [{"fields": []}] * 2
    assert {shape["channel"]["status_class"] for shape in responses} == {"none"}
    assert (
        f"source={CAPTURES} read=2 built=4 sent=4 unsent=0 pending=0 rejected=- skipped=- "
        "noted=unjoined:2 body_duplicate=0 body_rejected=-"
    ) in caplog.text


def _pointed_recording(pair: Mapping[str, Any]) -> tuple[list[Any], tuple[str, str]]:
    """The recording's requests, in order, with the worker trace's capture event naming `pair`,
    and the trace and span ids of the span that carries it. The orphan's event, in another
    trace, is left as recorded.
    """
    documents = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(RECORDING.glob("*.json"))
    ]
    spans = [
        item
        for document in documents
        for resource in document["resourceSpans"]
        for scope in resource["scopeSpans"]
        for item in scope["spans"]
    ]
    [worker_trace] = {item["traceId"] for item in spans if item["name"] == WORKER}
    [business] = [
        item for item in spans if item["name"] == BUSINESS and item["traceId"] == worker_trace
    ]
    [captured] = [item for item in business["events"] if item["name"] == CAPTURE_EVENT["name"]]
    uris = _names(BILLING, pair["name"])
    for attribute in captured["attributes"]:
        if attribute["key"] in uris:
            attribute["value"] = {"stringValue": uris.pop(attribute["key"])}
    assert uris == {}
    return documents, (business["traceId"], business["spanId"])


@pytest.mark.integration
async def test_a_recorded_checkouts_capture_pair_joins_its_node_and_feeds_the_work_index(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    joined, redelivered = _capture_fixture("billing/002"), _capture_fixture("billing/003")
    work = joined["request"]["metadata"]["work_unit_id"]
    assert redelivered["request"]["metadata"]["work_unit_id"] == work
    documents, (trace, span_id) = _pointed_recording(joined)
    tentacle = await harness.start()

    for document in documents:
        response = await tentacle.post(document)
        assert (response.status_code, response.json()) == (200, {})
    await harness.read_and_sent(RECORDED_SPANS, source=TRACES)
    for read, pair in enumerate((joined, redelivered), start=1):
        harness.put_pair(
            BILLING,
            pair["name"],
            request_document=pair["request"]["document"],
            response_document=pair["response"]["document"],
            metadata=pair["request"]["metadata"],
        )
        await harness.read_and_sent(read)
    assert await tentacle.finish() == EXIT_OK

    shapes = tentacle.transport.posted("shapes")
    assert sorted(
        (shape["trace_id"] is not None, shape["channel"]["part"]) for shape in shapes
    ) == [(False, "request"), (False, "response"), (True, "request"), (True, "response")]
    assert {(shape["trace_id"], shape["span_id"], shape["work_id"]) for shape in shapes} == {
        (trace, span_id, work),
        (None, None, work),
    }
    assert "noted=capture_indexed:4 " in caplog.text
    assert (
        f"source={CAPTURES} read=2 built=4 sent=4 unsent=0 pending=0 rejected=- skipped=- "
        "noted=unjoined:1 body_duplicate=0 body_rejected=-"
    ) in caplog.text

    harness.clock.advance(SETTLE)
    report = await _get("/v0/systems/shop/paths")
    [path] = report["paths"]
    assert (path["entry"], path["flows"], report["open_flows"]) == (ENTRY, 1, 0)
    assert (report["counters"]["orphan_flows"], report["counters"]["unattached_shapes"]) == (1, 0)
    pair_channels = {f"{INVOICE}:request": 1, f"{INVOICE}:response:5xx": 1}
    [node] = [node for node in path["nodes"] if node["channels"]]
    assert (node["operation"], node["channels"]) == (BUSINESS, pair_channels)
    assert path["flow_channels"] == pair_channels


@pytest.mark.integration
async def test_rejected_and_skipped_pairs_are_counted_and_a_second_run_adds_only_duplicates(
    harness: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    unattributed = f"unattributed/ENTITY1_create_shipment_{_stamp(6)}.json"
    harness.put(_key(NOTIFICATIONS, "responses", "stray.json"), RESPONSE)
    harness.put_pair(SHIPPING, f"{WORK}/ENTITY1_create_shipment_{_stamp(2)}.json")
    harness.put_pair(NOTIFICATIONS, f"{WORK}/{UUID}_{_stamp(3)}.json")
    harness.put_pair(
        NOTIFICATIONS, f"{WORK}/send_confirmation_{_stamp(4)}.json", request_document=None
    )
    harness.put_pair(
        NOTIFICATIONS, f"{WORK}/send_reminder_{_stamp(5)}.json", request_document=b"{x"
    )
    harness.put_pair(SHIPPING, unattributed, metadata={"action": "create_shipment"})
    counts = (
        f"source={CAPTURES} read=6 built=4 sent=4 unsent=0 pending=0 "
        "rejected=bad_action:1,missing_action:1,no_layout:1 "
        "skipped=object_missing:1,object_undecodable:1 noted=unjoined:3 "
    )

    first = await harness.start()
    await harness.read_and_sent(6)
    assert await first.finish() == EXIT_OK
    assert f"{counts}body_duplicate=0 body_rejected=-" in caplog.text
    streams = _observations(await _streams())
    assert streams == {
        f"{CONFIRMATION}:response:2xx": 1,
        "http:outbound:notifications:send_reminder:response:2xx": 1,
        f"{SHIPMENT}:request": 1,
        f"{SHIPMENT}:response:2xx": 1,
    }
    unattributed_shapes = [
        shape
        for shape in first.transport.posted("shapes")
        if shape["channel"]["name"] == "shipping:create_shipment"
    ]
    assert sorted(shape["observation_id"] for shape in unattributed_shapes) == [
        f"shipping:-:create_shipment:{_stamp(6)}:request",
        f"shipping:-:create_shipment:{_stamp(6)}:response",
    ]
    assert {shape["work_id"] for shape in unattributed_shapes} == {None}

    caplog.clear()
    second = await harness.start()
    await harness.read_and_sent(6)
    assert await second.finish() == EXIT_OK
    assert f"{counts}body_duplicate=4 body_rejected=-" in caplog.text
    assert _observations(await _streams()) == streams


@pytest.mark.integration
async def test_capture_canary(
    harness: Harness,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    # Every logger at DEBUG: the URLs httpx logs hold no planted value, so it stays at DEBUG too
    # (`caplog.set_level(..., logger=...)` would also raise the level of caplog's own handler).
    caplog.set_level(logging.DEBUG)

    def metadata(action: str | None = "create_shipment") -> dict[str, str]:
        planted = {"order_id": canary.string(), "work_unit_id": canary.token()}
        return planted if action is None else {"action": action, **planted}

    def response(status_code: int | None = 201) -> dict[str, Any]:
        planted = canary.plant(copy.deepcopy(RESPONSE))
        planted["status_code"] = status_code
        return planted

    joined = f"{WORK}/{canary.token()}_create_shipment_{_stamp(1)}.json"
    duplicate = f"{WORK}/{canary.token()}_create_shipment_{_stamp(1)}.json"
    planted_work = f"{canary.token()}/send_confirmation_{_stamp(2)}.json"
    refused = f"{WORK}/notify_{_stamp(3)}.json"
    no_action = f"{WORK}/{canary.token()}_create_shipment_{_stamp(4)}.json"
    uuid_action = f"{WORK}/{UUID}_{_stamp(5)}.json"
    undecodable = f"{WORK}/send_reminder_{_stamp(6)}.json"
    unwritten = f"{WORK}/{canary.token()}_create_shipment_{_stamp(7)}.json"
    no_response = {
        "status_code": None,
        "headers": canary.plant({"date": "x", "server": "y"}),
        "error_type": canary.token(),
        "error": canary.string(),
    }
    pairs: list[tuple[dict[str, str], str, dict[str, Any]]] = [
        (SHIPPING, joined, {"response_document": response(), "metadata": metadata()}),
        (SHIPPING, duplicate, {"response_document": response(), "metadata": metadata()}),
        (
            NOTIFICATIONS,
            planted_work,
            {"response_document": response(), "metadata": metadata(None)},
        ),
        (NOTIFICATIONS, refused, {"response_document": no_response}),
        (SHIPPING, no_action, {"response_document": response(), "metadata": metadata(None)}),
        (
            NOTIFICATIONS,
            uuid_action,
            {"response_document": response(), "metadata": metadata(None)},
        ),
        (
            NOTIFICATIONS,
            undecodable,
            {"request_document": f'{{"body": "{canary.token()}"'.encode()},
        ),
    ]
    for _, _, documents in pairs:
        documents.setdefault("request_document", canary.plant(copy.deepcopy(REQUEST)))
        documents.setdefault("response_document", response())
    event_attributes = {
        "acme.capture.status_code": canary.number(),
        "acme.environment": canary.token(),
        "service.name": canary.token(),
    }
    document = _capture_span(
        _names(SHIPPING, joined) | event_attributes,
        _names(SHIPPING, unwritten) | event_attributes,
        resource={"service.version": canary.token()},
    )
    tentacle = await harness.start()
    response_to_post = await tentacle.post(document)
    assert response_to_post.status_code == 200
    for rule, name, documents in pairs:
        harness.put_pair(rule, name, **documents)
    await harness.read_and_sent(len(pairs))
    assert await tentacle.finish() == EXIT_OK

    assert (
        f"source={CAPTURES} read=7 built=7 sent=7 unsent=0 pending=0 "
        "rejected=bad_action:1,missing_action:1 skipped=local_duplicate:1,object_undecodable:1 "
        "noted=unjoined:3 body_duplicate=0 body_rejected=-"
    ) in caplog.text
    assert "noted=capture_indexed:4 " in caplog.text
    shapes = tentacle.transport.posted("shapes")
    joined_shapes = [shape for shape in shapes if shape["trace_id"] is not None]
    assert sorted(shape["observation_id"] for shape in joined_shapes) == [
        f"shipping:{WORK}:create_shipment:{_stamp(1)}:request",
        f"shipping:{WORK}:create_shipment:{_stamp(1)}:response",
    ]

    streams = json.dumps(await _streams())
    catalog_state = repr(vars(app_module.get_container().catalog))
    captured = capfd.readouterr()
    places = {
        **{f"posted {index}": body for index, body in enumerate(tentacle.transport.bodies)},
        "catalog": catalog_state,
        "streams": streams,
        "logs": caplog.text,
        "stdout": captured.out,
        "stderr": captured.err,
    }
    keys = [
        _key(rule, part, name) for rule, name, _ in pairs for part in ("requests", "responses")
    ]
    held = ["s3://", BUCKET, NOTIFICATIONS["prefix"], SHIPPING["prefix"], *keys]
    for place, text in places.items():
        assert canary.leaks(text, envelope=False) == [], place
        assert [value for value in held if value in text] == [], place
