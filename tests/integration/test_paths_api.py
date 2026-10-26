"""Flows and path drift through the body's API: spans posted to /v0/observations assemble on the
body's clock, `/paths` completes and lists them, and a drifted flow notifies once. The acme
checkout recordings, mapped by the tentacle's own `parse_traces` and `SpanPipeline`, give one path
across the worker's link, and a copy without its billing call raises path drift.

The clock is injected and moved by hand, so nothing waits for `settle_s`. The console notifier
prints each notification to stdout.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import re
import time
from collections.abc import Iterator, Sequence
from functools import cache
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from spans import ENTRY, SERVICE, T0, Clock, order_spans, span_id, trace_id

import fire.apps.incident_engine.app as app_module
from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sources.otlp import parse_traces
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import Container, build_container
from fire.core.domain.dossier import Dossier, EvidenceQuality
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import SpanLink, SpanObservation
from fire.core.domain.operation import OperationNamer
from fire.core.services.diagnosis import DiagnosisService
from fire.core.services.span_pipeline import SpanPipeline

TOKEN = "paths-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SETTLE = 60
MIN_SAMPLES = 2
PUT = "DynamoDB.PutItem orders"
PAY = "POST payments.example.com/v1/charges"
TAX = "POST tax.example.com/v1/codes"
PROXY = 0xF00
REQUEST = f"http:inbound:{ENTRY}:request"
RESPONSE = f"http:inbound:{ENTRY}:response:2xx"
NOTIFY = "[fire] notify incident="
COUNTERS = {
    "orphan_flows": 0,
    "evicted_flows": 0,
    "late_spans": 0,
    "late_shapes": 0,
    "unattached_shapes": 0,
    "fan_in_links": 0,
}
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()
RECORDINGS = Path(__file__).resolve().parents[1] / "fixtures" / "acme" / "otlp"
API, WORKER_SERVICE = "acme-api", "acme-worker"
CHECKOUT = "POST /v1/orders/checkout"
WORKER = "worker.distribute_work"
BUSINESS = "billing-publisher.business_logic"
BILLING_CALL = "POST billing.partner.example.com/v2/invoices/place"

Run = tuple[Incident, Dossier]


def _container(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Container:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {"id": "traces", "kind": "otlp", "listen_env": "SHOP_OTLP_LISTEN"},
            {
                "id": "api-proxy",
                "kind": "http_proxy",
                "listen_port": 8199,
                "upstream_env": "SHOP_API_UPSTREAM",
            },
        ],
        "drift": {"min_samples": MIN_SAMPLES},
        "paths": {"settle_s": SETTLE},
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    return build_container(tmp_path / "fire.yml", clock=clock)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Iterator[TestClient]:
    app_module._container = _container(tmp_path, monkeypatch, clock)
    # Not entered as a context manager: the lifespan would build the repo's container.
    yield TestClient(app)
    app_module._container = None


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> list[Run]:
    """Incidents and dossiers handed to the background pipeline, in order."""
    captured: list[Run] = []

    async def capture(incident: Incident, dossier: Dossier) -> None:
        captured.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


def _order(trace: int, *calls: str) -> list[SpanObservation]:
    """An order flow whose server span continues the inbound proxy's trace context."""
    root, *children = order_spans(trace, *calls)
    return [root.model_copy(update={"parent_span_id": span_id(PROXY)}), *children]


def _proxy_shape(trace: int, part: str) -> dict[str, Any]:
    shape = GENERALIZER.generalize({"orderId": "fixture-order", "total": 10})
    channel: dict[str, Any] = {"kind": "http", "direction": "inbound", "name": ENTRY, "part": part}
    if part == "response":
        channel["status_class"] = "2xx"
    return {
        "system": "shop",
        "source": "api-proxy",
        "channel": channel,
        "observation_id": f"exchange-{trace}:{part}",
        "occurred_at": T0.isoformat(),
        "observed_at": T0.isoformat(),
        "trace_id": trace_id(trace),
        "span_id": span_id(PROXY),
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
    }


def _post(
    client: TestClient,
    spans: Sequence[SpanObservation] = (),
    shapes: Sequence[dict[str, Any]] = (),
) -> None:
    body = {"spans": [item.model_dump(mode="json") for item in spans], "shapes": list(shapes)}
    response = client.post("/v0/observations", json=body, headers=HEADERS)
    assert response.status_code == 202
    for kind, sent in (("spans", spans), ("shapes", shapes)):
        assert response.json()[kind] == {"accepted": len(sent), "duplicate": 0, "rejected": []}


def _paths(client: TestClient) -> dict[str, Any]:
    response = client.get("/v0/systems/shop/paths", headers=HEADERS)
    assert response.status_code == 200
    return response.json()


def _warm(client: TestClient, clock: Clock) -> None:
    for trace in range(1, MIN_SAMPLES + 1):
        _post(client, _order(trace, PUT, PAY))
    clock.advance(SETTLE)
    assert _paths(client)["paths"][0]["status"] == "warm"


@cache
def _recorded(name: str) -> tuple[SpanObservation, ...]:
    """One recording's spans as the tentacle sends them, in the order the collector sent them."""
    pipeline = SpanPipeline(
        system="shop",
        source="traces",
        sample_rate=1.0,
        namer=OperationNamer(),
        generalizer=GENERALIZER,
        fingerprinter=FINGERPRINTER,
    )
    spans = []
    for path in sorted((RECORDINGS / name).glob("*.json")):
        for raw in parse_traces(json.loads(path.read_text(encoding="utf-8"))):
            outcome = pipeline.process(raw)
            assert outcome.span is not None
            spans.append(outcome.span)
    return tuple(spans)


def _recorded_copy(number: int, name: str) -> list[SpanObservation]:
    """The recording with fresh ids: within one copy, each id always maps to the same new id."""

    def fresh(value: str) -> str:
        return hashlib.sha256(f"{number}:{value}".encode()).hexdigest()[: len(value)]

    return [
        item.model_copy(
            update={
                "trace_id": fresh(item.trace_id),
                "span_id": fresh(item.span_id),
                "parent_span_id": None
                if item.parent_span_id is None
                else fresh(item.parent_span_id),
                "links": [
                    SpanLink(trace_id=fresh(link.trace_id), span_id=fresh(link.span_id))
                    for link in item.links
                ],
            }
        )
        for item in _recorded(name)
    ]


def _named_edges(path: dict[str, Any]) -> set[tuple[Any, ...]]:
    nodes = [(node["service"], node["operation"]) for node in path["nodes"]]
    return {(nodes[edge["from"]], nodes[edge["to"]], edge["via"]) for edge in path["edges"]}


def _notifications(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


@pytest.mark.integration
def test_paths_needs_the_token_and_a_loaded_system(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert client.get("/v0/systems/shop/paths").status_code == 401
    assert (
        client.get("/v0/systems/billing/paths", headers={"X-FIRE-Token": "x"}).status_code == 401
    )
    assert client.get("/v0/systems/billing/paths", headers=HEADERS).status_code == 404
    assert _paths(client) == {"system": "shop", "open_flows": 0, "counters": COUNTERS, "paths": []}

    monkeypatch.delenv("FIRE_API_TOKEN")
    assert client.get("/v0/systems/shop/paths", headers=HEADERS).status_code == 503


@pytest.mark.integration
def test_a_flow_shows_in_paths_once_it_settles_with_the_proxys_shapes(
    client: TestClient, clock: Clock
) -> None:
    _post(client, _order(1, PUT), [_proxy_shape(1, "request"), _proxy_shape(1, "response")])

    clock.advance(SETTLE - 1)
    assert (_paths(client)["open_flows"], _paths(client)["paths"]) == (1, [])
    clock.advance(1)

    assert _paths(client) == {
        "system": "shop",
        "open_flows": 0,
        "counters": COUNTERS,
        "paths": [
            {
                "entry": {"service": SERVICE, "operation": ENTRY},
                "status": "baseline rebuilding",
                "flows": 1,
                "drifted_flows": 0,
                "broken_flows": 0,
                "truncated_flows": 0,
                "nodes": [
                    {
                        "service": SERVICE,
                        "operation": PUT,
                        "kinds": ["client"],
                        "flows": 1,
                        "spans": 1,
                        "errors": 0,
                        "channels": {},
                    },
                    {
                        "service": SERVICE,
                        "operation": ENTRY,
                        "kinds": ["server"],
                        "flows": 1,
                        "spans": 1,
                        "errors": 0,
                        "channels": {REQUEST: 1, RESPONSE: 1},
                    },
                ],
                "edges": [
                    {"from": 1, "to": 0, "via": "child", "flows": 1, "count": 1, "errors": 0}
                ],
                "flow_channels": {},
                "drift": [],
            }
        ],
    }


@pytest.mark.integration
def test_a_drifted_flow_notifies_once_with_every_signal_it_fired(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client, clock)
    assert _notifications(capfd.readouterr().out) == []

    _post(client, _order(10, PUT, TAX))
    clock.advance(SETTLE)
    _paths(client)

    out = capfd.readouterr().out
    [header] = _notifications(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=shop trigger=path_drift", header)
    assert f"hop_added: {TAX} on {ENTRY}" in out
    assert f"hop_missing: {PAY} on {ENTRY}" in out

    _post(client, _order(11, PUT, TAX))
    clock.advance(SETTLE)
    body = _paths(client)

    assert _notifications(capfd.readouterr().out) == []
    [path] = body["paths"]
    assert (path["status"], path["flows"], path["drifted_flows"]) == ("warm", 2, 2)
    assert [
        (signal["rule"], signal["node"]["operation"], signal["occurrences"])
        for signal in path["drift"]
    ] == [("hop_added", TAX, 2), ("hop_missing", PAY, 2)]
    assert path["drift"][0]["trace_ids"] == [trace_id(10)]


@pytest.mark.integration
@pytest.mark.parametrize("name", ["checkout_1", "checkout_2"])
def test_a_recorded_checkout_gives_one_path_across_the_workers_link(
    client: TestClient, clock: Clock, name: str
) -> None:
    _post(client, _recorded(name))
    clock.advance(SETTLE)
    body = _paths(client)

    assert (body["open_flows"], body["counters"]["orphan_flows"]) == (0, 1)
    [path] = body["paths"]
    assert (path["entry"], path["flows"]) == ({"service": API, "operation": CHECKOUT}, 1)
    edges = _named_edges(path)
    assert ((API, CHECKOUT), (WORKER_SERVICE, WORKER), "link") in edges
    assert [via for _, _, via in edges].count("link") == 1
    billing_publisher = "acme-billing-publisher"
    assert ((billing_publisher, BUSINESS), (billing_publisher, BILLING_CALL), "child") in edges


@pytest.mark.integration
def test_a_recorded_checkout_without_its_billing_call_notifies_once(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    # One copy of each recording: MIN_SAMPLES flows.
    for number, name in enumerate(("checkout_1", "checkout_2")):
        _post(client, _recorded_copy(number, name))
        clock.advance(SETTLE)
    assert _paths(client)["paths"][0]["status"] == "warm"
    assert _notifications(capfd.readouterr().out) == []

    drifted = [item for item in _recorded_copy(10, "checkout_1") if item.operation != BILLING_CALL]
    _post(client, drifted)
    clock.advance(SETTLE)
    [path] = _paths(client)["paths"]

    out = capfd.readouterr().out
    [header] = _notifications(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=shop trigger=path_drift", header)
    assert f"hop_missing: {BILLING_CALL} on {CHECKOUT}" in out
    assert [(signal["rule"], signal["node"]["operation"]) for signal in path["drift"]] == [
        ("hop_missing", BILLING_CALL)
    ]


@pytest.mark.integration
def test_the_path_drift_dossier_holds_the_flows_hops_and_its_signals(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _warm(client, clock)
    _post(client, _order(10, PUT, TAX))
    clock.advance(SETTLE)
    _paths(client)

    [(incident, dossier)] = runs
    assert (incident.trigger, incident.system, incident.summary) == (
        Trigger.PATH_DRIFT,
        "shop",
        f"Path drift on {ENTRY}",
    )
    assert incident.started_at == T0
    assert [(hop.ref, hop.operation, hop.status) for hop in dossier.process_path.hops] == [
        (SERVICE, ENTRY, "ok 201"),
        (SERVICE, PUT, "ok 200"),
        (SERVICE, TAX, "ok 200"),
    ]
    assert [(f.collector, f.quality, f.summary) for f in dossier.fragments] == [
        ("path_drift", EvidenceQuality.PRESENT, f"hop_added: {TAX} on {ENTRY}"),
        ("path_drift", EvidenceQuality.PRESENT, f"hop_missing: {PAY} on {ENTRY}"),
    ]
    assert dossier.missing == []


@pytest.mark.integration
def test_open_flows_raise_nothing_until_they_settle(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _warm(client, clock)
    _post(client, _order(10, PUT, TAX))

    clock.advance(SETTLE - 1)
    body = _paths(client)

    assert runs == []
    assert body["open_flows"] == 1
    assert body["paths"][0]["drift"] == []


@pytest.mark.integration
def test_the_running_app_completes_settled_flows_on_its_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock
) -> None:
    container = _container(tmp_path, monkeypatch, clock)
    # Not capfd: the app prints from its own thread, and a print that lands while
    # `readouterr()` reads and truncates the capture file is lost.
    stream = io.StringIO()
    container.diagnosis = DiagnosisService(
        reasoner=EchoReasoner(), narrator=TemplateNarrator(), notifier=ConsoleNotifier(stream)
    )
    monkeypatch.setattr(app_module, "build_container", lambda: container)
    monkeypatch.setattr(app_module, "load_fire_env", lambda: None)
    monkeypatch.setattr(app_module, "COMPLETE_EVERY_S", 0.01)

    def until(condition: Any) -> None:
        deadline = time.monotonic() + 5
        while not condition():
            assert time.monotonic() < deadline, "the app did not complete the flows"
            time.sleep(0.01)

    with TestClient(app) as client:
        for trace in range(1, MIN_SAMPLES + 1):
            _post(client, _order(trace, PUT, PAY))
        clock.advance(SETTLE)
        until(lambda: [path.flows for path in container.catalog.paths("shop").paths] == [2])
        _post(client, _order(10, PUT, TAX))
        clock.advance(SETTLE)
        until(lambda: _notifications(stream.getvalue()))

    assert len(_notifications(stream.getvalue())) == 1


@pytest.mark.integration
async def test_a_failing_completion_or_diagnosis_is_logged_and_completion_goes_on(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    container = _container(tmp_path, monkeypatch, clock)
    calls = 0
    complete = container.catalog.complete_flows

    def flaky() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("fixture-secret in a message")
        return complete()

    async def failing_pipeline(incident: Incident, dossier: Dossier) -> None:
        raise ValueError("fixture-secret in a message")

    monkeypatch.setattr(container.catalog, "complete_flows", flaky)
    monkeypatch.setattr(app_module, "_run_pipeline", failing_pipeline)
    monkeypatch.setattr(app_module, "COMPLETE_EVERY_S", 0.01)
    monkeypatch.setattr(app_module, "_container", container)
    for trace in range(1, MIN_SAMPLES + 1):
        for item in _order(trace, PUT, PAY):
            container.catalog.add_span(item)
    clock.advance(SETTLE)
    for item in _order(10, PUT, TAX):
        container.catalog.add_span(item)
    clock.advance(SETTLE)

    completing = asyncio.create_task(app_module._complete_flows_forever())
    deadline = time.monotonic() + 5
    while "diagnosis failed" not in caplog.text and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    completing.cancel()

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0] == "flow completion failed: error=RuntimeError"
    assert re.fullmatch(r"diagnosis failed: incident=\S+ error=ValueError", messages[1])
    assert "fixture-secret" not in caplog.text
