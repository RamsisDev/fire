"""Incident correlation end to end: triggers of one system posted to the body inside its
`triggers.correlation.window_s` are one incident, notified when it opens and once more, merged,
when its window closes; the same triggers in separate windows are separate incidents.

The body's clock is injected and moved by hand. The console notifier prints each notification to
stdout; a window's update is diagnosed as the completion loop does, through `_close_windows`.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from spans import ENTRY, SERVICE, T0, Clock, order_spans, span_id, trace_id

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.webhooks.signing import decode_secret, sign
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import Container, build_container
from fire.core.domain.dossier import EvidenceQuality
from fire.core.domain.incident import Trigger
from fire.core.domain.observation import SpanObservation

TOKEN = "correlation-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
WINDOW_S = 60
THRESHOLD = 3
MIN_SAMPLES = 2
PROXY = 0xF00
PUT = "DynamoDB.PutItem orders"
REQUEST = f"http:inbound:{ENTRY}:request"
EVENT = "event:internal:order.charged:payload"
NOTIFY = re.compile(r"^\[fire\] notify incident=(\S+) system=(\S+) trigger=(\S+)(.*)$", re.M)
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()
ORDER = {"orderId": "fixture-order-1", "total": 10, "currency": "fixture-currency"}
# Against ORDER: total turned into a string, then currency removed.
DRIFT_A = {"orderId": "fixture-order-2", "total": "fixture-total", "currency": "fixture-currency"}
DRIFT_B = {"orderId": "fixture-order-3", "total": 10}
CHARGE = {"order_id": "fixture-order", "amount": 9500}
CAPTURE = {"charge_ref": "fixture-charge", "status": "fixture-status"}
DLQ_DETAILS = {"source_queue_arn": "fixture-queue-arn", "receive_count": 5}
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "work_id": "id"}
SECRET_ENV = "FIRE_INCIDENT_IO_SIGNING_SECRET"
SECRET = "whsec_" + base64.b64encode(b"fire-correlation-test-secret-key").decode()
TEAM_ID = "01JPQA75EPNEES4479P16P4XAB"
INCIDENT_IO = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "incident_io"
    / "public_incident.incident_created_v2.json"
)


def _replay(source_id: str, payload: str = "payload", **extra: Any) -> dict[str, Any]:
    return {
        "id": source_id,
        "kind": "replay",
        "direction": "internal",
        "path_env": "SHOP_REPLAY",
        "envelope": {**ENVELOPE, "payload": payload},
        **extra,
    }


def _system(name: str, **triggers: Any) -> dict[str, Any]:
    return {
        "system": name,
        "display_name": name.title(),
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {"id": "traces", "kind": "otlp", "listen_env": "SHOP_OTLP_LISTEN"},
            {
                "id": "api-proxy",
                "kind": "http_proxy",
                "listen_port": 8199,
                "upstream_env": "SHOP_API_UPSTREAM",
            },
            _replay("order-events"),
            _replay("payment-captures"),
            _replay("task-outcomes", "details", outcome="abandoned"),
        ],
        "drift": {"min_samples": MIN_SAMPLES},
        "paths": {"settle_s": 30},
        "triggers": {
            "http_errors": {"threshold": THRESHOLD, "window_s": 10},
            "dlq": {"min_abandoned": 2, "min_rate": 0.5},
            "incident_io": {"match": [{"team_id": TEAM_ID}]},
            **triggers,
        },
    }


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    for system in (
        _system("shop", correlation={"window_s": WINDOW_S}),
        _system("plain"),
    ):
        path = tmp_path / "systems" / f"{system['system']}.yml"
        path.write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    monkeypatch.setenv(SECRET_ENV, SECRET)
    app_module._container = build_container(tmp_path / "fire.yml", clock=clock)
    # Not entered as a context manager: the lifespan would build the repo's container.
    yield TestClient(app)
    app_module._container = None


def _container() -> Container:
    assert app_module._container is not None
    return app_module._container


def _order(trace: int, status: int, system: str) -> list[SpanObservation]:
    """An order whose server span continues the proxy's trace context and answers `status`."""
    failing = status >= 500
    root, put = order_spans(trace, PUT, failing=[PUT] if failing else [], system=system)
    root = root.model_copy(
        update={"parent_span_id": span_id(PROXY), "status_code": status, "error": failing}
    )
    return [root, put]


def _shape(
    system: str,
    source: str,
    channel: dict[str, Any],
    observation_id: str,
    payload: Any,
    **ids: str,
) -> dict[str, Any]:
    shape = GENERALIZER.generalize(payload)
    return {
        "system": system,
        "source": source,
        "channel": channel,
        "observation_id": observation_id,
        "occurred_at": T0.isoformat(),
        "observed_at": T0.isoformat(),
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
        **ids,
    }


def _request(trace: int, payload: Any, system: str = "shop") -> dict[str, Any]:
    channel = {"kind": "http", "direction": "inbound", "name": ENTRY, "part": "request"}
    return _shape(
        system,
        "api-proxy",
        channel,
        f"exchange-{trace}:request",
        payload,
        trace_id=trace_id(trace),
        span_id=span_id(PROXY),
    )


def _work(system: str, source: str, kind: str, name: str, work_id: str, payload: Any) -> Any:
    channel = {"kind": kind, "direction": "internal", "name": name, "part": "payload"}
    return _shape(system, source, channel, work_id, payload, work_id=work_id)


def _post(
    client: TestClient,
    spans: Sequence[SpanObservation] = (),
    shapes: Sequence[dict[str, Any]] = (),
) -> None:
    body = {"spans": [item.model_dump(mode="json") for item in spans], "shapes": list(shapes)}
    response = client.post("/v0/observations", json=body, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["shapes"]["rejected"] == []
    assert response.json()["spans"]["rejected"] == []


def _warm(client: TestClient, system: str = "shop") -> None:
    """`MIN_SAMPLES` orders answered 201 through the proxy warm the route's request stream."""
    warm_up = [100 + index for index in range(MIN_SAMPLES)]
    _post(
        client,
        [item for trace in warm_up for item in _order(trace, 201, system)],
        [_request(trace, ORDER, system) for trace in warm_up],
    )


def _drift(client: TestClient, trace: int, payload: Any, system: str = "shop") -> None:
    _post(client, shapes=[_request(trace, payload, system)])


def _burst(client: TestClient, first: int, system: str = "shop") -> None:
    traces = range(first, first + THRESHOLD)
    _post(client, [item for trace in traces for item in _order(trace, 503, system)])


def _close_windows() -> None:
    """One tick of the completion loop's window closing, with its diagnoses."""
    for incident, dossier in app_module._close_windows(_container()):
        asyncio.run(app_module._run_pipeline(incident, dossier))


def _systems(out: str) -> list[tuple[str, str, str, str]]:
    """Each notification header's incident id, system, trigger, and trigger count."""
    return [(m[0], m[1], m[2], m[3].strip()) for m in NOTIFY.findall(out)]


def _notifications(out: str) -> list[tuple[str, str, str]]:
    return [(incident_id, trigger, more) for incident_id, _, trigger, more in _systems(out)]


@pytest.mark.integration
def test_two_drift_signals_and_a_burst_within_one_window_give_one_incident(
    client: TestClient,
    clock: Clock,
    capfd: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    _warm(client)
    assert _notifications(capfd.readouterr().out) == []

    _drift(client, 1, DRIFT_A)
    [(incident_id, trigger, more)] = _notifications(capfd.readouterr().out)
    assert (trigger, more) == ("shape_drift", "")
    clock.advance(20)
    _drift(client, 2, DRIFT_B)
    clock.advance(20)
    _burst(client, 3)
    assert _notifications(capfd.readouterr().out) == []
    assert [m for m in caplog.messages if m.startswith("incident joined")] == [
        f"incident joined: system=shop incident={incident_id} trigger=shape_drift triggers=2",
        f"incident joined: system=shop incident={incident_id} trigger=http_errors triggers=3",
    ]

    clock.advance(WINDOW_S - 41)
    _close_windows()
    assert _notifications(capfd.readouterr().out) == []
    clock.advance(1)
    _close_windows()

    out = capfd.readouterr().out
    assert _notifications(out) == [(incident_id, "shape_drift", "triggers=3")]
    assert "Triggers (3):" in out
    assert f"• `http_errors` HTTP errors on {ENTRY}" in out
    assert out.count(f"• `shape_drift` Shape drift on {REQUEST}") == 2
    assert [m for m in caplog.messages if m.startswith("incident updated")] == [
        f"incident updated: system=shop incident={incident_id} triggers=3"
    ]
    _close_windows()
    captured = capfd.readouterr()
    assert _notifications(captured.out) == []
    for output in (out, captured.err, caplog.text):
        assert "fixture" not in output


@pytest.mark.integration
def test_the_merged_dossier_holds_every_triggers_evidence(
    client: TestClient, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs: list[Any] = []

    async def capture(incident: Any, dossier: Any) -> None:
        runs.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    _warm(client)
    _drift(client, 1, DRIFT_A)
    clock.advance(1)
    _drift(client, 2, DRIFT_B)
    _burst(client, 3)
    clock.advance(WINDOW_S)
    _close_windows()

    [(opening, alone), (incident, dossier)] = runs
    assert opening.id == incident.id
    assert alone.missing[-1].collector == "process_path"
    assert incident.triggers == [Trigger.SHAPE_DRIFT, Trigger.SHAPE_DRIFT, Trigger.HTTP_ERRORS]
    assert (incident.trigger, incident.service, incident.started_at) == (
        Trigger.SHAPE_DRIFT,
        SERVICE,
        T0,
    )
    assert dossier.shape == GENERALIZER.generalize(DRIFT_A)
    assert [hop.operation for hop in dossier.process_path.hops] == [ENTRY, PUT]
    collectors = [(f.collector, f.quality) for f in dossier.fragments]
    assert collectors.count(("shape_drift", EvidenceQuality.PRESENT)) == 2
    assert ("http_errors", EvidenceQuality.PRESENT) in collectors
    # The burst's hops fill the process path the drift dossiers lacked.
    assert ("process_path", EvidenceQuality.MISSING) not in collectors
    assert [f.collector for f in dossier.missing] == ["coverage", "failing_shapes"]
    for trigger in dossier.triggers:
        assert trigger.fragments
        gathered = {dossier.fragments[index].collector for index in trigger.fragments}
        assert trigger.trigger.value in gathered


@pytest.mark.integration
def test_the_same_triggers_in_separate_windows_give_separate_incidents(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client)

    _drift(client, 1, DRIFT_A)
    clock.advance(WINDOW_S)
    _drift(client, 2, DRIFT_B)
    clock.advance(WINDOW_S)
    _burst(client, 3)
    clock.advance(WINDOW_S)
    _close_windows()

    notifications = _notifications(capfd.readouterr().out)
    assert [(trigger, more) for _id, trigger, more in notifications] == [
        ("shape_drift", ""),
        ("shape_drift", ""),
        ("http_errors", ""),
    ]
    assert len({incident_id for incident_id, _trigger, _more in notifications}) == 3


@pytest.mark.integration
def test_triggers_raised_by_one_batch_notify_once_with_all_of_them(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client)

    _post(
        client,
        [item for trace in range(3, 3 + THRESHOLD) for item in _order(trace, 503, "shop")],
        [_request(1, DRIFT_A), _request(2, DRIFT_B)],
    )
    clock.advance(WINDOW_S)
    _close_windows()

    out = capfd.readouterr().out
    [(_incident_id, trigger, more)] = _notifications(out)
    assert (trigger, more) == ("shape_drift", "triggers=3")
    assert "Triggers (3):" in out


@pytest.mark.integration
def test_the_dlq_fingerprints_of_one_work_unit_are_one_incident(
    client: TestClient, capfd: pytest.CaptureFixture[str]
) -> None:
    for system in ("shop", "plain"):
        work = [f"w-{n}" for n in range(2)]
        _post(
            client,
            shapes=[
                *(
                    _work(system, "order-events", "event", "order.charged", w, CHARGE)
                    for w in work
                ),
                *(
                    _work(system, "payment-captures", "event", "payment.captured", w, CAPTURE)
                    for w in work
                ),
            ],
        )
        _post(
            client,
            shapes=[
                _work(system, "task-outcomes", "outcome", "billing-processor", w, DLQ_DETAILS)
                for w in work
            ],
        )

    out = capfd.readouterr().out
    assert [(system, trigger, more) for _id, system, trigger, more in _systems(out)] == [
        ("shop", "dlq", "triggers=2"),
        ("plain", "dlq", ""),
        ("plain", "dlq", ""),
    ]
    assert "Triggers (2):\n• `dlq` Abandoned work units on " in out
    assert f"Abandoned work units on {EVENT}" in out


@pytest.mark.integration
def test_an_incident_io_incident_joins_the_open_window_and_answers_its_incident_id(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client)
    _drift(client, 1, DRIFT_A)
    [(incident_id, _trigger, _more)] = _notifications(capfd.readouterr().out)
    clock.advance(1)

    body = INCIDENT_IO.read_bytes()
    timestamp = str(int(clock.now.timestamp()))
    signature = sign(decode_secret(SECRET), "msg_fixture", timestamp, body)
    response = client.post(
        "/v0/webhooks/incident-io",
        content=body,
        headers={
            "webhook-id": "msg_fixture",
            "webhook-timestamp": timestamp,
            "webhook-signature": f"v1,{signature}",
            "content-type": "application/json",
        },
    )

    assert response.status_code == 202
    # Both systems map the incident: shop's event joins its window, plain's opens its own.
    assert response.json()["outcome"] == "opened"
    ids = response.json()["incident_ids"]
    [plain] = [other for other in ids if other != incident_id]
    assert sorted(ids) == sorted([incident_id, plain])
    assert _notifications(capfd.readouterr().out) == [(plain, "incident_io", "")]
    clock.advance(WINDOW_S)
    _close_windows()
    out = capfd.readouterr().out
    assert _notifications(out) == [(incident_id, "shape_drift", "triggers=2")]
    assert "• `incident_io` incident.io incident INC-123" in out


@pytest.mark.integration
def test_a_manual_diagnosis_inside_an_open_window_gets_its_own_incident(
    client: TestClient, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    _warm(client)
    _drift(client, 1, DRIFT_A)
    [(incident_id, _trigger, _more)] = _notifications(capfd.readouterr().out)

    response = client.post("/v0/diagnose", json={"system": "shop"}, headers=HEADERS)
    clock.advance(WINDOW_S)
    _close_windows()

    manual = response.json()["incident_id"]
    assert manual != incident_id
    assert _notifications(capfd.readouterr().out) == [(manual, "manual", "")]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_the_running_app_closes_windows_on_its_own(
    client: TestClient,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    _warm(client)
    _drift(client, 1, DRIFT_A)
    _drift(client, 2, DRIFT_B)
    capfd.readouterr()
    clock.advance(WINDOW_S)
    monkeypatch.setattr(app_module, "COMPLETE_EVERY_S", 0.01)

    completing = asyncio.create_task(app_module._complete_flows_forever())
    out = ""
    deadline = time.monotonic() + 5
    while not _notifications(out) and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
        out += capfd.readouterr().out
    completing.cancel()

    [(_incident_id, trigger, more)] = _notifications(out)
    assert (trigger, more) == ("shape_drift", "triggers=2")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_failing_window_close_is_logged_by_type_and_the_loop_goes_on(
    client: TestClient,
    clock: Clock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    container = _container()
    close = container.incidents.close
    calls = 0

    def flaky() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("fixture-secret in a message")
        return close()

    monkeypatch.setattr(container.incidents, "close", flaky)
    monkeypatch.setattr(app_module, "COMPLETE_EVERY_S", 0.01)

    completing = asyncio.create_task(app_module._complete_flows_forever())
    deadline = time.monotonic() + 5
    while calls < 3 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    completing.cancel()

    assert [record.getMessage() for record in caplog.records] == [
        "correlation close failed: error=RuntimeError"
    ]
    assert calls >= 3
