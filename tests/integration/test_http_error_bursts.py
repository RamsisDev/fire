"""5xx bursts through the body's API: failing `server` spans posted to /v0/observations open one
incident per burst, diagnosed and notified once.

The body's clock is injected and moved by hand, so the window passes without waiting. The console
notifier prints each notification to stdout.
"""

from __future__ import annotations

import logging
import re
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
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Dossier, EvidenceQuality, ProcessHop
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import SpanObservation

TOKEN = "http-error-bursts-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
THRESHOLD = 3
WINDOW_S = 10
MIN_SAMPLES = 2
PUT = "DynamoDB.PutItem orders"
REFUND = "POST /refunds"
PROXY = 0xF00
SOURCE = "api-proxy"
REQUEST = f"http:inbound:{ENTRY}:request"
NOTIFY = "[fire] notify incident="
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()
ORDER = {"orderId": "fixture-order-1", "total": 10, "currency": "fixture-currency"}
DRIFTED = {"orderId": "fixture-order-2", "total": "fixture-total"}
ERROR = {"detail": "fixture-upstream-failure"}

Run = tuple[Incident, Dossier]


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {"id": "traces", "kind": "otlp", "listen_env": "SHOP_OTLP_LISTEN"},
            {
                "id": SOURCE,
                "kind": "http_proxy",
                "listen_port": 8199,
                "upstream_env": "SHOP_API_UPSTREAM",
            },
        ],
        "drift": {"min_samples": MIN_SAMPLES},
        "paths": {"settle_s": 60},
        "triggers": {"http_errors": {"threshold": THRESHOLD, "window_s": WINDOW_S}},
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml", clock=clock)
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


def _order(trace: int, status: int = 503, operation: str = ENTRY) -> list[SpanObservation]:
    """An order whose server span continues the proxy's trace context and answers `status`;
    a failing one's DynamoDB write fails too.
    """
    failing = status >= 500
    root, put = order_spans(trace, PUT, failing=[PUT] if failing else [])
    root = root.model_copy(
        update={
            "parent_span_id": span_id(PROXY),
            "status_code": status,
            "error": failing,
            "operation": operation,
        }
    )
    return [root, put]


def _proxy_shape(trace: int, payload: dict[str, Any], part: str = "request") -> dict[str, Any]:
    shape = GENERALIZER.generalize(payload)
    channel: dict[str, Any] = {"kind": "http", "direction": "inbound", "name": ENTRY, "part": part}
    if part == "response":
        channel["status_class"] = "5xx"
    return {
        "system": "shop",
        "source": SOURCE,
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
    duplicate: int = 0,
) -> None:
    body = {"spans": [item.model_dump(mode="json") for item in spans], "shapes": list(shapes)}
    response = client.post("/v0/observations", json=body, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["spans"] == {
        "accepted": len(spans) - duplicate,
        "duplicate": duplicate,
        "rejected": [],
    }


def _failing(client: TestClient, *traces: int, operation: str = ENTRY) -> None:
    _post(client, [item for trace in traces for item in _order(trace, operation=operation)])


def _notifications(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


@pytest.mark.integration
def test_threshold_errors_on_one_route_notify_once_naming_the_route(
    client: TestClient, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    _failing(client, *range(1, THRESHOLD))
    assert _notifications(capfd.readouterr().out) == []

    _failing(client, THRESHOLD)

    out = capfd.readouterr().out
    [header] = _notifications(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=shop trigger=http_errors", header)
    assert f"(`shop/{SERVICE}`)" in out
    assert f"HTTP errors on {ENTRY}: 3 responses >= 500 within 10s (503 x3)" in out
    bursts = [line for line in caplog.messages if line.startswith("http errors raised")]
    assert bursts == [
        f"http errors raised: system=shop service={SERVICE} route={ENTRY} errors=3 "
        "status_min=500 window_s=10"
    ]

    _failing(client, *range(THRESHOLD + 1, THRESHOLD + 6))

    assert _notifications(capfd.readouterr().out) == []


@pytest.mark.integration
def test_errors_outside_the_window_on_two_routes_or_resent_raise_nothing(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _failing(client, 1, 2)
    clock.advance(WINDOW_S)
    _failing(client, 3)
    _failing(client, 4, operation=REFUND)
    _failing(client, 5, operation=REFUND)
    _post(client, _order(4, operation=REFUND), duplicate=2)
    _post(client, [item for trace in (6, 7, 8) for item in _order(trace, status=201)])

    assert runs == []


@pytest.mark.integration
def test_a_route_bursts_again_only_after_window_s_with_no_error(
    client: TestClient, clock: Clock, runs: list[Run]
) -> None:
    _failing(client, 1, 2, 3)
    assert len(runs) == 1
    for trace in range(4, 8):
        clock.advance(WINDOW_S - 1)
        _failing(client, trace)
    assert len(runs) == 1

    clock.advance(WINDOW_S)
    _failing(client, 8, 9)
    assert len(runs) == 1
    _failing(client, 10)

    [_first, (incident, dossier)] = runs
    assert incident.trigger is Trigger.HTTP_ERRORS
    assert dossier.fragments[0].data["trace_ids"] == [trace_id(8), trace_id(9), trace_id(10)]


def _bursts(runs: list[Run]) -> list[Run]:
    return [run for run in runs if run[0].trigger is Trigger.HTTP_ERRORS]


def _warm(client: TestClient) -> None:
    """`MIN_SAMPLES` orders answered 201 through the proxy warm the route's request stream."""
    warm_up = [100 + index for index in range(MIN_SAMPLES)]
    _post(
        client,
        [item for trace in warm_up for item in _order(trace, status=201)],
        [_proxy_shape(trace, ORDER) for trace in warm_up],
    )


def _failing_through_the_proxy(client: TestClient) -> None:
    """Three failing orders through the proxy: the second's request drifted, the third crosses
    the threshold.
    """
    _post(
        client,
        [item for trace in (1, 2) for item in _order(trace)],
        [_proxy_shape(1, ORDER), _proxy_shape(1, ERROR, "response"), _proxy_shape(2, DRIFTED)],
    )
    _post(client, _order(3), [_proxy_shape(3, ORDER)])


@pytest.mark.integration
def test_the_dossier_carries_the_failing_flows_path_and_fingerprints_next_to_the_baseline(
    client: TestClient, runs: list[Run]
) -> None:
    _warm(client)

    _failing_through_the_proxy(client)

    # The drifted request opened its own shape drift incident.
    assert [incident.trigger for incident, _dossier in runs] == [
        Trigger.SHAPE_DRIFT,
        Trigger.HTTP_ERRORS,
    ]
    [(incident, dossier)] = _bursts(runs)
    assert (incident.system, incident.trigger, incident.service) == (
        "shop",
        Trigger.HTTP_ERRORS,
        SERVICE,
    )
    assert incident.summary == f"HTTP errors on {ENTRY}"
    assert incident.started_at == T0
    assert (dossier.incident_id, dossier.service) == (incident.id, SERVICE)
    assert dossier.process_path.hops == [
        ProcessHop(ref=SERVICE, operation=ENTRY, span_kind="server", status="error 503"),
        ProcessHop(ref=SERVICE, operation=PUT, span_kind="client", status="error 502"),
    ]
    http_errors, failing_shapes, baseline, process_path = dossier.fragments
    assert http_errors.data["statuses"] == {"503": 3}
    assert http_errors.data["trace_ids"] == [trace_id(1), trace_id(2), trace_id(3)]
    order = FINGERPRINTER.fingerprint(GENERALIZER.generalize(ORDER)).schema_hash
    drifted = FINGERPRINTER.fingerprint(GENERALIZER.generalize(DRIFTED)).schema_hash
    error = FINGERPRINTER.fingerprint(GENERALIZER.generalize(ERROR)).schema_hash
    assert failing_shapes.quality is EvidenceQuality.PRESENT
    assert failing_shapes.data["shaped"] == 3
    assert {
        (entry["stream"], entry["schema_hash"]): (entry["requests"], entry["drifted"])
        for entry in failing_shapes.data["shapes"]
    } == {
        (REQUEST, order): (2, False),
        (REQUEST, drifted): (1, True),
        (f"http:inbound:{ENTRY}:response:5xx", error): (1, False),
    }
    assert baseline.data == {
        "source": SOURCE,
        "stream": REQUEST,
        "status": "warm",
        "samples": MIN_SAMPLES + 2,
        "fingerprints": 1,
        "observations": MIN_SAMPLES + 3,
        "drift_signals": 1,
    }
    assert process_path.data == {"flows": 3, "held": 3, "requests": 3}
    assert dossier.missing == []


@pytest.mark.integration
def test_no_value_of_the_failing_requests_reaches_notifications_or_logs(
    client: TestClient, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _warm(client)

    _failing_through_the_proxy(client)

    captured = capfd.readouterr()
    assert len(_notifications(captured.out)) == 2
    for output in (captured.out, captured.err, caplog.text):
        assert "fixture" not in output
