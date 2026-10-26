"""Drift incidents end to end: a drifted payload opens one incident, diagnosed and notified once.

One system, acme, whose inbound source reads its API's checkout requests: the acme
`order_payload.json` fixture, warm-up variants that toggle its optional fields, and a drifted copy.
The console notifier prints each notification to stdout.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Dossier, EvidenceFragment, EvidenceQuality
from fire.core.domain.incident import Incident, Trigger

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "tests" / "fixtures" / "acme" / "api_requests" / "order_payload.json"
TOKEN = "drift-incidents-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = "api-proxy"
EVENT_SOURCE = "domain-events"
CHANNEL = {
    "kind": "http",
    "direction": "inbound",
    "name": "POST /v1/orders/checkout",
    "part": "request",
}
STREAM = "http:inbound:POST /v1/orders/checkout:request"
EVENT_TYPE = "order.placed"
EVENT_CHANNEL = {"kind": "event", "direction": "internal", "name": EVENT_TYPE, "part": "payload"}
EVENT_STREAM = f"event:internal:{EVENT_TYPE}:payload"
UNDECLARED_CHANNEL = {**EVENT_CHANNEL, "name": "order.unlisted"}
EVENT_PAYLOAD = {"orderId": "fixture-order-1", "total": 10, "currency": "fixture-currency"}
MANIFEST = {"nodes": [{"id": EVENT_TYPE}, {"id": "fulfillment.allocated"}]}
MANIFEST_ENV = "ACME_MANIFEST_PATH"
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "body"}
DRIFT = {"min_samples": 20, "required_ratio": 1.0}
MIN_SAMPLES = DRIFT["min_samples"]
# The checkout request declares these optional.
OPTIONAL = ("adminId", "adminName", "partyId")
# The fixture's placeholder payment id; every other planted string contains "fixture".
PAYMENT_ID = "00000000-0000-4000-8000-000000000001"
START = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()
NOTIFY = "[fire] notify incident="
DIFF = {
    "added": [],
    "type_changed": [{"path": "referenceOrderId", "old": ["string"], "new": ["integer"]}],
    "removed": [{"path": "timezone", "value_type": "string"}],
}

Run = tuple[Incident, Dossier]


def _build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, discovery: dict[str, Any] | None = None
) -> TestClient:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system: dict[str, Any] = {
        "system": "acme",
        "display_name": "Acme Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {
                "id": SOURCE,
                "kind": "replay",
                "direction": "inbound",
                "path_env": "ACME_API_REPLAY_PATH",
                "envelope": ENVELOPE,
            },
            {
                "id": EVENT_SOURCE,
                "kind": "replay",
                "direction": "internal",
                "path_env": "ACME_REPLAY_PATH",
                "envelope": ENVELOPE,
            },
        ],
        "policy": {"critical": [EVENT_TYPE, "order.retired"]},
        "drift": DRIFT,
    }
    if discovery is not None:
        system["discovery"] = discovery
    (tmp_path / "systems" / "acme.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml")
    # Not entered as a context manager: the lifespan would replace this container with one
    # built from the repo's config.
    return TestClient(app)


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """A body with one system, acme, that declares no manifest."""
    yield _build(tmp_path, monkeypatch)
    app_module._container = None


@pytest.fixture
def manifest_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """The same body, with a manifest that declares two events."""
    manifest = tmp_path / "event_chains_graph.json"
    manifest.write_text(json.dumps(MANIFEST), encoding="utf-8")
    monkeypatch.setenv(MANIFEST_ENV, str(manifest))
    discovery = {"manifest": {"kind": "event_chains_graph", "path_env": MANIFEST_ENV}}
    yield _build(tmp_path, monkeypatch, discovery)
    app_module._container = None


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> list[Run]:
    """Incidents and dossiers handed to the background pipeline, in order."""
    captured: list[Run] = []

    async def capture(incident: Incident, dossier: Dossier) -> None:
        captured.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _without(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in keys}


def _warm_up_variant(index: int) -> dict[str, Any]:
    """Toggles the optional fields, never all three at once: that form first comes when warm."""
    payload = _fixture()
    if index % 2 == 0:
        return _without(payload, "adminId", "adminName")
    if index % 3 == 0:
        return _without(payload, "partyId")
    return payload


def _drifted() -> dict[str, Any]:
    """`timezone` removed and `referenceOrderId` an integer."""
    payload = _without(_fixture(), "timezone")
    payload["referenceOrderId"] = 12345
    return payload


def _at(index: int) -> str:
    return (START + timedelta(seconds=index)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _schema_hash(payload: dict[str, Any]) -> str:
    return FINGERPRINTER.fingerprint(GENERALIZER.generalize(payload)).schema_hash


def _observation(
    payload: dict[str, Any], index: int, source: str, channel: dict[str, Any]
) -> dict[str, Any]:
    """`payload` seen on `channel`: its shape and fingerprint only."""
    shape = GENERALIZER.generalize(payload)
    return {
        "system": "acme",
        "source": source,
        "channel": dict(channel),
        "observation_id": f"{channel['name']}-{index:04d}",
        "occurred_at": _at(index),
        "observed_at": _at(index),
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
    }


def _post(
    client: TestClient,
    payloads: list[dict[str, Any]],
    first: int,
    source: str = SOURCE,
    channel: dict[str, Any] = CHANNEL,
) -> httpx.Response:
    """Posts `payloads` as observations numbered from `first`; every one must be accepted."""
    shapes = [
        _observation(payload, first + offset, source, channel)
        for offset, payload in enumerate(payloads)
    ]
    response = client.post("/v0/observations", json={"shapes": shapes}, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["shapes"] == {"accepted": len(shapes), "duplicate": 0, "rejected": []}
    return response


def _warm(client: TestClient) -> list[httpx.Response]:
    """The warm-up variants, then the form with every optional field absent."""
    return [
        _post(client, [_warm_up_variant(index) for index in range(MIN_SAMPLES)], first=0),
        _post(client, [_without(_fixture(), *OPTIONAL)], first=MIN_SAMPLES),
    ]


def _notifications(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)
    elif isinstance(value, str):
        yield value


def _planted() -> list[str]:
    """The fixture's placeholder strings: the payment id and every value holding "fixture"."""
    strings = set(_strings(_fixture()))
    assert PAYMENT_ID in strings
    return sorted({value for value in strings if "fixture" in value} | {PAYMENT_ID})


@pytest.mark.integration
def test_drifted_copy_notifies_once_however_often_it_comes(
    client: TestClient, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    _warm(client)
    assert _notifications(capfd.readouterr().out) == []
    first = MIN_SAMPLES + 1

    _post(client, [_drifted()], first=first)

    out = capfd.readouterr().out
    [header] = _notifications(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=acme trigger=shape_drift", header)
    for name in (STREAM, "referenceOrderId", "timezone"):
        assert name in out

    _post(client, [_drifted()] * 99, first=first + 1)

    assert _notifications(capfd.readouterr().out) == []
    for path in ("referenceOrderId", "timezone"):
        assert path not in caplog.text


@pytest.mark.integration
def test_drift_incident_dossier_holds_the_diff_the_baseline_and_missing_evidence(
    client: TestClient, runs: list[Run]
) -> None:
    _warm(client)
    drifted = _drifted()
    first = MIN_SAMPLES + 1

    _post(client, [drifted], first=first)
    _post(client, [drifted] * 99, first=first + 1)

    [(incident, dossier)] = runs
    assert (incident.trigger, incident.system) == (Trigger.SHAPE_DRIFT, "acme")
    assert incident.summary == f"Shape drift on {STREAM}"
    assert incident.started_at == START + timedelta(seconds=first)
    assert dossier.incident_id == incident.id
    assert dossier.shape == GENERALIZER.generalize(drifted)
    assert dossier.process_path.hops == []
    assert [fragment.collector for fragment in dossier.fragments] == [
        "shape_drift",
        "baseline",
        "coverage",
        "process_path",
    ]
    shape_drift, baseline, coverage, process_path = dossier.fragments
    assert shape_drift.quality is EvidenceQuality.PRESENT
    assert shape_drift.data == {
        "source": SOURCE,
        "stream": STREAM,
        "schema_hash": _schema_hash(drifted),
        "diff": DIFF,
        "occurrences": 1,
        "first_seen": _at(first),
    }
    folded = {_schema_hash(_warm_up_variant(index)) for index in range(MIN_SAMPLES)}
    folded.add(_schema_hash(_without(_fixture(), *OPTIONAL)))
    assert baseline.quality is EvidenceQuality.PRESENT
    assert baseline.data == {
        "source": SOURCE,
        "stream": STREAM,
        "status": "warm",
        "samples": MIN_SAMPLES + 1,
        "fingerprints": len(folded),
        "observations": MIN_SAMPLES + 2,
        "drift_signals": 1,
    }
    assert (coverage.quality, coverage.summary) == (
        EvidenceQuality.MISSING,
        "No manifest declared (observation only)",
    )
    assert (process_path.quality, process_path.summary) == (
        EvidenceQuality.MISSING,
        "No complete flow",
    )


@pytest.mark.integration
def test_coverage_audits_the_observed_event_types_against_the_manifest(
    manifest_client: TestClient, runs: list[Run]
) -> None:
    drifted = {**_without(EVENT_PAYLOAD, "total"), "orderId": 12345}
    _post(manifest_client, [_fixture()], first=0)
    _post(manifest_client, [EVENT_PAYLOAD], 0, EVENT_SOURCE, UNDECLARED_CHANNEL)
    _post(manifest_client, [EVENT_PAYLOAD] * MIN_SAMPLES, 0, EVENT_SOURCE, EVENT_CHANNEL)
    assert runs == []

    _post(manifest_client, [drifted], MIN_SAMPLES, EVENT_SOURCE, EVENT_CHANNEL)

    [(incident, dossier)] = runs
    assert incident.summary == f"Shape drift on {EVENT_STREAM}"
    assert dossier.fragments[0].data["diff"] == {
        "added": [],
        "type_changed": [{"path": "orderId", "old": ["string"], "new": ["integer"]}],
        "removed": [{"path": "total", "value_type": "integer"}],
    }
    assert dossier.fragments[2] == EvidenceFragment(
        collector="coverage",
        quality=EvidenceQuality.PRESENT,
        summary="1 declared events unseen, 1 seen undeclared, 1 unknown references",
        data={
            "system": "acme",
            "declared_unseen": ["fulfillment.allocated"],
            "seen_undeclared": ["order.unlisted"],
            "unknown_references": ["order.retired"],
        },
    )


@pytest.mark.integration
def test_no_placeholder_reaches_notifications_logs_or_responses(
    client: TestClient, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.DEBUG)
    responses = _warm(client)
    responses.append(_post(client, [_drifted()] * 2, first=MIN_SAMPLES + 1))
    responses.append(_post(client, [_without(_fixture(), "totals")], first=MIN_SAMPLES + 3))
    responses.append(
        client.post(
            "/v0/diagnose",
            json={
                "system": "acme",
                "shape": GENERALIZER.generalize(_drifted()).model_dump(mode="json"),
            },
            headers=HEADERS,
        )
    )

    assert responses[-1].status_code == 202
    captured = capfd.readouterr()
    assert len(_notifications(captured.out)) == 3
    outputs = [response.text for response in responses]
    outputs += [caplog.text, captured.out, captured.err]
    for value in _planted():
        for output in outputs:
            assert value not in output


@pytest.mark.integration
def test_manual_diagnosis_notifies_once_as_manual(
    client: TestClient, capfd: pytest.CaptureFixture[str]
) -> None:
    response = client.post("/v0/diagnose", json={"system": "acme"}, headers=HEADERS)

    assert response.status_code == 202
    incident_id = response.json()["incident_id"]
    assert _notifications(capfd.readouterr().out) == [
        f"{NOTIFY}{incident_id} system=acme trigger=manual"
    ]
