"""DLQ attribution end to end: abandoned work units posted to /v0/observations join the
fingerprints of their events and open one `dlq` incident per fingerprint that crosses the rule;
`GET /v0/systems/{system}/dlq` reports the counts and the work units no source read; and a
DynamoDB stream outcome source filtered on status leaks no planted value.

The console notifier prints each notification to stdout.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from aws_streams import isolate_aws
from canary import WIRE_FORMS, Canary, WireContext
from event_harness import HEADERS as BODY_HEADERS
from event_harness import Body, counters, streams_text
from fastapi.testclient import TestClient
from moto import mock_aws

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK
from fire.core.domain.dossier import Dossier, EvidenceQuality
from fire.core.domain.incident import Incident, Trigger

TOKEN = "dlq-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()
STREAM = "event:internal:order.charged:payload"
OUTCOME = "outcome:internal:billing-processor:payload"
NOTIFY = "[fire] notify incident="
ORDER = {"order_id": "fixture-order", "amount": 9500}
DLQ_DETAILS = {"source_queue_arn": "fixture-queue-arn", "receive_count": 5}
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "work_id": "id"}

Run = tuple[Incident, Dossier]


def _system(name: str, **extra: Any) -> dict[str, Any]:
    return {
        "system": name,
        "display_name": name.title(),
        "notify": {"slack": {"channel_id": "C0123456789"}},
        **extra,
    }


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    shop = _system(
        "shop",
        sources=[
            {
                "id": "order-events",
                "kind": "replay",
                "direction": "internal",
                "path_env": "SHOP_EVENTS",
                "envelope": {**ENVELOPE, "payload": "payload"},
            },
            {
                "id": "task-outcomes",
                "kind": "replay",
                "direction": "internal",
                "outcome": "abandoned",
                "path_env": "SHOP_OUTCOMES",
                "envelope": {**ENVELOPE, "payload": "details"},
            },
        ],
        triggers={"dlq": {"min_abandoned": 2, "min_rate": 0.5}},
    )
    for system in (shop, _system("quiet")):
        path = tmp_path / "systems" / f"{system['system']}.yml"
        path.write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml")
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


def _shape(source: str, kind: str, name: str, work_id: str, payload: Any) -> dict[str, Any]:
    shape = GENERALIZER.generalize(payload)
    return {
        "system": "shop",
        "source": source,
        "channel": {"kind": kind, "direction": "internal", "name": name, "part": "payload"},
        "observation_id": work_id,
        "work_id": work_id,
        "occurred_at": T0.isoformat(),
        "observed_at": T0.isoformat(),
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
    }


def _event(work_id: str, payload: Any = ORDER) -> dict[str, Any]:
    return _shape("order-events", "event", "order.charged", work_id, payload)


def _abandoned(work_id: str) -> dict[str, Any]:
    return _shape("task-outcomes", "outcome", "billing-processor", work_id, DLQ_DETAILS)


def _post(client: TestClient, shapes: Sequence[dict[str, Any]], accepted: int) -> None:
    response = client.post("/v0/observations", json={"shapes": list(shapes)}, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["shapes"]["accepted"] == accepted


def _notifications(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


SCHEMA_HASH = FINGERPRINTER.fingerprint(GENERALIZER.generalize(ORDER)).schema_hash


@pytest.mark.integration
def test_a_fingerprint_whose_work_units_are_abandoned_notifies_once_naming_stream_and_hash(
    client: TestClient, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    _post(client, [_event(f"w-{n}") for n in range(4)], accepted=4)
    _post(client, [_abandoned("w-0")], accepted=1)
    assert _notifications(capfd.readouterr().out) == []

    _post(client, [_abandoned("w-1")], accepted=1)

    out = capfd.readouterr().out
    [header] = _notifications(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=shop trigger=dlq", header)
    assert (
        f"Abandoned work on {STREAM}: 2 of 4 work units with fingerprint {SCHEMA_HASH} (50%)"
    ) in out
    assert f"Shape hash: `{SCHEMA_HASH}`" in out
    assert [m for m in caplog.messages if m.startswith("dlq attribution raised")] == [
        f"dlq attribution raised: system=shop source=order-events stream={STREAM} "
        f"fingerprint={SCHEMA_HASH} abandoned=2 observed=4 rate=0.500"
    ]

    _post(client, [_abandoned("w-2"), _abandoned("w-1")], accepted=1)
    _post(client, [_abandoned("w-1")], accepted=0)
    assert _notifications(capfd.readouterr().out) == []


@pytest.mark.integration
def test_an_abandoned_work_unit_no_source_read_is_reported_missing(
    client: TestClient, runs: list[Run], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    _post(client, [_event("w-0"), _event("w-1")], accepted=2)
    _post(client, [_abandoned("w-9")], accepted=1)

    assert runs == []
    assert [m for m in caplog.messages if m.startswith("dlq unattributed")] == [
        f"dlq unattributed: system=shop source=task-outcomes outcome={OUTCOME} work_id=w-9"
    ]
    report = client.get("/v0/systems/shop/dlq", headers=HEADERS).json()
    assert (report["abandoned"], report["unattributed"], report["fingerprints"]) == (
        1,
        ["w-9"],
        [],
    )

    _post(client, [_abandoned("w-0"), _abandoned("w-1")], accepted=2)

    [(incident, dossier)] = runs
    assert (incident.system, incident.trigger, incident.summary) == (
        "shop",
        Trigger.DLQ,
        f"Abandoned work units on {STREAM}",
    )
    assert [(f.collector, f.quality) for f in dossier.fragments] == [
        ("dlq_attribution", EvidenceQuality.PRESENT),
        ("dlq_unattributed", EvidenceQuality.MISSING),
        ("process_path", EvidenceQuality.MISSING),
    ]
    assert dossier.fragments[1].data == {"abandoned": 3, "unattributed": 1}
    assert dossier.shape == GENERALIZER.generalize(ORDER)
    assert dossier.fingerprint is not None and dossier.fingerprint.schema_hash == SCHEMA_HASH


@pytest.mark.integration
def test_the_dlq_endpoint_reports_counts_rule_and_what_is_firing(
    client: TestClient, runs: list[Run]
) -> None:
    _post(client, [*(_event(f"w-{n}") for n in range(3)), _event("w-3", {"coupon": "x"})], 4)
    _post(client, [_abandoned("w-0"), _abandoned("w-1"), _abandoned("w-3")], accepted=3)

    response = client.get("/v0/systems/shop/dlq", headers=HEADERS)

    assert response.status_code == 200
    report = response.json()
    coupon = FINGERPRINTER.fingerprint(GENERALIZER.generalize({"coupon": "x"})).schema_hash
    expected = [
        {
            "source": "order-events",
            "stream": STREAM,
            "schema_hash": SCHEMA_HASH,
            "observed": 3,
            "abandoned": 2,
            "abandoned_by": {OUTCOME: 2},
            "first_abandoned": "2026-10-11T16:30:00Z",
            "last_abandoned": "2026-10-11T16:30:00Z",
            "rate": 2 / 3,
            "firing": True,
        },
        {
            "source": "order-events",
            "stream": STREAM,
            "schema_hash": coupon,
            "observed": 1,
            "abandoned": 1,
            "abandoned_by": {OUTCOME: 1},
            "first_abandoned": "2026-10-11T16:30:00Z",
            "last_abandoned": "2026-10-11T16:30:00Z",
            "rate": 1.0,
            "firing": False,
        },
    ]
    assert report == {
        "system": "shop",
        "sources": ["task-outcomes"],
        "window_s": 604800.0,
        "rule": {"min_abandoned": 2, "min_rate": 0.5},
        "abandoned": 3,
        "unattributed": [],
        "fingerprints": sorted(expected, key=lambda f: f["schema_hash"]),
        "streams": [{"source": "order-events", "stream": STREAM, "observed": 4, "abandoned": 3}],
    }
    assert len(runs) == 1
    streams = client.get("/v0/systems/shop/streams", headers=HEADERS).json()["streams"]
    assert [(s["source"], s["key"]) for s in streams] == [
        ("order-events", STREAM),
        ("task-outcomes", OUTCOME),
    ]


@pytest.mark.integration
def test_the_dlq_endpoint_checks_the_token_then_the_system(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert client.get("/v0/systems/shop/dlq").status_code == 401
    assert client.get("/v0/systems/nope/dlq").status_code == 401
    assert client.get("/v0/systems/nope/dlq", headers=HEADERS).status_code == 404
    assert client.get("/v0/systems/quiet/dlq", headers=HEADERS).json() == {
        "system": "quiet",
        "missing_reason": "No source declares outcome: abandoned",
    }
    monkeypatch.delenv("FIRE_API_TOKEN")
    assert client.get("/v0/systems/shop/dlq", headers=HEADERS).status_code == 503


@pytest.mark.integration
def test_a_failing_attribution_is_logged_by_type_and_the_batch_is_still_accepted(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)
    container = app_module.get_container()

    def fail(*_args: Any) -> None:
        raise RuntimeError("planted-message")

    monkeypatch.setattr(container.dlq, "evaluate", fail)
    _post(client, [_abandoned("w-0")], accepted=1)

    assert caplog.messages == ["dlq attribution failed: system=shop error=RuntimeError"]


# The canary: a DynamoDB stream outcome source filtered on status, after an event source


SYSTEM = "shop"
TABLE_EVENTS = {
    "id": "table-events",
    "kind": "dynamodb_stream",
    "direction": "internal",
    "table_env": "EVENTS_TABLE",
    "regions": ["us-east-1"],
    "poll_s": 0.05,
    "envelope": {
        "event_id": {"path": "SK", "strip_prefix": "EVENT#"},
        "event_type": "event_type",
        "occurred_at": "created_at",
        "work_id": {"path": "SK", "strip_prefix": "EVENT#"},
        "payload": "payload",
    },
}
TABLE_OUTCOMES = {
    "id": "table-outcomes",
    "kind": "dynamodb_stream",
    "direction": "internal",
    "outcome": "abandoned",
    "table_env": "OUTBOX_TABLE",
    "regions": ["us-east-1"],
    "poll_s": 0.05,
    "filter": {"attribute": "status", "equals": "ABANDONED"},
    "envelope": {
        "event_id": "work_unit_id",
        "event_type": "processor_name",
        "occurred_at": "last_updated",
        "work_id": "work_unit_id",
        "payload": "error_details",
    },
}


@pytest.fixture
async def body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> AsyncIterator[Body]:
    caplog.set_level(logging.DEBUG)
    isolate_aws(monkeypatch, tmp_path)
    system = _system(
        SYSTEM,
        sources=[TABLE_EVENTS, TABLE_OUTCOMES],
        triggers={"dlq": {"min_abandoned": 2, "min_rate": 0.5}},
    )
    with mock_aws():
        harness = Body.configure(tmp_path, monkeypatch, caplog, system)
        yield harness
        await harness.finish()


def _planted_event(canary: Canary, work_id: str, event_type: str) -> dict[str, Any]:
    # Numbers only in the payload, so every event carries one fingerprint.
    return {
        "PK": canary.string(),
        "SK": f"EVENT#{work_id}",
        "event_type": event_type,
        "created_at": canary.envelope_timestamp(),
        "customer": canary.plant({"email": "x", "name": "x"}),
        "payload": {"order": canary.number(), "lines": [{"qty": canary.number()}]},
    }


def _planted_row(canary: Canary, work_id: str, processor: str, status: str) -> dict[str, Any]:
    return {
        "PK": canary.string(),
        "SK": canary.string(),
        "work_unit_id": work_id,
        "processor_name": processor,
        "last_updated": canary.envelope_timestamp(),
        "status": status,
        "message": canary.string(),
        "error_details": canary.plant(
            {"source_queue_arn": "x", "receive_count": 1, "original_work_unit": {"id": "x"}}
        ),
    }


async def _dlq_text(system: str) -> str:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(f"/v0/systems/{system}/dlq", headers=BODY_HEADERS)
    assert response.status_code == 200
    return response.text


@pytest.mark.integration
async def test_an_outcome_source_filtered_on_status_leaks_no_planted_value(
    body: Body,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    event_type, processor = canary.envelope_name(), canary.envelope_name()
    work = [canary.envelope_id() for _ in range(4)]
    events = [_planted_event(canary, work_id, event_type) for work_id in work[:3]]
    abandoned = [_planted_row(canary, work_id, processor, "ABANDONED") for work_id in work[:2]]
    unattributed = _planted_row(canary, work[3], processor, "ABANDONED")
    filtered = [
        _planted_row(canary, canary.envelope_id(), canary.envelope_name(), canary.token())
        for _ in range(2)
    ]
    no_work = _planted_row(canary, canary.envelope_id(), canary.envelope_name(), "ABANDONED")
    del no_work["work_unit_id"]
    for row in (*filtered, no_work):
        keys = ("work_unit_id", "processor_name", "last_updated")
        canary.rejected(*(row[key] for key in keys if key in row))
    rows = [*abandoned, *filtered, no_work, unattributed]
    broken = [canary.token()]

    events_table = WIRE_FORMS["dynamodb_stream"](
        Path(), events, [], WireContext(SYSTEM, "table-events", canary.token)
    )
    outbox_table = WIRE_FORMS["dynamodb_stream"](
        Path(), rows, broken, WireContext(SYSTEM, "table-outcomes", canary.token)
    )
    environ = {"EVENTS_TABLE": events_table, "OUTBOX_TABLE": outbox_table}
    first = body.start(environ, source="table-events")
    await body.read_and_sent({"table-events": len(events)})
    assert await first.finish() == EXIT_OK
    second = body.start(environ, source="table-outcomes")
    await body.read_and_sent({"table-outcomes": len(rows) + 2 * len(broken)})
    assert await second.finish() == EXIT_OK

    # The broken item holds no status, so the filter drops it before its envelope is read.
    line = counters(caplog.messages)["table-outcomes"]
    assert (
        "built=3 sent=3 unsent=0 pending=0 rejected=missing_event_id:1 "
        "skipped=filtered:3,remove_record:1 "
    ) in line
    out = capfd.readouterr()
    assert len(_notifications(out.out)) == 1
    shapes = [
        *body.started[0].transport.shapes(),
        *body.started[1].transport.shapes(),
    ]
    assert len(shapes) == 6
    for shape in shapes:
        assert canary.observation_leaks(shape) == []
    for tentacle in body.started:
        for posted in tentacle.transport.bodies:
            assert canary.leaks(posted, envelope=False) == []
    catalog_state = repr(vars(app_module.get_container().catalog))
    assert canary.leaks(catalog_state, envelope=False) == []
    for text in (await streams_text(SYSTEM), await _dlq_text(SYSTEM)):
        assert canary.leaks(text, envelope=False) == []
    for output in (caplog.text, out.out, out.err):
        assert canary.leaks(output, envelope=False) == []
        assert events_table not in output and outbox_table not in output
