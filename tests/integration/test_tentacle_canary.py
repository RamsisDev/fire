"""Canary: no planted value reaches what the tentacle posts, the body's state, or any output."""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from canary import WIRE_FORMS, Canary, WireContext
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, run_tentacle
from fire.apps.tentacle.sources import READERS

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "acme" / "test_payloads"
TOKEN = "canary-test-token"
SOURCE = {
    "id": "samples",
    "kind": "replay",
    "direction": "internal",
    "path_env": "CANARY_REPLAY",
    "envelope": {
        "event_id": "work_unit_id",
        "event_type": "event_type",
        "occurred_at": "metadata.timestamp",
        "work_id": "work_unit_id",
        "correlation_id": "metadata.correlation_id",
        "payload": "payload",
    },
}


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    fire_yml = tmp_path / "config" / "fire.yml"
    (fire_yml.parent / "systems").mkdir(parents=True)
    fire_yml.write_text("{}", encoding="utf-8")
    system = {
        "system": "canary",
        "display_name": "Canary",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [SOURCE],
    }
    (fire_yml.parent / "systems" / "canary.yml").write_text(
        yaml.safe_dump(system), encoding="utf-8"
    )
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(fire_yml)
    yield fire_yml
    app_module._container = None


def _work_unit(
    canary: Canary,
    sample: dict[str, Any],
    *,
    event_id: str | None = None,
    event_type: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """A work unit sample with every value planted, then the given envelope values."""
    record = canary.plant(copy.deepcopy(sample))
    record["work_unit_id"] = event_id or canary.envelope_id()
    record["event_type"] = event_type or canary.envelope_name()
    record["metadata"]["timestamp"] = timestamp or canary.envelope_timestamp()
    record["metadata"]["correlation_id"] = canary.envelope_id()
    return record


@pytest.mark.integration
def test_every_kind_with_a_reader_has_a_canary_wire_form() -> None:
    assert set(READERS) <= set(WIRE_FORMS)


@pytest.mark.integration
def test_replay_canary(
    tmp_path: Path,
    config: Path,
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.DEBUG)
    samples = [json.loads(path.read_text()) for path in sorted(FIXTURES.glob("*.json"))]
    valid = [_work_unit(canary, sample) for sample in samples]

    missing_id = _work_unit(canary, samples[0])
    canary.rejected(missing_id.pop("work_unit_id"), missing_id["event_type"])
    bad_name = _work_unit(canary, samples[1], event_type=f"{canary.token()} has spaces")
    bad_timestamp = _work_unit(canary, samples[2], timestamp=canary.token())
    too_long_id = canary.envelope_id()
    too_long = _work_unit(canary, samples[3], event_id=too_long_id * 20)
    for record in (bad_name, bad_timestamp, too_long):
        canary.rejected(
            record["work_unit_id"],
            record["event_type"],
            record["metadata"]["timestamp"],
            record["metadata"]["correlation_id"],
        )
    canary.rejected(too_long_id)
    not_an_object = canary.string()
    broken = [f'{{"work_unit_id": "{canary.token()}", "payload": {{"x": "{canary.token()}"']

    records = [valid[0], missing_id, valid[1], bad_name, valid[2], not_an_object]
    records += [bad_timestamp, valid[3], too_long, valid[4], copy.deepcopy(valid[0])]
    context = WireContext("canary", SOURCE["id"], canary.token)
    location = WIRE_FORMS["replay"](tmp_path / "replay", records, broken, context)
    transport = RecordingTransport(httpx.ASGITransport(app=app))
    environ = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body", "CANARY_REPLAY": location}

    code = run_tentacle("canary", config_path=config, environ=environ, transport=transport)

    assert code == EXIT_OK
    assert (
        "source=samples read=12 built=5 sent=5 unsent=0 pending=0 "
        "rejected=bad_name:1,bad_timestamp:1,invalid:1,missing_event_id:1,undecodable:2 "
        "skipped=local_duplicate:1 noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text

    posted = [shape for body in transport.bodies for shape in json.loads(body)["shapes"]]
    assert sorted(shape["observation_id"] for shape in posted) == sorted(
        record["work_unit_id"] for record in valid
    )
    for body in transport.bodies:
        assert canary.leaks(body, envelope=False) == []
    for shape in posted:
        assert canary.observation_leaks(shape) == []

    catalog_state = repr(vars(app_module.get_container().catalog))
    assert canary.leaks(catalog_state, envelope=False) == []
    streams = TestClient(app).get("/v0/systems/canary/streams", headers={"X-FIRE-Token": TOKEN})
    assert streams.status_code == 200
    assert len(streams.json()["streams"]) == 5
    assert [leak for leak in canary.leaks(streams.text) if leak not in canary.names] == []

    captured = capfd.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert canary.leaks(output) == []
