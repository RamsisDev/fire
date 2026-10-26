"""The acme fixtures' five work unit samples, replayed through a replay source into the body."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, run_tentacle

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "acme" / "test_payloads"
TOKEN = "replay-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SOURCE = {
    "id": "work-unit-samples",
    "kind": "replay",
    "direction": "internal",
    "path_env": "ACME_REPLAY_PATH",
    "envelope": {
        "event_id": "work_unit_id",
        "event_type": "event_type",
        "occurred_at": "metadata.timestamp",
        "work_id": "work_unit_id",
        "payload": "payload",
    },
}
EVENT_TYPES = [
    "fulfillment.allocated",
    "fulfillment.return_received",
    "order.charged",
    "payments.refund_acknowledged",
    "payments.refund_initiated",
]


@pytest.fixture
def fire_yml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """An acme system with the replay source next to an empty fire.yml, and a body from them."""
    path = tmp_path / "fire.yml"
    path.write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "acme",
        "display_name": "Acme Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [SOURCE],
    }
    (tmp_path / "systems" / "acme.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(path)
    yield path
    app_module._container = None


def _replay(fire_yml: Path, replay_path: Path, **kwargs: Any) -> int:
    environ = {
        "FIRE_API_TOKEN": TOKEN,
        "FIRE_BODY_URL": "http://body",
        "ACME_REPLAY_PATH": str(replay_path),
    }
    transport = httpx.ASGITransport(app=app)
    return run_tentacle(
        "acme", config_path=fire_yml, environ=environ, transport=transport, **kwargs
    )


def _streams() -> list[dict[str, Any]]:
    response = TestClient(app).get("/v0/systems/acme/streams", headers=HEADERS)
    assert response.status_code == 200
    return response.json()["streams"]


@pytest.mark.integration
def test_work_unit_samples_give_five_event_channels_of_one_sample(fire_yml: Path) -> None:
    assert _replay(fire_yml, FIXTURES) == EXIT_OK

    streams = _streams()
    assert [(stream["source"], stream["key"], stream["observations"]) for stream in streams] == [
        ("work-unit-samples", f"event:internal:{event_type}:payload", 1)
        for event_type in EVENT_TYPES
    ]
    assert all(len(stream["fingerprints"]) == 1 for stream in streams)


@pytest.mark.integration
def test_second_replay_adds_only_duplicates(
    fire_yml: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    assert _replay(fire_yml, FIXTURES) == EXIT_OK
    first = _streams()

    assert _replay(fire_yml, FIXTURES, source="work-unit-samples") == EXIT_OK

    assert _streams() == first
    assert (
        "source=work-unit-samples read=5 built=5 sent=5 unsent=0 pending=0 rejected=- "
        "skipped=- noted=- body_duplicate=5 body_rejected=-"
    ) in caplog.text
