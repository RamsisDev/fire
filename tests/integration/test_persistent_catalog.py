"""The body on the SQLite catalog: `build_container` twice on one file is a restart, and what the
first body learned, baselines, drift signals, paths, and dedupe keys, the second one still knows.

The clock is injected and moved by hand, so nothing waits for `settle_s`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from spans import Clock, order_spans

import fire.apps.incident_engine.app as app_module
from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sqlite.observation_catalog import SqliteObservationCatalog
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import Container, build_container
from fire.core.domain.dossier import Dossier
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import SpanObservation

TOKEN = "catalog-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
MIN_SAMPLES = 2
SETTLE = 60
CANARY = "canary-3c9e1f7a5b2d4e8f"
PUT = "DynamoDB.PutItem orders"
PAY = "POST payments.example.com/v1/charges"
GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()

Run = tuple[Incident, Dossier]


def _write_config(tmp_path: Path, catalog: dict[str, Any]) -> Path:
    (tmp_path / "fire.yml").write_text(yaml.safe_dump({"catalog": catalog}), encoding="utf-8")
    (tmp_path / "systems").mkdir(exist_ok=True)
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [
            {
                "id": "order-events",
                "kind": "kinesis",
                "direction": "internal",
                "stream_env": "SHOP_STREAM",
                "regions": ["us-east-1"],
                "envelope": {
                    "event_id": "id",
                    "event_type": "type",
                    "occurred_at": "at",
                    "payload": "data",
                },
            },
            {"id": "traces", "kind": "otlp", "listen_env": "SHOP_OTLP_LISTEN"},
        ],
        "drift": {"min_samples": MIN_SAMPLES},
        "paths": {"settle_s": SETTLE},
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    return tmp_path / "fire.yml"


@pytest.fixture
def catalog_file(tmp_path: Path) -> Path:
    return tmp_path / "var" / "catalog.sqlite3"


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, catalog_file: Path, clock: Clock
) -> Iterator[Callable[[], TestClient]]:
    """Starts a body on the catalog file, stopping the one before: each call is a restart."""
    config = _write_config(tmp_path, {"provider": "sqlite", "path_env": "SHOP_CATALOG_PATH"})
    monkeypatch.setenv("SHOP_CATALOG_PATH", str(catalog_file))
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)

    def restart() -> TestClient:
        if app_module._container is not None:
            app_module._container.catalog.close()
        app_module._container = build_container(config, clock=clock)
        # Not entered as a context manager: the lifespan would build the repo's container.
        return TestClient(app)

    yield restart
    if app_module._container is not None:
        app_module._container.catalog.close()
    app_module._container = None


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> list[Run]:
    captured: list[Run] = []

    async def capture(incident: Incident, dossier: Dossier) -> None:
        captured.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


def _shape(observation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    shape = GENERALIZER.generalize(payload)
    return {
        "system": "shop",
        "source": "order-events",
        "channel": {
            "kind": "event",
            "direction": "internal",
            "name": "order.created",
            "part": "payload",
        },
        "observation_id": observation_id,
        "occurred_at": "2026-10-11T16:30:00Z",
        "observed_at": "2026-10-11T16:30:01Z",
        "work_id": "work-1",
        "shape": shape.model_dump(mode="json"),
        "fingerprint": FINGERPRINTER.fingerprint(shape).model_dump(mode="json"),
    }


ORDER = {"orderId": "fixture-order", "total": 10}
DRIFTED = {"orderId": "fixture-order", "total": "10"}


def _post(
    client: TestClient,
    shapes: Sequence[dict[str, Any]] = (),
    spans: Sequence[SpanObservation] = (),
) -> dict[str, Any]:
    body = {"shapes": list(shapes), "spans": [item.model_dump(mode="json") for item in spans]}
    response = client.post("/v0/observations", json=body, headers=HEADERS)
    assert response.status_code == 202
    return response.json()


def _streams(client: TestClient) -> list[dict[str, Any]]:
    response = client.get("/v0/systems/shop/streams", headers=HEADERS)
    assert response.status_code == 200
    return response.json()["streams"]


def _paths(client: TestClient) -> dict[str, Any]:
    response = client.get("/v0/systems/shop/paths", headers=HEADERS)
    assert response.status_code == 200
    return response.json()


@pytest.mark.integration
def test_a_warm_stream_is_still_warm_after_the_body_restarts(
    start: Callable[[], TestClient], runs: list[Run]
) -> None:
    client = start()
    warm = [_shape(f"warm-{index}", ORDER) for index in range(MIN_SAMPLES)]
    _post(client, warm)
    _post(client, [_shape("drift-0", DRIFTED)])
    before = _streams(client)
    assert (before[0]["status"], before[0]["samples"], len(before[0]["drift"])) == ("warm", 2, 1)
    assert [incident.trigger for incident, _ in runs] == [Trigger.SHAPE_DRIFT]

    client = start()

    assert _streams(client) == before
    assert _post(client, warm)["shapes"]["duplicate"] == MIN_SAMPLES
    assert _post(client, [_shape("drift-1", DRIFTED)])["shapes"]["accepted"] == 1
    assert len(runs) == 1
    [stream] = _streams(client)
    assert (stream["samples"], stream["drift"][0]["occurrences"]) == (2, 2)
    work = app_module.get_container().catalog.work("shop", "work-1")
    assert [entry.observation_id for entry in work] == ["drift-0", "drift-1", "warm-0", "warm-1"]


@pytest.mark.integration
def test_paths_and_path_drift_survive_the_body_restart(
    start: Callable[[], TestClient], clock: Clock, runs: list[Run]
) -> None:
    client = start()
    for trace in range(1, MIN_SAMPLES + 1):
        _post(client, spans=order_spans(trace, PUT))
    clock.advance(SETTLE)
    _post(client, spans=order_spans(3, PUT, PAY))
    clock.advance(SETTLE)
    before = _paths(client)
    assert before["paths"][0]["status"] == "warm"
    assert [incident.trigger for incident, _ in runs] == [Trigger.PATH_DRIFT]

    client = start()

    assert _paths(client) == before
    _post(client, spans=order_spans(4, PUT, PAY))
    clock.advance(SETTLE)
    [path] = _paths(client)["paths"]
    assert (path["drifted_flows"], path["drift"][0]["occurrences"]) == (2, 2)
    assert len(runs) == 1


@pytest.mark.integration
def test_a_rejected_payload_never_reaches_the_catalog_file(
    start: Callable[[], TestClient], catalog_file: Path
) -> None:
    client = start()
    leaky = {**_shape("leaky", ORDER), "payload": {"email": CANARY}}
    report = _post(client, [leaky, _shape("kept", {"note": "fixture"})])
    assert (report["shapes"]["accepted"], report["shapes"]["rejected"][0]["reason"]) == (
        1,
        "invalid",
    )
    app_module.get_container().catalog.close()
    app_module._container = None

    content = b"".join(path.read_bytes() for path in catalog_file.parent.iterdir())
    assert CANARY.encode() not in content
    assert b"fixture" not in content
    assert json.dumps("note").encode() in content


@pytest.mark.integration
def test_the_memory_catalog_is_the_default(tmp_path: Path) -> None:
    container = build_container(_write_config(tmp_path, {}))

    assert type(container.catalog) is MemoryObservationCatalog


@pytest.mark.integration
def test_the_sqlite_catalog_needs_its_path_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _write_config(tmp_path, {"provider": "sqlite", "path_env": "SHOP_CATALOG_PATH"})
    monkeypatch.delenv("SHOP_CATALOG_PATH", raising=False)

    with pytest.raises(ValueError, match="SHOP_CATALOG_PATH"):
        build_container(config)


@pytest.mark.integration
def test_the_lifespan_closes_the_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, catalog_file: Path
) -> None:
    config = _write_config(tmp_path, {"provider": "sqlite", "path_env": "SHOP_CATALOG_PATH"})
    monkeypatch.setenv("SHOP_CATALOG_PATH", str(catalog_file))
    container: Container = build_container(config)
    closed: list[bool] = []
    close = container.catalog.close

    def recorded_close() -> None:
        closed.append(True)
        close()

    monkeypatch.setattr(container.catalog, "close", recorded_close)
    monkeypatch.setattr(app_module, "build_container", lambda: container)

    with TestClient(app):
        assert isinstance(app_module.get_container().catalog, SqliteObservationCatalog)
    assert closed == [True]
