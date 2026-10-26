"""POST /v0/diagnose accepts an Origin and assembles the dossier."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Dossier, EvidenceQuality, Shape, ShapeFingerprint
from fire.core.domain.incident import Incident, Trigger

HEADERS = {"X-FIRE-Token": "test-fire-token"}
SHAPE = {"fields": [{"path": "order.id", "value_type": "string", "cardinality": "1"}]}
FINGERPRINT = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(SHAPE))
UNKNOWN_SYSTEM = "planted-system-7f3a9c1e"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, acme_fire_config: Path):
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container(acme_fire_config)
    with TestClient(app) as test_client:
        yield test_client
    app_module._container = None


@pytest.fixture
def incidents() -> list[Incident]:
    """Incidents handed to the background pipeline, in request order; filled by `dossiers`."""
    return []


@pytest.fixture
def dossiers(monkeypatch: pytest.MonkeyPatch, incidents: list[Incident]) -> list[Dossier]:
    """Dossiers handed to the background pipeline, in request order."""
    captured: list[Dossier] = []

    async def capture(incident: Incident, dossier: Dossier) -> None:
        incidents.append(incident)
        captured.append(dossier)

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    return captured


@pytest.mark.unit
def test_diagnose_with_origin_shape_and_fingerprint(
    client: TestClient, dossiers: list[Dossier], incidents: list[Incident]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "system": "acme",
            "service": "checkout",
            "origin": {
                "origin_id": "orig-orders",
                "kind": "http",
                "endpoint_ref": "orders.create",
            },
            "shape": SHAPE,
            "fingerprint": FINGERPRINT.model_dump(mode="json"),
            "process_hops": [
                {"from_ref": "orig-orders", "to_ref": "orig-payments", "operation": "POST /pay"}
            ],
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    body = response.json()
    assert body["origin_id"] == "orig-orders"
    assert body["system"] == "acme"
    [incident] = incidents
    assert (incident.trigger, incident.system) == (Trigger.MANUAL, "acme")
    assert incident.id == body["incident_id"]
    [dossier] = dossiers
    assert dossier.origin is not None
    assert dossier.origin.origin_id == "orig-orders"
    assert dossier.shape is not None
    assert [field.path for field in dossier.shape.fields] == ["order.id"]
    assert dossier.fingerprint == FINGERPRINT


@pytest.mark.unit
def test_diagnose_declares_missing_evidence_as_fragments(
    client: TestClient, dossiers: list[Dossier], incidents: list[Incident]
) -> None:
    response = client.post("/v0/diagnose", json={"system": "acme"}, headers=HEADERS)
    assert response.status_code == 202
    [incident] = incidents
    assert (incident.trigger, incident.system) == (Trigger.MANUAL, "acme")
    [dossier] = dossiers
    assert [(f.collector, f.quality) for f in dossier.fragments] == [
        ("origin", EvidenceQuality.MISSING),
        ("shape", EvidenceQuality.MISSING),
    ]
    assert dossier.missing == dossier.fragments


@pytest.mark.unit
def test_diagnose_shape_without_fingerprint_computes_it(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    shape = {"fields": [{"path": "order.id", "value_type": "string"}]}
    response = client.post(
        "/v0/diagnose",
        json={
            "system": "acme",
            "origin": {"origin_id": "orig-orders", "endpoint_ref": "orders.create"},
            "shape": shape,
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    [dossier] = dossiers
    assert dossier.fingerprint == Sha256ShapeFingerprinter().fingerprint(
        Shape.model_validate(shape)
    )
    assert dossier.missing == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "fingerprint",
    [
        {"schema_hash": "abc", "normalized_paths": ["order.id"]},
        {**FINGERPRINT.model_dump(mode="json"), "normalized_paths": []},
        {**FINGERPRINT.model_dump(mode="json"), "notes": "extra"},
    ],
    ids=["schema-hash", "normalized-paths", "notes"],
)
def test_diagnose_rejects_a_fingerprint_that_does_not_match_the_shape(
    client: TestClient, dossiers: list[Dossier], fingerprint: dict[str, Any]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={"system": "acme", "shape": SHAPE, "fingerprint": fingerprint},
        headers=HEADERS,
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": [{"loc": ["body", "fingerprint"], "type": "fingerprint_mismatch"}]
    }
    assert dossiers == []


@pytest.mark.unit
def test_diagnose_keeps_a_fingerprint_sent_without_a_shape(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={"system": "acme", "fingerprint": {"schema_hash": "abc"}},
        headers=HEADERS,
    )
    assert response.status_code == 202
    [dossier] = dossiers
    assert dossier.shape is None
    assert dossier.fingerprint == ShapeFingerprint(schema_hash="abc")


@pytest.mark.unit
@pytest.mark.parametrize(
    "extra",
    [{}, {"shape": SHAPE, "fingerprint": {"schema_hash": "abc"}}],
    ids=["system-only", "system-before-fingerprint"],
)
def test_diagnose_rejects_a_system_that_is_not_loaded(
    client: TestClient, dossiers: list[Dossier], extra: dict[str, Any]
) -> None:
    response = client.post(
        "/v0/diagnose", json={"system": UNKNOWN_SYSTEM, **extra}, headers=HEADERS
    )
    assert response.status_code == 422
    assert response.json() == {"detail": [{"loc": ["body", "system"], "type": "unknown_system"}]}
    assert UNKNOWN_SYSTEM not in response.text
    assert dossiers == []


@pytest.mark.unit
def test_diagnose_accepts_node_and_edge_hops(client: TestClient, dossiers: list[Dossier]) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "system": "acme",
            "fingerprint": {"schema_hash": "abc"},
            "process_hops": [
                {"ref": "checkout", "operation": "POST /orders"},
                {"from_ref": "orig-orders", "to_ref": "orig-payments"},
            ],
        },
        headers=HEADERS,
    )
    assert response.status_code == 202
    [dossier] = dossiers
    assert [hop.display_label() for hop in dossier.process_path.hops] == [
        "checkout/POST /orders",
        "orig-orders->orig-payments",
    ]


@pytest.mark.unit
def test_diagnose_rejects_hop_that_is_node_and_edge(
    client: TestClient, dossiers: list[Dossier]
) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "system": "acme",
            "process_hops": [{"ref": "checkout", "from_ref": "orig-orders"}],
        },
        headers=HEADERS,
    )
    assert response.status_code == 422
    assert dossiers == []
