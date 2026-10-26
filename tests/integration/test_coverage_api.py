"""GET /v0/systems/{system}/coverage through the FastAPI app.

The acme cases load an acme system, written to `tmp_path`, against the acme fixtures' manifest
and the OpenAPI documents of its two HTTP services, `acme-api` and `acme-pricing`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.manifests.openapi import OPERATIONS
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.dossier import Shape

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "acme"
MANIFEST = FIXTURES / "manifest" / "event_chains_graph.json"
API_OPENAPI = FIXTURES / "openapi" / "acme_api.json"
PRICING_OPENAPI = FIXTURES / "openapi" / "acme_pricing.json"
ENVELOPE = {
    "event_id": "id",
    "event_type": "eventType",
    "occurred_at": "occurredAt",
    "payload": "payload",
}
ACME = {
    "system": "acme",
    "display_name": "Acme Shop",
    "notify": {"slack": {"channel_id": "C0123456789"}},
    "discovery": {"manifest": {"kind": "event_chains_graph", "path_env": "ACME_MANIFEST_PATH"}},
    "sources": [
        {"id": "acme-traces", "kind": "otlp", "listen_env": "ACME_OTLP_LISTEN"},
        {
            "id": "acme-api",
            "kind": "http_proxy",
            "listen_port": 8180,
            "upstream_env": "ACME_API_UPSTREAM",
            "openapi_file_env": "ACME_API_OPENAPI_PATH",
        },
        {
            "id": "acme-pricing",
            "kind": "http_proxy",
            "listen_port": 8190,
            "upstream_env": "ACME_PRICING_UPSTREAM",
            "openapi_file_env": "ACME_PRICING_OPENAPI_PATH",
        },
        {
            "id": "domain-events",
            "kind": "kinesis",
            "direction": "internal",
            "stream_env": "ACME_EVENTS_STREAM",
            "regions": ["us-east-1"],
            "envelope": ENVELOPE,
        },
        {
            "id": "acme-captures",
            "kind": "s3_capture",
            "bucket_env": "ACME_CAPTURE_BUCKET",
            "captures": [
                {"prefix": "billing-payloads/", "label": "billing", "layout": "work_unit_entity"}
            ],
        },
    ],
}
DOCUMENT_ENVS = ("ACME_MANIFEST_PATH", "ACME_API_OPENAPI_PATH", "ACME_PRICING_OPENAPI_PATH")
TOKEN = "coverage-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
SHAPE = {
    "fields": [{"path": "order_id", "value_type": "string", "cardinality": "1", "optional": False}]
}
FINGERPRINT = Sha256ShapeFingerprinter().fingerprint(Shape.model_validate(SHAPE)).model_dump()
CHECKOUT = "POST /v1/orders/checkout"


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _routes(path: Path) -> list[str]:
    """Every `METHOD path` of an OpenAPI document, read here rather than by `openapi_routes`."""
    return sorted(
        f"{method.upper()} {route}"
        for route, item in _load(path)["paths"].items()
        for method in item
        if method in OPERATIONS
    )


def _published_events() -> list[str]:
    """The events the manifest says stream-publisher consumes."""
    consumers: dict[str, list[str]] = _load(MANIFEST)["consumers"]
    return sorted(name for name, names in consumers.items() if "stream-publisher" in names)


def _shape(source: str, channel: dict[str, Any], number: int) -> dict[str, Any]:
    return {
        "system": "acme",
        "source": source,
        "channel": channel,
        "observation_id": f"observation-{number}",
        "occurred_at": "2026-10-11T16:30:00Z",
        "observed_at": "2026-10-11T16:30:01Z",
        "shape": SHAPE,
        "fingerprint": FINGERPRINT,
    }


def _event(name: str, number: int) -> dict[str, Any]:
    channel = {"kind": "event", "direction": "internal", "name": name, "part": "payload"}
    return _shape("domain-events", channel, number)


def _http(source: str, direction: str, name: str, number: int) -> dict[str, Any]:
    channel = {"kind": "http", "direction": direction, "name": name, "part": "request"}
    return _shape(source, channel, number)


def _post(client: TestClient, shapes: list[dict[str, Any]]) -> None:
    response = client.post("/v0/observations", json={"shapes": shapes}, headers=HEADERS)
    assert response.status_code == 202
    assert response.json()["shapes"] == {
        "accepted": len(shapes),
        "duplicate": 0,
        "rejected": [],
    }


def _start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = build_container(tmp_path / "fire.yml")
    # Not entered as a context manager: the lifespan would replace this container with one
    # built from the repo's config.
    yield TestClient(app)
    app_module._container = None


def _write_acme(tmp_path: Path) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    (tmp_path / "systems" / "acme.yml").write_text(yaml.safe_dump(ACME), encoding="utf-8")


@pytest.fixture
def acme(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    _write_acme(tmp_path)
    for name, path in zip(DOCUMENT_ENVS, (MANIFEST, API_OPENAPI, PRICING_OPENAPI), strict=True):
        monkeypatch.setenv(name, str(path))
    yield from _start(tmp_path, monkeypatch)


@pytest.fixture
def acme_without_documents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    _write_acme(tmp_path)
    for name in DOCUMENT_ENVS:
        monkeypatch.delenv(name, raising=False)
    yield from _start(tmp_path, monkeypatch)


@pytest.mark.integration
def test_acme_observed_only_through_the_publisher_leaves_6_declared_events_unseen(
    acme: TestClient,
) -> None:
    consumed = _published_events()
    _post(acme, [_event(name, number) for number, name in enumerate(consumed)])

    response = acme.get("/v0/systems/acme/coverage", headers=HEADERS)

    assert response.status_code == 200
    events = response.json()["events"]
    assert len(consumed) == 11
    assert events["declared_unseen"] == [
        "inventory.stock_created",
        "inventory.stock_updated",
        "notification.send",
        "order.placement_unsettled",
        "payments.refund_acknowledged",
        "shipping.carrier_event_received",
    ]
    assert (events["seen_undeclared"], events["unknown_references"]) == ([], [])


@pytest.mark.integration
def test_acme_lists_the_routes_never_called(acme: TestClient) -> None:
    _post(
        acme,
        [
            _http("acme-api", "inbound", CHECKOUT, 1),
            _http("acme-traces", "outbound", "payments/v1/tax", 2),
            _http("acme-captures", "outbound", "billing:invoice_place", 3),
        ],
    )

    body = acme.get("/v0/systems/acme/coverage", headers=HEADERS).json()

    api, pricing = body["inbound_routes"]
    api_routes = _routes(API_OPENAPI)
    assert len(api_routes) == 14
    assert api == {
        "source": "acme-api",
        "declared_unseen": [route for route in api_routes if route != CHECKOUT],
        "seen_undeclared": [],
    }
    assert pricing == {
        "source": "acme-pricing",
        "declared_unseen": ["GET /healthcheck", "POST /v1/quote"],
        "seen_undeclared": [],
    }
    assert body["outbound_operations"] == [
        {"source": "acme-captures", "operations": ["billing:invoice_place"]},
        {"source": "acme-traces", "operations": ["payments/v1/tax"]},
    ]


@pytest.mark.integration
def test_acme_without_its_documents_reports_each_part_missing(
    acme_without_documents: TestClient,
) -> None:
    body = acme_without_documents.get("/v0/systems/acme/coverage", headers=HEADERS).json()

    assert body == {
        "system": "acme",
        "events": {"missing_reason": "ACME_MANIFEST_PATH is not set"},
        "inbound_routes": [
            {"source": "acme-api", "missing_reason": "ACME_API_OPENAPI_PATH is not set"},
            {
                "source": "acme-pricing",
                "missing_reason": "ACME_PRICING_OPENAPI_PATH is not set",
            },
        ],
        "outbound_operations": [],
    }


def _write_system(directory: Path, system: str, sources: list[dict[str, Any]]) -> None:
    raw = {
        "system": system,
        "display_name": system.title(),
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": sources,
    }
    (directory / f"{system}.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    proxy = {"id": "api", "kind": "http_proxy", "listen_port": 8120, "upstream_env": "U"}
    _write_system(systems, "shop", [proxy])
    _write_system(systems, "billing", [])
    yield from _start(tmp_path, monkeypatch)


@pytest.mark.integration
def test_a_system_without_documents_reports_why(client: TestClient) -> None:
    response = client.get("/v0/systems/shop/coverage", headers=HEADERS)
    assert response.status_code == 200
    assert response.json() == {
        "system": "shop",
        "events": {"missing_reason": "No manifest declared (observation only)"},
        "inbound_routes": [
            {"source": "api", "missing_reason": "No OpenAPI document declared (openapi_file_env)"}
        ],
        "outbound_operations": [],
    }


@pytest.mark.integration
def test_a_system_without_proxies_has_no_inbound_routes(client: TestClient) -> None:
    body = client.get("/v0/systems/billing/coverage", headers=HEADERS).json()
    assert body["inbound_routes"] == []


@pytest.mark.integration
def test_coverage_of_an_unknown_system_is_404(client: TestClient) -> None:
    assert client.get("/v0/systems/unknown/coverage", headers=HEADERS).status_code == 404


@pytest.mark.integration
@pytest.mark.parametrize("headers", [{}, {"X-FIRE-Token": "wrong"}], ids=["missing", "wrong"])
@pytest.mark.parametrize("system", ["shop", "unknown"])
def test_coverage_bad_token_is_401(
    client: TestClient, headers: dict[str, str], system: str
) -> None:
    assert client.get(f"/v0/systems/{system}/coverage", headers=headers).status_code == 401


@pytest.mark.integration
def test_coverage_unset_token_env_is_503(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN")
    assert client.get("/v0/systems/shop/coverage", headers=HEADERS).status_code == 503
