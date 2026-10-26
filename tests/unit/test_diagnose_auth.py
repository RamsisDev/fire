"""HTTP auth fail-closed on POST /v0/diagnose."""

from __future__ import annotations

import hmac
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, acme_fire_config: Path):
    monkeypatch.setenv("FIRE_API_TOKEN", "test-fire-token")
    app_module._container = build_container(acme_fire_config)
    with TestClient(app) as test_client:
        yield test_client
    app_module._container = None


@pytest.mark.unit
def test_diagnose_missing_token_401(client: TestClient) -> None:
    response = client.post("/v0/diagnose", json={"system": "acme"})
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_bad_token_401(client: TestClient) -> None:
    response = client.post(
        "/v0/diagnose",
        json={"system": "acme"},
        headers={"X-FIRE-Token": "wrong"},
    )
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_bad_token_compared_in_constant_time(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_compare_digest = hmac.compare_digest
    calls: list[tuple[object, object]] = []

    def spy(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return real_compare_digest(a, b)

    monkeypatch.setattr(hmac, "compare_digest", spy)
    response = client.post(
        "/v0/diagnose",
        json={"system": "acme"},
        headers={"X-FIRE-Token": "wrong"},
    )
    assert response.status_code == 401
    assert len(calls) == 1


@pytest.mark.unit
def test_diagnose_non_ascii_token_401(client: TestClient) -> None:
    response = client.post(
        "/v0/diagnose",
        json={"system": "acme"},
        headers={"X-FIRE-Token": "wrong-tökén".encode()},
    )
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_unset_token_env_503(
    monkeypatch: pytest.MonkeyPatch, acme_fire_config: Path
) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN", raising=False)
    app_module._container = build_container(acme_fire_config)
    with TestClient(app) as test_client:
        response = test_client.post(
            "/v0/diagnose",
            json={"system": "acme"},
            headers={"X-FIRE-Token": "anything"},
        )
    app_module._container = None
    assert response.status_code == 503


@pytest.mark.unit
@pytest.mark.parametrize("headers", [{}, {"X-FIRE-Token": "wrong"}], ids=["missing", "wrong"])
def test_diagnose_bad_token_401_before_unknown_system(
    client: TestClient, headers: dict[str, str]
) -> None:
    response = client.post("/v0/diagnose", json={"system": "unknown"}, headers=headers)
    assert response.status_code == 401


@pytest.mark.unit
def test_diagnose_unset_token_env_503_before_unknown_system(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FIRE_API_TOKEN", raising=False)
    app_module._container = build_container()
    with TestClient(app) as test_client:
        response = test_client.post(
            "/v0/diagnose",
            json={"system": "unknown"},
            headers={"X-FIRE-Token": "anything"},
        )
    app_module._container = None
    assert response.status_code == 503


@pytest.mark.unit
def test_diagnose_ok_202(client: TestClient) -> None:
    response = client.post(
        "/v0/diagnose",
        json={
            "system": "acme",
            "service": "api",
            "fingerprint": {"schema_hash": "abc"},
            "process_hops": [{"ref": "api", "operation": "POST /x"}],
        },
        headers={"X-FIRE-Token": "test-fire-token"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["system"] == "acme"
    assert body["service"] == "api"
