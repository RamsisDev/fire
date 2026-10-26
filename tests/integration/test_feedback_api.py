"""Decisions and feedback over HTTP: a manual diagnosis is recorded, a reviewer's verdict on it
is queued as pending retraining, and a laya reasoner answers from an `httpx.MockTransport`.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
import fire.apps.incident_engine.wiring as wiring
from fire.apps.incident_engine.app import app

TOKEN = "feedback-api-token"
HEADERS = {"X-FIRE-Token": TOKEN}
HOPS = [{"ref": "api"}, {"from_ref": "api", "to_ref": "db", "operation": "write"}]


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reasoner: dict[str, Any]) -> None:
    (tmp_path / "fire.yml").write_text(yaml.safe_dump({"reasoner": reasoner}), encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0SHOP0001"}},
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    app_module._container = wiring.build_container(tmp_path / "fire.yml")


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    _build(tmp_path, monkeypatch, {"provider": "echo"})
    # Not entered as a context manager: the lifespan would replace this container.
    yield TestClient(app)
    app_module._container = None


def _diagnose(client: TestClient) -> str:
    response = client.post(
        "/v0/diagnose", json={"system": "shop", "process_hops": HOPS}, headers=HEADERS
    )
    assert response.status_code == 202
    return response.json()["incident_id"]


def _decision(client: TestClient, incident_id: str) -> dict[str, Any]:
    response = client.get(f"/v0/incidents/{incident_id}/decision", headers=HEADERS)
    assert response.status_code == 200
    return response.json()


def _ids(client: TestClient, incident_id: str) -> tuple[str, str]:
    """The top possibility id of the decision, and another one."""
    possibilities = _decision(client, incident_id)["decision"]["possibilities"]
    ranked = sorted(possibilities, key=lambda p: p["probability"], reverse=True)
    assert len(ranked) > 1
    return ranked[0]["id"], ranked[1]["id"]


@pytest.mark.integration
def test_the_decision_is_recorded_without_its_dossier(client: TestClient) -> None:
    incident_id = _diagnose(client)

    body = _decision(client, incident_id)

    assert body["incident_id"] == incident_id
    assert (body["system"], body["reasoner"], body["model"]) == ("shop", "echo", None)
    assert body["feedback"] is None
    assert body["decision"]["possibilities"]
    assert "dossier" not in body and "decided_at" in body


@pytest.mark.integration
@pytest.mark.parametrize("verdict", ["confirmed", "corrected", "rejected"])
def test_a_verdict_is_queued_as_pending(client: TestClient, verdict: str) -> None:
    incident_id = _diagnose(client)
    top, other = _ids(client, incident_id)
    possibility = {"confirmed": None, "corrected": other, "rejected": None}[verdict]
    named = {"confirmed": top, "corrected": other, "rejected": None}[verdict]

    response = client.post(
        f"/v0/incidents/{incident_id}/feedback",
        json={"verdict": verdict, "possibility_id": possibility, "reviewer": "ana", "note": "n"},
        headers=HEADERS,
    )

    assert response.status_code == 201
    feedback = response.json()
    assert feedback["status"] == "pending"
    assert feedback["verdict"] == verdict
    assert feedback["possibility_id"] == named
    assert _decision(client, incident_id)["feedback"]["id"] == feedback["id"]
    listing = client.get("/v0/feedback?status=pending&system=shop", headers=HEADERS).json()
    assert [row["id"] for row in listing["items"]] == [feedback["id"]]
    assert listing["counts"] == {"pending": 1, "exported": 0, "trained": 0, "discarded": 0}


@pytest.mark.integration
def test_a_later_verdict_discards_the_earlier_one(client: TestClient) -> None:
    incident_id = _diagnose(client)
    url = f"/v0/incidents/{incident_id}/feedback"
    client.post(url, json={"verdict": "confirmed", "reviewer": "ana"}, headers=HEADERS)
    client.post(url, json={"verdict": "rejected", "reviewer": "bo"}, headers=HEADERS)

    counts = client.get("/v0/feedback", headers=HEADERS).json()["counts"]

    assert (counts["pending"], counts["discarded"]) == (1, 1)


@pytest.mark.integration
@pytest.mark.parametrize(
    "body",
    [
        {"verdict": "corrected", "reviewer": "ana"},
        {"verdict": "corrected", "possibility_id": "nope", "reviewer": "ana"},
        {"verdict": "rejected", "possibility_id": "unknown", "reviewer": "ana"},
    ],
)
def test_an_invalid_verdict_is_422_and_queues_nothing(
    client: TestClient, body: dict[str, str]
) -> None:
    incident_id = _diagnose(client)

    response = client.post(f"/v0/incidents/{incident_id}/feedback", json=body, headers=HEADERS)

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], str)
    assert client.get("/v0/feedback", headers=HEADERS).json()["items"] == []


@pytest.mark.integration
def test_a_malformed_body_is_422(client: TestClient) -> None:
    incident_id = _diagnose(client)
    url = f"/v0/incidents/{incident_id}/feedback"
    for body in (
        {"verdict": "maybe", "reviewer": "ana"},
        {"verdict": "confirmed"},
        {"verdict": "confirmed", "reviewer": "ana", "extra": 1},
    ):
        assert client.post(url, json=body, headers=HEADERS).status_code == 422


@pytest.mark.integration
def test_an_unknown_incident_is_404(client: TestClient) -> None:
    assert client.get("/v0/incidents/nope/decision", headers=HEADERS).status_code == 404
    response = client.post(
        "/v0/incidents/nope/feedback",
        json={"verdict": "confirmed", "reviewer": "ana"},
        headers=HEADERS,
    )
    assert response.status_code == 404


@pytest.mark.integration
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/v0/incidents/x/decision"),
        ("post", "/v0/incidents/x/feedback"),
        ("get", "/v0/feedback"),
    ],
)
def test_every_route_needs_the_token(client: TestClient, method: str, path: str) -> None:
    call: Callable[..., httpx.Response] = getattr(client, method)
    assert call(path).status_code == 401
    assert call(path, headers={"X-FIRE-Token": "wrong"}).status_code == 401


@pytest.mark.integration
def test_a_laya_reasoner_answers_from_its_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[dict[str, Any]] = []

    def laya(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        requests.append(body)
        first = next(iter(body["questions"]["cause"]["criteria"]))
        answers = {
            "cause": {"probabilities": {first: 1.0}, "confidence": 0.9, "answer_confidence": 0.9},
            "sufficient": {"noul": 0.9},
        }
        return httpx.Response(200, json={"answers": answers})

    monkeypatch.setenv("FIRE_SYSTEMONE_BASE_URL", "http://laya.test/v1/systemone")
    monkeypatch.setattr(
        wiring,
        "build_reasoner",
        functools.partial(wiring.build_reasoner, transport=httpx.MockTransport(laya)),
    )
    _build(tmp_path, monkeypatch, {"provider": "laya", "model": "fire"})
    client = TestClient(app)
    try:
        body = _decision(client, _diagnose(client))
    finally:
        app_module._container = None

    assert requests and requests[0]["model"] == "fire"
    assert (body["reasoner"], body["model"]) == ("laya", "fire")
    assert body["decision"]["confidence"] == pytest.approx(0.9)
