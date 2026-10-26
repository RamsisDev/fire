"""The body with `notifier.provider: slack`: a diagnosis posts its narration to the system's
channel, and a Slack failure leaves the `202` and the background task intact.

Slack is an `httpx.MockTransport` handed to `build_notifier`; nothing reaches the network.
"""

from __future__ import annotations

import functools
import json
import logging
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
import fire.apps.incident_engine.wiring as wiring
from fire.adapters.notifiers.slack import POST_MESSAGE_URL
from fire.apps.incident_engine.app import app

TOKEN = "slack-notifier-api-token"
BOT_TOKEN = "xoxb-slack-notifier-api"
HEADERS = {"X-FIRE-Token": TOKEN}
CHANNEL = "C0SHOP0001"


class Slack:
    """A `chat.postMessage` stand-in that answers `status` and records every request."""

    def __init__(self, status: int = 200) -> None:
        self.requests: list[httpx.Request] = []
        self._status = status

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self._status, json={"ok": self._status == 200})


@pytest.fixture
def slack() -> Slack:
    return Slack()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slack: Slack) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("notifier: {provider: slack}\n", encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": CHANNEL}},
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    monkeypatch.setenv("FIRE_SLACK_BOT_TOKEN", BOT_TOKEN)
    monkeypatch.setattr(
        wiring,
        "build_notifier",
        functools.partial(wiring.build_notifier, transport=httpx.MockTransport(slack)),
    )
    app_module._container = wiring.build_container(tmp_path / "fire.yml")
    # Not entered as a context manager: the lifespan would replace this container with one
    # built from the repo's config.
    yield TestClient(app)
    app_module._container = None


@pytest.mark.integration
def test_a_diagnosis_posts_its_narration_to_the_systems_channel(
    client: TestClient, slack: Slack
) -> None:
    response = client.post("/v0/diagnose", json={"system": "shop"}, headers=HEADERS)

    assert response.status_code == 202
    incident_id = response.json()["incident_id"]
    [request] = slack.requests
    assert str(request.url) == POST_MESSAGE_URL
    assert request.headers["Authorization"] == f"Bearer {BOT_TOKEN}"
    body = json.loads(request.content)
    assert body["channel"] == CHANNEL
    assert body["text"].startswith(f"*FIRE ENGINE* incident `{incident_id}` (`shop`)\n")


@pytest.mark.integration
@pytest.mark.parametrize("slack", [Slack(status=503)])
def test_a_slack_failure_still_answers_202_and_logs_the_status(
    client: TestClient, slack: Slack, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="fire.adapters.notifiers.slack"):
        response = client.post("/v0/diagnose", json={"system": "shop"}, headers=HEADERS)

    assert response.status_code == 202
    assert len(slack.requests) == 1
    incident_id = response.json()["incident_id"]
    assert [record.getMessage() for record in caplog.records] == [
        f"slack notify failed: incident={incident_id} system=shop channel={CHANNEL} error=http_503"
    ]
