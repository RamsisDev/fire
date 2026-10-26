"""incident.io webhook end to end: incident.io's documented samples, signed with a test secret,
posted to `POST /v0/webhooks/incident-io`.

Two systems are loaded: acme maps the samples' incident through its custom field's catalog entry,
and billing through a team the samples do not carry. Free text in the samples is `fixture-…`, so
no placeholder may reach stdout, the logs, or a response.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

import fire.apps.incident_engine.app as app_module
from fire.adapters.webhooks.signing import decode_secret, sign
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.core.domain.incident_io import IncidentIoOutcome

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "incident_io"
ROUTE = "/v0/webhooks/incident-io"
SECRET_ENV = "FIRE_INCIDENT_IO_SIGNING_SECRET"
SECRET = "whsec_" + base64.b64encode(b"fire-incident-io-test-secret-key").decode()
CREATED = "public_incident.incident_created_v2"
UPDATED = "public_incident.incident_updated_v2"
STATUS_UPDATED = "public_incident.incident_status_updated_v2"
INCIDENT_ID = "01FDAG4SAP5TYPT98WGR2N7W91"
FIELD_ID = "01FCNDV6P870EA6S7TK1DSYDG0"
EXTERNAL_ID = "761722cd-d1d7-477b-ac7e-90f9e079dc33"
NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
NOTIFY = re.compile(r"^\[fire\] notify incident=(\S+) system=(\S+) trigger=(\S+)$", re.MULTILINE)


def _system(name: str, rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "system": name,
        "display_name": name,
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "triggers": {"incident_io": {"match": [rule]}},
    }


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    for system in (
        _system("acme", {"custom_field_id": FIELD_ID, "values": [EXTERNAL_ID]}),
        _system("billing", {"team_id": "01ZZZZZZZZZZZZZZZZZZZZZZZZ"}),
    ):
        (systems / f"{system['system']}.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    monkeypatch.setenv(SECRET_ENV, SECRET)
    # The route never reads X-FIRE-Token.
    monkeypatch.delenv("FIRE_API_TOKEN", raising=False)
    app_module._container = build_container(tmp_path / "fire.yml", clock=lambda: NOW)
    # Not entered as a context manager: the lifespan would replace this container.
    yield TestClient(app)
    app_module._container = None


def _sample(event_type: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{event_type}.json").read_text(encoding="utf-8"))


def _signed(
    body: bytes,
    *,
    secret: str = SECRET,
    sent: datetime = NOW,
    webhook_id: str = "msg_fixture",
) -> dict[str, str]:
    timestamp = str(int(sent.timestamp()))
    signature = sign(decode_secret(secret), webhook_id, timestamp, body)
    return {
        "webhook-id": webhook_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": f"v1,{signature}",
        "content-type": "application/json",
    }


def _post(client: TestClient, document: dict[str, Any] | bytes, **signing: Any) -> Any:
    body = document if isinstance(document, bytes) else json.dumps(document).encode()
    return client.post(ROUTE, content=body, headers=_signed(body, **signing))


def _counts() -> dict[IncidentIoOutcome, int]:
    assert app_module._container is not None
    return dict(app_module._container.incident_io.counts)


@pytest.mark.integration
def test_a_signed_sample_opens_one_incident_io_incident_notified_once(
    client: TestClient, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="fire")
    response = _post(client, _sample(CREATED))
    assert response.status_code == 202
    body = response.json()
    assert (body["status"], body["outcome"]) == ("accepted", "opened")
    [incident_id] = body["incident_ids"]

    for event_type in (UPDATED, STATUS_UPDATED, CREATED):
        again = _post(client, _sample(event_type), webhook_id=f"msg_{event_type}")
        assert again.status_code == 202
        assert again.json() == {"status": "accepted", "outcome": "duplicate", "incident_ids": []}

    stdout = capsys.readouterr().out
    assert NOTIFY.findall(stdout) == [(incident_id, "acme", "incident_io")]
    assert "Insufficient evidence (MISSING fragments) _[process_path, shape]_" in stdout
    assert "Confidence: 20% — `noul`" in stdout
    assert _counts() == {IncidentIoOutcome.OPENED: 1, IncidentIoOutcome.DUPLICATE: 3}
    lines = [record.getMessage() for record in caplog.records]
    assert (
        f"incident.io webhook: event_type={CREATED} outcome=opened incident={INCIDENT_ID} raised=1"
    ) in lines
    assert "fixture-" not in stdout + "\n".join(lines) + response.text


@pytest.mark.integration
def test_the_incident_carries_ids_only(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs: list[Any] = []

    async def capture(incident: Any, dossier: Any) -> None:
        runs.append((incident, dossier))

    monkeypatch.setattr(app_module, "_run_pipeline", capture)
    assert _post(client, _sample(STATUS_UPDATED)).json()["outcome"] == "opened"
    [(incident, dossier)] = runs
    assert (incident.system, incident.trigger, incident.summary) == (
        "acme",
        "incident_io",
        "incident.io incident INC-123",
    )
    assert [fragment.collector for fragment in dossier.fragments] == [
        "incident_io",
        "process_path",
        "shape",
    ]
    assert dossier.fragments[0].data["event_type"] == STATUS_UPDATED
    assert "fixture-" not in dossier.model_dump_json() + incident.model_dump_json()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("signing", "reason"),
    [
        ({"secret": "whsec_" + base64.b64encode(b"another-secret").decode()}, "no_match"),
        ({"sent": NOW - timedelta(seconds=301)}, "stale_timestamp"),
        ({"sent": NOW + timedelta(seconds=301)}, "stale_timestamp"),
    ],
    ids=["other-secret", "stale", "future"],
)
def test_a_bad_signature_is_401_and_opens_nothing(
    client: TestClient,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    signing: dict[str, Any],
    reason: str,
) -> None:
    response = _post(client, _sample(CREATED), **signing)
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid or missing webhook signature"}
    assert f"incident.io webhook rejected: reason={reason}" in caplog.messages
    assert _counts() == {}
    assert NOTIFY.findall(capsys.readouterr().out) == []


@pytest.mark.integration
def test_a_changed_body_or_missing_headers_are_401(client: TestClient) -> None:
    body = json.dumps(_sample(CREATED)).encode()
    headers = _signed(body)
    assert client.post(ROUTE, content=body + b" ", headers=headers).status_code == 401
    assert client.post(ROUTE, content=body).status_code == 401
    unsigned = {key: value for key, value in headers.items() if key != "webhook-signature"}
    assert client.post(ROUTE, content=body, headers=unsigned).status_code == 401
    assert _counts() == {}


@pytest.mark.integration
@pytest.mark.parametrize(
    ("secret", "detail"),
    [
        (None, f"server misconfigured: {SECRET_ENV} is not set"),
        ("", f"server misconfigured: {SECRET_ENV} is not set"),
        ("whsec_not base64!", f"server misconfigured: {SECRET_ENV} is not a signing secret"),
    ],
    ids=["unset", "empty", "not-base64"],
)
def test_an_unusable_signing_secret_is_503_before_the_signature(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, secret: str | None, detail: str
) -> None:
    if secret is None:
        monkeypatch.delenv(SECRET_ENV)
    else:
        monkeypatch.setenv(SECRET_ENV, secret)
    response = client.post(ROUTE, content=b"{}")
    assert (response.status_code, response.json()) == (503, {"detail": detail})


@pytest.mark.integration
def test_an_unmapped_incident_is_accepted_counted_and_ignored(
    client: TestClient, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="fire")
    document = _sample(CREATED)
    document[CREATED]["custom_field_entries"] = []
    response = _post(client, document)
    assert (response.status_code, response.json()) == (
        202,
        {"status": "accepted", "outcome": "unmapped", "incident_ids": []},
    )
    assert _counts() == {IncidentIoOutcome.UNMAPPED: 1}
    assert (
        f"incident.io webhook: event_type={CREATED} outcome=unmapped incident={INCIDENT_ID} "
        "raised=0"
    ) in caplog.messages
    assert NOTIFY.findall(capsys.readouterr().out) == []


@pytest.mark.integration
@pytest.mark.parametrize(
    ("body", "outcome"),
    [
        (_sample("private_incident.incident_created_v2"), "private"),
        (_sample("public_incident.follow_up_created_v2"), "ignored_event_type"),
        (b"not json", "malformed"),
    ],
    ids=["private", "follow-up", "malformed"],
)
def test_bodies_without_a_public_incident_are_accepted_and_counted(
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
    body: dict[str, Any] | bytes,
    outcome: str,
) -> None:
    caplog.set_level(logging.INFO, logger="fire")
    response = _post(client, body)
    assert (response.status_code, response.json()["outcome"]) == (202, outcome)
    assert _counts() == {IncidentIoOutcome(outcome): 1}
    assert (
        f"incident.io webhook: event_type=- outcome={outcome} incident=- raised=0"
    ) in caplog.messages
