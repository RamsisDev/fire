"""SlackNotifier posts the narration to its system's channel, and logs every failed post with an
error code only; `build_notifier` selects it from `notifier.provider` with its channel map.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import ValidationError

from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.notifiers.slack import POST_MESSAGE_URL, SlackNotifier
from fire.apps.config import FireConfig, NotifierConfig, load_fire_config, load_system_configs
from fire.apps.incident_engine.wiring import build_container, build_notifier
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.narration import Narration

TOKEN = "xoxb-test-slack-token"
CHANNEL = "C0123456789"
INCIDENT = Incident(id="inc-7", system="example", trigger=Trigger.SHAPE_DRIFT)
NARRATION = Narration(incident_id="inc-7", channel_text="*FIRE ENGINE* first line\nsecond line")
REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "fire.yml"

Handler = Callable[[httpx.Request], httpx.Response]


class Slack:
    """A `chat.postMessage` stand-in that records every request it receives."""

    def __init__(self, respond: Handler | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._respond = respond or (lambda _request: httpx.Response(200, json={"ok": True}))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)


def _notifier(slack: Slack, channels: dict[str, str] | None = None) -> SlackNotifier:
    return SlackNotifier(
        TOKEN, {"example": CHANNEL} if channels is None else channels, transport=slack.transport
    )


def _assert_nothing_leaked(caplog: pytest.LogCaptureFixture) -> None:
    for record in caplog.records:
        message = record.getMessage()
        assert TOKEN not in message
        assert "first line" not in message


@pytest.mark.unit
async def test_posts_the_narration_to_the_systems_channel_with_the_bot_token() -> None:
    slack = Slack()
    await _notifier(slack).publish(INCIDENT, NARRATION)
    [request] = slack.requests
    assert (request.method, str(request.url)) == ("POST", POST_MESSAGE_URL)
    assert POST_MESSAGE_URL == "https://slack.com/api/chat.postMessage"
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["Content-Type"] == "application/json; charset=utf-8"
    assert json.loads(request.content) == {"channel": CHANNEL, "text": NARRATION.channel_text}


@pytest.mark.unit
async def test_a_post_logs_the_incident_and_channel_without_the_narration(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="fire.adapters.notifiers.slack")
    await _notifier(Slack()).publish(INCIDENT, NARRATION)
    assert [record.getMessage() for record in caplog.records] == [
        f"slack notified: incident=inc-7 system=example channel={CHANNEL}"
    ]
    _assert_nothing_leaked(caplog)


@pytest.mark.unit
async def test_each_system_posts_to_its_own_channel() -> None:
    slack = Slack()
    notifier = _notifier(slack, {"example": CHANNEL, "other": "G0ABCDEFGH"})
    await notifier.publish(INCIDENT.model_copy(update={"system": "other"}), NARRATION)
    assert json.loads(slack.requests[0].content)["channel"] == "G0ABCDEFGH"


@pytest.mark.unit
async def test_a_system_without_a_channel_logs_and_posts_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slack = Slack()
    await _notifier(slack, {}).publish(INCIDENT, NARRATION)
    assert slack.requests == []
    assert [record.getMessage() for record in caplog.records] == [
        "slack notify skipped: incident=inc-7 system=example reason=no_channel"
    ]


def _raise(error: Exception) -> Handler:
    def respond(_request: httpx.Request) -> httpx.Response:
        raise error

    return respond


@pytest.mark.unit
@pytest.mark.parametrize(
    ("respond", "error"),
    [
        (
            lambda _r: httpx.Response(200, json={"ok": False, "error": "channel_not_found"}),
            "channel_not_found",
        ),
        (
            lambda _r: httpx.Response(200, json={"ok": False, "error": "not in\nslack's form"}),
            "unknown_error",
        ),
        (lambda _r: httpx.Response(200, json={"ok": False}), "unknown_error"),
        (lambda _r: httpx.Response(200, text="<html>"), "invalid_response"),
        (lambda _r: httpx.Response(200, json=["ok"]), "invalid_response"),
        (lambda _r: httpx.Response(500, text="oops"), "http_500"),
        (lambda _r: httpx.Response(403, json={"ok": False, "error": "x"}), "http_403"),
        (
            lambda _r: httpx.Response(
                429, headers={"Retry-After": "30"}, json={"ok": False, "error": "ratelimited"}
            ),
            "http_429 retry_after=30",
        ),
        (lambda _r: httpx.Response(429), "http_429 retry_after=unset"),
        (_raise(httpx.ReadTimeout("timed out")), "ReadTimeout"),
        (_raise(httpx.ConnectError("refused")), "ConnectError"),
    ],
    ids=[
        "ok-false",
        "ok-false-odd-code",
        "ok-false-no-code",
        "not-json",
        "not-an-object",
        "5xx",
        "4xx",
        "rate-limited",
        "rate-limited-no-retry-after",
        "timeout",
        "connect-error",
    ],
)
async def test_a_failed_post_logs_one_error_code_and_never_raises(
    respond: Handler, error: str, caplog: pytest.LogCaptureFixture
) -> None:
    await _notifier(Slack(respond)).publish(INCIDENT, NARRATION)
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (
            logging.WARNING,
            f"slack notify failed: incident=inc-7 system=example channel={CHANNEL} error={error}",
        )
    ]
    _assert_nothing_leaked(caplog)


@pytest.mark.unit
async def test_the_timeout_applies_to_the_request() -> None:
    slack = Slack()
    await SlackNotifier(
        TOKEN, {"example": CHANNEL}, timeout_s=1.5, transport=slack.transport
    ).publish(INCIDENT, NARRATION)
    assert slack.requests[0].extensions["timeout"] == {
        "connect": 1.5,
        "read": 1.5,
        "write": 1.5,
        "pool": 1.5,
    }


@pytest.mark.unit
def test_notifier_defaults_match_the_spec_and_the_repo_fire_yml() -> None:
    assert NotifierConfig() == NotifierConfig(
        provider="console", bot_token_env="FIRE_SLACK_BOT_TOKEN", timeout_s=5
    )
    assert load_fire_config(REPO_CONFIG).notifier == NotifierConfig()


@pytest.mark.unit
@pytest.mark.parametrize(
    "notifier",
    [{"token_env": "X"}, {"bot_token_env": ""}, {"timeout_s": 0}],
    ids=["unknown-key", "empty-token-env", "timeout"],
)
def test_notifier_rejects_bad_values(notifier: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate({"notifier": notifier})


def _systems(tmp_path: Path) -> Path:
    systems = tmp_path / "systems"
    systems.mkdir()
    for name, channel in (("shop", "C0SHOP0001"), ("billing", "G0BILLING1")):
        system = {
            "system": name,
            "display_name": name,
            "notify": {"slack": {"channel_id": channel}},
        }
        (systems / f"{name}.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    return systems


@pytest.mark.unit
def test_console_is_the_default_and_needs_no_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIRE_SLACK_BOT_TOKEN", raising=False)
    assert isinstance(build_notifier(NotifierConfig(), {}), ConsoleNotifier)


@pytest.mark.unit
async def test_slack_provider_posts_to_each_loaded_systems_channel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOP_SLACK_TOKEN", TOKEN)
    slack = Slack()
    notifier = build_notifier(
        NotifierConfig(provider="slack", bot_token_env="SHOP_SLACK_TOKEN"),
        load_system_configs(_systems(tmp_path)),
        transport=slack.transport,
    )
    assert isinstance(notifier, SlackNotifier)
    for system in ("shop", "billing"):
        await notifier.publish(INCIDENT.model_copy(update={"system": system}), NARRATION)
    assert [json.loads(request.content)["channel"] for request in slack.requests] == [
        "C0SHOP0001",
        "G0BILLING1",
    ]
    assert {request.headers["Authorization"] for request in slack.requests} == {f"Bearer {TOKEN}"}


@pytest.mark.unit
@pytest.mark.parametrize("token", [None, ""], ids=["unset", "empty"])
def test_slack_provider_without_its_token_fails_startup_naming_the_env_var(
    token: str | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if token is None:
        monkeypatch.delenv("FIRE_SLACK_BOT_TOKEN", raising=False)
    else:
        monkeypatch.setenv("FIRE_SLACK_BOT_TOKEN", token)
    _systems(tmp_path)
    (tmp_path / "fire.yml").write_text("notifier: {provider: slack}\n", encoding="utf-8")
    with pytest.raises(
        ValueError, match="notifier provider 'slack' needs FIRE_SLACK_BOT_TOKEN set"
    ):
        build_container(tmp_path / "fire.yml")


@pytest.mark.unit
def test_an_unknown_provider_fails_startup(tmp_path: Path) -> None:
    (tmp_path / "fire.yml").write_text("notifier: {provider: teams}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="notifier provider 'teams' not wired"):
        build_container(tmp_path / "fire.yml")
