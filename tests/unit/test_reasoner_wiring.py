"""build_reasoner and build_decision_store: provider selection and the config blocks they read."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from fire.adapters.memory.decision_store import MemoryDecisionStore
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.reasoners.laya import LayaReasoner
from fire.adapters.sqlite.decision_store import SqliteDecisionStore
from fire.apps.config import FeedbackConfig, FireConfig, ReasonerConfig, load_fire_config
from fire.apps.incident_engine.wiring import build_decision_store, build_reasoner

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "fire.yml"


@pytest.mark.unit
def test_repo_fire_yml_defaults_to_echo_and_memory() -> None:
    config = load_fire_config(REPO_CONFIG)
    assert config.reasoner == ReasonerConfig()
    assert config.feedback == FeedbackConfig()
    assert config.reasoner.provider == "echo"
    assert config.feedback.provider == "memory"
    assert (config.retraining.min_feedback, config.retraining.epochs) == (20, 8)
    assert config.retraining.train_command == "laya-train"


@pytest.mark.unit
@pytest.mark.parametrize(
    "block",
    [
        {"reasoner": {"timeout_s": 0}},
        {"reasoner": {"min_sufficient": 2}},
        {"reasoner": {"unknown": 1}},
        {"feedback": {"provider": "redis"}},
        {"retraining": {"epochs": 0}},
        {"retraining": {"min_feedback": 0}},
    ],
)
def test_config_rejects_bad_values(block: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate(block)


@pytest.mark.unit
def test_echo_is_the_default_reasoner() -> None:
    assert isinstance(build_reasoner(ReasonerConfig()), EchoReasoner)


@pytest.mark.unit
def test_laya_needs_its_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FIRE_SYSTEMONE_BASE_URL", raising=False)
    with pytest.raises(ValueError, match="FIRE_SYSTEMONE_BASE_URL"):
        build_reasoner(ReasonerConfig(provider="laya"))


@pytest.mark.unit
async def test_laya_is_built_from_the_env_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRE_SYSTEMONE_BASE_URL", "http://laya.test/v1/systemone")
    monkeypatch.setenv("FIRE_SYSTEMONE_API_KEY", "secret")
    reasoner = build_reasoner(
        ReasonerConfig(provider="laya", model="fire"),
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
    )
    assert isinstance(reasoner, LayaReasoner)
    await reasoner.aclose()


@pytest.mark.unit
def test_an_unknown_reasoner_provider_fails() -> None:
    with pytest.raises(ValueError, match="not wired"):
        build_reasoner(ReasonerConfig(provider="gpt"))


@pytest.mark.unit
def test_memory_store_is_the_default() -> None:
    assert isinstance(build_decision_store(FeedbackConfig()), MemoryDecisionStore)


@pytest.mark.unit
def test_sqlite_store_needs_its_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = FeedbackConfig(provider="sqlite")
    monkeypatch.delenv("FIRE_FEEDBACK_PATH", raising=False)
    with pytest.raises(ValueError, match="FIRE_FEEDBACK_PATH"):
        build_decision_store(config)
    monkeypatch.setenv("FIRE_FEEDBACK_PATH", str(tmp_path / "feedback.sqlite3"))
    store = build_decision_store(config)
    assert isinstance(store, SqliteDecisionStore)
    store.close()
