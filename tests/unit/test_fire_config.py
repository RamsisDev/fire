"""FireConfig: the global config, the tentacle block it gained in step 1.3, and the catalog block
of step 3.1.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from fire.apps.config import CatalogConfig, FireConfig, TentacleConfig, load_fire_config

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "fire.yml"


@pytest.mark.unit
def test_tentacle_defaults_match_the_spec() -> None:
    tentacle = FireConfig().tentacle
    assert tentacle.body_url_env == "FIRE_BODY_URL"
    assert (tentacle.batch_size, tentacle.flush_ms, tentacle.buffer_size) == (200, 1000, 10000)
    assert (tentacle.retry.max_attempts, tentacle.retry.backoff_ms) == (6, 500)
    assert tentacle.shutdown_s == 10


@pytest.mark.unit
def test_repo_fire_yml_sets_the_defaults() -> None:
    assert load_fire_config(REPO_CONFIG).tentacle == TentacleConfig()


@pytest.mark.unit
@pytest.mark.parametrize(
    "tentacle",
    [
        {"batchsize": 10},
        {"retry": {"attempts": 3}},
        {"batch_size": 0},
        {"flush_ms": 0},
        {"buffer_size": 0},
        {"retry": {"max_attempts": 0}},
        {"retry": {"backoff_ms": -1}},
        {"shutdown_s": 0},
        {"body_url_env": ""},
    ],
    ids=[
        "unknown-key",
        "unknown-retry-key",
        "batch-size",
        "flush-ms",
        "buffer-size",
        "max-attempts",
        "backoff-ms",
        "shutdown-s",
        "body-url-env",
    ],
)
def test_tentacle_rejects_bad_values(tentacle: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate({"tentacle": tentacle})


@pytest.mark.unit
def test_catalog_defaults_to_memory_with_a_seven_day_retention() -> None:
    catalog = FireConfig().catalog
    assert (catalog.provider, catalog.path_env, catalog.retention_s) == (
        "memory",
        "FIRE_CATALOG_PATH",
        7 * 24 * 3600,
    )
    assert load_fire_config(REPO_CONFIG).catalog == CatalogConfig()


@pytest.mark.unit
def test_catalog_takes_sqlite() -> None:
    raw = {"catalog": {"provider": "sqlite", "path_env": "SHOP_CATALOG", "retention_s": 60}}
    catalog = FireConfig.model_validate(raw).catalog
    assert (catalog.provider, catalog.path_env, catalog.retention_s) == (
        "sqlite",
        "SHOP_CATALOG",
        60,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "catalog",
    [
        {"path": "/tmp/catalog.sqlite3"},
        {"provider": "postgres"},
        {"path_env": ""},
        {"retention_s": 0},
        {"retention_s": float("inf")},
    ],
    ids=["unknown-key", "provider", "path-env", "retention-zero", "retention-inf"],
)
def test_catalog_rejects_bad_values(catalog: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate({"catalog": catalog})


@pytest.mark.unit
def test_incident_io_webhook_defaults_match_the_spec() -> None:
    webhook = FireConfig().webhooks.incident_io
    assert (webhook.signing_secret_env, webhook.tolerance_s) == (
        "FIRE_INCIDENT_IO_SIGNING_SECRET",
        300,
    )


@pytest.mark.unit
def test_repo_fire_yml_names_the_signing_secret_in_env_example() -> None:
    webhook = load_fire_config(REPO_CONFIG).webhooks.incident_io
    assert webhook == FireConfig().webhooks.incident_io
    env_example = (REPO_CONFIG.parents[1] / ".env.example").read_text(encoding="utf-8")
    assert f"\n{webhook.signing_secret_env}=" in env_example


@pytest.mark.unit
@pytest.mark.parametrize(
    "webhooks",
    [
        {"incident-io": {}},
        {"incident_io": {"secret_env": "X"}},
        {"incident_io": {"signing_secret_env": ""}},
        {"incident_io": {"tolerance_s": 0}},
    ],
    ids=["unknown-webhook", "unknown-key", "empty-env", "tolerance"],
)
def test_webhooks_reject_bad_values(webhooks: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        FireConfig.model_validate({"webhooks": webhooks})
